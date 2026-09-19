package unolog

import (
	"sync"
	"sync/atomic"
	"time"
)

// The WAL state word packs a generation counter (high bits) and a state
// (low bit). Mutations do one atomic load and compare both: the
// generation defeats the recycle ABA (a straggler holding a recycled
// event sees a stale generation and no-ops), and the state implements
// sealing.
//
// Every open event is guarded: appends and snapshots serialize under
// the per-event mutex, and seal takes the same mutex. Therefore an
// append that began before End either lands before the seal or observes
// the sealed state on its recheck and drops — a write after End is
// guaranteed to be a no-op, even across pool recycle. The guarded
// append costs one uncontended mutex round trip (~0.8 ns/field on the
// 1.0 gate machine); the guarantee is worth it.
const (
	walStateBits = 1
	walStateMask = uint64(1<<walStateBits) - 1
	walGenOne    = uint64(1) << walStateBits
)

type walState uint64

const (
	walActive walState = iota // open: appends and snapshots serialize under mu
	walSealed                 // committed or dropped: mutations are no-ops
)

// event is the per-request write-ahead log: an append-only slice of
// typed fields, request-confined, pooled. Every append serializes under
// the per-event mutex, and seal takes the same mutex, so no append can
// race the seal. After End the event is sealed — a straggler write from
// async work can never touch a recycled buffer: its generation check
// rejects the old handle even after the pool hands the event to a new
// request.
type event struct {
	state atomic.Uint64

	mu sync.Mutex // serializes appends/snapshots/sealing

	fields []Field // append-only, insertion order; backing array owned for pooling
	msg    string

	hasErr          bool
	requestedLevel  Level
	hasRequestedLvl bool

	startedAt time.Time
}

var eventPool = sync.Pool{
	New: func() any {
		return &event{
			fields: make([]Field, 0, 16),
		}
	},
}

// walRef is the immutable handle stored in the request context; it pins
// the generation the request's writes belong to.
type walRef struct {
	ev  *event
	gen uint64
}

func newEvent() *event {
	ev := eventPool.Get().(*event) //nolint:forcetypeassert // the pool's New stores exactly *event
	ev.reset()
	return ev
}

// reset prepares the event for its next generation. It takes the same
// mutex as appends, seals, and snapshots: a stale watchdog snapshot can
// hold the lock with a matching old generation, and without the lock
// the reset would truncate the fields under its copy.
func (e *event) reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.state.Add(walGenOne) // new generation; any straggler now mismatches
	e.state.Store(s&^walStateMask | uint64(walActive))
	clear(e.fields) // Do not retain a previous request's values beyond the next reset.
	e.fields = e.fields[:0]
	e.msg = ""
	e.hasErr = false
	e.requestedLevel = 0
	e.hasRequestedLvl = false
	e.startedAt = time.Now()
}

// seal ends all mutations for this generation. It takes the append
// mutex, so an in-flight append either lands before the seal or
// observes the sealed state on its recheck and drops. Idempotent.
func (e *event) seal() {
	e.mu.Lock()
	e.state.Or(uint64(walSealed))
	e.mu.Unlock()
}

// append adds one field for the given generation. One atomic load
// decides the fast drop: a stale generation or a sealed event returns
// without the lock. Otherwise the append serializes under the mutex and
// rechecks there — seal takes the same mutex — so a straggler that
// raced End can never land in a recycled buffer.
func (e *event) append(gen uint64, f Field) {
	s := e.state.Load()
	if s>>walStateBits != gen || walState(s&walStateMask) == walSealed {
		return
	}
	e.mu.Lock()
	if cur := e.state.Load(); cur>>walStateBits == gen && walState(cur&walStateMask) == walActive {
		e.fields = append(e.fields, f)
	}
	e.mu.Unlock()
}

// addKV appends the leading pair plus any well-formed kv pairs
// (k, v, k, v, ...). Empty keys and non-string keys are skipped
// uniformly; an odd trailing value is dropped.
func (e *event) addKV(ref *walRef, key string, value any, kv ...any) {
	s := e.state.Load()
	if s>>walStateBits != ref.gen || walState(s&walStateMask) == walSealed {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if cur := e.state.Load(); cur>>walStateBits != ref.gen || walState(cur&walStateMask) != walActive {
		return
	}
	// fieldOf only inspects types: one lock covers the whole batch without
	// calling user code while locked.
	if key != "" {
		e.fields = append(e.fields, fieldOf(key, value))
	}
	for i := 0; i+1 < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok || k == "" {
			continue
		}
		e.fields = append(e.fields, fieldOf(k, kv[i+1]))
	}
}

// Typed append helpers for the canonical fields: no interface boundary,
// no boxing, unlike fieldOf.
func (e *event) appendStr(gen uint64, key, value string) {
	e.append(gen, Field{key: key, kind: KindString, str: value})
}

func (e *event) appendAny(gen uint64, key string, value any) {
	e.append(gen, Field{key: key, kind: KindAny, val: value})
}

// setError records the structured error field and latches hasErr. The
// append and the latch serialize under mu with the seal, so they cannot
// split.
func (e *event) setError(ref *walRef, err error) {
	if err == nil {
		return
	}
	s := e.state.Load()
	if s>>walStateBits != ref.gen || walState(s&walStateMask) == walSealed {
		return
	}
	// Build the structured field before the lock: Error()/Unwrap()
	// implementations are user code and must not run under the event
	// mutex (a slow or reentrant error would stall or deadlock other
	// writers and the watchdog).
	field := Field{key: KeyError, kind: KindAny, val: structuredErrorField(err)}
	e.mu.Lock()
	defer e.mu.Unlock()
	if cur := e.state.Load(); cur>>walStateBits == ref.gen && walState(cur&walStateMask) == walActive {
		e.fields = append(e.fields, field)
		e.hasErr = true // only when the write belonged to this generation
	}
}

// setMessage overrides the event message. An empty message is unset.
// Same guarded discipline as append.
func (e *event) setMessage(ref *walRef, msg string) {
	if msg == "" {
		return
	}
	s := e.state.Load()
	if s>>walStateBits != ref.gen || walState(s&walStateMask) == walSealed {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if cur := e.state.Load(); cur>>walStateBits == ref.gen && walState(cur&walStateMask) == walActive {
		e.msg = msg
	}
}

func (e *event) setRoute(ref *walRef, route string) {
	if route == "" {
		return
	}
	e.appendStr(ref.gen, KeyHTTPRoute, route)
}

// setLevel records the requested level floor. Same guarded discipline
// as append.
func (e *event) setLevel(ref *walRef, level Level) {
	if !IsValidLevel(level) {
		return
	}
	s := e.state.Load()
	if s>>walStateBits != ref.gen || walState(s&walStateMask) == walSealed {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if cur := e.state.Load(); cur>>walStateBits == ref.gen && walState(cur&walStateMask) == walActive {
		e.requestedLevel = level
		e.hasRequestedLvl = true
	}
}

// snapshotFields returns a copy of the WAL tail for the watchdog. It
// rejects a stale generation; the copy shares the append mutex, so it
// is race-clean against appends and the owner's post-seal writes in
// every state. ok is false when the snapshot is refused.
func (e *event) snapshotFields(gen uint64) (out []Field, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s := e.state.Load(); s>>walStateBits != gen {
		return nil, false
	}
	out = make([]Field, len(e.fields))
	copy(out, e.fields)
	return out, true
}

// lookup returns the last value written under key (last-write-wins view
// of the un-deduped WAL). The caller must hold no concurrent writer:
// the sampler and the encoder read after seal.
func (e *event) lookup(key string) (any, bool) {
	return lookupField(e.fields, key)
}

// pooledFieldCap is the maximum field-slice capacity returned to the
// pool. Wider events are dropped from the pool and left for the GC so
// a single pathological request cannot pin a large buffer for the
// process lifetime.
const pooledFieldCap = 1024

func (e *event) release() {
	e.seal()
	if cap(e.fields) <= pooledFieldCap {
		eventPool.Put(e)
	}
}
