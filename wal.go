package unolog

import (
	"slices"
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

	requestedLevel  Level
	hasErr          bool // the bools share one word after requestedLevel
	hasRequestedLvl bool
	saturated       bool // full after a compaction; see pushLimited

	// limits bound the WAL; dropped and truncated count the writes the
	// limits changed. All four are read and written under mu.
	limits    limits
	dropped   int64
	truncated int64

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

func newEvent(lim limits) *event {
	ev := eventPool.Get().(*event) //nolint:forcetypeassert // the pool's New stores exactly *event
	ev.reset(lim)
	return ev
}

// reset prepares the event for its next generation. It takes the same
// mutex as appends, seals, and snapshots: a stale watchdog snapshot can
// hold the lock with a matching old generation, and without the lock
// the reset would truncate the fields under its copy.
func (e *event) reset(lim limits) {
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
	e.saturated = false
	e.limits = lim
	e.dropped = 0
	e.truncated = 0
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
		if e.fits(len(f.str), f.kind) {
			e.fields = append(e.fields, f)
		} else {
			e.pushLimited(&f)
		}
	}
	e.mu.Unlock()
}

// push appends f under the event limits. The caller holds mu and has
// verified the generation. Hot callers repeat these lines inline: a
// call here costs a copy of f and the call itself on every write.
func (e *event) push(f Field) {
	if e.fits(len(f.str), f.kind) {
		e.fields = append(e.fields, f)
		return
	}
	e.pushLimited(&f)
}

// fits reports whether a field can be appended as is: the event has
// room, and the value is not one the byte limit could cut. strLen is
// len(f.str); only KindString fields carry str, so the check covers
// every kind but KindAny. It takes scalars, not the Field: an inlined
// struct parameter costs a stack copy of the whole Field.
func (e *event) fits(strLen int, kind FieldKind) bool {
	return len(e.fields) < e.limits.maxFields && strLen <= e.limits.maxValueBytes && kind != KindAny
}

// pushLimited is push for a full event or a value that may need a cut.
// At the field limit, a key the event holds takes the new value in
// place (its position stays). A canonical key replaces only a write of
// the same kind, because scanWAL reads the last write of the kind it
// needs. If that write is not the key's last write, it is removed and
// the new value appended, so the new value is still the one the line
// shows; with no write of the same kind, the new value is appended. A
// new key first compacts the WAL, and is dropped and counted only if
// the event is still full. It takes a pointer so the hot callers do not
// reserve stack space to pass a Field, and it copies *p before a cut,
// so the caller's Field never changes.
func (e *event) pushLimited(p *Field) {
	f := *p
	if len(e.fields) < e.limits.maxFields {
		e.limitValue(&f)
		e.fields = append(e.fields, f)
		return
	}
	canonical := isCanonicalKey(f.key)
	last := -1
	for i := len(e.fields) - 1; i >= 0; i-- {
		if e.fields[i].key != f.key {
			continue
		}
		if last < 0 {
			last = i
		}
		if canonical && e.fields[i].kind != f.kind {
			continue
		}
		e.limitValue(&f)
		if i == last {
			e.fields[i] = f
			return
		}
		// A later write of another kind would hide a replacement in
		// place: the line shows the last write of a key.
		e.fields = append(slices.Delete(e.fields, i, i+1), f)
		return
	}
	if !canonical {
		// Once a compaction leaves the event full, no later write can
		// add a write to remove: a full event only replaces in place,
		// and the canonical writes it appends are never removed.
		if !e.saturated {
			e.compact()
		}
		if len(e.fields) >= e.limits.maxFields {
			e.saturated = true
			e.dropped++
			return
		}
	}
	e.limitValue(&f)
	e.fields = append(e.fields, f)
}

// compact removes each non-canonical write that a later write of the
// same key replaces, so MaxFields bounds distinct keys rather than
// writes: a key set in a loop does not fill the event. The encoder,
// the sink bridges, and Lookup all resolve a key to its last write at
// its last position, so the event encodes the same before and after.
// Canonical writes stay, because scanWAL reads earlier writes of
// another kind. The caller holds mu.
func (e *event) compact() {
	seen := make(map[string]struct{}, len(e.fields))
	w := len(e.fields)
	for _, f := range slices.Backward(e.fields) {
		if !isCanonicalKey(f.key) {
			if _, later := seen[f.key]; later {
				continue
			}
			seen[f.key] = struct{}{}
		}
		// w never falls below the loop index, so the write lands on a
		// field the loop has already read.
		w--
		e.fields[w] = f
	}
	n := copy(e.fields, e.fields[w:])
	clear(e.fields[n:]) // Do not retain the removed values.
	e.fields = e.fields[:n]
}

// limitValue cuts the string content of f to the byte limit in place
// and counts each cut. The caller holds mu.
func (e *event) limitValue(f *Field) {
	switch f.kind {
	case KindString:
		if len(f.str) > e.limits.maxValueBytes {
			f.str = truncateUTF8(f.str, e.limits.maxValueBytes)
			e.truncated++
		}
	case KindAny:
		if m, ok := f.val.(map[string]any); ok {
			if limited, cuts := limitMapStrings(m, e.limits.maxValueBytes); cuts > 0 {
				f.val = limited
				e.truncated += int64(cuts)
			}
		}
	default:
	}
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
		if f := fieldOf(key, value); e.fits(len(f.str), f.kind) {
			e.fields = append(e.fields, f)
		} else {
			e.pushLimited(&f)
		}
	}
	for i := 0; i+1 < len(kv); i += 2 {
		k, ok := kv[i].(string)
		if !ok || k == "" {
			continue
		}
		if f := fieldOf(k, kv[i+1]); e.fits(len(f.str), f.kind) {
			e.fields = append(e.fields, f)
		} else {
			e.pushLimited(&f)
		}
	}
}

// addFields appends typed fields under one lock. Fields with an empty
// key, including the zero Field, are skipped like Add's empty keys.
func (e *event) addFields(gen uint64, fields []Field) {
	s := e.state.Load()
	if s>>walStateBits != gen || walState(s&walStateMask) == walSealed {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if cur := e.state.Load(); cur>>walStateBits != gen || walState(cur&walStateMask) != walActive {
		return
	}
	for i := range fields {
		switch f := &fields[i]; {
		case f.key == "":
		case e.fits(len(f.str), f.kind):
			e.fields = append(e.fields, *f)
		default:
			e.pushLimited(f)
		}
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
		e.push(field)   // error is a canonical key: never dropped
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
		if len(msg) > e.limits.maxValueBytes {
			msg = truncateUTF8(msg, e.limits.maxValueBytes)
			e.truncated++
		}
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
