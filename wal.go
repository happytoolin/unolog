package unolog

import (
	"context"
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
// The context form of every mutation (unolog.Add/Error/SetLevel/…,
// reached through the Operation context node) is guarded: appends and
// snapshots serialize
// under the per-event mutex, and seal takes the same mutex. Therefore
// an append that began before End either lands before the seal or
// observes the sealed state on its recheck and drops — which is what
// makes concurrent writers from child goroutines safe, and a write
// after End a no-op even across pool recycle.
//
// The handle form (Operation.Add) takes no lock: the handle is
// documented single-goroutine and End runs on that same goroutine, so
// those appends cannot interleave with anything. MEASURED at ~18% of a
// twelve-field kept lifecycle for the mutex round trips it saves. The
// generation check is kept on that path, so a post-End or post-recycle
// write still drops exactly; only a concurrent context writer
// overlapping a handle Add is excluded by contract rather than by lock.
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

	// parent is the context the request was started under. It lives here,
	// on the pooled event, rather than on the per-request context node:
	// the node is handed out once per request and its size is billed to
	// every request's allocation, while this word is billed once per
	// pooled event. The node's Deadline/Done/Err/Value delegate through
	// it, so the node stays the two words it needs to be.
	parent context.Context

	hasErr          bool
	requestedLevel  Level
	hasRequestedLvl bool

	// startWall is the request's start stamp in unix nanoseconds. The
	// event carries no time.Time at all: one int64 word is the whole
	// stamp (no Location pointer for the GC to walk), and the default
	// clock's duration base is startMono below.
	startWall int64
	// startMono is the monotonic reading taken with startedAt when the
	// runtime uses the default clock; End derives the duration from it.
	// Zero (a configured clock, or a bare test event) means "use
	// startedAt", which is what a configured clock's Sub already does.
	startMono int64

	// start is the normalized OperationStart. It lives here rather than
	// on the per-request Operation because it is read only inside End
	// (commit and the post-seal annotations): the 80-byte struct leaves
	// the one allocation the request still pays.
	start OperationStart

	// rec is the pooled Record view for this generation's single commit.
	// It lives on the pooled event rather than on the per-request
	// Operation: the Operation is the one heap allocation a request
	// pays, so carrying the 64-byte Record there inflates the
	// allocation (and the GC traffic it drives) by more than half.
	// resetAt clears it before the next generation's commit.
	rec Record
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
	return newEventAt(time.Now().UnixNano())
}

// newEventAt is newEvent with the caller's clock reading instead of the
// system wall clock — the runtime's configured clock when set.
func newEventAt(startWall int64) *event {
	ev := eventPool.Get().(*event) //nolint:forcetypeassert // the pool's New stores exactly *event
	ev.resetAt(startWall)
	return ev
}

// reset prepares the event for its next generation. It takes the same
// mutex as appends, seals, and snapshots: a stale watchdog snapshot can
// hold the lock with a matching old generation, and without the lock
// the reset would truncate the fields under its copy.
func (e *event) reset() {
	e.resetAt(time.Now().UnixNano())
}

// resetAt is reset with an explicit start timestamp.
func (e *event) resetAt(startWall int64) {
	e.mu.Lock()
	// REJECTED: replacing the atomic add with load+add was MEASURED as a
	// wash (the RMW is under the mutex, so the contended form costs the
	// same here) — kept as the atomic add for the plainest possible
	// statement of "bump the generation".
	// One atomic RMW. The packed word is gen<<1|state, so adding
	// walGenOne-walSealed bumps the generation and clears the sealed bit
	// at once: a sealed event is odd, so +1 makes it even and carries into
	// the generation; a never-used pooled event is already even and must
	// not carry. The delta is read from the current state bit, so the RMW
	// is still single (the old Add+Store pair was two).
	for {
		cur := e.state.Load()
		if e.state.CompareAndSwap(cur, cur+walGenOne-uint64(cur&walStateMask)) {
			break
		}
	}
	// The backing array is NOT cleared. The old word clear (a barrier-free
	// word loop over the used prefix) measured ~7% of a kept lifecycle on
	// its own, and the slots it zeroed are unreachable: a slot only becomes
	// readable when reserveSlot hands it out for THIS generation, and every
	// fill site writes the key word and either writes both payload words or
	// nils the pointer word. So no reader can observe a previous
	// generation's payload, and a spare slot's stale pointer is released by
	// the next fill that reaches it. The one residual cost is that slots
	// past the current length can retain the previous request's values
	// until overwritten — bounded by the slice capacity per pooled event.
	e.fields = e.fields[:0]
	e.msg = ""
	e.hasErr = false
	e.requestedLevel = 0
	e.hasRequestedLvl = false
	// A node that outlives its generation must not read the NEXT request's
	// parent through a recycled event: clearing here means a straggler
	// loses cancellation propagation instead of inheriting another
	// request's parent. The in-flight request's own node always reads its
	// parent, which stays set until release.
	e.parent = nil
	e.startWall = startWall
	// Drop the previous generation's record view: level/msg/fields are
	// rewritten by commit, but the Encoded() cache must start nil or a
	// new event would inherit a stale encoded line. Field-by-field, not
	// a Record{} struct copy (the atomic cache word carries noCopy).
	// Only the encoded cache is cleared: commit writes level, msg, fields,
	// and completedAt before the record reaches any sink, so clearing them
	// per generation was four dead stores.
	e.rec.encoded.Store(nil)
	e.mu.Unlock()
}

// seal ends all mutations for this generation. It takes the append
// mutex, so an in-flight append either lands before the seal or
// observes the sealed state on its recheck and drops. Idempotent.
func (e *event) seal() {
	// Already sealed: the bit is only ever set under the lock, so a
	// visible bit means the earlier seal did the full handshake. End
	// seals once explicitly and once more through release; the second
	// call skips the lock.
	if walState(e.state.Load()&walStateMask) == walSealed {
		return
	}
	e.mu.Lock()
	e.state.Or(uint64(walSealed))
	e.mu.Unlock()
}

// slotZero is the zero Field the append-in-place path appends before
// filling every word; as a package variable it never materializes a
// stack temporary to memmove from.
var slotZero Field

// appendField is the Add path: it reserves one slot and fills it in
// place, so no 48-byte Field value crosses a call boundary. fieldOf
// can't be used here without reintroducing the typedmemmove the slot
// fill exists to avoid.
//
// The generation/seal check happens once, under the mutex. A lock-free
// pre-check of the state word used to guard the mutex; it was MEASURED
// to cost more than it saved (a second acquire load on every field of
// the hot path, ~8% of a twelve-field lifecycle) and to change nothing
// observable: a stale or post-seal writer just takes the mutex and
// drops on the recheck below, which is what the pre-check did without
// it. seal() takes the same mutex, so the recheck is authoritative.
func (e *event) appendField(gen uint64, key string, value any) {
	e.mu.Lock()
	if cur := e.state.Load(); cur>>walStateBits != gen || walState(cur&walStateMask) != walActive {
		e.mu.Unlock()
		return
	}
	// Dispatch and slot fill inline, for the same reason as Operation.Add:
	// the helper frame per field is pure overhead. Rare kinds fall back to
	// the full fillField switch.
	f := e.reserveSlot()
	switch v := value.(type) {
	case string:
		f.key, f.kind, f.num, f.pay = key, KindString, 0, packString(v)
	case int:
		f.key, f.kind, f.num = key, KindInt, int64(v)
		f.pay.data = nil
	default:
		fillField(f, key, value)
	}
	e.mu.Unlock()
}

// addKVFields is the shared body of addKV and addKVOwner: append the
// leading pair plus every well-formed kv pair. The scalar dispatch is
// inlined here for the same reason as Operation.Add — the per-pair
// helper frame was most of the cost — and the leading pair is loop
// iteration zero (i == -2) instead of a second copy of the switch.
// fieldOf only inspects types, so no user code runs here.
func (e *event) addKVFields(key string, value any, kv ...any) {
	for i := -2; i+1 < len(kv); i += 2 {
		k, v := key, value
		if i >= 0 {
			s, ok := kv[i].(string)
			if !ok || s == "" {
				continue
			}
			k, v = s, kv[i+1]
		} else if k == "" {
			continue
		}
		f := e.reserveSlot()
		switch val := v.(type) {
		case string:
			f.key, f.kind, f.num, f.pay = k, KindString, 0, packString(val)
		case int:
			f.key, f.kind, f.num = k, KindInt, int64(val)
			f.pay.data = nil // see Operation.Add: release the previous generation's pointer
		case bool:
			f.key, f.kind, f.num = k, KindBool, b2i(val)
			f.pay.data = nil
		case int64:
			f.key, f.kind, f.num = k, KindInt, val
			f.pay.data = nil
		case uint64:
			f.key, f.kind, f.num = k, KindUint, int64(val)
			f.pay.data = nil
		case time.Duration:
			f.key, f.kind, f.num = k, KindDuration, int64(val)
			f.pay.data = nil
		default:
			fillField(f, k, v)
		}
	}
}

// addKVOwner is addKV (the variadic handle form) without the mutex; same
// contract as appendFieldOwner.
func (e *event) addKVOwner(ref *walRef, key string, value any, kv ...any) {
	if cur := e.state.Load(); cur>>walStateBits == ref.gen && walState(cur&walStateMask) == walActive {
		e.addKVFields(key, value, kv...)
	}
}

// reserveSlot appends one raw slot and returns a pointer to it, so the
// caller can fill it in place. The slot is not pre-cleared: after the
// first generation a spare slot still holds the previous request's
// bytes (resetAt no longer scrubs the array), so a fill site must write
// every word its kind can read — the key, the numeric word, and either
// both payload words or a nil pointer word. The caller must hold e.mu.
func (e *event) reserveSlot() *Field {
	n := len(e.fields)
	if n == cap(e.fields) {
		// The growth branch must stay inlined: splitting it out (a
		// //go:noinline helper) pushed this function to inline cost 88
		// against a budget of 80, and the lost inlining cost MORE (a
		// call per slot, ~10% of a kept lifecycle) than the smaller hot
		// body saved.
		e.fields = append(e.fields, slotZero)
	} else {
		e.fields = e.fields[:n+1]
	}
	return &e.fields[n]
}

// pushStr and pushInt are the typed in-place appends for the canonical
// annotations: no interface boundary, no boxing, and no 48-byte Field
// value crossing a call boundary (which would cost a typedmemmove plus
// a bulk write-barrier walk per field). The caller must hold e.mu.
func (e *event) pushStr(key, value string) {
	f := e.reserveSlot()
	f.key, f.kind, f.num, f.pay = key, KindString, 0, packString(value)
}

func (e *event) pushInt(key string, v int64) {
	f := e.reserveSlot()
	f.key, f.kind, f.num, f.pay = key, KindInt, v, fieldPayload{}
}

// pushScalar is the typed in-place append for every kind whose value
// lives entirely in the numeric word (int, uint, float, bool, duration).
// The payload words are zeroed deliberately: a spare slot may still hold
// the previous generation's type descriptor, and the union must not read
// as an any box. Kept separate from pushInt so the hottest caller keeps
// its own minimal body. The caller must hold e.mu.
func (e *event) pushScalar(key string, kind FieldKind, v int64) {
	f := e.reserveSlot()
	f.key, f.kind, f.num, f.pay = key, kind, v, fieldPayload{}
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
	e.addKVFields(key, value, kv...)
}

// addStrKV is addKV for string-only fields: the pairs never cross an
// interface boundary, so nothing is boxed and nothing is packed. One
// guarded acquisition covers the whole list.
func (e *event) addStrKV(ref *walRef, key, value string, kv []string) {
	s := e.state.Load()
	if s>>walStateBits != ref.gen || walState(s&walStateMask) == walSealed {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if cur := e.state.Load(); cur>>walStateBits != ref.gen || walState(cur&walStateMask) != walActive {
		return
	}
	if key != "" {
		e.pushStr(key, value)
	}
	for i := 0; i+1 < len(kv); i += 2 {
		if kv[i] != "" {
			e.pushStr(kv[i], kv[i+1])
		}
	}
}

// Typed append helpers for the canonical fields: no interface boundary,
// no boxing, and no 48-byte Field value crossing a call boundary (the
// slot is filled in place, as the field-write paths are).
func (e *event) appendStr(gen uint64, key, value string) {
	s := e.state.Load()
	if s>>walStateBits != gen || walState(s&walStateMask) == walSealed {
		return
	}
	e.mu.Lock()
	if cur := e.state.Load(); cur>>walStateBits == gen && walState(cur&walStateMask) == walActive {
		e.pushStr(key, value)
	}
	e.mu.Unlock()
}

// appendPacked is the locked ctx-form slot fill behind the typed Add
// family (unolog.AddString and friends). The value arrives already
// unpacked into the three words a Field stores, so the call never
// builds an any box. Same guarded discipline as appendStr: the
// generation/seal recheck under the mutex is authoritative against a
// concurrent End.
func (e *event) appendPacked(gen uint64, key string, kind FieldKind, num int64, pay fieldPayload) {
	s := e.state.Load()
	if s>>walStateBits != gen || walState(s&walStateMask) == walSealed {
		return
	}
	e.mu.Lock()
	if cur := e.state.Load(); cur>>walStateBits == gen && walState(cur&walStateMask) == walActive {
		f := e.reserveSlot()
		f.key, f.kind, f.num, f.pay = key, kind, num, pay
	}
	e.mu.Unlock()
}

func (e *event) appendAny(gen uint64, key string, value any) {
	// Pack before the lock: packAny only reads the box, and the caller
	// (the panic annotation) has already run the user's Error()/String().
	pay := packAny(value)
	s := e.state.Load()
	if s>>walStateBits != gen || walState(s&walStateMask) == walSealed {
		return
	}
	e.mu.Lock()
	if cur := e.state.Load(); cur>>walStateBits == gen && walState(cur&walStateMask) == walActive {
		f := e.reserveSlot()
		f.key, f.kind, f.num, f.pay = key, KindAny, 0, pay
	}
	e.mu.Unlock()
}

// setError records the structured error field and latches hasErr. The
// append and the latch serialize under mu with the seal, so they cannot
// split.
func (e *event) setError(ref *walRef, err error) {
	if err == nil {
		return
	}
	// A stale or sealed target must not run user code: structuredErrorField
	// calls Error()/Unwrap(), so the lock-free generation check is load
	// bearing here, not an optimisation. Same reason the pack below is
	// outside the lock.
	s := e.state.Load()
	if s>>walStateBits != ref.gen || walState(s&walStateMask) == walSealed {
		return
	}
	// Build the structured field before the lock: Error()/Unwrap()
	// implementations are user code and must not run under the event
	// mutex (a slow or reentrant error would stall or deadlock other
	// writers and the watchdog). Only the packed words cross the lock;
	// the slot is filled in place, so no 48-byte Field value is copied.
	pay := packAny(structuredErrorField(err))
	e.mu.Lock()
	defer e.mu.Unlock()
	if cur := e.state.Load(); cur>>walStateBits == ref.gen && walState(cur&walStateMask) == walActive {
		f := e.reserveSlot()
		f.key, f.kind, f.num, f.pay = KeyError, KindAny, 0, pay
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
