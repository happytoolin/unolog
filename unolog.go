package unolog

import (
	"context"
	"math"
	"time"
)

// walNode is the request context node Context() hands out. It is exactly
// the 16-byte walRef the request pins: the 16-byte size class instead of
// the 32-byte one, one pointer word instead of three, no extra write
// barrier, and no conversion at all — the node is the ref. The parent
// context it once held lives on the pooled event instead (event.parent),
// where it costs nothing per request.
type walNode walRef

func (n *walNode) Deadline() (time.Time, bool) {
	if p := n.ev.parent; p != nil {
		return p.Deadline()
	}
	return time.Time{}, false
}

func (n *walNode) Done() <-chan struct{} {
	if p := n.ev.parent; p != nil {
		return p.Done()
	}
	return nil
}

func (n *walNode) Err() error {
	if p := n.ev.parent; p != nil {
		return p.Err()
	}
	return nil
}

func (n *walNode) Value(key any) any {
	if _, ok := key.(contextKey); ok {
		return (*walRef)(n)
	}
	if p := n.ev.parent; p != nil {
		return p.Value(key)
	}
	return nil
}

// contextKey carries the request's WAL handle. The accessors are
// unexported: the in-flight WAL is written by the unolog.Add helpers and
// read once at End — nothing reads it mid-flight by design.
type contextKey struct{}

func eventFromContext(ctx context.Context) *walRef {
	// Fast path: the operation's own node is what Add is handed in the
	// common case. A direct assertion skips the interface dispatch into
	// Value, the key boxing, and the *walRef assertion. Kept small enough
	// to inline into every Add; a wrapped context takes the slow call.
	if n, ok := ctx.(*walNode); ok {
		return (*walRef)(n)
	}
	return walRefFromValue(ctx)
}

// walRefFromValue is the slower path, taken by anything that is not this
// package's own node: the operation handle itself (a caller that passes a
// *Operation straight in instead of op.Context()) and any wrapped context.
// It checks the handle directly, then walks Value for the package key.
// Split out of eventFromContext so the common node check stays inlineable.
//
//go:noinline
func walRefFromValue(ctx context.Context) *walRef {
	if ctx == nil {
		return nil
	}
	if o, ok := ctx.(*Operation); ok {
		return &o.ref
	}
	ref, _ := ctx.Value(contextKey{}).(*walRef)
	return ref
}

// Add records one or more fields on the request's event. With no event
// attached (or after End) it is a silent no-op, exactly like the slog
// helper family.
//
// Writes serialize under the event mutex, so concurrent Add calls are
// safe: order is not defined, but no write is lost or torn. After End
// the event is sealed and every later write is dropped, including a
// write that began before the seal.
//
// Additional pairs may be passed variadically: Add(ctx, "a", 1, "b", 2).
// Every pair key must be a non-empty string; empty and non-string keys
// are skipped uniformly, leading pair included.
//
// The request goroutine is the natural single writer, and concurrent
// Add calls from child goroutines are safe: they serialize under the
// event mutex, so the field set is complete — only the order is not
// defined.
//
// When the whole operation stays on one goroutine, Operation.Add is the
// faster form: same write path, minus the context lookup and the mutex.
func Add(ctx context.Context, key string, value any, kv ...any) {
	ref := eventFromContext(ctx)
	if ref == nil {
		return
	}
	if len(kv) == 0 {
		if key != "" { // single-pair fast path
			ref.ev.appendField(ref.gen, key, value)
		}
		return
	}
	ref.ev.addKV(ref, key, value, kv...)
}

// The typed Add family: unolog.Add with the kind known at the call site.
// The value crosses in registers instead of an any, so the caller pays
// no interface store and no heap box.
//
// An any box is not free. A non-constant string is boxed into a fresh
// 16-byte heap object on every call, and a large integer likewise: one
// extra allocation per field, MEASURED at ~8-18 ns on the request hot
// path and 17% of a route-resolving HTTP request's total cost. Reaching
// for AddString when the value is a variable — an id, a route template,
// a status — is the single cheapest thing a ctx-form caller can do.
// Literals cost nothing (the compiler emits a static box), so the plain
// Add stays right for the constant keys and small constant values.

// AddString records one string field. See Add for the key rules.
func AddString(ctx context.Context, key, value string) {
	if ref := eventFromContext(ctx); ref != nil && key != "" {
		ref.ev.appendPacked(ref.gen, key, KindString, 0, packString(value))
	}
}

// AddStrings records one or more string fields under a single guarded
// acquisition: AddStrings(ctx, "a", "1", "b", "2") records both pairs
// with one lock instead of one per field, and — unlike Add — the values
// never cross an interface boundary, so no variable is boxed.
//
// MEASURED on twelve runtime string fields: 157 ns/op against Add's
// 284 ns/op, and one allocation against thirteen (the twelve 16-byte
// value boxes Add cannot avoid). Prefer it over a typed AddString loop
// when several fields are written together; a single field is still
// AddString's job.
//
// The key rules match Add: a pair with an empty key is skipped, and an
// odd number of trailing values drops the last one.
func AddStrings(ctx context.Context, key, value string, kv ...string) {
	ref := eventFromContext(ctx)
	if ref == nil {
		return
	}
	ref.ev.addStrKV(ref, key, value, kv)
}

// AddInt records one int64 field (int, int8..int64 all land here).
func AddInt(ctx context.Context, key string, value int64) {
	if ref := eventFromContext(ctx); ref != nil && key != "" {
		ref.ev.appendPacked(ref.gen, key, KindInt, value, fieldPayload{})
	}
}

// AddUint records one uint64 field.
func AddUint(ctx context.Context, key string, value uint64) {
	if ref := eventFromContext(ctx); ref != nil && key != "" {
		ref.ev.appendPacked(ref.gen, key, KindUint, int64(value), fieldPayload{})
	}
}

// AddFloat64 records one float64 field.
func AddFloat64(ctx context.Context, key string, value float64) {
	if ref := eventFromContext(ctx); ref != nil && key != "" {
		ref.ev.appendPacked(ref.gen, key, KindFloat, int64(math.Float64bits(value)), fieldPayload{})
	}
}

// AddBool records one bool field.
func AddBool(ctx context.Context, key string, value bool) {
	if ref := eventFromContext(ctx); ref != nil && key != "" {
		ref.ev.appendPacked(ref.gen, key, KindBool, b2i(value), fieldPayload{})
	}
}

// AddDuration records one time.Duration field.
func AddDuration(ctx context.Context, key string, value time.Duration) {
	if ref := eventFromContext(ctx); ref != nil && key != "" {
		ref.ev.appendPacked(ref.gen, key, KindDuration, int64(value), fieldPayload{})
	}
}

// AddTime records one time.Time field, round-tripping the whole value
// (zone and monotonic reading included) exactly as Add does.
func AddTime(ctx context.Context, key string, value time.Time) {
	if ref := eventFromContext(ctx); ref != nil && key != "" {
		num, pay := packTime(value)
		ref.ev.appendPacked(ref.gen, key, KindTime, num, pay)
	}
}

// Error records err on the request's event as the structured canonical
// error field.
func Error(ctx context.Context, err error) {
	if err == nil {
		return
	}
	if ref := eventFromContext(ctx); ref != nil {
		ref.ev.setError(ref, err)
	}
}

// SetMessage sets a per-event final message override. An empty message
// is treated as unset during finalization.
func SetMessage(ctx context.Context, msg string) {
	if ref := eventFromContext(ctx); ref != nil {
		ref.ev.setMessage(ref, msg)
	}
}

// SetRoute sets a normalized route template (http.route) on the event.
func SetRoute(ctx context.Context, route string) {
	if ref := eventFromContext(ctx); ref != nil {
		ref.ev.setRoute(ref, route)
	}
}

// SetLevel requests a severity floor for the final event; the emitted
// level is never lower than the request.
func SetLevel(ctx context.Context, level Level) {
	if ref := eventFromContext(ctx); ref != nil {
		ref.ev.setLevel(ref, level)
	}
}
