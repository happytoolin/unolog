package unolog

import (
	"context"
)

// contextKey carries the request's WAL handle. The accessors are
// unexported: the in-flight WAL is written by the unolog.Add helpers and
// read once at End — nothing reads it mid-flight by design.
type contextKey struct{}

func eventFromContext(ctx context.Context) *walRef {
	if ctx == nil {
		return nil
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
// Writes are bounded by Config.MaxFields and Config.MaxValueBytes.
func Add(ctx context.Context, key string, value any, kv ...any) {
	ref := eventFromContext(ctx)
	if ref == nil {
		return
	}
	if len(kv) == 0 {
		if key != "" { // single-pair fast path
			ref.ev.append(ref.gen, fieldOf(key, value))
		}
		return
	}
	ref.ev.addKV(ref, key, value, kv...)
}

// AddFields records typed fields on the request's event, built with
// String, Int, Bool, and the other Field constructors. It is the
// allocation-free form of Add for dynamic values: Add boxes each value
// into an interface, AddFields does not. Fields with an empty key,
// including the zero Field, are skipped. Concurrency, sealing, and the
// event limits follow Add.
func AddFields(ctx context.Context, fields ...Field) {
	if len(fields) == 0 {
		return
	}
	if ref := eventFromContext(ctx); ref != nil {
		ref.ev.addFields(ref.gen, fields)
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
