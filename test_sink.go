package unolog

import (
	"context"
	"reflect"
	"sync"
)

// CapturedEvent is one event captured by TestSink — a retained, copied
// snapshot (the Record view is only valid during Write).
type CapturedEvent struct {
	level   Level
	message string
	fields  []Field
}

// Level returns the captured event's severity.
func (c CapturedEvent) Level() Level { return c.level }

// Message returns the captured event's message.
func (c CapturedEvent) Message() string { return c.message }

// Fields returns the captured fields in insertion order.
func (c CapturedEvent) Fields() []Field { return c.fields }

// Lookup returns the last value written under key.
func (c CapturedEvent) Lookup(key string) (any, bool) {
	return lookupField(c.fields, key)
}

// TestSink captures events in memory for tests, copying retained values
// out of the transient Record.
type TestSink struct {
	mu     sync.Mutex
	events []CapturedEvent
}

// NewTestSink returns an empty in-memory sink.
func NewTestSink() *TestSink {
	return &TestSink{}
}

// Write captures one event, deep-copying the field values that are
// mutable (maps, slices, arrays — including through reflect); typed
// scalars are immutable by construction. Pointer payloads are NOT
// cloned (the pointed-to value stays shared with the caller), except
// that error identity is deliberately preserved so errors.Is works
// on captured fields. Struct payloads copy shallowly: a struct that
// wraps a map or slice stays shared with the caller, because a
// reflect field copy cannot set unexported fields (and would break
// time.Time). Mutate-through-a-pointer after End is visible in the
// capture — copy it yourself before End if you need a frozen snapshot
// of pointer-bearing or struct-bearing data.
func (t *TestSink) Write(_ context.Context, rec *Record) {
	if t == nil || rec == nil {
		return
	}
	captured := CapturedEvent{
		level:   rec.Level(),
		message: rec.Message(),
		fields:  copyFields(rec.Fields()),
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = append(t.events, captured)
}

// Events returns the captured events.
func (t *TestSink) Events() []CapturedEvent {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]CapturedEvent, len(t.events))
	copy(out, t.events)
	return out
}

// Reset drops all captured events.
func (t *TestSink) Reset() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.events = nil
}

func copyFields(fields []Field) []Field {
	if fields == nil {
		return nil
	}
	out := make([]Field, len(fields))
	for i, f := range fields {
		switch f.kind {
		case KindAny, KindErr:
			out[i] = Field{key: f.key, kind: f.kind, val: deepCopyValue(f.val, newVisitSet())}
		default:
			out[i] = f
		}
	}
	return out
}

// deepCopyValue clones the mutable containers TestSink can retain
// (map[string]any, []any, and — via reflect — other map/slice kinds) so
// captures survive caller mutation. Immutable scalars return unchanged.
// The visit set threads through the recursive cases so cyclic values
// terminate (a fresh set per call would recurse forever — fatal stack
// overflow, not a recoverable panic).
func deepCopyValue(v any, seen *visitSet) any {
	switch val := v.(type) {
	case nil:
		return nil
	case map[string]any:
		return deepCopyMap(val, seen)
	case []any:
		return deepCopySlice(val, seen)
	default:
		rv := reflect.ValueOf(v)
		switch rv.Kind() {
		case reflect.Map, reflect.Slice, reflect.Array:
			if rv.CanInterface() {
				cp := reflect.New(rv.Type()).Elem()
				local := map[visitKey]reflect.Value{}
				deepCopyReflect(rv, cp, local)
				return cp.Interface()
			}
		default:
			// Immutable scalars pass through unchanged.
		}
		return v
	}
}

func deepCopyReflect(src, dst reflect.Value, seen map[visitKey]reflect.Value) {
	switch k := src.Kind(); k {
	case reflect.Map:
		key := visitKey{typ: src.Type(), ptr: src.Pointer()}
		if prior, ok := seen[key]; ok {
			dst.Set(prior)
			return
		}
		dst.Set(reflect.MakeMapWithSize(src.Type(), src.Len()))
		seen[key] = dst
		for _, k := range src.MapKeys() {
			ev := reflect.New(src.Type().Elem()).Elem()
			deepCopyReflect(src.MapIndex(k), ev, seen)
			dst.SetMapIndex(k, ev)
		}
	case reflect.Slice:
		// Zero-length slices all report Pointer() == 0. Copy them
		// directly instead of caching them: a shared cache key would
		// alias two empty slices of different types and panic on Set.
		if src.Len() == 0 {
			if src.IsNil() {
				dst.SetZero()
				return
			}
			dst.Set(reflect.MakeSlice(src.Type(), 0, src.Cap()))
			return
		}
		key := visitKey{typ: src.Type(), ptr: src.Pointer()}
		if prior, ok := seen[key]; ok {
			dst.Set(prior)
			return
		}
		dst.Set(reflect.MakeSlice(src.Type(), src.Len(), src.Cap()))
		seen[key] = dst
		for i := range src.Len() {
			deepCopyReflect(src.Index(i), dst.Index(i), seen)
		}
	case reflect.Array:
		for i := range src.Len() {
			deepCopyReflect(src.Index(i), dst.Index(i), seen)
		}
	case reflect.Pointer:
		if src.IsNil() {
			return
		}
		key := visitKey{typ: src.Type(), ptr: src.Pointer()}
		if prior, ok := seen[key]; ok {
			dst.Set(prior)
			return
		}
		cp := reflect.New(src.Type().Elem())
		seen[key] = cp
		deepCopyReflect(src.Elem(), cp.Elem(), seen)
		dst.Set(cp)
	case reflect.Interface:
		if src.IsNil() {
			return
		}
		ev := reflect.New(src.Elem().Type()).Elem()
		deepCopyReflect(src.Elem(), ev, seen)
		dst.Set(ev)
	default:
		dst.Set(src)
	}
}

type visitSet struct {
	seen map[visitKey]any
}

type visitKey struct {
	typ reflect.Type
	ptr uintptr
}

func newVisitSet() *visitSet { return &visitSet{seen: map[visitKey]any{}} }

func deepCopyMap(m map[string]any, seen *visitSet) map[string]any {
	key := visitKey{typ: reflect.TypeOf(m), ptr: reflect.ValueOf(m).Pointer()}
	if prior, ok := seen.seen[key]; ok {
		return prior.(map[string]any) //nolint:forcetypeassert // the visit set stores exactly this type
	}
	out := make(map[string]any, len(m))
	seen.seen[key] = out
	for k, v := range m {
		out[k] = deepCopyValue(v, seen)
	}
	return out
}

func deepCopySlice(s []any, seen *visitSet) []any {
	key := visitKey{typ: reflect.TypeOf(s), ptr: reflect.ValueOf(s).Pointer()}
	if prior, ok := seen.seen[key]; ok {
		return prior.([]any) //nolint:forcetypeassert // the visit set stores exactly this type
	}
	out := make([]any, len(s))
	seen.seen[key] = out
	for i, v := range s {
		out[i] = deepCopyValue(v, seen)
	}
	return out
}

var _ Sink = (*TestSink)(nil)
