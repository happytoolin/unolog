package unolog

import (
	"math"
	"time"
	"unsafe"
)

// FieldKind discriminates the typed value slot of a Field, following the
// slog.Value convention: one kind per representable type plus KindAny for
// everything else (marshalled at encode time).
type FieldKind uint8

const (
	KindInvalid FieldKind = iota
	KindString
	KindInt
	KindUint
	KindFloat
	KindFloat32
	KindBool
	KindTime
	KindDuration
	KindErr
	KindAny
)

// Field is one typed key/value record in the request WAL. Fields are
// append-only, carry insertion order, and are read through the typed
// getters; the zero value is an unset field (KindInvalid).
//
// The layout is deliberately pointer-light. Every append copies a Field
// by value, and the runtime's write barrier charges per pointer word, so
// every pointer-bearing value kind shares one slot instead of sitting
// side by side: a Field has exactly one kind, so a string, an any box,
// and a time's location never coexist. That takes the struct from 104
// bytes / 5 pointer words to 48 bytes / 2 — the kind word is the only
// tag, and time.Time rides the same union (see packTime).
type Field struct {
	key  string
	kind FieldKind

	num int64        // KindInt, KindUint (uint64 bits), KindDuration, KindFloat bits, KindBool, KindTime wall
	pay fieldPayload // KindString, KindErr, KindAny; KindTime ext (meta) + location (data)
}

// fieldPayload is the union of the pointer-bearing value slots. data is
// the only GC-visible word — the string's bytes, or the any box's value.
// meta is not traced: it is a string length, or the any box's static type
// descriptor, which lives in the binary's read-only data and is never
// collected. typePointers walks this as one pointer, which is the whole
// point of the union.
type fieldPayload struct {
	data unsafe.Pointer
	meta uintptr
}

// eface mirrors the runtime's empty-interface header. The gc ABI lays
// every non-empty `any` out as {type, data} in that order, which is what
// makes the pack/unpack pair below a move rather than a conversion.
type eface struct {
	typ  unsafe.Pointer
	data unsafe.Pointer
}

// packString stores a string in the payload. An empty string maps to the
// zero payload; length zero is indistinguishable from "unset" but both
// read back as "".
func packString(s string) fieldPayload {
	if len(s) == 0 {
		return fieldPayload{}
	}
	return fieldPayload{data: unsafe.Pointer(unsafe.StringData(s)), meta: uintptr(len(s))}
}

// stringValue reads back a string. It must only be called for a
// KindString field: for any-typed payloads the metadata word is a type
// descriptor, not a length.
func (p fieldPayload) stringValue() string {
	if p.meta == 0 {
		return ""
	}
	return unsafe.String((*byte)(p.data), int(p.meta))
}

// packAny stores an any box by copying its two words. Neither the local
// nor its address escapes, so this stays on the stack.
func packAny(v any) fieldPayload {
	e := (*eface)(unsafe.Pointer(&v))
	return fieldPayload{data: e.data, meta: uintptr(e.typ)}
}

// anyValue rebuilds the any box from the two words. The type descriptor
// round-trips through uintptr by design: it is static data, so it is
// always reachable and never moved by the collector.
func (p fieldPayload) anyValue() any {
	if p.meta == 0 {
		return nil
	}
	e := eface{typ: unsafe.Pointer(p.meta), data: p.data} //nolint:govet // static type descriptor, always reachable
	return *(*any)(unsafe.Pointer(&e))
}

// timeLayout mirrors the runtime's time.Time: a wall clock word, an
// extension word (monotonic reading, or nanoseconds), and a location
// pointer. The kind-union slot is exactly three words wide, so a time
// rides it with no extra Field bytes. Location is the union's only
// GC-visible word for a KindTime field.
type timeLayout struct {
	wall uint64
	ext  int64
	loc  *time.Location
}

// packTime splits a time.Time into the union's wall word and payload
// (ext in the metadata word, location in the data word). Every word is
// moved verbatim, so the value round-trips exactly — zone and monotonic
// reading included — which is what the RFC3339 wire stamp and the
// public Time getters require.
func packTime(v time.Time) (int64, fieldPayload) {
	tl := (*timeLayout)(unsafe.Pointer(&v))
	return int64(tl.wall), fieldPayload{data: unsafe.Pointer(tl.loc), meta: uintptr(tl.ext)}
}

// timeValue rebuilds the time.Time from the union words. Only valid for
// KindTime: for other kinds the metadata/data words are a length/type
// descriptor, not ext/location.
func (f Field) timeValue() time.Time {
	tl := timeLayout{wall: uint64(f.num), ext: int64(f.pay.meta), loc: (*time.Location)(f.pay.data)}
	return *(*time.Time)(unsafe.Pointer(&tl))
}

// Key returns the field key as written on the WAL (unaliased).
func (f Field) Key() string { return f.key }

// WireKey returns the key as it appears on the canonical JSON line:
// user fields named "message", "time", or "level" become
// "fields.message", "fields.time", and "fields.level".
func (f Field) WireKey() string { return aliasedFieldKey(f.key) }

// Kind returns the field's value kind.
func (f Field) Kind() FieldKind { return f.kind }

// Str returns the value as a string; ok reports whether the field is a
// string kind.
func (f Field) Str() (s string, ok bool) {
	if f.kind == KindString {
		return f.pay.stringValue(), true
	}
	return "", false
}

// Int returns the value as an int64 for integer kinds.
func (f Field) Int() (i int64, ok bool) {
	if f.kind == KindInt {
		return f.num, true
	}
	return 0, false
}

// Uint returns the value as a uint64 for unsigned kinds.
func (f Field) Uint() (u uint64, ok bool) {
	if f.kind == KindUint {
		return uint64(f.num), true
	}
	return 0, false
}

// Float returns the value as a float64 for float kinds.
func (f Field) Float() (fl float64, ok bool) {
	if f.kind == KindFloat || f.kind == KindFloat32 {
		return math.Float64frombits(uint64(f.num)), true
	}
	return 0, false
}

// Bool returns the value for boolean kinds.
func (f Field) Bool() (b bool, ok bool) {
	if f.kind == KindBool {
		return f.num != 0, true
	}
	return false, false
}

// Time returns the value for time kinds.
func (f Field) Time() (t time.Time, ok bool) {
	if f.kind == KindTime {
		return f.timeValue(), true
	}
	return time.Time{}, false
}

// Duration returns the value for duration kinds.
func (f Field) Duration() (d time.Duration, ok bool) {
	if f.kind == KindDuration {
		return time.Duration(f.num), true
	}
	return 0, false
}

// Err returns the value for error kinds.
func (f Field) Err() (err error, ok bool) {
	if f.kind == KindErr {
		if e, isErr := f.pay.anyValue().(error); isErr {
			return e, true
		}
	}
	return nil, false
}

// Any returns the value for any-kinds. It never boxes: typed kinds
// (including error) return nil — use Err().
func (f Field) Any() any {
	if f.kind == KindAny {
		return f.pay.anyValue()
	}
	return nil
}

// fieldOf builds a typed Field from key and an arbitrary value using the
// same type mapping the v0 zerolog adapter used, so wire output is
// unchanged for every supported type.
func fieldOf(key string, value any) Field {
	var f Field
	fillField(&f, key, value)
	return f
}

// fillField writes one typed field into *f with the same type mapping
// fieldOf has always used. It exists so the WAL append can build the
// field directly in its slice slot: returning a 48-byte Field by value
// and appending it costs a typedmemmove plus a bulk write-barrier walk
// (spanOf + typePointersOfType) on every field, which the counters
// charge to the GC path. The pointer word is nil'd up front instead of
// in each scalar branch: *f is usually a recycled slot that may still
// hold the previous generation's string pointer, and a nil store emits
// no write barrier while the pointer-bearing branches overwrite it.
func fillField(f *Field, key string, value any) {
	f.key = key
	f.pay.data = nil
	switch v := value.(type) {
	case string:
		f.kind, f.num, f.pay = KindString, 0, packString(v)
	case int:
		f.kind, f.num = KindInt, int64(v)
	case int8:
		f.kind, f.num = KindInt, int64(v)
	case int16:
		f.kind, f.num = KindInt, int64(v)
	case int32:
		f.kind, f.num = KindInt, int64(v)
	case int64:
		f.kind, f.num = KindInt, v
	case uint:
		f.kind, f.num = KindUint, int64(uint64(v))
	case uint8:
		f.kind, f.num = KindUint, int64(uint64(v))
	case uint16:
		f.kind, f.num = KindUint, int64(uint64(v))
	case uint32:
		f.kind, f.num = KindUint, int64(uint64(v))
	case uint64:
		f.kind, f.num = KindUint, int64(v)
	case float32:
		// kept as its own kind: the wire must render 0.1, not the
		// widened 0.10000000149011612 (v0 adapter parity)
		f.kind, f.num = KindFloat32, int64(math.Float64bits(float64(v)))
	case float64:
		f.kind, f.num = KindFloat, int64(math.Float64bits(v))
	case bool:
		f.kind, f.num = KindBool, b2i(v)
	case time.Time:
		num, pay := packTime(v)
		f.kind, f.num, f.pay = KindTime, num, pay
	case time.Duration:
		f.kind, f.num = KindDuration, int64(v)
	case error:
		f.kind, f.num, f.pay = KindErr, 0, packAny(v)
	default:
		f.kind, f.num, f.pay = KindAny, 0, packAny(v)
	}
}

// Typed field constructors for the canonical annotations (no boxing).
func fieldStr(key, value string) Field {
	return Field{key: key, kind: KindString, pay: packString(value)}
}
func fieldInt64(key string, v int64) Field { return Field{key: key, kind: KindInt, num: v} }

// b2i maps a bool onto the scalar slot without a branchless-table trick:
// the compiler already turns this into a setcc.
func b2i(v bool) int64 {
	if v {
		return 1
	}
	return 0
}

// valueOf converts a Field back to an any, mirroring the v0 map-based
// values (used by Lookup and the TestSink).
func valueOf(f Field) any {
	switch f.kind {
	case KindString:
		return f.pay.stringValue()
	case KindInt:
		return f.num
	case KindUint:
		return uint64(f.num)
	case KindFloat:
		return math.Float64frombits(uint64(f.num))
	case KindFloat32:
		return float32(math.Float64frombits(uint64(f.num)))
	case KindBool:
		return f.num != 0
	case KindTime:
		return f.timeValue()
	case KindDuration:
		return time.Duration(f.num)
	case KindErr, KindAny:
		return f.pay.anyValue()
	default:
		return nil
	}
}
