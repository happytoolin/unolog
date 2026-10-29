package unolog

import (
	"time"
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
type Field struct {
	key  string
	kind FieldKind

	num int64   // KindInt, KindUint (uint64 bits), KindDuration
	f   float64 // KindFloat
	b   bool    // KindBool
	str string  // KindString
	t   time.Time
	val any // KindErr (error), KindAny
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
		return f.str, true
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
		return f.f, true
	}
	return 0, false
}

// Bool returns the value for boolean kinds.
func (f Field) Bool() (b bool, ok bool) {
	if f.kind == KindBool {
		return f.b, true
	}
	return false, false
}

// Time returns the value for time kinds.
func (f Field) Time() (t time.Time, ok bool) {
	if f.kind == KindTime {
		return f.t, true
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
		if e, isErr := f.val.(error); isErr {
			return e, true
		}
	}
	return nil, false
}

// Any returns the value for any-kinds. It never boxes: typed kinds
// (including error) return nil — use Err().
func (f Field) Any() any {
	if f.kind == KindAny {
		return f.val
	}
	return nil
}

// fieldOf builds a typed Field from key and an arbitrary value using the
// same type mapping the v0 zerolog adapter used, so wire output is
// unchanged for every supported type.
func fieldOf(key string, value any) Field {
	switch v := value.(type) {
	case string:
		return Field{key: key, kind: KindString, str: v}
	case int:
		return Field{key: key, kind: KindInt, num: int64(v)}
	case int8:
		return Field{key: key, kind: KindInt, num: int64(v)}
	case int16:
		return Field{key: key, kind: KindInt, num: int64(v)}
	case int32:
		return Field{key: key, kind: KindInt, num: int64(v)}
	case int64:
		return Field{key: key, kind: KindInt, num: v}
	case uint:
		return Field{key: key, kind: KindUint, num: int64(uint64(v))}
	case uint8:
		return Field{key: key, kind: KindUint, num: int64(uint64(v))}
	case uint16:
		return Field{key: key, kind: KindUint, num: int64(uint64(v))}
	case uint32:
		return Field{key: key, kind: KindUint, num: int64(uint64(v))}
	case uint64:
		return Field{key: key, kind: KindUint, num: int64(v)}
	case float32:
		// kept as its own kind: the wire must render 0.1, not the
		// widened 0.10000000149011612 (v0 adapter parity)
		return Field{key: key, kind: KindFloat32, f: float64(v)}
	case float64:
		return Field{key: key, kind: KindFloat, f: v}
	case bool:
		return Field{key: key, kind: KindBool, b: v}
	case time.Time:
		return Field{key: key, kind: KindTime, t: v}
	case time.Duration:
		return Field{key: key, kind: KindDuration, num: int64(v)}
	case error:
		return Field{key: key, kind: KindErr, val: v}
	default:
		return Field{key: key, kind: KindAny, val: v}
	}
}

// Typed field constructors for AddFields. They store the value in its
// typed slot, so a dynamic value is not boxed into an interface: no
// allocation, unlike passing the same value to Add.

// String returns a string field.
func String(key, value string) Field { return Field{key: key, kind: KindString, str: value} }

// Int returns an integer field.
func Int(key string, value int) Field { return Field{key: key, kind: KindInt, num: int64(value)} }

// Int64 returns an integer field.
func Int64(key string, value int64) Field { return Field{key: key, kind: KindInt, num: value} }

// Uint64 returns an unsigned integer field.
func Uint64(key string, value uint64) Field {
	return Field{key: key, kind: KindUint, num: int64(value)}
}

// Float64 returns a float field.
func Float64(key string, value float64) Field { return Field{key: key, kind: KindFloat, f: value} }

// Bool returns a boolean field.
func Bool(key string, value bool) Field { return Field{key: key, kind: KindBool, b: value} }

// Time returns a time field.
func Time(key string, value time.Time) Field { return Field{key: key, kind: KindTime, t: value} }

// Duration returns a duration field.
func Duration(key string, value time.Duration) Field {
	return Field{key: key, kind: KindDuration, num: int64(value)}
}

// Any returns a field for value with the same type mapping Add uses.
func Any(key string, value any) Field { return fieldOf(key, value) }

// valueOf converts a Field back to an any, mirroring the v0 map-based
// values (used by Lookup and the TestSink).
func valueOf(f Field) any {
	switch f.kind {
	case KindString:
		return f.str
	case KindInt:
		return f.num
	case KindUint:
		return uint64(f.num)
	case KindFloat:
		return f.f
	case KindFloat32:
		return float32(f.f)
	case KindBool:
		return f.b
	case KindTime:
		return f.t
	case KindDuration:
		return time.Duration(f.num)
	case KindErr, KindAny:
		return f.val
	default:
		return nil
	}
}
