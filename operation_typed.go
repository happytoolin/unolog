package unolog

import (
	"math"
	"time"
)

// The typed Add family: the Operation.Add write path with the kind
// known at the call site. The value crosses in registers instead of an
// `any`, so the caller pays no interface store and the callee pays no
// itab load, no type-descriptor compare, and no indirect payload load.
// The handle therefore stays on the caller's stack, exactly as with Add.
//
// MEASURED on the twelve-field lifecycle: routing all twelve fields
// through the typed methods instead of Add cut ~4-5% of the per-request
// cost (the string and int cases are 11 of those 12 fields). The saving
// is per field, so it scales with the field count.
//
// Every method has Add's exact semantics: a nil handle, a zero
// Operation, an empty key, and a call after End all drop silently, and
// the kind recorded on the wire is the one the method names.

// open returns the event when the handle is live for this generation,
// and nil when the write must be dropped. It is the shared prologue of
// the typed family, kept small enough to inline into each of them.
func (op *Operation) open() *event {
	if op == nil || op.ref.ev == nil {
		return nil
	}
	e := op.ref.ev
	// Same single-compare guard as Add: the packed state word rejects
	// both a recycled event (stale generation) and a sealed one.
	if e.state.Load() != op.ref.gen<<walStateBits {
		return nil
	}
	return e
}

// AddString records a string field.
func (op *Operation) AddString(key, value string) {
	if e := op.open(); e != nil && key != "" {
		e.pushStr(key, value)
	}
}

// AddInt records an int64 field (int, int8..int64 all land here).
func (op *Operation) AddInt(key string, value int64) {
	if e := op.open(); e != nil && key != "" {
		e.pushInt(key, value)
	}
}

// AddUint records a uint64 field.
func (op *Operation) AddUint(key string, value uint64) {
	if e := op.open(); e != nil && key != "" {
		e.pushScalar(key, KindUint, int64(value))
	}
}

// AddFloat64 records a float64 field.
func (op *Operation) AddFloat64(key string, value float64) {
	if e := op.open(); e != nil && key != "" {
		e.pushScalar(key, KindFloat, int64(math.Float64bits(value)))
	}
}

// AddBool records a bool field.
func (op *Operation) AddBool(key string, value bool) {
	if e := op.open(); e != nil && key != "" {
		e.pushScalar(key, KindBool, b2i(value))
	}
}

// AddDuration records a time.Duration field.
func (op *Operation) AddDuration(key string, value time.Duration) {
	if e := op.open(); e != nil && key != "" {
		e.pushScalar(key, KindDuration, int64(value))
	}
}

// AddTime records a time.Time field, round-tripping the whole value
// (zone and monotonic reading included) exactly as Add does.
func (op *Operation) AddTime(key string, value time.Time) {
	if e := op.open(); e != nil && key != "" {
		num, pay := packTime(value)
		f := e.reserveSlot()
		f.key, f.kind, f.num, f.pay = key, KindTime, num, pay
	}
}

// AddError records err as the canonical error field. A nil error is a
// no-op, matching unolog.Error.
func (op *Operation) AddError(key string, err error) {
	if e := op.open(); e != nil && key != "" && err != nil {
		f := e.reserveSlot()
		f.key, f.kind, f.num, f.pay = key, KindErr, 0, packAny(err)
	}
}
