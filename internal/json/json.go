// Package json is a minimal append-only JSON encoder, vendored from
// zerolog v1.34.0's internal/json package (MIT; see LICENSE and README.md
// in this directory). The hot paths the unolog core uses (string/bytes
// escaping, scalars, time, interface fallback) were kept verbatim in
// shape — including the unused-width scalar constructors — for easy
// diffing against the upstream original.
//
// The upgrade over the vendored original is a hybrid SWAR escape fast path
// in AppendString/AppendBytes: strings of 16 bytes or more are scanned 8
// bytes at a time; if every byte is clean printable ASCII the string is
// appended without any per-byte table lookups. Shorter or suspicious
// strings fall back to the original byte-by-byte table path, so the output
// is byte-identical to the zerolog reference for every input (verified by
// property and fuzz tests in this package).
//
// All operations append to dst and return the extended slice; nothing here
// allocates on its own beyond what strconv/json.Marshal require.
package json

import "strconv"

// Encoder is a stateless value type; methods are safe for concurrent use.
type Encoder struct{}

// AppendKey appends a new key to the output JSON. It requires a non-empty
// dst (typically starting from AppendBeginMarker), matching the vendored
// zerolog contract.
func (e Encoder) AppendKey(dst []byte, key string) []byte {
	if dst[len(dst)-1] != '{' {
		dst = append(dst, ',')
	}
	return append(e.AppendString(dst, key), ':')
}

// AppendKeyString appends one encoded "key":<string> field member. It
// exists because AppendKey and AppendString are both past the inliner
// budget, so the natural two-call spelling pays two calls and three
// growth checks per field. Fusing them costs one call and one scan of
// each string on the clean path — which is what the record encoder
// emits for the great majority of fields.
func (e Encoder) AppendKeyString(dst []byte, key, value string) []byte {
	if dst[len(dst)-1] != '{' {
		dst = append(dst, ',')
	}
	if firstEscape(key) == len(key) && firstEscape(value) == len(value) {
		dst = append(dst, '"')
		dst = append(dst, key...)
		dst = append(dst, '"', ':', '"')
		dst = append(dst, value...)
		return append(dst, '"')
	}
	// The comma is already written; AppendKey would add a second one.
	return e.AppendString(append(e.AppendString(dst, key), ':'), value)
}

// AppendKeyInt64 is AppendKeyString for the KindInt slot: one call and
// one key scan for the common "count":7 shape.
func (e Encoder) AppendKeyInt64(dst []byte, key string, val int64) []byte {
	if dst[len(dst)-1] != '{' {
		dst = append(dst, ',')
	}
	if firstEscape(key) != len(key) {
		return appendInt(append(e.AppendString(dst, key), ':'), val)
	}
	dst = append(dst, '"')
	dst = append(dst, key...)
	dst = append(dst, '"', ':')
	return appendInt(dst, val)
}

// digits2 is the two-digit rendering of 00..99, indexed by 2*v. It
// turns a small integer into two byte loads instead of strconv's
// division and call.
const digits2 = "00010203040506070809101112131415161718192021222324252627282930313233343536373839404142434445464748495051525354555657585960616263646566676869707172737475767778798081828384858687888990919293949596979899"

// appendInt renders val as decimal. Status codes, counts, and indices
// dominate real events and almost always land in 0..99, so those sizes
// skip strconv entirely: one or two table bytes instead of a call.
func appendInt(dst []byte, val int64) []byte {
	if u := uint64(val); u < 10 {
		return append(dst, '0'+byte(u))
	} else if u < 100 {
		i := u * 2
		return append(dst, digits2[i], digits2[i+1])
	} else if u < 1000 {
		i := u / 10 * 2
		return append(dst, digits2[i], digits2[i+1], '0'+byte(u%10))
	}
	return strconv.AppendInt(dst, val, 10)
}
