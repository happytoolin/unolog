package unolog

import (
	"unicode/utf8"
)

const (
	// DefaultMaxFields is the per-event field limit when Config.MaxFields
	// is zero. It is far above a typical wide event and bounds the
	// pathological case: Add called in a loop.
	DefaultMaxFields = 256

	// DefaultMaxValueBytes is the per-value byte limit when
	// Config.MaxValueBytes is zero. It keeps a request body, SQL text,
	// or stack trace from making a line unbounded. It does not keep a
	// line under a log pipeline's line limit: set Config.MaxValueBytes
	// (and Config.MaxFields) to match that limit.
	DefaultMaxValueBytes = 32 << 10
)

// limits is the compiled per-event bound. Both values are positive;
// math.MaxInt means unlimited, so the hot path needs no extra branch.
type limits struct {
	maxFields     int
	maxValueBytes int
}

var defaultLimits = limits{maxFields: DefaultMaxFields, maxValueBytes: DefaultMaxValueBytes}

// truncateUTF8 cuts s to at most n bytes without splitting a valid
// UTF-8 sequence. The result shares the backing array of s: no copy.
func truncateUTF8(s string, n int) string {
	if len(s) <= n {
		return s
	}
	cut := n
	// A UTF-8 sequence has at most three continuation bytes. Invalid
	// input can have longer runs; stop there, the encoder replaces
	// invalid bytes anyway.
	for range utf8.UTFMax - 1 {
		if cut == 0 || utf8.RuneStart(s[cut]) {
			break
		}
		cut--
	}
	return s[:cut]
}

// limitMapStrings returns m with every string member cut to n bytes and
// the number of members it cut. It never mutates m: when a cut is
// needed, it returns a shallow copy.
func limitMapStrings(m map[string]any, n int) (map[string]any, int) {
	cuts := 0
	for _, v := range m {
		if s, ok := v.(string); ok && len(s) > n {
			cuts++
		}
	}
	if cuts == 0 {
		return m, 0
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		if s, ok := v.(string); ok && len(s) > n {
			v = truncateUTF8(s, n)
		}
		out[k] = v
	}
	return out, cuts
}

// isCanonicalKey reports whether key is one of the keys unolog itself
// writes. The field limit never drops these: each holds at most one
// slot past the limit, so the event stays bounded.
func isCanonicalKey(key string) bool {
	switch key {
	case KeyOpDomain, KeyOpName, KeyOpID, KeyOpSource, KeyOpAttempt,
		KeyOpMaxAttempts, KeyOpCode, KeyOpOutcome,
		KeyHTTPMethod, KeyHTTPPath, KeyHTTPRoute, KeyHTTPStatus,
		KeyDurationMS, KeyError, KeyPanic, KeyJobScheduledAt:
		return true
	default:
		return false
	}
}
