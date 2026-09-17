package unolog

import (
	"slices"
	"sync/atomic"
	"time"

	"github.com/happytoolin/unolog/internal/json"
	"github.com/happytoolin/unolog/wire"
)

// Record is the read-only view of one finalized event, handed to sinks
// inside Write (valid only for the duration of the call — copy anything
// you retain). It follows slog.Record conventions: typed, insertion
// ordered, zero copy.
type Record struct {
	level       Level
	msg         string
	fields      []Field // the sealed WAL slice; nobody mutates it after End
	completedAt time.Time

	// computed lazily on first Encoded
	encoded atomic.Pointer[encodedLine]
}

// encodedInline covers the canonical line for the common ≤12-field
// event in the record's own node, so the encoder usually allocates one
// object instead of two. Wider or unusually long lines still grow into
// their own buffer. The node is never shared between records, so
// retained bytes keep satisfying the recycling guarantee.
const encodedInline = 448

type encodedLine struct {
	b   []byte
	arr [encodedInline]byte
}

// Level returns the final severity.
func (r *Record) Level() Level { return r.level }

// Message returns the final message.
func (r *Record) Message() string { return r.msg }

// Time returns the event's completion timestamp — the instant the
// canonical line stamps under the "time" member. Mirrors
// slog.Record.Time: the record's own clock read, not the sink's.
func (r *Record) Time() time.Time { return r.completedAt }

// Fields returns the event's fields in insertion order (last-write-wins
// duplicates included; see Lookup for the resolved view). No map is
// built and no clone occurs.
func (r *Record) Fields() []Field { return r.fields }

// Lookup returns the last value written under key.
func (r *Record) Lookup(key string) (any, bool) {
	return lookupField(r.fields, key)
}

// lookupField is the single last-write-wins backward scan shared by
// Record.Lookup, CapturedEvent.Lookup, and the event's live lookup.
func lookupField(fields []Field, key string) (any, bool) {
	for _, f := range slices.Backward(fields) {
		if f.key == key {
			return valueOf(f), true
		}
	}
	return nil, false
}

// Encoded returns the canonical JSON line (one line, trailing newline)
// for this record, computing it at most once: concurrent callers race
// benignly on the publish and share the winner. The bytes belong to
// the record — copy them if you retain them past Write.
func (r *Record) Encoded() []byte {
	if e := r.encoded.Load(); e != nil {
		return e.b
	}
	e := &encodedLine{}
	e.b = r.encodeInto(e.arr[:0])
	// The winner's publish wins the CAS; losers return the same buffer.
	if !r.encoded.CompareAndSwap(nil, e) {
		return r.encoded.Load().b
	}
	return e.b
}

// encode builds the canonical line: level prefix, fields (deduped
// last-write-wins), RFC3339 completion time, message — the exact field
// order and shapes the v0.6 zerolog-parity JSON sink established.
func (r *Record) encodeInto(b []byte) []byte {
	// 96 covers the envelope (level + time + message ≈ 82 bytes at
	// minimum); 24/field covers the common short-key/value shapes
	// without a grow-and-copy (measured: the old 64-byte base forced
	// one growth on every ≤12-field event). A line that outgrows the
	// record's inline array falls back to a freshly sized buffer.
	if need := 96 + len(r.fields)*24; cap(b) < need {
		b = make([]byte, 0, need)
	} else {
		b = b[:0]
	}
	b = append(b, jsonLevelPrefix(r.level)...)
	fields := r.fields
	if needsFieldAliasing(fields) {
		fields = aliasFields(fields)
	}
	b = appendDedupedFields(b, fields)
	b = jsonEnc.AppendKey(b, "time")
	b = json.AppendTimeRFC3339(b, r.completedAt)
	b = jsonEnc.AppendKey(b, "message")
	b = jsonEnc.AppendString(b, r.msg)
	b = append(b, '}', '\n')
	return b
}

// Canonical-key collision policy (the logrus precedent, §5 of the T5
// plan): a user field named "message", "time", or "level" would collide
// with the canonical envelope members, producing duplicate JSON keys
// that parsers disagree on. The colliding user keys are RENAMED to
// "fields.message", "fields.time", and "fields.level" on the wire only:
// Record.Fields() and Lookup keep returning the user's original key.
// The dedupe runs over the aliased keys, so a user "message" and a user
// "fields.message" resolve as one last-write-wins member.

var aliasKey = map[string]string{
	"message": "fields.message",
	"time":    "fields.time",
	"level":   "fields.level",
}

func aliasedFieldKey(key string) string {
	if aliased, ok := aliasKey[key]; ok {
		return aliased
	}
	return key
}

// needsFieldAliasing reports whether any field key collides with the
// envelope.
func needsFieldAliasing(fields []Field) bool {
	for i := range fields {
		switch fields[i].key {
		case "message", "time", "level":
			return true
		}
	}
	return false
}

// aliasFields returns a copy of the field list with colliding keys
// renamed. Only called when a collision exists, so the common path
// stays allocation-free.
func aliasFields(fields []Field) []Field {
	out := make([]Field, len(fields))
	for i, f := range fields {
		out[i] = f
		out[i].key = aliasedFieldKey(f.key)
	}
	return out
}

// dedupeScanLimit sets the crossover between the allocation-free
// last-occurrence scan and the seen-set path for wide events. It is the
// same constant the sink bridges use (wire.NarrowLimit), so the
// canonical line and every bridge resolve duplicates identically
// (pinned by the golden parity tests).
const dedupeScanLimit = wire.NarrowLimit

// keyDigest is a cheap, collision-tolerant fingerprint of a field key:
// length plus three bytes. It only short-circuits the dedupe scan — a
// digest hit still goes through the full string comparison — so any
// collision is a (small) slowdown, never a behavior change.
func keyDigest(key string) uint64 {
	n := len(key)
	if n == 0 {
		return 0
	}
	return uint64(n)<<32 | uint64(key[0])<<16 | uint64(key[n-1])<<8 | uint64(key[n/2])
}

// appendDedupedFields emits each key once — its last value, at its last
// position (amendment 3). Narrow events use the allocation-free scan;
// wide events track seen keys in a stack array and fall back to a map
// past its capacity, so allocation is tied to genuinely wide events.
func appendDedupedFields(dst []byte, fields []Field) []byte {
	if len(fields) <= dedupeScanLimit {
		// The last-occurrence scan compares a cheap key digest first and
		// only falls back to the full string compare on a digest hit, so
		// digest collisions stay correct. Unique keys — the common case —
		// never touch memequal.
		var digests [dedupeScanLimit]uint64
		for i := range fields {
			digests[i] = keyDigest(fields[i].key)
		}
		for i := range fields {
			f := fields[i]
			last := true
			for j := i + 1; j < len(fields); j++ {
				if digests[j] == digests[i] && fields[j].key == f.key {
					last = false
					break
				}
			}
			if last {
				dst = appendFieldJSON(dst, f)
			}
		}
		return dst
	}

	var seenArr [32]string
	n := 0
	var seen map[string]struct{}
	kept := make([]int, 0, len(fields)) // last-occurrence indices, found backward
	for i, field := range slices.Backward(fields) {
		key := field.key
		dup := false
		for j := range n {
			if seenArr[j] == key {
				dup = true
				break
			}
		}
		if !dup && seen != nil {
			_, dup = seen[key]
		}
		if dup {
			continue
		}
		switch {
		case n < len(seenArr):
			seenArr[n] = key
			n++
		case seen == nil:
			seen = make(map[string]struct{}, len(fields))
			for j := range n {
				seen[seenArr[j]] = struct{}{}
			}
			seen[key] = struct{}{}
		default:
			seen[key] = struct{}{}
		}
		kept = append(kept, i)
	}
	for _, k := range slices.Backward(kept) {
		dst = appendFieldJSON(dst, fields[k])
	}
	return dst
}

// appendFieldJSON encodes one typed field with the vendored encoder,
// using the same type mapping the v0 zerolog adapter established (error
// → message string, duration → float ms, time → RFC3339).
func appendFieldJSON(dst []byte, f Field) []byte {
	dst = jsonEnc.AppendKey(dst, f.key)
	switch f.kind {
	case KindString:
		return jsonEnc.AppendString(dst, f.str)
	case KindInt:
		return jsonEnc.AppendInt64(dst, f.num)
	case KindUint:
		return jsonEnc.AppendUint64(dst, uint64(f.num))
	case KindFloat:
		return jsonEnc.AppendFloat64(dst, f.f, -1)
	case KindFloat32:
		return jsonEnc.AppendFloat32(dst, float32(f.f), -1)
	case KindBool:
		return jsonEnc.AppendBool(dst, f.b)
	case KindTime:
		return jsonEnc.AppendTime(dst, f.t, time.RFC3339)
	case KindDuration:
		return jsonEnc.AppendDuration(dst, time.Duration(f.num), time.Millisecond, false, -1)
	case KindErr:
		// safeErrorMessage fences typed-nils (direct or %w-wrapped)
		// and panicking Error() implementations.
		err, _ := f.val.(error)
		return jsonEnc.AppendString(dst, safeErrorMessage(err))
	default:
		return jsonEnc.AppendInterface(dst, f.val)
	}
}

var jsonEnc = json.Encoder{}

// Precomputed level prefixes: no key encoding, one append.
var (
	jsonLevelPrefixDebug = []byte(`{"level":"debug"`)
	jsonLevelPrefixInfo  = []byte(`{"level":"info"`)
	jsonLevelPrefixWarn  = []byte(`{"level":"warn"`)
	jsonLevelPrefixError = []byte(`{"level":"error"`)
)

func jsonLevelPrefix(level Level) []byte {
	switch level {
	case LevelDebug:
		return jsonLevelPrefixDebug
	case LevelWarn:
		return jsonLevelPrefixWarn
	case LevelError:
		return jsonLevelPrefixError
	default:
		return jsonLevelPrefixInfo
	}
}
