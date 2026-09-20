package unolog

import (
	"math"
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
	level  Level
	msg    string
	fields []Field // the sealed WAL slice; nobody mutates it after End
	// completedAt is the completion instant as unix nanoseconds. A
	// time.Time in this slot costs 24 bytes (wall + ext + *Location), and
	// Operation embeds Record by value, so those 16 bytes are paid on
	// every operation: measured at ~0.26 ns per byte of garbage per
	// operation under the cpu=8 lifecycle gate (16 B = ~5.6%). The
	// instant is all the record needs; Time() re-materializes it in the
	// local zone, which is the zone the default clock already stamps and
	// the zone the cached RFC3339 renderer documents.
	completedAt int64

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
//
// The returned Time is always in time.Local, the zone the default
// clock and the shared RFC3339 cache render in; a configured Clock
// that stamps another zone has that zone folded away here.
func (r *Record) Time() time.Time { return time.Unix(0, r.completedAt) }

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
	b = appendDedupedFields(b, r.fields)
	// The envelope tail is written from precomputed prefixes: the comma
	// is unconditional (the level prefix never ends in '{'), and both
	// keys are fixed, so AppendKey's scan and comma probe buy nothing.
	b = append(b, jsonTailTime...)
	b = json.AppendTimeRFC3339(b, r.Time())
	b = append(b, jsonTailMessage...)
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

func aliasedFieldKey(key string) string {
	// A switch, not a map: WireKey is called twice per field on every
	// bridge write, and a three-entry map lookup (hash + probe) costs
	// more than three length-disjoint string compares.
	switch key {
	case "message":
		return "fields.message"
	case "time":
		return "fields.time"
	case "level":
		return "fields.level"
	default:
		return key
	}
}

// needsFieldAliasing reports whether any field key collides with the
// envelope. The narrow dedupe path folds the same test into its walk
// (narrowKeep's alias result); this stays for the wide path.
func needsFieldAliasing(fields []Field) bool {
	for i := range fields {
		if isEnvelopeKey(fields[i].key) {
			return true
		}
	}
	return false
}

// isEnvelopeKey reports whether key collides with a canonical envelope
// member and must therefore be renamed on the wire.
func isEnvelopeKey(key string) bool {
	switch key {
	case "message", "time", "level":
		return true
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

// narrowDedupeLimit is the crossover between the open-addressed narrow
// scan and the seen-set path for wide events. It is the same constant
// the sink bridges use (wire.NarrowLimit), so the canonical line and
// every bridge resolve duplicates identically (pinned by the golden
// parity tests).
const narrowDedupeLimit = wire.NarrowLimit

// dedupeTableSize is the narrow path's open-addressing table: a power of
// two above narrowDedupeLimit, so a full narrow list stays at a 0.75
// load factor and probes stay short.
const (
	dedupeTableSize = 32
	dedupeTableMask = dedupeTableSize - 1
	dedupeSlotShift = 64 - 5 // dedupeTableSize is 1<<5

	// dedupeIdxMask splits a slot word: the low 32 bits hold the 1-based
	// field index (zero = empty slot), the high 32 hold the fingerprint.
	// One array instead of a fingerprint table plus an index table: half
	// the per-call zeroing and one store per field instead of two.
	dedupeIdxMask = 1<<32 - 1
)

// hashFieldKey is the narrow dedupe's slot/equality fingerprint: length
// plus three sampled bytes, multiplied by an odd golden-ratio constant
// so the high bits (the slot) mix every input bit. A fingerprint hit is
// still confirmed by a full string compare, so any collision is a
// (small) slowdown, never a behavior change.
func hashFieldKey(key string) uint64 {
	n := len(key)
	if n == 0 {
		return 0
	}
	d := uint64(n)<<32 | uint64(key[0])<<16 | uint64(key[n-1])<<8 | uint64(key[n/2])
	return d * 0x9E3779B97F4A7C15
}

// appendLastIndices appends the ascending indices of each key's last
// occurrence in fields — last-write-wins duplicate resolution. wireKey
// selects the aliased (canonical wire) key view so the zerolog bridge
// resolves envelope collisions exactly like Encoded() does.
//
// Narrow lists walk backward through an open-addressed fingerprint
// table, which is ~4x cheaper than the pairwise scan it replaced — that
// scan was the single largest bridge cost (9-29% of Write). Wide lists
// fall back to the shared generic helper, so their allocation is tied
// to genuinely wide events and the resolution stays identical to every
// other path.
func appendLastIndices(dst []int, fields []Field, wireKey bool) []int {
	n := len(fields)
	if n > narrowDedupeLimit {
		if wireKey {
			return wire.AppendLastIndices(dst, fields, Field.WireKey)
		}
		return wire.AppendLastIndices(dst, fields, Field.Key)
	}
	keep, _ := narrowKeep(fields, wireKey)
	for i := range n {
		if keep&(1<<uint(i)) != 0 {
			dst = append(dst, i)
		}
	}
	return dst
}

// narrowKeep is the single last-write-wins primitive for narrow events:
// it walks the fields backward through an open-addressed fingerprint
// table and returns the bit set of surviving indices (bit i set => fields[i]
// is the last occurrence of its key). A bitset instead of an index slice
// means the encoder emits in one forward pass with no reverse and no
// scratch append, and the bridges turn the same mask into indices.
//
// wireKey selects the aliased (canonical wire) key view so the zerolog
// bridge resolves envelope collisions exactly like Encoded() does; the
// unaliased walk also reports whether any key collided with the
// envelope, so the encoder gets the alias test for free instead of
// scanning every field a second time.
//
// Width is capped at 64 bits: callers only reach it with n <=
// narrowDedupeLimit (24).
func narrowKeep(fields []Field, wireKey bool) (keep uint64, alias bool) {
	var table [dedupeTableSize]uint64
	for i := len(fields) - 1; i >= 0; i-- {
		key := fields[i].key
		if wireKey {
			key = aliasedFieldKey(key)
		} else if isEnvelopeKey(key) {
			alias = true
		}
		h := hashFieldKey(key)
		slot := h >> dedupeSlotShift
		dup := false
		for e := table[slot]; e != 0; e = table[slot] {
			j := int(e&dedupeIdxMask) - 1
			if uint32(e>>32) == uint32(h) {
				other := fields[j].key
				if wireKey {
					other = aliasedFieldKey(other)
				}
				if other == key {
					dup = true
					break
				}
			}
			slot = (slot + 1) & dedupeTableMask
		}
		if dup {
			continue
		}
		table[slot] = uint64(uint32(h))<<32 | uint64(i+1)
		keep |= 1 << uint(i)
	}
	return keep, alias
}

// AppendLastIndices appends to dst the ascending indices of each field
// key's last occurrence — the last-write-wins resolution the bridges
// apply before mapping fields onto their host logger. It is the
// concrete form of the generic wire helper: there the func-value key
// accessor alone cost ~40 ns per event.
func (r *Record) AppendLastIndices(dst []int) []int {
	return appendLastIndices(dst, r.fields, false)
}

// AppendLastWireIndices is AppendLastIndices over the aliased key view:
// a key colliding with the JSON envelope resolves as its "fields.*"
// form, exactly matching Encoded(). The zerolog bridge uses this one.
func (r *Record) AppendLastWireIndices(dst []int) []int {
	return appendLastIndices(dst, r.fields, true)
}

// appendDedupedFields emits each key once — its last value, at its last
// position (amendment 3). Narrow events use the open-addressed scan;
// wide events track seen keys in a stack array and fall back to a map
// past its capacity, so allocation is tied to genuinely wide events.
func appendDedupedFields(dst []byte, fields []Field) []byte {
	if len(fields) <= narrowDedupeLimit {
		keep, alias := narrowKeep(fields, false)
		if alias {
			// Rare: a user key collides with the envelope. Aliasing can
			// merge two keys, so the resolution is redone on the wire view.
			fields = aliasFields(fields)
			keep, _ = narrowKeep(fields, false)
		}
		for i := range fields {
			if keep&(1<<uint(i)) == 0 {
				continue
			}
			f := fields[i]
			// The two dominant kinds are spelled out here so the emit
			// loop skips appendFieldJSON's dispatch call for them.
			switch f.kind {
			case KindString:
				dst = jsonEnc.AppendKeyString(dst, f.key, f.pay.stringValue())
			case KindInt:
				dst = jsonEnc.AppendKeyInt64(dst, f.key, f.num)
			default:
				dst = appendFieldJSON(dst, f)
			}
		}
		return dst
	}
	if needsFieldAliasing(fields) {
		fields = aliasFields(fields)
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
	// Fused key+value writers for the two shapes that dominate real
	// events: AppendKey and AppendString are both past the inliner
	// budget, so spelling them out costs two calls per string field.
	switch f.kind {
	case KindString:
		return jsonEnc.AppendKeyString(dst, f.key, f.pay.stringValue())
	case KindInt:
		return jsonEnc.AppendKeyInt64(dst, f.key, f.num)
	}
	dst = jsonEnc.AppendKey(dst, f.key)
	switch f.kind {
	case KindUint:
		return jsonEnc.AppendUint64(dst, uint64(f.num))
	case KindFloat:
		return jsonEnc.AppendFloat64(dst, math.Float64frombits(uint64(f.num)), -1)
	case KindFloat32:
		return jsonEnc.AppendFloat32(dst, float32(math.Float64frombits(uint64(f.num))), -1)
	case KindBool:
		return jsonEnc.AppendBool(dst, f.num != 0)
	case KindTime:
		return jsonEnc.AppendTime(dst, f.timeValue(), time.RFC3339)
	case KindDuration:
		return jsonEnc.AppendDuration(dst, time.Duration(f.num), time.Millisecond, false, -1)
	case KindErr:
		// safeErrorMessage fences typed-nils (direct or %w-wrapped)
		// and panicking Error() implementations.
		err, _ := f.pay.anyValue().(error)
		return jsonEnc.AppendString(dst, safeErrorMessage(err))
	default:
		return jsonEnc.AppendInterface(dst, f.pay.anyValue())
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

// Precomputed envelope-tail prefixes: comma included, keys fixed.
var (
	jsonTailTime    = []byte(`,"time":`)
	jsonTailMessage = []byte(`,"message":`)
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
