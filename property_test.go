package unolog

// Model-driven verification: the lifecycle fuzz model (the executable
// mirror of End/commit semantics) drives both the fuzz target and the
// seeded property tests.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// rtFieldValue generates one typed value for a kind. The value space
// per kind includes hostile edges (int64 extremes, float subnormals,
// invalid UTF-8, exotic zones).
func rtFieldValue(rng *rand.Rand, kind FieldKind) any {
	edge := func(p float64) bool { return rng.Float64() < p }
	switch kind {
	case KindString:
		if edge(0.15) {
			// invalid UTF-8: broken sequences, lone continuation bytes
			n := 1 + rng.IntN(12)
			b := make([]byte, n)
			for i := range b {
				b[i] = []byte{0x80, 0xff, 0xc0, 0xfe, 0x22, 0x5c}[rng.IntN(6)]
			}
			return string(b)
		}
		n := rng.IntN(24)
		b := make([]byte, n)
		for i := range b {
			switch rng.IntN(10) {
			case 0:
				b[i] = byte(rng.IntN(0x20)) // control bytes
			case 1:
				b[i] = '"'
			case 2:
				b[i] = '\\'
			case 3:
				b[i] = 0x7f
			default:
				b[i] = byte(0x20 + rng.IntN(0x5f))
			}
		}
		if edge(0.2) {
			return string(b) + " héllo ☃ 🍜"
		}
		return string(b)
	case KindInt:
		switch rng.IntN(6) {
		case 0:
			return int64(math.MinInt64)
		case 1:
			return int64(math.MaxInt64)
		case 2:
			return int64(1) << 62
		case 3:
			return -(int64(1) << 62)
		case 4:
			return int64(int8(rng.Uint64()))
		default:
			return int64(rng.Uint64())
		}
	case KindUint:
		if edge(0.3) {
			return uint64(math.MaxUint64)
		}
		return uint64(rng.Uint64())
	case KindFloat:
		switch rng.IntN(8) {
		case 0:
			return 0.0
		case 1:
			return math.Copysign(0, -1) // -0 survives bitwise
		case 2:
			return math.SmallestNonzeroFloat64
		case 3:
			return math.MaxFloat64
		case 4:
			return float64(1 << 53)
		case 5:
			return math.NaN() // quoted-string policy
		case 6:
			return math.Inf(int(rng.Uint64()%2)*2 - 1)
		default:
			return rng.Float64()*2e21 - 1e21
		}
	case KindFloat32:
		switch rng.IntN(5) {
		case 0:
			return float32(0.1) // the not-widened gate
		case 1:
			return float32(math.SmallestNonzeroFloat32)
		case 2:
			return float32(math.MaxFloat32)
		case 3:
			return float32(1) / 3
		default:
			return float32(rng.Float64())
		}
	case KindBool:
		return rng.Uint64()&1 == 0
	case KindTime:
		zones := []*time.Location{
			time.UTC,
			time.FixedZone("+14", 14*3600),
			time.FixedZone("-12", -12*3600),
			time.FixedZone("+05:30", 5*3600+1800),
		}
		year := 1 + rng.IntN(9999)
		return time.Date(year, time.Month(1+rng.IntN(12)), 1+rng.IntN(28),
			0+rng.IntN(24), 0+rng.IntN(60), 0+rng.IntN(60), rng.IntN(1e9),
			zones[rng.IntN(len(zones))])
	case KindDuration:
		switch rng.IntN(4) {
		case 0:
			return -time.Duration(rng.Uint64()) // negative
		case 1:
			return time.Duration(rng.IntN(1e6)) * time.Nanosecond
		default:
			return time.Duration(int64(rng.Uint64())) // full range
		}
	case KindErr:
		switch rng.IntN(3) {
		case 0:
			return fmt.Errorf("boom-%d", rng.IntN(1e6))
		case 1:
			return errString("plain")
		default:
			return fmt.Errorf("wrapped: %w", errors.New("inner"))
		}
	default:
		return nil
	}
}

// rtWireCheck checks one encoded member against the modeled value via
// checkFieldWire, with the special float policy (NaN/±Inf are quoted
// strings) and invalid-UTF-8 handled before the generic checker.
func rtWireCheck(t *testing.T, kind FieldKind, val any, raw []byte) {
	t.Helper()
	f := fieldOf("k", val)
	switch kind {
	case KindFloat:
		fv := val.(float64)
		if math.IsNaN(fv) || math.IsInf(fv, 0) {
			var s string
			if err := json.Unmarshal(raw, &s); err != nil {
				t.Fatalf("NaN/Inf float not quoted: %s (%v)", raw, err)
			}
			want := map[bool]string{true: "NaN", false: ""}[math.IsNaN(fv)]
			if want == "" {
				if math.IsInf(fv, 1) {
					want = "+Inf"
				} else {
					want = "-Inf"
				}
			}
			if s != want {
				t.Fatalf("special float wire %q, want %q", s, want)
			}
			return
		}
	case KindString:
		if s, ok := val.(string); ok && !utf8.ValidString(s) {
			// invalid UTF-8: parseability only (mapping pinned by the
			// internal/json suites)
			var decoded string
			if err := json.Unmarshal(raw, &decoded); err != nil {
				t.Fatalf("invalid-UTF-8 string did not parse: %v (%s)", err, raw)
			}
			return
		}
	}
	if err := checkFieldWire(f, raw); err != nil {
		t.Fatalf("%v", err)
	}
}

// TestFieldRoundTripProperty drives every kind through encode → parse
// → compare. Key names are unique per iteration so the fold never
// hides a kind.
func TestFieldRoundTripProperty(t *testing.T) {
	kinds := []struct {
		kind FieldKind
		name string
	}{
		{KindString, "string"},
		{KindInt, "int"},
		{KindUint, "uint"},
		{KindFloat, "float"},
		{KindFloat32, "float32"},
		{KindBool, "bool"},
		{KindTime, "time"},
		{KindDuration, "duration"},
		{KindErr, "err"},
		{KindAny, "any"},
	}
	for _, k := range kinds {
		t.Run(k.name, func(t *testing.T) {
			rng := rand.New(rand.NewPCG(uint64(k.kind)<<32|0x51EE5eed, 0x600D5eed))
			const n = 3000
			for i := range n {
				val := rtFieldValue(rng, k.kind)
				r := recOf(LevelInfo, "m", fieldOf("k", val))
				line := r.Encoded()
				_, members := decodeLineStrict(t, line)
				if len(members) != 4 { // level, k, time, message
					t.Fatalf("iter %d: wire members %v, want 4 (level k time message)", i, memberKeys(members))
				}
				if members[1].key != "k" {
					t.Fatalf("iter %d: member 1 = %q", i, members[1].key)
				}
				rtWireCheck(t, k.kind, val, members[1].val)
			}
		})
	}
}

// TestRecordEncodeDeterminismProperty pins encoding as a pure function
// of (level, msg, fields, completedAt): two separately built records
// with equal content encode to equal bytes (action plan P6).
func TestRecordEncodeDeterminismProperty(t *testing.T) {
	rng := rand.New(rand.NewPCG(0xD371E5eed, 0xD371E5eed))
	for i := range 2000 {
		width := 1 + rng.IntN(40)
		fields := make([]Field, width)
		for j := range fields {
			kind := FieldKind(1 + rng.IntN(11))
			fields[j] = fieldOf(fmt.Sprintf("k%d", j%7), rtFieldValue(rng, kind))
		}
		completedAt := time.Date(2026, 9, 1, 10, 0, 0, 0, time.Local)
		build := func() []byte {
			r := &Record{level: LevelInfo, msg: "m", fields: fields, completedAt: completedAt}
			return r.Encoded()
		}
		a, b := build(), build()
		if !bytes.Equal(a, b) {
			t.Fatalf("iter %d: equal records encoded differently", i)
		}
	}
}

// This file implements the record-level round-trip properties from the
// DST/fuzzing research (§7.1 field round-trip per kind, §7.2 encode
// determinism, §6.2 dedupe fuzz with an independent reference fold):
// Encoded() must produce a JSON line that parses back to exactly the
// record's resolved (last-write-wins) view, for every field kind, at
// every width, under adversarial duplicate structure.

// decodeLineStrict parses one canonical line and returns both the
// last-wins map (Go's json semantics) and the ordered raw members.
type rawMember struct {
	key string
	val json.RawMessage
}

func decodeLineStrict(t *testing.T, line []byte) (map[string]json.RawMessage, []rawMember) {
	t.Helper()
	dec := json.NewDecoder(bytes.NewReader(line))
	dec.UseNumber()
	var top map[string]json.RawMessage
	if err := dec.Decode(&top); err != nil {
		t.Fatalf("Encoded() is not valid JSON: %v (%q)", err, line)
	}
	// ordered re-walk for order assertions
	var ordered []rawMember
	dec2 := json.NewDecoder(bytes.NewReader(line))
	dec2.Token() // {
	for {
		tok, err := dec2.Token()
		if err != nil {
			break
		}
		if key, ok := tok.(string); ok {
			var raw json.RawMessage
			if err := dec2.Decode(&raw); err != nil {
				t.Fatalf("member %q: %v", key, err)
			}
			ordered = append(ordered, rawMember{key, raw})
		}
		if tok == json.Delim('}') {
			break
		}
	}
	return top, ordered
}

// TestEncodedRoundTripAllKinds walks every field kind through
// Encoded() → parse → compare, on both friendly and adversarial values.
func TestEncodedRoundTripAllKinds(t *testing.T) {
	utc := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	ist := time.Date(2026, 9, 1, 15, 30, 0, 0, time.FixedZone("IST", 5*3600+30*60))

	cases := []struct {
		key     string
		field   Field
		check   func(t *testing.T, raw json.RawMessage)
		skipOrd bool // kinds whose wire form is embedded/structured
	}{
		{"s", fieldOf("s", "v"), rawStringIs("v"), false},
		{"s_quote", fieldOf("s_quote", `she said "hi"`), rawStringIs(`she said "hi"`), false},
		{"s_nl", fieldOf("s_nl", "a\nb\tc"), rawStringIs("a\nb\tc"), false},
		{"s_uni", fieldOf("s_uni", "héllo ☃ 🍜"), rawStringIs("héllo ☃ 🍜"), false},
		{"s_ctl", fieldOf("s_ctl", "x\x00\x7fy"), rawStringIs("x\x00\x7fy"), false},
		{"i", fieldOf("i", -42), rawNumberIs("-42"), false},
		{"i_min", fieldOf("i_min", int64(math.MinInt64)), rawNumberIs(strconv.FormatInt(math.MinInt64, 10)), false},
		{"i_max", fieldOf("i_max", int64(math.MaxInt64)), rawNumberIs(strconv.FormatInt(math.MaxInt64, 10)), false},
		{"u_max", fieldOf("u_max", uint64(math.MaxUint64)), rawNumberIs(strconv.FormatUint(math.MaxUint64, 10)), false},
		{"f", fieldOf("f", 2.5), rawNumberIs("2.5"), false},
		{"f_neg0", fieldOf("f_neg0", math.Copysign(0, -1)), rawNumberIs("-0"), false},
		{"f_big", fieldOf("f_big", 1e21), rawNumberIs("1e+21"), false},
		{"f32", fieldOf("f32", float32(0.1)), rawNumberIs("0.1"), false},
		{"f_nan", fieldOf("f_nan", math.NaN()), rawStringIs("NaN"), false},
		{"f_pinf", fieldOf("f_pinf", math.Inf(1)), rawStringIs("+Inf"), false},
		{"b", fieldOf("b", true), rawIs("true"), false},
		{"t_utc", fieldOf("t_utc", utc), rawStringIs(utc.Format(time.RFC3339)), false},
		{"t_zone", fieldOf("t_zone", ist), rawStringIs(ist.Format(time.RFC3339)), false},
		{"d", fieldOf("d", 1500*time.Millisecond), rawNumberIs("1500"), false},
		{"d_frac", fieldOf("d_frac", 123456789*time.Nanosecond), rawNumberIs("123.456789"), false},
		{"d_neg", fieldOf("d_neg", -time.Second), rawNumberIs("-1000"), false},
		{"e", fieldOf("e", errString("boom")), rawStringIs("boom"), false},
		{"any_obj", fieldAny("any_obj", map[string]any{"n": 1}), rawIs(`{"n":1}`), true},
		{"any_nil", fieldAny("any_nil", nil), rawIs("null"), false},
	}

	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			r := recOf(LevelInfo, "m", c.field)
			got, ordered := decodeLineStrict(t, r.Encoded())
			raw, ok := got[c.key]
			if !ok {
				t.Fatalf("key %q missing from %v", c.key, got)
			}
			c.check(t, raw)
			if !c.skipOrd {
				found := false
				for _, m := range ordered {
					if m.key == c.key {
						found = true
						break
					}
				}
				if !found {
					t.Fatalf("key %q not in ordered members", c.key)
				}
			}
		})
	}
}

func rawStringIs(want string) func(*testing.T, json.RawMessage) {
	return func(t *testing.T, raw json.RawMessage) {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			t.Fatalf("not a string: %s (%v)", raw, err)
		}
		if s != want {
			t.Fatalf("string = %q, want %q", s, want)
		}
	}
}

func rawNumberIs(want string) func(*testing.T, json.RawMessage) {
	return func(t *testing.T, raw json.RawMessage) {
		if n := strings.TrimPrefix(string(raw), `"`); n != want {
			t.Fatalf("number = %s, want %s", raw, want)
		}
	}
}

func rawIs(want string) func(*testing.T, json.RawMessage) {
	return func(t *testing.T, raw json.RawMessage) {
		if string(raw) != want {
			t.Fatalf("value = %s, want %s", raw, want)
		}
	}
}

// TestEncodedRoundTripProperty is the generated counterpart: PCG-seeded
// random field sets (strings with injected specials, ints across the
// 2^53 boundary, floats, durations) must survive the encode → parse →
// compare cycle exactly.
func TestEncodedRoundTripProperty(t *testing.T) {
	const total = 20_000
	rng := rand.New(rand.NewPCG(0x600D5eed, 0x51EE5eed))

	specials := []byte{0x00, 0x09, 0x0a, '"', '\\', 0x7f, 0x80, 0xff}
	for i := range total {
		width := 1 + rng.IntN(8)
		fields := make([]Field, width)
		for w := range fields {
			key := "k" + strconv.Itoa(rng.IntN(6))
			switch rng.IntN(4) {
			case 0:
				b := make([]byte, rng.IntN(24))
				for j := range b {
					if rng.IntN(8) == 0 {
						b[j] = specials[rng.IntN(len(specials))]
					} else {
						b[j] = byte(0x20 + rng.IntN(0x5f))
					}
				}
				fields[w] = fieldOf(key, string(b))
			case 1:
				fields[w] = fieldOf(key, rng.Int64())
			case 2:
				fields[w] = fieldOf(key, rng.Float64())
			case 3:
				fields[w] = fieldOf(key, time.Duration(rng.Int64())*time.Millisecond)
			}
		}
		r := recOf(LevelInfo, "prop", fields...)
		got, _ := decodeLineStrict(t, r.Encoded())

		// reference fold: last write wins per key, forward order
		var wantKeys []string
		wantVals := map[string]Field{}
		for _, f := range fields {
			if _, seen := wantVals[f.key]; !seen {
				wantKeys = append(wantKeys, f.key)
			}
			wantVals[f.key] = f
		}
		if len(got) != len(wantKeys)+3 { // + level, time, message
			t.Fatalf("i=%d: %d keys on wire, want %d (+3 canonical)", i, len(got), len(wantKeys))
		}
		for k, wf := range wantVals {
			raw, ok := got[k]
			if !ok {
				t.Fatalf("i=%d: key %q missing", i, k)
			}
			var s string
			var num int64
			var fnum float64
			switch wf.kind {
			case KindString:
				if err := json.Unmarshal(raw, &s); err != nil || (!utf8.ValidString(wf.str) && err != nil) {
					t.Fatalf("i=%d %q: %v", i, k, err)
				}
				if utf8.ValidString(wf.str) && s != wf.str {
					t.Fatalf("i=%d %q: string round-trip %q != %q", i, k, s, wf.str)
				}
			case KindInt:
				if err := json.Unmarshal(raw, &num); err != nil || num != wf.num {
					t.Fatalf("i=%d %q: int round-trip %d != %d (%s)", i, k, num, wf.num, raw)
				}
			case KindFloat:
				if err := json.Unmarshal(raw, &fnum); err != nil || fnum != wf.f {
					t.Fatalf("i=%d %q: float round-trip %v != %v", i, k, fnum, wf.f)
				}
			case KindDuration:
				var ms float64
				if err := json.Unmarshal(raw, &ms); err != nil || ms != float64(wf.num)/float64(time.Millisecond) {
					t.Fatalf("i=%d %q: duration round-trip %v != %v", i, k, ms, float64(wf.num)/float64(time.Millisecond))
				}
			}
		}
	}
}

// FuzzRecordEncodedDedupe is the dedupe-boundary fuzz target
// (dst-research §6.2): generated field sequences with dense duplicates
// and canonical-key collisions must produce a line where every user key
// appears exactly once with its last value, in last-occurrence order —
// checked against an independent reference fold, not the encoder's own
// dedupe code.
// TestEncodedRoundTripProperty: the encoded canonical line must
// unmarshal to exactly the record's resolved members.
//
// Canonical-key collision follows the logrus rename policy (record.go
// aliasKey, T5 decision): user fields named "level"/"time"/"message"
// are emitted as "fields.level"/"fields.time"/"fields.message", so the
// envelope keys — member 0 level, last two members time and message —
// stay unique on the wire. The reference fold aliases the keys the
// same way (a user "message" and a user "fields.message" fold into one
// member).
func FuzzRecordEncodedDedupe(f *testing.F) {
	f.Add(uint8(0), []byte{0, 1, 2, 0, 1}, []byte{7, 7, 7})
	f.Add(uint8(24), []byte{0, 1, 2, 3, 0, 1, 2, 3}, []byte{1, 2})
	f.Add(uint8(25), []byte{0, 0, 0, 0}, []byte{1, 2, 3})
	f.Add(uint8(31), []byte{0, 1, 2, 3, 4, 5, 6, 7}, []byte{0})
	f.Add(uint8(32), []byte{0}, []byte{1}) // seenArr → map handoff
	f.Add(uint8(64), []byte{3, 3, 3}, []byte{9})
	f.Add(uint8(5), []byte{4, 4, 4}, []byte{1}) // all keys are "message"
	f.Add(uint8(5), []byte{5, 5}, []byte{1})    // all keys are "time"
	f.Add(uint8(5), []byte{6, 6}, []byte{1})    // all keys are "level"
	f.Fuzz(func(t *testing.T, widthRaw uint8, keySeed []byte, valSeed []byte) {
		width := int(widthRaw) % 80
		if len(keySeed) == 0 {
			return
		}
		alphabet := []string{"a", "bb", "ccc", "message", "time", "level", "zz"}
		fields := make([]Field, width)
		for i := range fields {
			key := alphabet[int(keySeed[i%len(keySeed)])%len(alphabet)]
			val := int64(0)
			if len(valSeed) > 0 {
				val = int64(valSeed[i%len(valSeed)])
			}
			fields[i] = fieldOf(key, val)
		}

		r := recOf(LevelInfo, "m", fields...)
		got, ordered := decodeLineStrict(t, r.Encoded())

		// reference fold (independent of appendDedupedFields)
		type lastWrite struct {
			val int64
			pos int // last-occurrence position, for order
		}
		lww := map[string]lastWrite{}
		for i, fl := range fields {
			lww[aliasedFieldKey(fl.key)] = lastWrite{fl.num, i}
		}
		var wantOrder []string
		{
			type kv struct {
				k   string
				pos int
			}
			var kvs []kv
			for k, lw := range lww {
				kvs = append(kvs, kv{k, lw.pos})
			}
			// sort by last-occurrence position ascending = emission order
			for i := 1; i < len(kvs); i++ {
				for j := i; j > 0 && kvs[j].pos < kvs[j-1].pos; j-- {
					kvs[j], kvs[j-1] = kvs[j-1], kvs[j]
				}
			}
			for _, kv := range kvs {
				wantOrder = append(wantOrder, kv.k)
			}
		}

		// 1. every aliased user key exactly once on the wire. The
		// envelope keys (level/time/message) never appear among the user
		// members — collisions were renamed to fields.* — so every fold
		// key must be a user member.
		for k := range lww {
			if k == "level" || k == "time" || k == "message" {
				continue // envelope members, checked by check 3
			}
			n := 0
			for _, m := range ordered {
				if m.key == k {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("key %q emitted %d times (width %d): %s", k, n, width, r.Encoded())
			}
			var v int64
			if err := json.Unmarshal(got[k], &v); err != nil || v != lww[k].val {
				t.Fatalf("key %q = %s, want last value %d", k, got[k], lww[k].val)
			}
		}

		// 1b. colliding user keys never appear under their raw names in
		// the user span, and their fields.* copies are covered by the
		// fold loop above.
		for _, m := range ordered {
			switch m.key {
			case "level", "time", "message":
				// envelope members only — the user span excludes them
				// by construction (check 3)
			default:
				if _, user := lww[m.key]; !user {
					t.Fatalf("wire member %q is not a fold key (line %s)", m.key, r.Encoded())
				}
			}
		}

		// 2. emission order == last-occurrence order. The envelope keys
		// occupy fixed slots (member 0 is the canonical level; the last two
		// members are the canonical time and message), so the user span is
		// strictly between them regardless of collisions.
		var gotOrder []string
		for _, m := range ordered {
			gotOrder = append(gotOrder, m.key)
		}
		userGot := gotOrder[1 : len(gotOrder)-2]
		for i := range wantOrder {
			if userGot[i] != wantOrder[i] {
				t.Fatalf("order mismatch at %d: got %v want %v (line %s)", i, userGot, wantOrder, r.Encoded())
			}
		}

		// 3. canonical envelope pin: level/time/message appear exactly
		// once each as envelope members regardless of colliding user
		// fields — the rename guarantees the wire is duplicate-free.
		for _, canon := range []string{"level", "time", "message"} {
			n := 0
			for _, m := range ordered {
				if m.key == canon {
					n++
				}
			}
			if n != 1 {
				t.Fatalf("canonical key %q appears %d times, want exactly 1 "+
					"(rename policy; line %s)", canon, n, r.Encoded())
			}
		}
	})
}

// TestSamplingErrorBypassProperty drives generated failure and healthy
// programs through a drop-everything runtime (NeverSampler at rate 0):
// failures must still emit — the amendment-4 structural bypass — while
// healthy events drop. The model's error predicate (end error, panic,
// unolog.Error, or a non-success outcome) is the oracle.
func TestSamplingErrorBypassProperty(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x5A9A1E5eed, 0x8A55E5eed))
	for i := range 3000 {
		buf := make([]byte, 2+rng.IntN(120))
		for j := range buf {
			buf[j] = byte(rng.Uint64())
		}
		prog := decodeProgram(buf)
		prog.mode = modeRate0
		m := buildModel(prog)
		outcome := m.outcome()

		sink := &lifeSink{}
		rt := MustCompile(Config{Sink: sink, SamplingRate: 0, Sampler: NeverSampler()})
		op := Start(context.Background(), rt, OperationStart{Domain: prog.start, Name: "n"})
		executeProgramOn(prog, op)
		got := len(sink.events)
		want := 0
		if m.emitted(outcome) {
			want = 1
		}
		if got != want {
			t.Fatalf("iter %d: drop-everything runtime emitted %d events, want %d (outcome %s errOp=%v endErr=%v panicked=%v)",
				i, got, want, outcome, m.errOp, m.endErr != nil, m.endPanicked)
		}
	}
}

// TestSamplingRateBoundaryProperty pins the deterministic rate edges
// over generated healthy programs: rate 0 drops every healthy event,
// rate 1 keeps every one, and error events are emitted at both.
func TestSamplingRateBoundaryProperty(t *testing.T) {
	rng := rand.New(rand.NewPCG(0xB0A0A5eed, 0xD8A7A5eed))
	run := func(rate float64, prog lifeProgram) int {
		sink := &lifeSink{}
		rt := MustCompile(Config{Sink: sink, SamplingRate: rate})
		op := Start(context.Background(), rt, OperationStart{Domain: prog.start, Name: "n"})
		executeProgramOn(prog, op)
		return len(sink.events)
	}
	for i := range 1500 {
		buf := make([]byte, 2+rng.IntN(120))
		for j := range buf {
			buf[j] = byte(rng.Uint64())
		}
		prog := decodeProgram(buf)
		prog.mode = modeRate0 // runtime selection irrelevant: rate passed explicitly
		m := buildModel(prog)
		outcome := m.outcome()

		// Only programs that reach an End on a live runtime and stay
		// healthy belong here: failures (bypass test) and never-ended
		// streams (no event at any rate) are filtered out.
		if !m.ended || m.mode == modeNilRuntime || m.mode == modeNilSink || m.hasError(outcome) {
			continue
		}
		// healthy program: rate 0 drops, rate 1 keeps
		if got := run(0, prog); got != 0 {
			t.Fatalf("iter %d: healthy event survived rate 0", i)
		}
		if got := run(1, prog); got != 1 {
			t.Fatalf("iter %d: healthy event dropped at rate 1", i)
		}
	}
}

// chainCase describes one generated ChainSampler configuration.
type chainCase struct {
	hasErr    bool
	code      int
	status    int
	duration  time.Duration
	path      string
	keepErr   bool
	minDur    time.Duration
	prefixes  []string
	useSlower bool
	usePrefix bool
}

func chainReference(c chainCase) bool {
	// The reference union: each middleware keeps its predicate or
	// defers to the (dropping) base, so the composed decision is the OR
	// of the middlewares' predicates. KeepSlowerThan clamps negative
	// minimums to zero; KeepPathPrefix filters empty prefixes.
	keep := c.keepErr && (c.hasErr || c.code >= 500 || c.status >= 500)
	if c.useSlower {
		min := max(c.minDur, 0)
		if c.duration >= min {
			keep = true
		}
	}
	if c.usePrefix {
		for _, p := range c.prefixes {
			if p != "" && len(c.path) >= len(p) && c.path[:len(p)] == p {
				keep = true
			}
		}
	}
	return keep
}

// TestChainSamplerProperty compares ChainSampler's composed decision
// against the reference union formula over generated inputs.
func TestChainSamplerProperty(t *testing.T) {
	rng := rand.New(rand.NewPCG(0xC4A1A5eed, 0xE5EED5E))
	for i := range 5000 {
		c := chainCase{
			hasErr:    rng.Uint64()&1 == 0,
			code:      int(rng.IntN(700)),
			status:    int(rng.IntN(700)),
			duration:  time.Duration(rng.IntN(200)-50) * time.Millisecond,
			path:      []string{"", "/", "/api/v1/items", "/healthz", "/other"}[rng.IntN(5)],
			keepErr:   rng.Uint64()&1 == 0,
			minDur:    time.Duration(rng.IntN(100)-20) * time.Millisecond,
			useSlower: rng.Uint64()&1 == 0,
			usePrefix: rng.Uint64()&1 == 0,
		}
		switch rng.IntN(4) {
		case 0:
			c.prefixes = nil
		case 1:
			c.prefixes = []string{"/api"}
		case 2:
			c.prefixes = []string{"", "/healthz"}
		default:
			c.prefixes = []string{"/nope", "", "/api/v1"}
		}

		in := SampleInput{
			Domain: DomainHTTP, Operation: "GET /x", Outcome: OutcomeSuccess,
			Code: c.code, StatusCode: c.status, Duration: c.duration,
			Path: c.path, Level: LevelInfo, HasError: c.hasErr,
		}
		var middlewares []SamplerMiddleware
		if c.keepErr {
			middlewares = append(middlewares, KeepErrors())
		}
		if c.useSlower {
			middlewares = append(middlewares, KeepSlowerThan(c.minDur))
		}
		if c.usePrefix {
			middlewares = append(middlewares, KeepPathPrefix(c.prefixes...))
		}
		// exercise nil and empty middlewares too
		if rng.Uint64()&1 == 0 {
			middlewares = append(middlewares, nil)
		}
		chained := ChainSampler(NeverSampler(), middlewares...)

		want := chainReference(c)
		if got := chained(in); got != want {
			t.Fatalf("iter %d: chain(%+v) = %v, reference = %v", i, c, got, want)
		}
	}
}

// poolState snapshots everything a straggler write could mutate on a
// sealed event: the WAL, the message, the requested level, and the
// error latch.
type poolState struct {
	fields   []Field
	msg      string
	level    Level
	hasLevel bool
	hasErr   bool
	sealed   bool
}

func snapshotState(ev *event) poolState {
	s := ev.state.Load()
	state := walState(s & walStateMask)
	return poolState{
		fields:   slices.Clone(ev.fields),
		msg:      ev.msg,
		level:    ev.requestedLevel,
		hasLevel: ev.hasRequestedLvl,
		hasErr:   ev.hasErr,
		sealed:   state == walSealed,
	}
}

func statesEqual(a, b poolState) bool {
	return a.msg == b.msg && a.level == b.level &&
		a.hasLevel == b.hasLevel && a.hasErr == b.hasErr &&
		a.sealed == b.sealed &&
		reflect.DeepEqual(a.fields, b.fields)
}

// capturedEqual compares two captured events (used to pin that the
// records handed to sinks are immutable snapshots).
func capturedEqual(a, b lifeCapture) bool {
	return a.level == b.level && a.message == b.message && bytes.Equal(a.line, b.line)
}

// TestPoolSafetyReplayProperty replays generated program prefixes
// through a stale context after both requests completed.
func TestPoolSafetyReplayProperty(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x90F15E5eed, 0xE6A1E5eed))
	for i := range 400 {
		buf := make([]byte, 2+rng.IntN(160))
		for j := range buf {
			buf[j] = byte(rng.Uint64())
		}
		prog := decodeProgram(buf)
		prog.mode = modeRate1 // pool safety needs live emissions

		// Request 1 runs the program on its own runtime.
		sink1 := &lifeSink{}
		rt1 := MustCompile(Config{Sink: sink1, SamplingRate: 1})
		op1 := Start(context.Background(), rt1, OperationStart{Domain: prog.start, Name: "one"})
		executeProgramOn(prog, op1) // ctx1 := op1.Context() is the stale handle replayed below
		if len(sink1.events) != 1 {
			continue // dropped or never-ended: nothing to protect
		}
		firstCapture := sink1.events[0]

		// Request 2 completes on the same pool.
		sink2 := &lifeSink{}
		rt2 := MustCompile(Config{Sink: sink2, SamplingRate: 1})
		op2 := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "two"})
		Add(op2.Context(), "second", "owned")
		op2.End(nil)
		if len(sink2.events) != 1 {
			t.Fatalf("iter %d: request 2 emitted %d events", i, len(sink2.events))
		}
		secondCapture := sink2.events[0]
		ev2 := op2.ev
		before2 := snapshotState(ev2)

		// Replay every prefix of the first program through the stale
		// context, including the empty prefix and the full program.
		for k := 0; k <= len(prog.ops); k++ {
			prefix := lifeProgram{mode: prog.mode, start: prog.start, ops: prog.ops[:k]}
			executeProgramOn(prefix, op1) // stale ctx writes: must all no-op
		}

		if !statesEqual(snapshotState(ev2), before2) {
			t.Fatalf("iter %d: replay through the stale context mutated request 2's event", i)
		}
		if !capturedEqual(sink2.events[0], secondCapture) {
			t.Fatalf("iter %d: replay corrupted the captured second event", i)
		}
		if !capturedEqual(sink1.events[0], firstCapture) {
			t.Fatalf("iter %d: replay corrupted the captured first event", i)
		}
	}
}

// The lifecycle model's wire oracle.
//
// The wire comparison pins the full canonical line: envelope level,
// every user field at its last-occurrence position with its last value
// (independent LWW fold), the resolved op.outcome, and the resolved
// message. The members that depend on the wall clock (time,
// duration_ms) are excluded.
// FuzzEndLifecycle scope note.
//
// The canonical-key collision policy (user fields named "message"/
// "time"/"level") is deliberately OUT of this target's key alphabet —
// those keys duplicate envelope members and are pinned separately by
// FuzzDedupeFields (P2), which owns the collision documentation.

// Program encoding
//
// A program is a byte stream decoded by a cursor. The first two bytes
// select the runtime mode and domain; the remaining bytes decode into a
// command stream. The decoder is total: any byte sequence yields a
// well-formed program (truncation stops the stream, the op count is
// capped), so the fuzzer's mutations explore the command/key/value
// decision space while the executor only ever sees valid operations —
// the "coverage-guided structure" hybrid from dst-research §6.6.

type lifeMode uint8

const (
	modeRate1      lifeMode = iota // normal runtime, SamplingRate 1
	modeRate0                      // healthy events sampled away (errors bypass)
	modeNilRuntime                 // nil *Runtime: nothing emits
	modeNilSink                    // nil Sink: nothing emits
)

// lifeOpKind enumerates the decoded commands.
type lifeOpKind uint8

const (
	opAdd    lifeOpKind = iota // typed value from the table
	opAddStr                   // raw string value from the stream (may be invalid UTF-8)
	opAddVar                   // variadic Add with (possibly malformed) kv tails
	opErr                      // unolog.Error with a generated message
	opSetMsg
	opSetLevel
	opSetRoute
	opEndErr
	opEndPanic
)

type lifeValueKind uint8

const (
	valInt8 lifeValueKind = iota
	valInt64
	valUint
	valFloatFrac
	valFloatBig
	valFloat32
	valBool
	valTime
	valDuration
	valAny
)

// lifeOp is one decoded command. Operands are materialized at decode
// time so the executor and the model share the same data — the oracle
// is the *resolution* of the stream, not its decoding.
type lifeOp struct {
	kind lifeOpKind
	key  string
	val  any // typed value for opAdd, string for opAddStr/opSetMsg/opSetRoute,
	// error for opErr/opEndErr, Level for opSetLevel, panic payload for opEndPanic
	raw   []byte   // for opEndPanic: nil or the co-delivered error message
	pairs []kvPair // for opAddVar: extra (key, value) pairs after the leading one
}

type kvPair struct {
	key   any // string, or a non-string marker (int) to exercise malformed-key skipping
	valid bool
	val   any
}

type lifeProgram struct {
	mode  lifeMode
	start Domain
	ops   []lifeOp
}

// progCursor decodes bytes with bounded reads; a short stream yields a
// truncated (but valid) program.
type progCursor struct {
	b   []byte
	pos int
}

func (c *progCursor) next() byte {
	if c.pos >= len(c.b) {
		return 0
	}
	b := c.b[c.pos]
	c.pos++
	return b
}

// window returns up to n bytes from the cursor (fewer at the end).
func (c *progCursor) window(n int) []byte {
	if c.pos+n > len(c.b) {
		n = max(len(c.b)-c.pos, 0)
	}
	w := c.b[c.pos : c.pos+n]
	c.pos += n
	return w
}

// Domain choices per byte: job and http exercise the message-default
// split and the op.code/http.status canonical split; "" exercises the
// alias normalization to "operation".
var progDomains = []Domain{DomainJob, DomainHTTP, "", DomainJob}

// Add-key table: plain keys plus the canonical keys the scan reads
// (op.outcome/http.status/op.code) and the keys canonical writes may
// duplicate (op.name via Start, http.route via SetRoute). Keys that
// collide with the envelope members (level/time/message) are excluded
// by design — see the file comment.
var progKeys = []string{
	"k0", "k1", "k2", "k3", "k4", "dup", "op.outcome", "http.status",
	"op.code", "op.name", "op.id", "http.method", "http.path", "http.route",
}

var progLevels = []Level{LevelDebug, LevelInfo, LevelWarn, LevelError, Level(99)}

var progTimes = []time.Time{
	time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC),
	time.Date(2026, 9, 1, 15, 30, 0, 123456789, time.FixedZone("IST", 5*3600+30*60)),
	time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
	time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
}

var progDurUnits = []time.Duration{time.Nanosecond, time.Microsecond, time.Millisecond, time.Second}

// maxProgOps caps decoded streams (width seeds need up to ~82 appends).
const maxProgOps = 100

func decodeProgram(b []byte) lifeProgram {
	c := &progCursor{b: b}
	mode := lifeMode(c.next() % 4)
	if mode > modeNilSink {
		mode = modeRate1
	}
	start := progDomains[int(c.next())%len(progDomains)]

	var ops []lifeOp
	for len(ops) < maxProgOps {
		if c.pos >= len(c.b) {
			break // the stream ended at an op boundary
		}
		switch lifeOpKind(c.next() % 11) {
		case opAdd:
			key := progKeys[int(c.next())%len(progKeys)]
			vk := lifeValueKind(c.next() % 10)
			ops = append(ops, lifeOp{kind: opAdd, key: key, val: progValue(vk, c)})
		case opAddStr:
			key := progKeys[int(c.next())%len(progKeys)]
			n := int(c.next() % 17)
			ops = append(ops, lifeOp{kind: opAddStr, key: key, val: string(c.window(n))})
		case opAddVar:
			// One leading pair plus 0-4 extra pairs; extra keys may be
			// empty strings or non-strings (malformed tails the public
			// Add must skip, amendment 19).
			key := progKeys[int(c.next())%len(progKeys)]
			op := lifeOp{kind: opAddVar, key: key, val: progValue(lifeValueKind(c.next()%10), c)}
			for n := int(c.next() % 5); n > 0; n-- {
				var pair kvPair
				switch c.next() % 4 {
				case 0:
					pair = kvPair{key: progKeys[int(c.next())%len(progKeys)], valid: true, val: progValue(lifeValueKind(c.next()%10), c)}
				case 1:
					pair = kvPair{key: "", valid: true, val: progValue(lifeValueKind(c.next()%10), c)} // empty key: skipped
				case 2:
					// non-string key: the encode side stores no key
					// bytes, so decode a fixed sentinel (any non-string
					// key is skipped by the core the same way).
					pair = kvPair{key: int64(-1), valid: false, val: progValue(lifeValueKind(c.next()%10), c)}
				case 3:
					// odd tail: the value is dropped and the loop ends
					_ = progValue(lifeValueKind(c.next()%10), c)
				}
				op.pairs = append(op.pairs, pair)
			}
			ops = append(ops, op)
		case opErr:
			n := int(c.next() % 9)
			ops = append(ops, lifeOp{kind: opErr, val: errors.New(string(c.window(n)))})
		case opSetMsg:
			var msg string
			switch c.next() % 4 {
			case 0:
				msg = ""
			case 1:
				msg = "hello"
			case 2:
				msg = fmt.Sprintf("custom-msg-%d", c.next())
			case 3:
				msg = string(c.window(int(c.next() % 8)))
			}
			ops = append(ops, lifeOp{kind: opSetMsg, val: msg})
		case opSetLevel:
			ops = append(ops, lifeOp{kind: opSetLevel, val: progLevels[int(c.next())%len(progLevels)]})
		case opSetRoute:
			var route string
			switch c.next() % 3 {
			case 0:
				route = ""
			case 1:
				route = "/api/v1/items"
			case 2:
				route = fmt.Sprintf("/route-%d", c.next())
			}
			ops = append(ops, lifeOp{kind: opSetRoute, val: route})
		case opEndErr:
			var err error
			switch c.next() % 4 {
			case 0:
				err = nil
			case 1:
				n := int(c.next() % 9)
				err = errors.New(string(c.window(n)))
			case 2:
				err = context.Canceled
			case 3:
				err = context.DeadlineExceeded
			}
			ops = append(ops, lifeOp{kind: opEndErr, val: err})
		case opEndPanic:
			payload := progPanicPayload(c.next())
			var errp error
			if c.next()%3 != 0 {
				n := int(c.next() % 9)
				errp = errors.New(string(c.window(n)))
			}
			ops = append(ops, lifeOp{kind: opEndPanic, val: payload, raw: errBytes(errp)})
			// A panic unwinds: nothing after it is ever decoded or run.
			return lifeProgram{mode: mode, start: start, ops: ops}
		}
	}
	return lifeProgram{mode: mode, start: start, ops: ops}
}

// errBytes marks a co-delivered End error: nil bytes mean no error.
func errBytes(err error) []byte {
	if err == nil {
		return nil
	}
	return []byte(err.Error())
}

func progPanicPayload(b byte) any {
	switch b % 3 {
	case 0:
		return "boom"
	case 1:
		return int64(42)
	default:
		return errors.New("panic-error")
	}
}

// progValue materializes one typed value from the cursor for a value
// kind byte. The same decode feeds the executor and the model.
func progValue(vk lifeValueKind, c *progCursor) any {
	switch vk {
	case valInt8:
		return int64(int8(c.next()))
	case valInt64:
		return int64(int8(c.next()))<<32 | int64(c.next())<<8 | int64(c.next())
	case valUint:
		return uint64(c.next())
	case valFloatFrac:
		return float64(int8(c.next())) / 3
	case valFloatBig:
		return float64(int8(c.next())) * 1e21 // e-format side of the boundary
	case valFloat32:
		return float32(int8(c.next())) / 7
	case valBool:
		return c.next()&1 == 0
	case valTime:
		return progTimes[int(c.next())%len(progTimes)]
	case valDuration:
		d := time.Duration(int64(int8(c.next()))) * progDurUnits[int(c.next())%len(progDurUnits)]
		return d
	default:
		return map[string]any{"v": fmt.Sprintf("%c", 'a'+c.next()%26)}
	}
}

// Execution

// lifeCapture is one captured event: the level/message the sink saw
// and the encoded canonical line.
type lifeCapture struct {
	level   Level
	message string
	line    []byte
}

// lifeSink captures the canonical line (Encoded) plus envelope scalars.
type lifeSink struct {
	events []lifeCapture
}

func (s *lifeSink) Write(_ context.Context, rec *Record) {
	s.events = append(s.events, lifeCapture{
		level:   rec.Level(),
		message: rec.Message(),
		line:    bytes.Clone(rec.Encoded()),
	})
}

// executeProgram runs the decoded ops against a live Operation on the
// runtime the program's mode selects. A plain End does not stop the
// stream (later writes are stragglers and must no-op); a panic-End
// unwinds and stops it, mirroring real panic semantics.
func executeProgram(prog lifeProgram) *lifeSink {
	sink := &lifeSink{}
	var rt *Runtime
	switch prog.mode {
	case modeNilRuntime:
		rt = nil
	case modeNilSink:
		rt = MustCompile(Config{SamplingRate: 1}) // no Sink: nothing emits
	case modeRate0:
		rt = MustCompile(Config{Sink: sink, SamplingRate: 0})
	default:
		rt = MustCompile(Config{Sink: sink, SamplingRate: 1})
	}
	op := Start(context.Background(), rt, OperationStart{Domain: prog.start, Name: "n"})
	executeProgramOn(prog, op)
	return sink
}

// executeProgramOn applies the decoded ops to an already-started
// operation (shared by the lifecycle executor and the sampling and
// pool-safety properties, which wire their own runtimes). Stragglers
// after a plain End must no-op; a panic-End unwinds the stream.
func executeProgramOn(prog lifeProgram, op *Operation) {
	ctx := op.Context()
	ended := false
	for i := range prog.ops {
		o := &prog.ops[i]
		switch o.kind {
		case opAdd, opAddStr:
			Add(ctx, o.key, o.val)
		case opAddVar:
			kv := make([]any, 0, len(o.pairs)*2)
			for _, p := range o.pairs {
				kv = append(kv, p.key, p.val)
			}
			Add(ctx, o.key, o.val, kv...)
		case opErr:
			Error(ctx, o.val.(error))
		case opSetMsg:
			SetMessage(ctx, o.val.(string))
		case opSetLevel:
			SetLevel(ctx, o.val.(Level))
		case opSetRoute:
			SetRoute(ctx, o.val.(string))
		case opEndErr:
			if ended {
				continue // one-shot End; later ends are no-ops
			}
			var err error
			if e, ok := o.val.(error); ok && e != nil {
				err = e
			}
			op.End(&err)
			ended = true
		case opEndPanic:
			if ended {
				continue
			}
			panicErr := error(nil)
			if raw := o.raw; raw != nil {
				panicErr = errors.New(string(raw))
			}
			func() {
				defer func() { _ = recover() }() // swallow End's re-panic
				defer op.End(&panicErr)          // direct defer: End observes the panic
				panic(o.val)
			}()
			return // the panic unwound the stream
		}
	}
}

// The model — an independent implementation of the spec

// lifeModel tracks the event the program SHOULD produce, derived only
// from program semantics plus the documented resolution rules.
type lifeModel struct {
	mode  lifeMode
	start Domain

	// appends is the ordered WAL the spec says the program produces
	// (start fields + executed ops), before the encoder's LWW dedupe.
	appends []Field

	msg         string
	level       Level
	hasLevel    bool
	errOp       bool // an unolog.Error op executed while live
	endErr      error
	endPanicked bool
	endPayload  any

	ended bool
}

func buildModel(prog lifeProgram) *lifeModel {
	m := &lifeModel{mode: prog.mode, start: prog.start}
	// Start fields are lazy: the real implementation appends
	// op.domain/op.name only at End (see endAnnotations), so the live
	// WAL carries no start metadata during the request.

	for i := range prog.ops {
		o := &prog.ops[i]
		if m.ended {
			continue // stragglers after End are no-ops
		}
		switch o.kind {
		case opAdd, opAddStr:
			m.append(fieldOf(o.key, o.val))
		case opAddVar:
			m.append(fieldOf(o.key, o.val))
			for _, p := range o.pairs {
				// amendment 19: pairs with non-string or empty keys are
				// skipped silently, and an odd tail never starts a pair.
				key, ok := p.key.(string)
				if !p.valid || !ok || key == "" {
					continue
				}
				m.append(fieldOf(key, p.val))
			}
		case opErr:
			m.errOp = true
			m.append(Field{key: "error", kind: KindAny, val: modelErrorField(o.val.(error))})
		case opSetMsg:
			if msg := o.val.(string); msg != "" {
				m.msg = msg
			}
		case opSetLevel:
			if lvl := o.val.(Level); IsValidLevel(lvl) {
				m.level = lvl
				m.hasLevel = true
			}
		case opSetRoute:
			if route := o.val.(string); route != "" {
				m.append(fieldStr("http.route", route))
			}
		case opEndErr:
			if e, ok := o.val.(error); ok && e != nil {
				m.endErr = e
			}
			m.ended = true
		case opEndPanic:
			m.endPayload = o.val
			m.endPanicked = true
			if o.raw != nil {
				m.endErr = errors.New(string(o.raw))
			}
			m.ended = true
			goto ended
		}
	}
ended:
	return m
}

func (m *lifeModel) append(f Field) { m.appends = append(m.appends, f) }

// modelErrorField and modelPanicField reproduce the structured error
// and panic field shapes (message/type, plus cause.* for unwrapped
// chains) without calling structuredErrorField — the model builds the
// documented map from the error value directly.
func modelErrorField(err error) map[string]any {
	field := map[string]any{
		"message": err.Error(),
		"type":    fmt.Sprintf("%T", err),
	}
	return field
}

func modelPanicField(payload any) map[string]any {
	return map[string]any{
		"type":  fmt.Sprintf("%T", payload),
		"value": fmt.Sprint(payload),
	}
}

// scan re-derives the scan scalars the spec reads from the WAL: a
// backward walk accepting the first field of the matching key+kind.
func (m *lifeModel) scan() (outcome Outcome, hasOutcome bool, code int, hasCode bool, opCode int, hasOpCode bool) {
	for _, f := range slices.Backward(m.appends) {

		switch f.key {
		case "op.outcome":
			if !hasOutcome && f.kind == KindString {
				if o := Outcome(f.str); IsValidOutcome(o) {
					outcome = o
					hasOutcome = true
				}
			}
		case "http.status":
			if !hasCode && f.kind == KindInt {
				code = int(f.num)
				hasCode = true
			}
		case "op.code":
			if !hasOpCode && f.kind == KindInt {
				opCode = int(f.num)
				hasOpCode = true
			}
		}
	}
	return outcome, hasOutcome, code, hasCode, opCode, hasOpCode
}

// outcome applies the documented precedence: panic > error > explicit
// op.outcome > 5xx status > success.
func (m *lifeModel) outcome() Outcome {
	if m.endPanicked {
		return OutcomePanic
	}
	if m.endErr != nil {
		switch {
		case errors.Is(m.endErr, context.Canceled):
			return OutcomeCanceled
		case errors.Is(m.endErr, context.DeadlineExceeded):
			return OutcomeTimeout
		default:
			return OutcomeFailure
		}
	}
	explicit, hasExplicit, code, hasCode, opCode, hasOpCode := m.scan()
	if hasExplicit {
		return explicit
	}
	// The 5xx rule reads the canonical code: http.status for HTTP,
	// op.code for everything else (mirrors resolveOutcome).
	if normalizeDomain(m.start) == DomainHTTP {
		if hasCode && code >= 500 {
			return OutcomeFailure
		}
	} else if hasOpCode && opCode >= 500 {
		return OutcomeFailure
	}
	return OutcomeSuccess
}

// resolveLevel applies the zero-policy outcome levels (success→info,
// panic→error, every other outcome→error) plus the requested-level
// floor.
func (m *lifeModel) resolveLevel(outcome Outcome) Level {
	auto := LevelError
	if outcome == OutcomeSuccess {
		auto = LevelInfo
	}
	if m.hasLevel && m.level > auto {
		return m.level
	}
	return auto
}

// message applies the SetMessage → config → domain-default chain (the
// fuzz configs never set a default message).
func (m *lifeModel) message() string {
	if m.msg != "" {
		return m.msg
	}
	if normalizeDomain(m.start) == DomainHTTP {
		return DefaultMessage
	}
	return DefaultOperationMessage
}

// hasError mirrors buildSampleInput's error predicate: any error
// source latched, or a resolved outcome that is neither success nor an
// explicit retry. A retry is not an error, so it samples like a healthy
// event.
func (m *lifeModel) hasError(outcome Outcome) bool {
	return m.endErr != nil || m.endPanicked || m.errOp ||
		(outcome != OutcomeSuccess && outcome != OutcomeRetry)
}

// emitted predicts whether commit writes the event: nil runtime/sink
// never emit; errors bypass sampling structurally; healthy events drop
// at rate 0 and keep at rate 1.
func (m *lifeModel) emitted(outcome Outcome) bool {
	if !m.ended {
		return false // the program never reached an End: no event
	}
	if m.mode == modeNilRuntime || m.mode == modeNilSink {
		return false
	}
	if m.hasError(outcome) {
		return true
	}
	return m.mode == modeRate1
}

// endAnnotations appends the canonical End writes the spec performs
// after sealing, in order: the structured failure fields, then the
// post-seal block — the lazy start metadata (op.domain/op.name, with
// Name "n", skipped when the request already wrote that key — the
// user's override must not be clobbered), then op.code/op.outcome.
// Called with the model in its pre-End state. Start metadata lands
// here rather than at Start, so the live WAL never carries it; on the
// wire it still precedes the completion fields.
func (m *lifeModel) endAnnotations() {
	if m.endPanicked {
		m.append(Field{key: "panic", kind: KindAny, val: modelPanicField(m.endPayload)})
	}
	if m.endErr != nil {
		m.append(Field{key: "error", kind: KindAny, val: modelErrorField(m.endErr)})
	} else if m.endPanicked {
		m.append(Field{key: "error", kind: KindAny, val: modelErrorField(fmt.Errorf("panic: %v", m.endPayload))})
	}
	if !m.wrote("op.domain") {
		m.append(fieldStr("op.domain", string(normalizeDomain(m.start))))
	}
	if !m.wrote("op.name") {
		m.append(fieldStr("op.name", "n"))
	}
	_, _, _, _, opCode, hasOpCode := m.scan()
	if normalizeDomain(m.start) != DomainHTTP && hasOpCode {
		m.append(fieldInt64("op.code", int64(opCode)))
	}
	// The resolved outcome is always written last (annotatePostSeal);
	// the scan above ran before it, so this append cannot feed back.
	m.append(fieldStr("op.outcome", string(m.outcome())))
}

// wrote reports whether the model WAL already carries a write under
// key (any kind) — the lazy start-metadata suppression condition.
func (m *lifeModel) wrote(key string) bool {
	for i := range m.appends {
		if m.appends[i].key == key {
			return true
		}
	}
	return false
}

// Wire comparison helpers (shared with the dedupe fuzz and property
// tests in this package)

// wireNumber decodes a JSON number member as json.Number.
func wireNumber(raw []byte) (json.Number, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var n json.Number
	if err := dec.Decode(&n); err != nil {
		return "", err
	}
	return n, nil
}

// checkFieldWire compares one modeled field against its decoded wire
// member using the per-kind contract (record.go appendFieldJSON):
//
//	string   → exact, when the modeled string is valid UTF-8 (invalid
//	           input normalizes to U+FFFD; parseability only)
//	int/uint → exact decimal (json.Number, so >2^53 is not mangled)
//	float    → parses back to the exact float64 (shortest round-trip)
//	float32  → parses back to the exact float32
//	bool     → true/false
//	time     → RFC3339 of the modeled instant
//	duration → float milliseconds of the modeled duration
//	err      → the error's message string
//	raw      → embedded verbatim
//	any      → semantic JSON equality (numbers normalized)
func checkFieldWire(f Field, raw []byte) error {
	switch f.kind {
	case KindString:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return fmt.Errorf("string member %q is not a JSON string: %v (raw %s)", f.key, err, raw)
		}
		if utf8.ValidString(f.str) && s != f.str {
			return fmt.Errorf("string %q: wire %q != modeled %q", f.key, s, f.str)
		}
		return nil
	case KindInt:
		n, err := wireNumber(raw)
		if err != nil {
			return fmt.Errorf("int member %q: %v", f.key, err)
		}
		if n.String() != strconv.FormatInt(f.num, 10) {
			return fmt.Errorf("int %q: wire %s != %d", f.key, n, f.num)
		}
		return nil
	case KindUint:
		n, err := wireNumber(raw)
		if err != nil {
			return fmt.Errorf("uint member %q: %v", f.key, err)
		}
		if n.String() != strconv.FormatUint(uint64(f.num), 10) {
			return fmt.Errorf("uint %q: wire %s != %d", f.key, n, uint64(f.num))
		}
		return nil
	case KindFloat:
		v, err := strconv.ParseFloat(string(raw), 64)
		if err != nil {
			return fmt.Errorf("float member %q: %v (raw %s)", f.key, err, raw)
		}
		if v != f.f {
			return fmt.Errorf("float %q: wire %v != %v", f.key, v, f.f)
		}
		return nil
	case KindFloat32:
		v, err := strconv.ParseFloat(string(raw), 32)
		if err != nil {
			return fmt.Errorf("float32 member %q: %v (raw %s)", f.key, err, raw)
		}
		if float32(v) != float32(f.f) {
			return fmt.Errorf("float32 %q: wire %v != %v", f.key, float32(v), float32(f.f))
		}
		return nil
	case KindBool:
		switch string(raw) {
		case "true":
			if !f.b {
				return fmt.Errorf("bool %q: wire true != modeled false", f.key)
			}
		case "false":
			if f.b {
				return fmt.Errorf("bool %q: wire false != modeled true", f.key)
			}
		default:
			return fmt.Errorf("bool member %q is not a bool: %s", f.key, raw)
		}
		return nil
	case KindTime:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return fmt.Errorf("time member %q is not a string: %v", f.key, err)
		}
		if want := f.t.Format(time.RFC3339); s != want {
			return fmt.Errorf("time %q: wire %q != %q", f.key, s, want)
		}
		return nil
	case KindDuration:
		v, err := strconv.ParseFloat(string(raw), 64)
		if err != nil {
			return fmt.Errorf("duration member %q: %v", f.key, err)
		}
		if want := float64(f.num) / float64(time.Millisecond); v != want {
			return fmt.Errorf("duration %q: wire %v != %v ms", f.key, v, want)
		}
		return nil
	case KindErr:
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return fmt.Errorf("err member %q is not a string: %v", f.key, err)
		}
		if want := f.val.(error).Error(); s != want {
			return fmt.Errorf("err %q: wire %q != %q", f.key, s, want)
		}
		return nil
	case KindAny:
		var modelV, wireV any
		modelBytes, err := json.Marshal(f.val)
		if err != nil {
			return fmt.Errorf("any member %q: modeled value not marshalable: %v", f.key, err)
		}
		if err := json.Unmarshal(modelBytes, &modelV); err != nil {
			return fmt.Errorf("any member %q: modeled JSON invalid: %v", f.key, err)
		}
		dec := json.NewDecoder(bytes.NewReader(raw))
		dec.UseNumber()
		if err := dec.Decode(&wireV); err != nil {
			return fmt.Errorf("any member %q: wire not parseable: %v", f.key, err)
		}
		if !jsonSemanticEqual(modelV, wireV) {
			return fmt.Errorf("any %q: wire %v != modeled %v", f.key, wireV, modelV)
		}
		return nil
	default:
		return fmt.Errorf("field %q: unsupported kind %d in wire check", f.key, f.kind)
	}
}

// jsonSemanticEqual compares two decoded JSON values, treating
// json.Number and float64 as equal when the numeric value matches.
// The internal json package mirrors this as jsonDecodedEqual (plain
// json.Unmarshal domain, float64 only) in fuzz_interface_test.go; the
// helpers cannot be shared because test-only code is package-private.
func jsonSemanticEqual(a, b any) bool {
	num := func(v any) (float64, bool) {
		switch n := v.(type) {
		case json.Number:
			f, err := n.Float64()
			return f, err == nil
		case float64:
			return n, true
		default:
			return 0, false
		}
	}
	switch av := a.(type) {
	case nil:
		return b == nil
	case bool:
		bv, ok := b.(bool)
		return ok && av == bv
	case string:
		bv, ok := b.(string)
		return ok && av == bv
	case json.Number, float64:
		af, aok := num(a)
		bf, bok := num(b)
		return aok && bok && af == bf
	case []any:
		bv, ok := b.([]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for i := range av {
			if !jsonSemanticEqual(av[i], bv[i]) {
				return false
			}
		}
		return true
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok || len(av) != len(bv) {
			return false
		}
		for k, v := range av {
			bvv, ok := bv[k]
			if !ok || !jsonSemanticEqual(v, bvv) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

// wireLevel renders the envelope level member value (the lowercase
// jsonLevelPrefix forms).
func wireLevel(l Level) string {
	switch l {
	case LevelDebug:
		return "debug"
	case LevelWarn:
		return "warn"
	case LevelError:
		return "error"
	default:
		return "info"
	}
}

// foldLastWrites resolves a WAL into the expected wire members: each
// key once, its last value, at its last-occurrence position — the
// encoder's dedupe contract, re-derived independently.
func foldLastWrites(appends []Field) []Field {
	last := map[string]int{}
	for i, f := range appends {
		last[f.key] = i
	}
	order := make([]int, 0, len(last))
	for _, i := range last {
		order = append(order, i)
	}
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && order[j] < order[j-1]; j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}
	out := make([]Field, 0, len(order))
	for _, i := range order {
		out = append(out, appends[i])
	}
	return out
}

// Oracle

// verifyLifecycle checks one executed program against the model. The
// wire members time and duration_ms are wall-clock dependent and
// excluded from both sides.
func verifyLifecycle(t *testing.T, prog lifeProgram, got *lifeSink) {
	t.Helper()
	m := buildModel(prog)
	outcome := m.outcome()

	wantEvents := 0
	if m.emitted(outcome) {
		wantEvents = 1
	}
	if len(got.events) != wantEvents {
		t.Fatalf("emitted %d events, want %d (mode %d outcome %s)", len(got.events), wantEvents, m.mode, outcome)
	}
	if wantEvents == 0 {
		return
	}
	ev := got.events[0]

	if ev.level != m.resolveLevel(outcome) {
		t.Fatalf("level = %v, want %v (outcome %s)", ev.level, m.resolveLevel(outcome), outcome)
	}
	if ev.message != m.message() {
		t.Fatalf("message = %q, want %q", ev.message, m.message())
	}

	// The modeled WAL: appends plus the canonical End annotations.
	m.endAnnotations()
	gotKeys, gotMembers := decodeLineStrict(t, ev.line)
	_ = gotKeys

	// Drop the wall-clock members from the wire.
	var wire []rawMember
	for _, mem := range gotMembers {
		if mem.key == "time" || mem.key == "duration_ms" {
			continue
		}
		wire = append(wire, mem)
	}

	// Modeled wire members: the envelope level, the LWW fold of the
	// modeled WAL, then the resolved message.
	want := []Field{{key: "level", kind: KindString, str: wireLevel(m.resolveLevel(outcome))}}
	for _, f := range foldLastWrites(m.appends) {
		if f.key == "duration_ms" {
			continue
		}
		want = append(want, f)
	}
	want = append(want, Field{key: "message", kind: KindString, str: m.message()})

	if len(wire) != len(want) {
		t.Fatalf("wire member count = %d, want %d\ngot  %v\nwant %v",
			len(wire), len(want), memberKeys(wire), memberFields(want))
	}
	for i := range want {
		if wire[i].key != want[i].key {
			t.Fatalf("member %d: wire key %q, want %q (got %v want %v)",
				i, wire[i].key, want[i].key, memberKeys(wire), memberFields(want))
		}
		if err := checkFieldWire(want[i], wire[i].val); err != nil {
			t.Fatalf("member %d: %v", i, err)
		}
	}
}

func memberKeys(members []rawMember) []string {
	out := make([]string, len(members))
	for i, m := range members {
		out[i] = m.key
	}
	return out
}

func memberFields(fields []Field) []string {
	out := make([]string, len(fields))
	for i, f := range fields {
		out[i] = f.key
	}
	return out
}

// Seed corpus and fuzz target

// seedPrograms are the curated corpus streams from action plan P1,
// registered as f.Add() seeds in FuzzEndLifecycle so plain `go test`
// replays them as regression seeds and -fuzz runs start from them.
// Every seed must round-trip through encodeProgram losslessly
// (asserted by TestLifecycleSeedPrograms).
type seedProg struct {
	name string
	prog lifeProgram
}

func seedPrograms() []seedProg {
	p := func(mode lifeMode, start Domain, ops ...lifeOp) lifeProgram {
		return lifeProgram{mode: mode, start: start, ops: ops}
	}
	endErr := func(err error) lifeOp { return lifeOp{kind: opEndErr, val: err} } // err stays a typed error (nil-safe)
	endPanic := func(payload any, errp error) lifeOp {
		return lifeOp{kind: opEndPanic, val: payload, raw: errBytes(errp)}
	}
	// String values travel as opAddStr (raw stream bytes) — the typed
	// value table has no string kind.
	strOp := func(k, v string) lifeOp { return lifeOp{kind: opAddStr, key: k, val: v} }
	intOp := func(k string, v int64) lifeOp { return lifeOp{kind: opAdd, key: k, val: v} }
	errOp := func(msg string) lifeOp { return lifeOp{kind: opErr, val: errors.New(msg)} }
	anyOp := func(k string, v any) lifeOp { return lifeOp{kind: opAdd, key: k, val: v} }

	var seeds []seedProg
	add := func(name string, prog lifeProgram) { seeds = append(seeds, seedProg{name, prog}) }

	// panic-then-error: error pointer already set when the panic hits.
	add("panic-then-error", p(modeRate1, DomainJob,
		errOp("before"), endPanic("boom", errors.New("co-err"))))
	// error-then-panic: unolog.Error op, then a bare panic (panic-fallback error).
	add("error-then-panic", p(modeRate1, DomainJob,
		errOp("u-error"), endPanic(int64(42), nil)))
	// straggler-after-seal: writes after End must no-op.
	add("straggler-after-seal", p(modeRate1, DomainJob,
		strOp("k0", "before"), endErr(nil), strOp("k0", "after"), errOp("late"), intOp("op.code", 5)))
	// duplicate keys at the dedupe width boundaries (keys cycle k0-k4).
	for _, w := range []int{1, 24, 25, 32, 33, 80} {
		ops := make([]lifeOp, 0, w+1)
		for i := range w - 1 {
			ops = append(ops, strOp(fmt.Sprintf("k%d", i%5), fmt.Sprintf("v%d", i)))
		}
		ops = append(ops, strOp("k0", "last-wins"), endErr(nil))
		add(fmt.Sprintf("dup-width-%d", w), p(modeRate1, DomainJob, ops...))
	}
	// explicit outcome override vs error outcome.
	add("explicit-outcome-retry", p(modeRate1, DomainHTTP,
		strOp("op.outcome", "retry"), intOp("http.status", 503), endErr(nil)))
	add("explicit-outcome-vs-error", p(modeRate1, DomainHTTP,
		strOp("op.outcome", "retry"), endErr(errors.New("boom"))))
	// wrong-kind canonical writes are ignored by the scan.
	add("outcome-wrong-kind", p(modeRate1, DomainJob,
		intOp("op.outcome", 7), strOp("http.status", "500"), endErr(nil)))
	// level permutations: requested floor × failure outcome. Level(99)
	// is invalid and must be a no-op; its label is distinct from
	// LevelInfo because Level(99).String() falls back to "INFO" (a
	// collision that would silently drop a corpus file).
	levelNames := []string{"DEBUG", "INFO", "WARN", "ERROR", "INVALID"}
	for i, lvl := range []Level{LevelDebug, LevelInfo, LevelWarn, LevelError, Level(99)} {
		add("level-"+levelNames[i], p(modeRate1, DomainJob,
			lifeOp{kind: opSetLevel, val: lvl}, endErr(errors.New("e"))))
	}
	// malformed kv tails through the public variadic Add.
	add("malformed-kv-tail", p(modeRate1, DomainJob,
		lifeOp{
			kind: opAddVar, key: "k0", val: int64(5),
			pairs: []kvPair{
				{key: "k1", valid: true, val: int64(1)},
				{key: "", valid: true, val: int64(2)},         // empty key: skipped
				{key: int64(-1), valid: false, val: int64(3)}, // non-string key: skipped (sentinel, see decode)
				{key: "k2", valid: true, val: int64(7)},       // last write wins
			},
		},
		endErr(nil)))
	// all-kinds value sweep (values chosen from the representable
	// encode space; kind coverage is the point).
	add("all-kinds-sweep", p(modeRate1, DomainJob,
		strOp("k0", "héllo ☃"),
		intOp("k1", -42),
		intOp("k2", 1<<33),
		anyOp("k3", 1.0/3),
		anyOp("k4", float32(3)/7),
		anyOp("dup", true),
		anyOp("op.name", progTimes[0]),
		anyOp("op.id", 42*time.Millisecond),
		errOp("err-9"),
		anyOp("http.method", map[string]any{"v": "x"}),
		lifeOp{kind: opSetMsg, val: "custom"},
		endErr(context.Canceled)))
	// canceled/deadline errors.
	add("canceled", p(modeRate1, DomainJob, endErr(context.Canceled)))
	add("deadline", p(modeRate1, DomainHTTP, intOp("http.status", 200), endErr(context.DeadlineExceeded)))
	// invalid setter no-ops.
	add("invalid-setters", p(modeRate1, DomainJob,
		lifeOp{kind: opSetLevel, val: Level(99)},
		lifeOp{kind: opSetMsg, val: ""},
		lifeOp{kind: opSetRoute, val: ""},
		endErr(nil)))
	// duplicate canonical keys: op.name three times, route rewrite.
	add("canonical-dups", p(modeRate1, DomainHTTP,
		strOp("op.name", "first"), strOp("op.name", "second"), strOp("op.name", "third"),
		lifeOp{kind: opSetRoute, val: "/route-3"},
		lifeOp{kind: opSetRoute, val: "/route-4"},
		strOp("http.route", "/added"),
		endErr(nil)))
	// nil runtime with a full program: nothing emits, nothing panics.
	add("nil-runtime", p(modeNilRuntime, DomainHTTP,
		strOp("k0", "v"), errOp("e"), intOp("http.status", 500), endErr(errors.New("boom"))))
	// rate 0: healthy events drop, errors bypass. An explicit retry is
	// not an error, so it drops like a healthy event.
	add("rate0-healthy-drops", p(modeRate0, DomainJob, strOp("k0", "v"), endErr(nil)))
	add("rate0-error-kept", p(modeRate0, DomainJob, strOp("k0", "v"), endErr(errors.New("e"))))
	add("rate0-retry-drops", p(modeRate0, DomainJob, strOp("op.outcome", "retry"), endErr(nil)))
	// 5xx without error, 4xx success.
	add("http-500", p(modeRate1, DomainHTTP, intOp("http.status", 500), endErr(nil)))
	add("http-404", p(modeRate1, DomainHTTP, intOp("http.status", 404), endErr(nil)))
	add("job-500-status", p(modeRate1, DomainJob, intOp("http.status", 500), endErr(nil)))
	add("job-500-opcode", p(modeRate1, DomainJob, intOp("op.code", 500), endErr(nil)))
	add("job-503-opcode-err", p(modeRate1, DomainJob, intOp("op.code", 503), errOp("boom"), endErr(nil)))
	return seeds
}

// FuzzEndLifecycle drives full request lifecycles from fuzz bytes and
// checks each against the independent model.
func FuzzEndLifecycle(f *testing.F) {
	for _, s := range seedPrograms() {
		f.Add(encodeProgram(s.prog))
	}
	f.Fuzz(func(t *testing.T, program []byte) {
		prog := decodeProgram(program)
		sink := executeProgram(prog)
		verifyLifecycle(t, prog, sink)
	})
}

// TestLifecycleSeedPrograms replays the curated corpus as regression
// tests and asserts the corpus materialization round-trips: the
// encoded bytes must decode back to the same program, so the f.Add()
// seeds exercise exactly these scenarios.
func TestLifecycleSeedPrograms(t *testing.T) {
	for _, s := range seedPrograms() {
		t.Run(s.name, func(t *testing.T) {
			decoded := decodeProgram(encodeProgram(s.prog))
			if !programsEqual(decoded, s.prog) {
				t.Fatal("seed does not round-trip through encode/decode")
			}
			sink := executeProgram(s.prog)
			verifyLifecycle(t, s.prog, sink)
		})
	}
}

// programsEqual compares two decoded programs structurally.
func programsEqual(a, b lifeProgram) bool {
	if a.mode != b.mode || a.start != b.start || len(a.ops) != len(b.ops) {
		return false
	}
	for i := range a.ops {
		x, y := &a.ops[i], &b.ops[i]
		if x.kind != y.kind || x.key != y.key || string(x.raw) != string(y.raw) {
			return false
		}
		if !valuesEqual(x.val, y.val) {
			return false
		}
		if len(x.pairs) != len(y.pairs) {
			return false
		}
		for j := range x.pairs {
			if !valuesEqual(x.pairs[j].key, y.pairs[j].key) || !valuesEqual(x.pairs[j].val, y.pairs[j].val) {
				return false
			}
		}
	}
	return true
}

// valuesEqual compares operand values (typed identity for table kinds).
func valuesEqual(a, b any) bool {
	nilErr := func(v any) bool {
		err, ok := v.(error)
		return ok && err == nil
	}
	switch av := a.(type) {
	case error:
		if av == nil {
			return nilErr(b) || b == nil
		}
		bv, ok := b.(error)
		return ok && bv != nil && av.Error() == bv.Error()
	case time.Time:
		bv, ok := b.(time.Time)
		return ok && av.Equal(bv)
	case map[string]any:
		bv, ok := b.(map[string]any)
		return ok && av["v"] == bv["v"]
	default:
		if b == nil {
			return a == nil
		}
		return a == b
	}
}

// TestLifecyclePropertyRandom drives random PCG-generated programs
// through the same oracle — the seed-only check that keeps the model
// honest without the fuzzer.
func TestLifecyclePropertyRandom(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x11FE5eed, 0x9A11FE5))
	for range 2000 {
		buf := make([]byte, 2+rng.IntN(260))
		for j := range buf {
			buf[j] = byte(rng.Uint64())
		}
		prog := decodeProgram(buf)
		sink := executeProgram(prog)
		verifyLifecycle(t, prog, sink)
	}
}

// encodeProgram renders a program back to the byte form decodeProgram
// reads, so corpus files can be materialized deterministically from
// the seed specs. Lossless for every seedPrograms entry.
func encodeProgram(prog lifeProgram) []byte {
	var b []byte
	b = append(b, byte(prog.mode))
	b = append(b, byte(indexOf(progDomains, prog.start)))
	for _, o := range prog.ops {
		// opAdd with a string operand travels as opAddStr: the typed
		// value table has no string kind (decoder-side strings come
		// from raw stream bytes).
		kind := o.kind
		if kind == opAdd {
			if _, isStr := o.val.(string); isStr {
				kind = opAddStr
			}
		}
		b = append(b, byte(kind))
		switch kind {
		case opAdd:
			b = append(b, keyByte(o.key))
			encodeValue(&b, o.val)
		case opAddStr:
			s := o.val.(string)
			b = append(b, keyByte(o.key), byte(len(s)))
			b = append(b, s...)
		case opAddVar:
			b = append(b, keyByte(o.key))
			encodeValue(&b, o.val)
			b = append(b, byte(len(o.pairs)))
			for _, pr := range o.pairs {
				switch k := pr.key.(type) {
				case string:
					if k == "" {
						b = append(b, 1)
					} else {
						b = append(b, 0, keyByte(k))
					}
				default:
					b = append(b, 2)
				}
				encodeValue(&b, pr.val)
			}
		case opErr:
			msg := o.val.(error).Error()
			b = append(b, byte(len(msg)))
			b = append(b, msg...)
		case opSetMsg:
			msg := o.val.(string)
			switch msg {
			case "":
				b = append(b, 0)
			case "hello":
				b = append(b, 1)
			default:
				b = append(b, 3, byte(len(msg)))
				b = append(b, msg...)
			}
		case opSetLevel:
			b = append(b, byte(indexOf(progLevels, o.val.(Level))))
		case opSetRoute:
			switch route := o.val.(string); route {
			case "":
				b = append(b, 0)
			case "/api/v1/items":
				b = append(b, 1)
			default:
				// decode rebuilds "/route-<digit>"; write the digit value
				b = append(b, 2, route[len(route)-1]-'0')
			}
		case opEndErr:
			if o.val == nil {
				b = append(b, 0)
				break
			}
			err := o.val.(error)
			switch err {
			case nil:
				b = append(b, 0)
			case context.Canceled:
				b = append(b, 2)
			case context.DeadlineExceeded:
				b = append(b, 3)
			default:
				msg := err.Error()
				b = append(b, 1, byte(len(msg)))
				b = append(b, msg...)
			}
		case opEndPanic:
			switch o.val.(type) {
			case string:
				b = append(b, 0)
			case int64:
				b = append(b, 1)
			default:
				b = append(b, 2)
			}
			if o.raw != nil {
				b = append(b, 1)
				msg := string(o.raw)
				b = append(b, byte(len(msg)))
				b = append(b, msg...)
			} else {
				b = append(b, 0)
			}
		}
	}
	return b
}

// encodeValue appends the value-kind byte and operand bytes for a
// typed value, mirroring progValue's decode.
func encodeValue(b *[]byte, v any) {
	switch x := v.(type) {
	case int64:
		if x >= -128 && x <= 127 {
			*b = append(*b, byte(valInt8), byte(int8(x)))
			return
		}
		// valInt64 encodes (int8(hi)<<32)|uint32(lo): keep seeds within
		// the representable range (|hi| <= 127).
		hi, lo := x>>32, uint32(x)
		*b = append(*b, byte(valInt64), byte(int8(hi)), byte(lo>>8), byte(lo))
	case uint64:
		*b = append(*b, byte(valUint), byte(x))
	case float64:
		if x != 0 && (x < -1e6 || x > 1e6) {
			// valFloatBig decodes as int8(mult)*1e21.
			*b = append(*b, byte(valFloatBig), byte(int8(x/1e21)))
			return
		}
		if x != 0 && x < 1e6 && x > -1e6 {
			*b = append(*b, byte(valFloatFrac), byte(int8(x*3)))
			return
		}
		// fall back to frac for anything else; decode loss is possible
		// only for values seeds do not use.
		*b = append(*b, byte(valFloatFrac), byte(int8(x*3)))
	case float32:
		*b = append(*b, byte(valFloat32), byte(int8(x*7)))
	case bool:
		if x {
			*b = append(*b, byte(valBool), 0) // decode: b&1 == 0 → true
		} else {
			*b = append(*b, byte(valBool), 1)
		}
	case time.Time:
		*b = append(*b, byte(valTime), byte(indexOf(progTimes, x)))
	case time.Duration:
		// valDuration decodes as int8(mult)*unit: pick the largest unit
		// that divides x with an int8 multiplier.
		for i, u := range slices.Backward(progDurUnits) {

			if x%u == 0 {
				m := x / u
				if m >= -128 && m <= 127 {
					*b = append(*b, byte(valDuration), byte(int8(m)), byte(i))
					return
				}
			}
		}
		*b = append(*b, byte(valDuration), 0, 0)
	default:
		// map[string]any with a single "v" string leaf; anything else
		// cannot be materialized losslessly — refuse loudly.
		m, ok := x.(map[string]any)
		if !ok {
			panic(fmt.Sprintf("encodeValue: unsupported value %T", x))
		}
		s, ok := m["v"].(string)
		if !ok || len(s) == 0 {
			panic(fmt.Sprintf("encodeValue: unsupported map leaf %v", m))
		}
		*b = append(*b, byte(valAny), s[0]-'a')
	}
}

func indexOf[T comparable](xs []T, x T) int {
	for i, v := range xs {
		if v == x {
			return i
		}
	}
	return 0
}

func keyByte(k string) byte {
	for i, c := range progKeys {
		if c == k {
			return byte(i)
		}
	}
	return 0
}

// fieldAny constructs a KindAny field (test-only; production uses
// event.appendAny for the canonical structured maps).
func fieldAny(key string, value any) Field {
	return Field{key: key, kind: KindAny, val: value}
}
