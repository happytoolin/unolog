package json

import (
	"bytes"
	stdjson "encoding/json"
	"math"
	"strconv"
	"sync"
	"testing"
	"time"
)

// float64Tests and float32Tests are ported from zerolog v1.35.0's
// internal/json/float_test.go (MIT — same lineage as the vendored
// encoder), extended with the −0 and float32-precision cases the DST
// research flagged (V2_PLAN §05 / dst-research §6.4).
var float64Tests = []struct {
	name string
	val  float64
	want string
}{
	{"Positive integer", 1234.0, "1234"},
	{"Negative integer", -5678.0, "-5678"},
	{"Positive decimal", 12.3456, "12.3456"},
	{"Negative decimal", -78.9012, "-78.9012"},
	{"Large positive number", 123456789.0, "123456789"},
	{"Large negative number", -987654321.0, "-987654321"},
	{"Zero", 0.0, "0"},
	{"Negative zero", math.Copysign(0, -1), "-0"},
	{"Smallest positive value", math.SmallestNonzeroFloat64, "5e-324"},
	{"Largest positive value", math.MaxFloat64, "1.7976931348623157e+308"},
	{"Smallest negative value", -math.SmallestNonzeroFloat64, "-5e-324"},
	{"Largest negative value", -math.MaxFloat64, "-1.7976931348623157e+308"},
	{"NaN", math.NaN(), `"NaN"`},
	{"+Inf", math.Inf(1), `"+Inf"`},
	{"-Inf", math.Inf(-1), `"-Inf"`},
	{"Clean up e-09 to e-9 case 1", 1e-9, "1e-9"},
	{"Clean up e-09 to e-9 case 2", -2.236734e-9, "-2.236734e-9"},
	{"2^53 boundary", 1 << 53, "9007199254740992"},
	{"2^53 plus one ulp", float64(1<<53) + 2, "9007199254740994"},
}

var float32Tests = []struct {
	name string
	val  float32
	want string
}{
	{"Positive integer", 1234.0, "1234"},
	{"Negative integer", -5678.0, "-5678"},
	{"Positive decimal", 12.3456, "12.3456"},
	{"Negative decimal", -78.9012, "-78.9012"},
	{"Large positive number", 123456789.0, "123456790"},
	{"Large negative number", -987654321.0, "-987654340"},
	{"Zero", 0.0, "0"},
	{"Negative zero", float32(math.Copysign(0, -1)), "-0"},
	{"Smallest positive value", math.SmallestNonzeroFloat32, "1e-45"},
	{"Largest positive value", math.MaxFloat32, "3.4028235e+38"},
	{"Smallest negative value", -math.SmallestNonzeroFloat32, "-1e-45"},
	{"Largest negative value", -math.MaxFloat32, "-3.4028235e+38"},
	{"NaN", float32(math.NaN()), `"NaN"`},
	{"+Inf", float32(math.Inf(1)), `"+Inf"`},
	{"-Inf", float32(math.Inf(-1)), `"-Inf"`},
	{"Clean up e-09 to e-9 case 1", 1e-9, "1e-9"},
	{"Clean up e-09 to e-9 case 2", -2.236734e-9, "-2.236734e-9"},
	// the float32-preserving wire contract: 0.1 must not widen to
	// 0.10000000149011612 (v0 adapter parity; slog bridge documents the
	// host limitation)
	{"Float32 precision preserved", 0.1, "0.1"},
}

func TestAppendFloat64Table(t *testing.T) {
	for _, tc := range float64Tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := string((Encoder{}).AppendFloat64(nil, tc.val, -1)); got != tc.want {
				t.Errorf("AppendFloat64(%v) = %s, want %s", tc.val, got, tc.want)
			}
		})
	}
}

func TestAppendFloat32Table(t *testing.T) {
	for _, tc := range float32Tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := string((Encoder{}).AppendFloat32(nil, tc.val, -1)); got != tc.want {
				t.Errorf("AppendFloat32(%v) = %s, want %s", tc.val, got, tc.want)
			}
		})
	}
}

// FuzzAppendFloat64 checks the vendored float policy continuously: for
// NaN/±Inf the three quoted-string shapes, for finite values exact
// parity with encoding/json and an exact stdjson.Unmarshal round-trip
// (the DST research's shared-blind-spot closer for the numeric path —
// a differential-only oracle could not catch a policy both paths got
// wrong).
func FuzzAppendFloat64(f *testing.F) {
	for _, tc := range float64Tests {
		f.Add(tc.val)
	}
	f.Add(0.1)
	f.Add(-0.1)
	f.Add(math.Float64frombits(0x7ff0000000000001)) // signaling NaN payload
	f.Add(math.Float64frombits(0x7ff8000000000000)) // quiet NaN
	f.Fuzz(func(t *testing.T, val float64) {
		actual := (Encoder{}).AppendFloat64(nil, val, -1)
		if len(actual) == 0 {
			t.Fatal("empty buffer")
		}
		if actual[0] == '"' {
			switch string(actual) {
			case `"NaN"`:
				if !math.IsNaN(val) {
					t.Fatalf("expected %v got NaN", val)
				}
			case `"+Inf"`:
				if !math.IsInf(val, 1) {
					t.Fatalf("expected %v got +Inf", val)
				}
			case `"-Inf"`:
				if !math.IsInf(val, -1) {
					t.Fatalf("expected %v got -Inf", val)
				}
			default:
				t.Fatalf("unexpected string rendering: %s", actual)
			}
			return
		}
		if expected, err := stdjson.Marshal(val); err != nil {
			t.Error(err)
		} else if !bytes.Equal(actual, expected) {
			t.Errorf("stdjson.Marshal parity: expected %s, got %s", expected, actual)
		}
		var parsed float64
		if err := stdjson.Unmarshal(actual, &parsed); err != nil {
			t.Fatal(err)
		}
		if parsed != val && !(parsed != parsed && val != val) { // NaN already handled above
			t.Fatalf("round-trip: expected %v, got %v (wire %s)", val, parsed, actual)
		}
		// −0 must survive the round trip bitwise (sign preserved)
		if val == 0 && math.Signbit(val) && !math.Signbit(parsed) {
			t.Fatalf("negative zero lost: wire %s", actual)
		}
	})
}

// FuzzAppendFloat32 is the float32 mirror; the round-trip comparison is
// done in float64 after re-widening, matching the wire contract (the
// bytes are parsed as a JSON number, then compared against the original
// float32 value widened — exact equality, no tolerance).
func FuzzAppendFloat32(f *testing.F) {
	for _, tc := range float32Tests {
		f.Add(tc.val)
	}
	f.Add(float32(0.1))
	f.Add(float32(1) / 3)
	f.Fuzz(func(t *testing.T, val float32) {
		actual := (Encoder{}).AppendFloat32(nil, val, -1)
		if len(actual) == 0 {
			t.Fatal("empty buffer")
		}
		if actual[0] == '"' {
			switch string(actual) {
			case `"NaN"`:
				if !math.IsNaN(float64(val)) {
					t.Fatalf("expected %v got NaN", val)
				}
			case `"+Inf"`:
				if !math.IsInf(float64(val), 1) {
					t.Fatalf("expected %v got +Inf", val)
				}
			case `"-Inf"`:
				if !math.IsInf(float64(val), -1) {
					t.Fatalf("expected %v got -Inf", val)
				}
			default:
				t.Fatalf("unexpected string rendering: %s", actual)
			}
			return
		}
		if expected, err := stdjson.Marshal(val); err != nil {
			t.Error(err)
		} else if !bytes.Equal(actual, expected) {
			t.Errorf("stdjson.Marshal parity: expected %s, got %s", expected, actual)
		}
		var parsed32 float32
		if err := stdjson.Unmarshal(actual, &parsed32); err != nil {
			t.Fatal(err)
		}
		// the wire contract is 32-bit: parse back at float32 width and
		// require exact equality (parsing the shortest-32 decimal as
		// float64 would land on a different float64 than the widened
		// value — that gap is expected and not a bug)
		if parsed32 != val && !(parsed32 != parsed32 && val != val) {
			t.Fatalf("round-trip: expected %v, got %v (wire %s)", val, parsed32, actual)
		}
	})
}

func TestAppendTimeFormats(t *testing.T) {
	cases := []struct {
		t      time.Time
		format string
		want   string
	}{
		{time.Date(2026, 8, 30, 12, 34, 56, 0, time.UTC), time.RFC3339, `"2026-08-30T12:34:56Z"`},
		{time.Date(2026, 8, 30, 12, 34, 56, 0, time.FixedZone("CET", 2*3600)), time.RFC3339, `"2026-08-30T12:34:56+02:00"`},
		{time.Date(2026, 8, 30, 12, 34, 56, 123456789, time.UTC), time.RFC3339, `"2026-08-30T12:34:56Z"`},
		{time.Date(2026, 8, 30, 12, 34, 56, 0, time.UTC), "2006-01-02", `"2026-08-30"`},
	}
	for _, c := range cases {
		if got := string(Encoder{}.AppendTime(nil, c.t, c.format)); got != c.want {
			t.Errorf("AppendTime(%v, %q) = %s, want %s", c.t, c.format, got, c.want)
		}
	}
}

func TestAppendDuration(t *testing.T) {
	e := Encoder{}
	cases := []struct {
		got  string
		want string
	}{
		// zerolog adapter defaults: float milliseconds, shortest precision
		{string(e.AppendDuration(nil, 1500*time.Millisecond, time.Millisecond, false, -1)), "1500"},
		{string(e.AppendDuration(nil, 2500*time.Microsecond, time.Millisecond, false, -1)), "2.5"},
		{string(e.AppendDuration(nil, 0, time.Millisecond, false, -1)), "0"},
		{string(e.AppendDuration(nil, 123456789*time.Nanosecond, time.Millisecond, false, -1)), "123.456789"},
		{string(e.AppendDuration(nil, 1500*time.Millisecond, time.Millisecond, true, -1)), "1500"},
		{string(e.AppendDuration(nil, time.Second, time.Second, false, -1)), "1"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("AppendDuration = %s, want %s", c.got, c.want)
		}
	}
}

func TestAppendTimeRFC3339Cached(t *testing.T) {
	// prime the cache with a known local second
	base := time.Now().Truncate(time.Second)
	same := base.Add(900 * time.Millisecond) // same wall second

	a := AppendTimeRFC3339(nil, base)
	b := AppendTimeRFC3339(nil, same)
	if !bytes.Equal(a, b) {
		t.Fatalf("same-second renders differ: %s vs %s", a, b)
	}
	want := string(Encoder{}.AppendTime(nil, base, time.RFC3339))
	if string(a) != want {
		t.Fatalf("cached render %s != Format %s", a, want)
	}

	// next second must re-render
	next := base.Add(time.Second + time.Millisecond)
	c := AppendTimeRFC3339(nil, next)
	wantNext := string(Encoder{}.AppendTime(nil, next, time.RFC3339))
	if string(c) != wantNext {
		t.Fatalf("next-second render %s != %s", c, wantNext)
	}
	if base.Add(time.Second).Format(time.RFC3339) == base.Format(time.RFC3339) {
		t.Fatalf("test clock granularity too coarse to observe rollover")
	}

	// appends into non-empty dst keep the prefix
	dst := AppendTimeRFC3339([]byte(`{"time":`), base)
	if !bytes.HasPrefix(dst, []byte(`{"time":`)) || dst[len(dst)-1] != '"' {
		t.Fatalf("prefix/suffix mangled: %s", dst)
	}
}

func TestAppendTimeRFC3339CachedConcurrent(t *testing.T) {
	base := time.Now().Truncate(time.Second)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for j := range 1000 {
				tt := base.Add(time.Duration(j%1000) * time.Millisecond)
				got := string(AppendTimeRFC3339(nil, tt))
				want := `"` + tt.Format(time.RFC3339) + `"`
				if got != want {
					t.Errorf("cached render %s != Format %s", got, want)
					return
				}
			}
		})
	}
	wg.Wait()
}

// FuzzAppendTimeRFC3339 checks the cached formatter continuously
// (dst-research §6.4): for times expressed in the process's local zone
// — the documented cache contract — the output must equal
// time.Format(time.RFC3339) exactly, whatever the second, and must
// parse back to the same wall-clock second. Times from other zones go
// through the documented-caveat path (the cache may reuse the local
// rendering), so for those the oracle is parse-back-to-the-same-second
// only, which is the guarantee downstream parsers rely on.
func FuzzAppendTimeRFC3339(f *testing.F) {
	seeds := []time.Time{
		time.Unix(0, 0),
		time.Unix(-1, 0),
		time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC),
		time.Date(2026, 9, 1, 10, 0, 0, 999999999, time.UTC),
		time.Date(2026, 9, 1, 10, 0, 0, 1, time.FixedZone("+14", 14*3600)),
		time.Date(2026, 9, 1, 10, 0, 0, 0, time.FixedZone("-12", -12*3600)),
		time.Now(),
	}
	for _, s := range seeds {
		f.Add(s.Unix(), int32(s.Nanosecond()), uint8(0))
	}
	f.Fuzz(func(t *testing.T, sec int64, nsec int32, zoneChoice uint8) {
		// keep years within RFC3339's four-digit range so the parse-back
		// oracle is well-defined for every generated instant — including
		// after conversion to the +14:00 extreme zone (16h headroom)
		const year9999 = 253402300799 // 9999-12-31T23:59:59Z
		if sec < 0 || sec > year9999-16*3600 {
			sec %= year9999 - 16*3600 + 1
			if sec < 0 {
				sec += year9999 - 16*3600 + 1
			}
		}
		if nsec < 0 {
			nsec = -nsec
		}
		inst := time.Unix(sec, int64(nsec))
		if zoneChoice&1 == 0 {
			local := inst.In(time.Local)
			// the cache is shared process-wide and keyed by Unix second, so a
			// prior iteration in another zone could pollute this second (the
			// documented limitation); force a miss to test the fresh-format
			// path, then call again to pin the cached-hit path.
			rfc3339Cache.Store(&rfc3339Second{sec: -1 << 62})
			got := string(AppendTimeRFC3339(nil, local))
			want := `"` + local.Format(time.RFC3339) + `"`
			if got != want {
				t.Fatalf("local-zone render %s != Format %s (t=%v)", got, want, local)
			}
			if hit := string(AppendTimeRFC3339(nil, local)); hit != want {
				t.Fatalf("cached render %s != Format %s (t=%v)", hit, want, local)
			}
		} else {
			zones := []*time.Location{
				time.UTC,
				time.FixedZone("+14", 14*3600),
				time.FixedZone("-12", -12*3600),
				time.FixedZone("+05:30", 5*3600+1800),
			}
			other := inst.In(zones[int(zoneChoice)%len(zones)])
			got := string(AppendTimeRFC3339(nil, other))
			var s string
			if err := stdjson.Unmarshal([]byte(got), &s); err != nil {
				t.Fatalf("not a valid JSON string: %v (%q)", err, got)
			}
			parsed, err := time.Parse(time.RFC3339, s)
			if err != nil {
				t.Fatalf("rendering %q is not RFC3339: %v", s, err)
			}
			// second precision: whatever rendering the cache served, the
			// parsed instant must be the same second (sub-second dropped)
			if parsed.Unix() != other.Unix() {
				t.Fatalf("parsed second %d != %d (rendering %q)", parsed.Unix(), other.Unix(), s)
			}
		}
	})
}

// TestAppendTypes ports the relevant zerolog v1.34.0 internal/json type
// encoding tests, covering every append the unolog sink uses.
func TestAppendTypes(t *testing.T) {
	// bools
	if got := string(Encoder{}.AppendBool(nil, true)); got != "true" {
		t.Errorf("AppendBool(true) = %s", got)
	}
	if got := string(Encoder{}.AppendBool(nil, false)); got != "false" {
		t.Errorf("AppendBool(false) = %s", got)
	}

	// ints, each width, extremes
	intCases := []struct {
		got string
		n   int64
	}{
		{string(Encoder{}.AppendInt(nil, 0)), 0},
		{string(Encoder{}.AppendInt(nil, -1)), -1},
		{string(Encoder{}.AppendInt(nil, math.MaxInt)), math.MaxInt},
		{string(Encoder{}.AppendInt(nil, math.MinInt)), math.MinInt},
		{string(Encoder{}.AppendInt8(nil, math.MaxInt8)), math.MaxInt8},
		{string(Encoder{}.AppendInt8(nil, math.MinInt8)), math.MinInt8},
		{string(Encoder{}.AppendInt16(nil, math.MaxInt16)), math.MaxInt16},
		{string(Encoder{}.AppendInt32(nil, math.MaxInt32)), math.MaxInt32},
		{string(Encoder{}.AppendInt64(nil, math.MinInt64)), math.MinInt64},
	}
	for _, c := range intCases {
		if want := itoa(c.n); c.got != want {
			t.Errorf("int append = %s, want %s", c.got, want)
		}
	}

	// uints, each width, extremes
	uintCases := []struct {
		got string
		n   uint64
	}{
		{string(Encoder{}.AppendUint(nil, 0)), 0},
		{string(Encoder{}.AppendUint(nil, 42)), 42},
		{string(Encoder{}.AppendUint8(nil, math.MaxUint8)), math.MaxUint8},
		{string(Encoder{}.AppendUint16(nil, math.MaxUint16)), math.MaxUint16},
		{string(Encoder{}.AppendUint32(nil, math.MaxUint32)), math.MaxUint32},
		{string(Encoder{}.AppendUint64(nil, math.MaxUint64)), math.MaxUint64},
	}
	for _, c := range uintCases {
		if want := utoa(c.n); c.got != want {
			t.Errorf("uint append = %s, want %s", c.got, want)
		}
	}

	// nil and markers
	if got := string(Encoder{}.AppendNil(nil)); got != "null" {
		t.Errorf("AppendNil = %s", got)
	}
	if got := string(Encoder{}.AppendLineBreak(nil)); got != "\n" {
		t.Errorf("AppendLineBreak = %q", got)
	}
}

func itoa(n int64) string  { return strconv.FormatInt(n, 10) }
func utoa(u uint64) string { return strconv.FormatUint(u, 10) }

func TestAppendFloats(t *testing.T) {
	cases := []struct {
		got  string
		want string
	}{
		{string(Encoder{}.AppendFloat64(nil, 0, -1)), "0"},
		{string(Encoder{}.AppendFloat64(nil, -1, -1)), "-1"},
		{string(Encoder{}.AppendFloat64(nil, 1e-7, -1)), "1e-7"},
		{string(Encoder{}.AppendFloat64(nil, 1e-9, -1)), "1e-9"},
		{string(Encoder{}.AppendFloat64(nil, 1e21, -1)), "1e+21"},
		{string(Encoder{}.AppendFloat64(nil, 0.000001, -1)), "0.000001"},
		{string(Encoder{}.AppendFloat64(nil, 0.0000001, -1)), "1e-7"},
		{string(Encoder{}.AppendFloat32(nil, 1.5, -1)), "1.5"},
		{string(Encoder{}.AppendFloat64(nil, math.NaN(), -1)), `"NaN"`},
		{string(Encoder{}.AppendFloat64(nil, math.Inf(1), -1)), `"+Inf"`},
		{string(Encoder{}.AppendFloat64(nil, math.Inf(-1), -1)), `"-Inf"`},
		{string(Encoder{}.AppendFloat64(nil, 1.25, 2)), "1.25"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("float append = %s, want %s", c.got, c.want)
		}
	}
}

// TestAppendInterfaceNoHTMLEscape pins the any-fallback bytes to
// zerolog's InterfaceMarshalFunc semantics: <, >, & emitted raw (std
// json.Marshal would emit \u003c — parses equal, differs on the wire)
// and no trailing newline from json.Encoder.Encode.
func TestAppendInterfaceNoHTMLEscape(t *testing.T) {
	cases := []struct {
		val  any
		want string
	}{
		{map[string]any{"note": "<b>a&b</b>"}, `{"note":"<b>a&b</b>"}`},
		{map[string]any{"u": "café ☃"}, `{"u":"café ☃"}`},
		{[]any{"<x>", 1}, `["<x>",1]`},
		{map[string]any{"script": "</script><script>"}, `{"script":"</script><script>"}`},
	}
	for _, c := range cases {
		got := string(Encoder{}.AppendInterface(nil, c.val))
		if got != c.want {
			t.Errorf("AppendInterface(%v) = %s, want %s (HTML escaping or newline leaked)", c.val, got, c.want)
		}
	}
}

func TestAppendInterface(t *testing.T) {
	cases := []struct {
		val  any
		want string
	}{
		{nil, "null"},
		{map[string]any{"a": 1, "b": "two"}, `{"a":1,"b":"two"}`},
		{[]any{1, "x"}, `[1,"x"]`},
		{struct {
			A int    `json:"a"`
			B string `json:"b"`
		}{1, "s"}, `{"a":1,"b":"s"}`},
	}
	for _, c := range cases {
		got := string(Encoder{}.AppendInterface(nil, c.val))
		if got != c.want {
			t.Errorf("AppendInterface(%v) = %s, want %s", c.val, got, c.want)
		}
	}
}

func TestAppendKey(t *testing.T) {
	e := Encoder{}
	dst := e.AppendBeginMarker(nil)
	dst = e.AppendKey(dst, "level")
	if string(dst) != `{"level":` {
		t.Fatalf("first key = %s", dst)
	}
	dst = append(dst, '1')
	dst = e.AppendKey(dst, "msg")
	if string(dst) != `{"level":1,"msg":` {
		t.Fatalf("second key = %s", dst)
	}
}
