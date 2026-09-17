package unolog

// Record/encoder tests: canonical line shape, accessors, sinks, and
// the dedupe fuzz target.

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func recOf(level Level, msg string, fields ...Field) *Record {
	return &Record{level: level, msg: msg, fields: fields, completedAt: time.Date(2026, 9, 1, 10, 0, 0, 0, time.Local)}
}

func TestRecordTimeAccessor(t *testing.T) {
	at := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	r := recOf(LevelInfo, "m", fieldStr("k", "v"))
	r.completedAt = at
	if got := r.Time(); !got.Equal(at) {
		t.Fatalf("Time() = %v, want %v", got, at)
	}
}

func TestRecordFieldsZeroCopy(t *testing.T) {
	fields := []Field{fieldOf("a", 1), fieldOf("b", 2)}
	r := recOf(LevelInfo, "m", fields...)
	got := r.Fields()
	if &got[0] != &fields[0] {
		t.Fatal("Fields() copied the slice")
	}
	if len(got) != 2 {
		t.Fatalf("len = %d", len(got))
	}
}

func TestRecordLookup(t *testing.T) {
	r := recOf(LevelInfo, "m", fieldOf("k", 1), fieldOf("k", 2), fieldOf("s", "x"))
	if v, ok := r.Lookup("k"); !ok || v.(int64) != 2 {
		t.Fatalf("Lookup(k) = %v %v, want last value 2", v, ok)
	}
	if v, ok := r.Lookup("s"); !ok || v.(string) != "x" {
		t.Fatalf("Lookup(s) = %v %v", v, ok)
	}
	if _, ok := r.Lookup("missing"); ok {
		t.Fatal("missing key found")
	}
}

func TestRecordEncodedDedupLastWriteWins(t *testing.T) {
	r := recOf(
		LevelInfo, "m",
		fieldOf("k", 1),
		fieldOf("other", true),
		fieldOf("k", 2),
	)
	line := string(r.Encoded())
	if strings.Count(line, `"k":`) != 1 {
		t.Fatalf("duplicate key emitted more than once: %s", line)
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(line), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["k"] != float64(2) {
		t.Fatalf("k = %v, want 2 (last write)", payload["k"])
	}
}

func TestRecordEncodedWideDedupePath(t *testing.T) {
	fields := make([]Field, 0, 40)
	for i := range 30 {
		fields = append(fields, fieldOf(strings.Repeat("a", i+1), i))
	}
	fields = append(fields, fieldOf(strings.Repeat("a", 1), 999)) // dup of first key
	r := recOf(LevelInfo, "m", fields...)
	line := string(r.Encoded())
	if strings.Count(line, `"aa":`) != 1 {
		t.Fatalf("wide-path duplicate emitted more than once")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(line), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["a"] != float64(999) {
		t.Fatalf("a = %v, want last write 999", payload["a"])
	}
	if len(payload) != 33 { // 30 unique keys + level + time + message
		t.Fatalf("payload size = %d", len(payload))
	}
}

// TestFloat32WireParity pins the float32 kind: 0.1 must render as 0.1,
// not the widened double digits the v0 adapter never emitted.
func TestFloat32WireParity(t *testing.T) {
	r := recOf(LevelInfo, "m", fieldOf("f32", float32(0.1)), fieldOf("f64", 0.1))
	line := string(r.Encoded())
	if !strings.Contains(line, `"f32":0.1`) {
		t.Fatalf("float32 widened on the wire: %s", line)
	}
	if !strings.Contains(line, `"f64":0.1`) {
		t.Fatalf("float64 broken: %s", line)
	}
	// round-trip through the typed getter
	f := r.Fields()[0]
	if v, ok := f.Float(); !ok || math.Abs(v-0.1) > 1e-7 { // float64 getter: float32 epsilon
		t.Fatalf("Float() = %v %v", v, ok)
	}
	if _, isF32 := valueOf(f).(float32); !isF32 {
		t.Fatalf("valueOf lost float32-ness: %T", valueOf(f))
	}
}

// TestDedupeCrossover pins the 24/25-field boundary between the
// allocation-free scan and the slot-array path.
func TestDedupeCrossover(t *testing.T) {
	for _, n := range []int{23, 24, 25, 26, 33, 40} {
		fields := make([]Field, 0, n)
		for i := range n - 1 {
			fields = append(fields, fieldStr(strings.Repeat("k", i+2), "v"))
		}
		fields = append(fields, fieldStr("kk", "last")) // dup of the first key
		r := recOf(LevelInfo, "m", fields...)
		line := string(r.Encoded())
		if strings.Count(line, `"kk":`) != 1 {
			t.Fatalf("n=%d: dup emitted more than once", n)
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(line), &payload); err != nil {
			t.Fatalf("n=%d: %v", n, err)
		}
		if payload["kk"] != "last" {
			t.Fatalf("n=%d: last write lost: %v", n, payload["kk"])
		}
	}
}

func TestRecordEncodedShape(t *testing.T) {
	now := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	r := &Record{
		level:       LevelError,
		msg:         "job failed",
		fields:      []Field{fieldOf("op.code", 500), fieldOf("dur", 1500*time.Millisecond)},
		completedAt: now,
	}
	line := r.Encoded()
	if !bytes.HasSuffix(line, []byte("}\n")) {
		t.Fatalf("line must end with }\\n: %q", line)
	}
	var payload map[string]any
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["level"] != "error" {
		t.Fatalf("level = %v", payload["level"])
	}
	if payload["message"] != "job failed" {
		t.Fatalf("message = %v", payload["message"])
	}
	if ts, ok := payload["time"].(string); !ok || ts != "2026-09-01T10:00:00Z" {
		t.Fatalf("time = %v", payload["time"])
	}
	if payload["dur"] != 1500.0 { // float ms
		t.Fatalf("dur = %v", payload["dur"])
	}
	// ordering: fields before time before message
	if !(strings.Index(string(line), `"op.code"`) < strings.Index(string(line), `"time"`) &&
		strings.Index(string(line), `"time"`) < strings.Index(string(line), `"message"`)) {
		t.Fatalf("field order wrong: %s", line)
	}
}

// TestRecordEncodedOnce pins amendment 6: concurrent callers race
// benignly and share one encoded buffer.
func TestRecordEncodedOnce(t *testing.T) {
	fields := make([]Field, 16)
	for i := range fields {
		fields[i] = fieldOf("k"+strings.Repeat("x", i), i)
	}
	r := recOf(LevelInfo, "m", fields...)

	var wg sync.WaitGroup
	first := make([][]byte, 8)
	for i := range 8 {
		wg.Go(func() {
			first[i] = r.Encoded()
		})
	}
	wg.Wait()
	for i := 1; i < 8; i++ {
		if &first[i][0] != &first[0][0] {
			t.Fatalf("caller %d got a different buffer: encoding ran twice", i)
		}
	}
}

type errString string

func (e errString) Error() string { return string(e) }

// TestCanonicalCollisionRename: user "message"/"time"/"level" appear
// on the wire as fields.*, and the canonical envelope members stay
// unique.
func TestCanonicalCollisionRename(t *testing.T) {
	cases := []struct {
		userKey string
		wireKey string
		value   any
	}{
		{"message", "fields.message", "user-msg"},
		{"time", "fields.time", "user-time"},
		{"level", "fields.level", "user-level"},
	}
	for _, c := range cases {
		t.Run(c.userKey, func(t *testing.T) {
			r := recOf(LevelInfo, "m", fieldOf(c.userKey, c.value))
			line := string(r.Encoded())

			// The user value travels under the renamed fields.* key.
			var s string
			if raw := memberValue(t, line, c.wireKey); raw == nil {
				t.Fatalf("wire lacks %q: %s", c.wireKey, line)
			} else if err := json.Unmarshal(raw, &s); err != nil || s != c.value {
				t.Fatalf("%s = %s, want %q", c.wireKey, raw, c.value)
			}
			// The raw key appears exactly once — the canonical envelope
			// member, carrying the canonical value (no duplicate keys).
			if n := strings.Count(line, `"`+c.userKey+`":`); n != 1 {
				t.Fatalf("raw key %q appears %d times on the wire: %s", c.userKey, n, line)
			}
			wantCanonical := map[string]string{
				"message": "m",
				"time":    r.completedAt.Format(time.RFC3339), // recOf's fixed clock
				"level":   "info",
			}[c.userKey]
			if raw := memberValue(t, line, c.userKey); raw == nil || string(raw) != `"`+wantCanonical+`"` {
				t.Fatalf("canonical %q = %s, want %q: %s", c.userKey, raw, wantCanonical, line)
			}
		})
	}
}

// memberValue returns the raw value of the FIRST member with the key
// (nil when absent).
func memberValue(t *testing.T, line, key string) json.RawMessage {
	t.Helper()
	_, members := decodeLineStrict(t, []byte(line))
	for _, m := range members {
		if m.key == key {
			return m.val
		}
	}
	return nil
}

// TestCanonicalCollisionEnvelopeIntact: the canonical message/time/
// level values are untouched by a same-named user field, and the wire
// parses without duplicate keys.
func TestCanonicalCollisionEnvelopeIntact(t *testing.T) {
	ts := NewTestSink()
	rt := MustCompile(Config{Sink: ts, SamplingRate: 1})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "collide"})
	Add(op.Context(), "message", "user-message")
	Add(op.Context(), "time", "user-time")
	Add(op.Context(), "level", "user-level")
	op.End(nil)

	ev := ts.Events()[0]
	if ev.Message() != DefaultOperationMessage {
		t.Fatalf("message = %q, want the canonical default", ev.Message())
	}
	// the parsed map must have exactly one of each envelope key: parse
	// the captured record's canonical line
	line := capturedLine(t, ev)
	var payload map[string]any
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatalf("line not parseable: %v (%s)", err, line)
	}
	for _, k := range []string{"level", "time", "message"} {
		if _, ok := payload[k]; !ok {
			t.Fatalf("canonical %q missing from the parsed payload", k)
		}
	}
	if payload["message"] != DefaultOperationMessage {
		t.Fatalf("parsed message = %v, want the canonical default", payload["message"])
	}
	if payload["fields.message"] != "user-message" {
		t.Fatalf("fields.message = %v", payload["fields.message"])
	}
}

// TestCanonicalCollisionLWW: a user "message" and a user
// "fields.message" fold into ONE member — the encoder dedupes over
// the aliased keys (last write wins at its last occurrence).
func TestCanonicalCollisionLWW(t *testing.T) {
	// fields.message first, message later → message wins
	r := recOf(
		LevelInfo, "m",
		fieldOf("fields.message", "first"),
		fieldOf("message", "second"),
	)
	line := string(r.Encoded())
	if strings.Count(line, `"fields.message"`) != 1 {
		t.Fatalf("fields.message emitted %d times: %s", strings.Count(line, `"fields.message"`), line)
	}
	if !strings.Contains(line, `"fields.message":"second"`) {
		t.Fatalf("last write did not win: %s", line)
	}

	// message first, fields.message later → fields.message wins
	r = recOf(
		LevelInfo, "m",
		fieldOf("message", "first"),
		fieldOf("fields.message", "second"),
	)
	line = string(r.Encoded())
	if strings.Count(line, `"fields.message"`) != 1 || !strings.Contains(line, `"fields.message":"second"`) {
		t.Fatalf("last write did not win: %s", line)
	}
}

// TestCanonicalCollisionLookupKeepsRawKey: the rename is wire-only —
// the typed view keeps the user's original key.
func TestCanonicalCollisionLookupKeepsRawKey(t *testing.T) {
	r := recOf(LevelInfo, "m", fieldOf("message", "user-msg"))
	if v, ok := r.Lookup("message"); !ok || v != "user-msg" {
		t.Fatalf("Lookup(message) = %v %v", v, ok)
	}
	fields := r.Fields()
	if len(fields) != 1 || fields[0].Key() != "message" {
		t.Fatalf("Fields() = %v", fields)
	}
}

// capturedLine re-encodes a captured event for parsing (TestSink does
// not retain the wire bytes).
func capturedLine(t *testing.T, ev CapturedEvent) []byte {
	t.Helper()
	r := &Record{
		level: ev.Level(), msg: ev.Message(), fields: ev.Fields(),
		completedAt: time.Date(2026, 9, 1, 10, 0, 0, 0, time.Local),
	}
	return r.Encoded()
}

// Allocation gates for the record encode paths, in the shape of slog's
// TestJSONAllocs (log/slog/json_handler_test.go): exact or generous
// allocation budgets enforced in the ordinary test run, not left to
// benchstat discipline. The gated property is the dedupe crossover
// (record.go dedupeScanLimit = 24): events of up to 24 fields must
// encode without any allocation beyond the mandatory line buffer, and
// wider events take the seen-set path (which allocates by design).
// Found as a gap by mutation testing (M10): collapsing the crossover to
// 1 kept every behavior test green — only an alloc gate can see it.

func allocFields(n int) []Field {
	fields := make([]Field, 0, n)
	for i := range n {
		fields = append(fields, fieldStr(fmt.Sprintf("k%02d", i), "v"))
	}
	return fields
}

// TestRecordEncodeAllocNarrowPath pins the narrow side of the
// crossover: deduping and encoding 20 fields (one duplicate, so the
// last-wins scan actually runs) allocates zero — the whole point of the
// allocation-free scan. The dst is pre-sized past the encoded line so
// append growth cannot allocate.
func TestRecordEncodeAllocNarrowPath(t *testing.T) {
	fields := allocFields(19)
	fields = append(fields, fieldStr("k00", "last")) // duplicate of the first key
	if len(fields) != 20 {
		t.Fatalf("setup: %d fields, want 20", len(fields))
	}

	dst := make([]byte, 1, 4096)
	dst[0] = '{'
	if allocs := testing.AllocsPerRun(200, func() {
		appendDedupedFields(dst, fields)
	}); allocs != 0 {
		t.Fatalf("narrow path (20 fields) allocated %.0f times, want 0 — "+
			"the allocation-free scan no longer runs (dedupeScanLimit?)", allocs)
	}
}

// TestRecordEncodeAllocWidePath pins the wide side of the crossover:
// 25 fields must take the seen-set path (which allocates by design —
// the kept index slice), and 33 unique fields force the map handoff
// past the 32-slot stack array, which allocates beyond the kept slice.
// The field slices are built OUTSIDE the measured closures: setup
// allocations inside the closure would satisfy the gate vacuously for
// any crossover value (review finding DS-6). Together with
// TestRecordEncodeAllocNarrowPath the boundary sits between 20 and 25.
func TestRecordEncodeAllocWidePath(t *testing.T) {
	fields25 := allocFields(25)
	fields33 := allocFields(33)
	dst := make([]byte, 1, 4096)
	dst[0] = '{'

	allocs25 := testing.AllocsPerRun(200, func() {
		appendDedupedFields(dst, fields25)
	})
	if allocs25 < 1 {
		t.Fatalf("wide path (25 fields) allocated %.0f times, want >= 1 — "+
			"the crossover no longer routes wide events to the seen-set path", allocs25)
	}
	allocs33 := testing.AllocsPerRun(200, func() {
		appendDedupedFields(dst, fields33)
	})
	if allocs33 < 1 {
		t.Fatalf("wide path (33 unique fields) allocated %.0f times, want >= 1 "+
			"(the map handoff must allocate)", allocs33)
	}
	if allocs33 <= allocs25 {
		t.Fatalf("33 unique fields allocated %.0f times, want more than the 25-field "+
			"path (%.0f) — the 32-slot stack array did not hand off to the map", allocs33, allocs25)
	}
}

// TestRecordEncodeAllocFreshRecord is the end-to-end gate with a
// generous ceiling, slog-style: each measured run encodes a fresh
// 20-field record (a fresh *Record per run so the lazy-once publish in
// Encoded cannot mask the encode cost; the RFC3339 second cache is
// primed by AllocsPerRun's warmup call). The healthy baseline is 3
// allocs — the Record struct, the encodedLine node, and the line
// buffer. The ceiling is set wide (6) so ordinary churn never trips
// it, but a regression that falls back to per-field json.Marshal
// (which allocates ~44 for a 20-member map on this toolchain) fails
// loudly.
func TestRecordEncodeAllocFreshRecord(t *testing.T) {
	fields := allocFields(20)
	completedAt := time.Date(2026, 9, 1, 10, 0, 0, 0, time.Local)
	encode := func() {
		r := &Record{level: LevelInfo, msg: "m", fields: fields, completedAt: completedAt}
		_ = r.Encoded()
	}
	const ceiling = 6 // baseline 3: Record + encodedLine + line buffer
	if allocs := testing.AllocsPerRun(200, encode); allocs > ceiling {
		t.Fatalf("fresh 20-field record encode allocated %.0f times, want <= %d — "+
			"a json.Marshal-style regression added per-field allocations", allocs, ceiling)
	}
}

// TestRecordEncodeSizeMonotonicProperty generates a record, encodes
// it, appends one field with a brand-new key, and asserts the encoded
// line strictly grows.
func TestRecordEncodeSizeMonotonicProperty(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x51E5E5eed, 0x600D5eed))
	for width := 1; width <= 80; width++ {
		for iter := range 50 {
			used := map[string]bool{}
			fields := make([]Field, 0, width+1)
			for range width {
				key := fmt.Sprintf("k%03d", rng.IntN(width+5))
				if used[key] {
					continue // keep the generated list unique-keyed
				}
				used[key] = true
				kind := FieldKind(1 + rng.IntN(11)) // every value kind
				fields = append(fields, fieldOf(key, rtFieldValue(rng, kind)))
			}
			// find a brand-new key
			newKey := "k-new"
			for used[newKey] {
				newKey += "x"
			}
			base := recOf(LevelInfo, "m", fields...)
			grown := recOf(LevelInfo, "m", append(slices.Clone(fields), fieldOf(newKey, rtFieldValue(rng, FieldKind(1+rng.IntN(11)))))...)
			before, after := base.Encoded(), grown.Encoded()
			if !bytes.Contains(after, []byte(`"`+newKey+`":`)) {
				t.Fatalf("width %d iter %d: new key %q missing from %s", width, iter, newKey, after)
			}
			if len(after) <= len(before) {
				t.Fatalf("width %d iter %d: adding %q shrank the line (%d -> %d)\nbefore %s\nafter  %s",
					width, iter, newKey, len(before), len(after), before, after)
			}
		}
	}
}

func TestJSONSinkWritesEncodedRecord(t *testing.T) {
	var buf bytes.Buffer
	sink := NewJSONSink(&buf)
	rec := recOf(LevelError, "job failed", fieldOf("op.code", 500), fieldOf("dur", time.Second))
	sink.Write(context.Background(), rec)

	line := buf.Bytes()
	if !bytes.HasSuffix(line, []byte("}\n")) || bytes.Count(line, []byte("\n")) != 1 {
		t.Fatalf("one line expected, got %q", line)
	}
	var payload map[string]any
	if err := json.Unmarshal(line, &payload); err != nil {
		t.Fatal(err)
	}
	if payload["level"] != "error" || payload["message"] != "job failed" || payload["op.code"] != 500.0 {
		t.Fatalf("payload = %v", payload)
	}
	if _, err := time.Parse(time.RFC3339, payload["time"].(string)); err != nil {
		t.Fatalf("time not RFC3339: %v", payload["time"])
	}
}

func TestJSONSinkEncodeOnceReuse(t *testing.T) {
	var buf1, buf2 bytes.Buffer
	s1 := NewJSONSink(&buf1)
	s2 := NewJSONSink(&buf2)
	rec := recOf(LevelInfo, "m", fieldOf("k", 1))
	s1.Write(context.Background(), rec)
	s2.Write(context.Background(), rec)
	if !bytes.Equal(buf1.Bytes(), buf2.Bytes()) {
		t.Fatal("two sinks saw different encodings of the same record")
	}
}

func TestJSONSinkDefaultLevelMessage(t *testing.T) {
	var buf bytes.Buffer
	NewJSONSink(&buf).Write(context.Background(), recOf(Level(99), "", fieldOf("x", 1)))
	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["level"] != "info" {
		t.Fatalf("unknown level rendered %v", payload["level"])
	}
}

func TestJSONSinkFloatEdgesOnWire(t *testing.T) {
	var buf bytes.Buffer
	rec := recOf(
		LevelInfo, "f",
		fieldOf("nan", math.NaN()),
		fieldOf("pinf", math.Inf(1)),
		fieldOf("ninf", math.Inf(-1)),
		fieldOf("tiny", 1e-7),
	)
	NewJSONSink(&buf).Write(context.Background(), rec)
	var payload map[string]any
	if err := json.Unmarshal(buf.Bytes(), &payload); err != nil {
		t.Fatal(err)
	}
	if payload["nan"] != "NaN" || payload["pinf"] != "+Inf" || payload["ninf"] != "-Inf" {
		t.Fatalf("float edges = %v %v %v", payload["nan"], payload["pinf"], payload["ninf"])
	}
	if payload["tiny"] != 1e-7 {
		t.Fatalf("tiny = %v", payload["tiny"])
	}
}

func TestJSONSinkEscapesAndUnicode(t *testing.T) {
	var buf bytes.Buffer
	rec := recOf(
		LevelInfo, "m",
		fieldOf("url", "/search?q="+strings.Repeat("héllo☃", 8)),
		fieldOf("agent", `Mozilla/5.0 ("quote" back\slash)`),
		fieldOf("nul", "x\x00y"),
		fieldOf("del", "d\x7f"),
	)
	NewJSONSink(&buf).Write(context.Background(), rec)
	line := buf.String()
	if strings.Contains(line, "\x00") || strings.Contains(line, "\x7f") {
		t.Fatal("control bytes leaked unescaped")
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(line), &payload); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	if payload["nul"] != "x\x00y" || payload["del"] != "d\x7f" {
		t.Fatalf("round-trip broke: %v %v", payload["nul"], payload["del"])
	}
}

func TestJSONSinkConcurrency(t *testing.T) {
	var buf bytes.Buffer
	sink := NewJSONSink(&buf)
	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 100 {
				rec := recOf(LevelInfo, "concurrent", fieldOf("g", g), fieldOf("i", i))
				sink.Write(context.Background(), rec)
			}
		})
	}
	wg.Wait()
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 800 {
		t.Fatalf("lines = %d", len(lines))
	}
	for _, ln := range lines {
		var payload map[string]any
		if err := json.Unmarshal([]byte(ln), &payload); err != nil {
			t.Fatalf("corrupt line: %q", ln)
		}
	}
}

func TestJSONSinkNilSafety(t *testing.T) {
	var sink *JSONSink
	sink.Write(context.Background(), recOf(LevelInfo, "m")) // no panic
	NewJSONSink(nil).Write(context.Background(), recOf(LevelInfo, "m"))
}

func TestTestSinkCapture(t *testing.T) {
	ts := NewTestSink()
	rec := recOf(LevelWarn, "m", fieldOf("i", 1), fieldOf("s", "x"), fieldOf("any", map[string]any{"k": 1}))
	ts.Write(context.Background(), rec)

	events := ts.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d", len(events))
	}
	ev := events[0]
	if ev.Level() != LevelWarn || ev.Message() != "m" {
		t.Fatalf("event = %v %v", ev.Level(), ev.Message())
	}
	if v, ok := ev.Lookup("i"); !ok || v.(int64) != 1 {
		t.Fatalf("i = %v", v)
	}
	if v, _ := ev.Lookup("any"); v.(map[string]any)["k"] != 1 {
		// map values are deep-copied on capture; originals may mutate freely
		t.Fatalf("any = %v", v)
	}
}

func TestTestSinkCopiesMutableValues(t *testing.T) {
	ts := NewTestSink()
	shared := map[string]any{"k": 1}
	ts.Write(context.Background(), recOf(LevelInfo, "m", fieldOf("m", shared)))
	shared["k"] = 999 // mutate after capture
	ev := ts.Events()[0]
	v, _ := ev.Lookup("m")
	if v.(map[string]any)["k"] != 1 {
		t.Fatal("capture retained a reference to caller state")
	}
	ts.Reset()
	if len(ts.Events()) != 0 {
		t.Fatal("Reset did not clear")
	}
}

// FuzzDedupeFields pins the canonical dedupe: every key emitted once,
// at its last-occurrence position, with its last value.
//
// Canonical-key collisions (user fields named "message"/"time"/"level")
// follow the logrus rename policy (record.go aliasKey): colliding user
// keys are emitted as "fields.message"/"fields.time"/"fields.level" so
// the wire never carries duplicate envelope keys. The envelope keys
// occupy fixed slots — level is member 0, time and message are the
// last two members — and the user span sits strictly between them. The
// oracle folds over the ALIASED keys (a user "message" and a user
// "fields.message" resolve as one last-write-wins member), matching
// the encoder's dedupe.

// dedupeKeys is the small key alphabet: k0-k3 for dense duplicates plus
// the canonical-colliding names and a wide unique tail.
var dedupeKeys = []string{"k0", "k1", "k2", "k3", "message", "time", "level", "zz"}

// FuzzDedupeFields generates field lists from three byte streams:
// keys picks the per-field key (alphabet size controls duplicate
// density), vals picks the per-field value kind and operand, widthRaw
// picks the field count.
func FuzzDedupeFields(f *testing.F) {
	// Width boundary seeds: 0, 1, 23/24/25/26 (the crossover), 31/32/33
	// (the seenArr→map handoff), 40, 64, 80, with dense duplicates.
	for _, w := range []int{0, 1, 23, 24, 25, 26, 31, 32, 33, 40, 64, 80} {
		// byte 0 carries the width; byte 1 picks a 4-key alphabet
		// (size = 1 + keyBytes[1]), and keys cycle k0-k3 densely.
		keys := make([]byte, w+1)
		vals := make([]byte, w+1)
		keys[0] = byte(w)
		if w >= 1 {
			keys[1] = 3 // 4-key alphabet for non-empty streams
		}
		for i := 2; i <= w; i++ {
			keys[i] = byte((i - 2) % 4)
			vals[i] = byte(i)
		}
		f.Add(keys, vals)
	}
	// all-same-key and canonical-collision seeds (width in byte 0).
	f.Add([]byte{5, 7, 7, 7, 7, 7}, []byte{0, 1, 2, 3, 4, 5})                     // width 5, all "zz"
	f.Add([]byte{3, 4, 4, 4}, []byte{0, 1, 2, 3})                                 // width 3, all "message"
	f.Add([]byte{3, 5, 5, 5}, []byte{0, 1, 2, 3})                                 // width 3, all "time"
	f.Add([]byte{3, 6, 6, 6}, []byte{0, 1, 2, 3})                                 // width 3, all "level"
	f.Add([]byte{6, 0, 0, 1, 2, 3, 0}, []byte{0, 1, 9, 9, 9, 9, 2})               // width 6, dup at first+last
	f.Add([]byte{}, []byte{})                                                     // empty program
	f.Add([]byte{7, 7, 1, 2, 3, 4, 5, 6}, []byte{10, 10, 10, 10, 10, 10, 10, 10}) // width 7, seven unique keys (8-key alphabet)

	f.Fuzz(func(t *testing.T, keyBytes []byte, valBytes []byte) {
		fields := dedupeFieldsFromBytes(keyBytes, valBytes)
		verifyDedupeFields(t, fields)
	})
}

// dedupeFieldsFromBytes builds the generated field list. The key
// alphabet size is derived from keyBytes so the fuzzer explores every
// duplicate density: size 1 is 100% duplicates, size == width is 0%.
func dedupeFieldsFromBytes(keyBytes, valBytes []byte) []Field {
	width := 0
	if len(keyBytes) > 0 {
		width = int(keyBytes[0]) % 81 // 0-80 fields
	}
	size := 1
	if len(keyBytes) > 1 {
		size = 1 + int(keyBytes[1])%len(dedupeKeys) // 1-8 keys
	}
	fields := make([]Field, 0, width)
	for i := range width {
		var kb byte
		if len(keyBytes) > 1 {
			kb = keyBytes[1+i%(len(keyBytes)-1)]
		}
		key := dedupeKeys[int(kb)%size]
		var vb byte
		if len(valBytes) > 0 {
			vb = valBytes[i%len(valBytes)]
		}
		fields = append(fields, dedupeField(key, vb, i))
	}
	return fields
}

func dedupeField(key string, b byte, i int) Field {
	switch b % 5 {
	case 0:
		return fieldOf(key, int64(int8(b))*int64(i+1))
	case 1:
		return fieldOf(key, fmt.Sprintf("v-%d-%d", i, b))
	case 2:
		return fieldOf(key, float64(int8(b))/7)
	case 3:
		return fieldOf(key, b&1 == 0)
	default:
		return fieldOf(key, time.Duration(int64(int8(b)))*time.Millisecond)
	}
}

// verifyDedupeFields checks one generated field list against the
// reference fold. The user span sits strictly between the envelope
// slots (level at member 0; time and message the last two members), so
// canonical-colliding user keys are compared inside the span and the
// envelope pins stay position-based.
func verifyDedupeFields(t *testing.T, fields []Field) {
	t.Helper()
	r := recOf(LevelInfo, "m", fields...)
	line := r.Encoded()
	if !json.Valid(line) {
		t.Fatalf("encoded line is not valid JSON: %q", line)
	}

	_, members := decodeLineStrict(t, line)
	// Envelope slots: level first, time/message last.
	if len(members) < 3 {
		t.Fatalf("wire has %d members, want >= 3 (level, time, message): %s", len(members), line)
	}
	if members[0].key != "level" {
		t.Fatalf("member 0 = %q, want the canonical level", members[0].key)
	}
	last, secondLast := members[len(members)-1], members[len(members)-2]
	if last.key != "message" || secondLast.key != "time" {
		t.Fatalf("last two members = %q, %q, want time, message", secondLast.key, last.key)
	}
	userSpan := members[1 : len(members)-2]

	// Reference fold over the ALIASED keys: each aliased key's last
	// write, in last-occurrence order (record.go aliases colliding user
	// keys to fields.* before deduping, so the fold keyspace is the
	// aliased one).
	type lw struct {
		field Field
		pos   int
	}
	lastWrite := map[string]lw{}
	for i, f := range fields {
		key := aliasedFieldKey(f.key)
		lastWrite[key] = lw{f, i}
	}
	order := make([]int, 0, len(lastWrite))
	for _, v := range lastWrite {
		order = append(order, v.pos)
	}
	for i := 1; i < len(order); i++ {
		for j := i; j > 0 && order[j] < order[j-1]; j-- {
			order[j], order[j-1] = order[j-1], order[j]
		}
	}

	if len(userSpan) != len(order) {
		t.Fatalf("user span has %d members, want %d unique keys (%s)",
			len(userSpan), len(order), line)
	}
	for i, pos := range order {
		want := lastWrite[aliasedFieldKey(fields[pos].key)].field
		got := userSpan[i]
		if got.key != aliasedFieldKey(want.key) {
			t.Fatalf("user member %d = %q, want %q (last-occurrence order)", i, got.key, aliasedFieldKey(want.key))
		}
		if err := checkFieldWire(want, got.val); err != nil {
			t.Fatalf("user member %d: %v", i, err)
		}
	}
}

// TestDedupeFieldsProperty runs the same oracle over PCG-generated
// field lists — seed-only coverage without the fuzzer. Total: the
// full width × density matrix (81 widths 0-80 × 4 alphabet sizes =
// 324 combos) plus 500 random extra sweeps = 824 checks.
func TestDedupeFieldsProperty(t *testing.T) {
	rng := rand.New(rand.NewPCG(0xDEDE5eed, 0xF1E1D5E))
	check := func(width int, size int) {
		fields := make([]Field, 0, width)
		for i := range width {
			key := dedupeKeys[rng.IntN(size)]
			fields = append(fields, dedupeField(key, byte(rng.Uint64()), i))
		}
		verifyDedupeFields(t, fields)
	}
	for width := range 81 {
		for _, size := range []int{1, 2, 4, 8} {
			check(width, size)
		}
	}
	// random extra sweeps
	for range 500 {
		check(rng.IntN(81), 1+rng.IntN(8))
	}
}
