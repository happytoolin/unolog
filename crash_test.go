package unolog

// crash_test.go — the adversarial crash-test suites for the v2 core,
// consolidated from the two crash-testing rounds. Each section is one
// independent workstream (agent) with its own charter; helpers and
// fixtures are shared at package scope. The module-local legs (nil
// abuse per bridge) live in each adapter's crash_test.go; the std
// passthrough tests live with the middleware and wire tests.
//
//   A  panic torture (sampler/marshal/sink panics, panic(nil), one-shot)
//   B  concurrency hammer (straggler storms, recycling, shared runtime)
//   C  encoder abuse (UTF-8, control chars, cycles, raw injection)
//   D  pool lifecycle (reuse, abandonment, cap boundary, retention)
//   E  config/API misuse (hostile rates, nil receivers, bad tails)
//   G  time and metadata abuse (extreme clocks, hostile start metadata)
//   H  deep and wide payloads (nesting, mega fields, huge strings)
//   I  lifecycle misuse (chains, scrambled ends, stale contexts)
//   J  sink contract edges (Encoded race, fanout, recycle hazards)
//   K  adversarial fuzz targets (values, guarded interleavings)
//   L  guarded-mode DST (mixed writers vs single End, snapshot-vs-seal)
//   N  testing/synctest bubbles (per-test goroutine hygiene, watchdog)
//      (M was the ad-hoc live-chaos runs; intentionally not a file here)

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"unicode/utf8"
)

// ════════════════════════════════════════════════════════════════════
// Agent A — panic torture
// ════════════════════════════════════════════════════════════════════

// panickingSampler explodes during commit — inside End, after seal.
func TestCrashPanicInSamplerDuringCommit(t *testing.T) {
	rt, _ := testRT(t, func(c *Config) {
		c.Sampler = func(in SampleInput) bool { panic("sampler boom") }
	})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Add(op.Context(), "k", "v")

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Fatal("sampler panic must propagate out of End")
			}
		}()
		_ = op.End(nil)
	}()

	// Second End is the published no-op, and the pool still works.
	if op.End(nil) {
		t.Fatal("second End after crash must report not-emitted")
	}
	rt2, ts2 := testRT(t, nil)
	op2 := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "j2"})
	Add(op2.Context(), "after", "crash")
	if !op2.End(nil) || len(ts2.Events()) != 1 {
		t.Fatal("pool unusable after sampler panic")
	}
	if v, _ := ts2.Events()[0].Lookup("after"); v != "crash" {
		t.Fatalf("post-crash event corrupted: %v", v)
	}
}

// boomMarshal panics inside MarshalJSON. encoding/json contains
// marshaler panics as errors, so the pipeline must survive: the event
// is emitted with a marshaling-error string instead of crashing.
type boomMarshal struct{}

func (boomMarshal) MarshalJSON() ([]byte, error) { panic("marshal boom") }

// errMarshal errors inside MarshalJSON — must become a string field,
// never a dropped event or a broken line.
type errMarshal struct{}

func (errMarshal) MarshalJSON() ([]byte, error) { return nil, errors.New("nope") }

func TestCrashMarshalJSONPanicContained(t *testing.T) {
	// The panic fires inside Encoded(), so the sink must encode:
	// JSONSink serves the canonical line on Write. encoding/json
	// recovers marshaler panics as errors — no panic may escape End,
	// and the line must stay parseable with the error as a string.
	var buf strings.Builder
	rt := MustCompile(Config{Sink: NewJSONSink(&buf), SamplingRate: 1})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Add(op.Context(), "boom", boomMarshal{}, "ok", 1)

	emitted := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("marshaler panic escaped End: %v", r)
			}
		}()
		emitted = op.End(nil)
	}()
	if !emitted {
		t.Fatal("event dropped on marshaler panic")
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(buf.String()), &m); err != nil {
		t.Fatalf("line not parseable: %v: %s", err, buf.String())
	}
	if s, _ := m["boom"].(string); !strings.Contains(s, "marshaling error") || !strings.Contains(s, "boom") {
		t.Fatalf("boom = %#v, want contained marshaler-panic error", m["boom"])
	}
	if m["ok"] != float64(1) {
		t.Fatalf("sibling field lost: %#v", m["ok"])
	}

	// The pool must serve the next request untouched.
	rt2, ts2 := testRT(t, nil)
	op2 := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "j2"})
	Add(op2.Context(), "ok", 1)
	if !op2.End(nil) || len(ts2.Events()) != 1 {
		t.Fatal("pool unusable after marshaler panic")
	}
}

func TestCrashMarshalErrorBecomesStringField(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Add(op.Context(), "bad", errMarshal{})
	if !op.End(nil) || len(ts.Events()) != 1 {
		t.Fatal("event dropped on marshal error")
	}
	if _, ok := ts.Events()[0].Lookup("bad"); !ok {
		t.Fatal("field dropped from capture")
	}
	// TestSink never marshals, so assert on the encoded line: the
	// error becomes a marshaling-error string and the line stays
	// parseable JSON.
	line := recOf(LevelInfo, "m", fieldOf("bad", errMarshal{})).Encoded()
	m := map[string]any{}
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatalf("line not parseable: %v: %s", err, line)
	}
	s, _ := m["bad"].(string)
	if !strings.Contains(s, "marshaling error") {
		t.Fatalf("bad = %#v, want marshaling-error string", m["bad"])
	}
}

// panic(nil) becomes *runtime.PanicNilError on go >= 1.21: the event
// must still carry a structured panic field and outcome=panic.
func TestCrashPanicNilValue(t *testing.T) {
	rt, ts := testRT(t, nil)
	func() {
		defer func() { _ = recover() }()
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
		defer op.End(nil)
		panic(nil)
	}()
	evs := ts.Events()
	if len(evs) != 1 {
		t.Fatalf("events = %d, want 1", len(evs))
	}
	if o, _ := evs[0].Lookup("op.outcome"); o != string(OutcomePanic) {
		t.Fatalf("outcome = %v, want panic", o)
	}
	if _, ok := evs[0].Lookup("panic"); !ok {
		t.Fatal("panic field missing for panic(nil)")
	}
}

// The documented footgun: End called from inside a closure loses
// panic capture. Pin the exact behavior — the panic escapes and End
// has already committed a success-shaped event.
func TestCrashEndFromClosureLosesPanicCapture(t *testing.T) {
	rt, ts := testRT(t, nil)
	func() {
		defer func() { _ = recover() }()
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
		defer func() { _ = op.End(nil) }() // closure form: no capture
		panic("escaped")
	}()
	evs := ts.Events()
	if len(evs) != 1 {
		t.Fatalf("events = %d, want 1 (committed as non-panic)", len(evs))
	}
	if o, _ := evs[0].Lookup("op.outcome"); o != string(OutcomeSuccess) {
		t.Fatalf("closure End outcome = %v, want success (capture disabled)", o)
	}
}

// End after a caught panic-repanic cycle is a published no-op.
func TestCrashDoubleEndAfterPanic(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	func() {
		defer func() { _ = recover() }()
		defer op.End(nil)
		panic("first")
	}()
	if second := op.End(nil); !second {
		t.Fatal("one-shot second End must return the first result (emitted)")
	}
	if len(ts.Events()) != 1 {
		t.Fatalf("events = %d, want 1", len(ts.Events()))
	}
}

// A panicking sink must not poison the pool for later requests, even
// when a straggler write lands between the crash and the next start.
func TestCrashPanicInSinkWithStragglerAftermath(t *testing.T) {
	rt := MustCompile(Config{Sink: crashPanicSink{}, SamplingRate: 1})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Add(op.Context(), "k", "v")
	func() {
		defer func() { _ = recover() }()
		_ = op.End(nil)
	}()
	// Straggler after the crashed (and released) event: silent no-op.
	Add(op.Context(), "straggler", true)

	rt2, ts2 := testRT(t, nil)
	op2 := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "j2"})
	Add(op2.Context(), "clean", true)
	if !op2.End(nil) || len(ts2.Events()) != 1 {
		t.Fatal("pool unusable after sink panic + straggler")
	}
	if v, _ := ts2.Events()[0].Lookup("clean"); v != true {
		t.Fatalf("post-crash event corrupted: %v", v)
	}
}

type crashPanicSink struct{}

func (crashPanicSink) Write(context.Context, *Record) { panic("sink boom") }

// Nested operations: an inner op committing during an outer panic
// storm stays isolated (context shadowing is innermost-wins).
func TestCrashNestedOpsDuringOuterPanic(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	Add(op.Context(), "outer", 1)

	func() {
		defer func() { _ = recover() }()
		inner := Start(op.Context(), rt, OperationStart{Domain: DomainJob, Name: "inner"})
		Add(inner.Context(), "inner", 2)
		defer inner.End(nil)
		panic("outer handler panic")
	}()

	_ = op.End(nil)
	evs := ts.Events()
	if len(evs) != 2 {
		t.Fatalf("events = %d, want 2 (inner + outer)", len(evs))
	}
	var innerSeen, outerSeen bool
	for _, ev := range evs {
		if _, ok := ev.Lookup("inner"); ok {
			innerSeen = true
			// Defer LIFO: the inner End is the innermost frame on the
			// panic path, so IT captures the panic and re-panics.
			if o, _ := ev.Lookup("op.outcome"); o != string(OutcomePanic) {
				t.Fatalf("inner outcome = %v, want panic (innermost defer captures)", o)
			}
		}
		if _, ok := ev.Lookup("outer"); ok {
			outerSeen = true
			// The re-panic was consumed by the test's recover before the
			// outer End ran: it commits clean.
			if o, _ := ev.Lookup("op.outcome"); o != string(OutcomeSuccess) {
				t.Fatalf("outer outcome = %v, want success (panic already consumed)", o)
			}
		}
	}
	if !innerSeen || !outerSeen {
		t.Fatalf("inner=%v outer=%v", innerSeen, outerSeen)
	}
}

// ════════════════════════════════════════════════════════════════════
// Agent B — concurrency and straggler hammering
// ════════════════════════════════════════════════════════════════════

// High-volume straggler storm over a recycled pool: every request
// spawns stragglers that write after End returns, while the pool keeps
// recycling the same few events. No request may ever observe another
// request's fields (generation guard), and counts must be exact.
func TestCrashStragglerStormOverRecycledPool(t *testing.T) {
	rt, ts := testRT(t, nil)
	const workers = 16
	const perWorker = 60

	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
				mine := fmt.Sprintf("w%d-i%d", w, i)
				Add(op.Context(), "mine", mine)
				_ = op.End(nil)
				// Stragglers begin after End returned: sealed no-ops.
				var sw sync.WaitGroup
				for s := range 4 {
					sw.Go(func() {
						Add(op.Context(), "straggler", fmt.Sprintf("w%d-i%d-s%d", w, i, s))
					})
				}
				sw.Wait()
			}
		})
	}
	wg.Wait()

	evs := ts.Events()
	if len(evs) != workers*perWorker {
		t.Fatalf("events = %d, want %d", len(evs), workers*perWorker)
	}
	for _, ev := range evs {
		if _, ok := ev.Lookup("straggler"); ok {
			t.Fatal("post-seal straggler field reached the wire")
		}
		mine, _ := ev.Lookup("mine")
		if s, ok := mine.(string); !ok || !strings.HasPrefix(s, "w") {
			t.Fatalf("foreign or corrupt field: %#v", mine)
		}
	}
}

// Straggler storm on guarded events: concurrent writers must not race,
// lose fields, or corrupt the pool.
func TestCrashStragglerStormGuarded(t *testing.T) {
	rt, ts := testRT(t, nil)
	const workers = 8
	const perWorker = 40
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
				Add(op.Context(), "mine", fmt.Sprintf("a%d-%d", w, i))
				_ = op.End(nil)
				Add(op.Context(), "straggler", 1)
			}
		})
	}
	wg.Wait()
	evs := ts.Events()
	if len(evs) != workers*perWorker {
		t.Fatalf("events = %d, want %d", len(evs), workers*perWorker)
	}
	for _, ev := range evs {
		if _, ok := ev.Lookup("straggler"); ok {
			t.Fatal("guarded straggler field reached the wire")
		}
	}
}

// A realistic pipeline retains the ENCODED bytes (computed during
// Write, cached on the record). Recycling must never change what a
// retained record yields from later Encoded() calls.
func TestCrashRetainedEncodedBytesSurviveRecycling(t *testing.T) {
	retain := &retainingSink{}
	rt2 := MustCompile(Config{Sink: retain, SamplingRate: 1})

	for i := range 300 {
		op := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "j"})
		Add(op.Context(), "seq", i)
		_ = op.End(nil)
	}
	if len(retain.records) != 300 {
		t.Fatalf("records = %d, want 300", len(retain.records))
	}
	for i, rec := range retain.records {
		line := string(rec.Encoded())
		want := fmt.Sprintf(`"seq":%d`, i)
		if !strings.Contains(line, want) {
			t.Fatalf("record %d mutated after recycling: %s", i, line)
		}
	}
}

type retainingSink struct {
	records []*Record
}

func (r *retainingSink) Write(_ context.Context, rec *Record) {
	rec.Encoded() // cache the bytes while the record is valid
	r.records = append(r.records, rec)
}

// TestCrashRetainedEncodedSliceIsStable pins the strongest form of the
// recycling guarantee: the []byte handed out during Write keeps its
// bytes even after the record's event has been recycled and hundreds of
// other events have been encoded. Reusing a pooled encode buffer would
// fail this — the inline encoded buffer is deliberately per record.
func TestCrashRetainedEncodedSliceIsStable(t *testing.T) {
	retain := &sliceRetainingSink{}
	rt := MustCompile(Config{Sink: retain, SamplingRate: 1})

	for i := range 300 {
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
		Add(op.Context(), "seq", i)
		_ = op.End(nil)
	}
	if len(retain.entries) != 300 {
		t.Fatalf("records = %d, want 300", len(retain.entries))
	}
	for i, e := range retain.entries {
		if !bytes.Equal(e.retained, e.want) {
			t.Fatalf("record %d retained slice mutated after recycling:\n got %s\nwant %s", i, e.retained, e.want)
		}
		if got := e.rec.Encoded(); !bytes.Equal(got, e.want) {
			t.Fatalf("record %d re-encode drifted:\n got %s\nwant %s", i, got, e.want)
		}
	}
}

type retainedLine struct {
	rec      *Record
	retained []byte // the slice handed out during Write
	want     []byte // its snapshot at that moment
}

type sliceRetainingSink struct{ entries []retainedLine }

func (s *sliceRetainingSink) Write(_ context.Context, rec *Record) {
	b := rec.Encoded()
	s.entries = append(s.entries, retainedLine{rec: rec, retained: b, want: bytes.Clone(b)})
}

// One immutable Runtime shared by concurrent operations across every
// domain, with policies and level rates compiled in — no shared-state
// races (pinned by -race) and exact counts.
func TestCrashSharedRuntimeAllDomains(t *testing.T) {
	rt, ts := testRT(t, func(c *Config) {
		c.LevelSamplingRates = map[Level]float64{LevelInfo: 1, LevelError: 1}
		c.OperationPolicies = map[Domain]OperationPolicy{
			DomainJob: {SamplingRate: ptrRate(1.0)},
		}
	})
	domains := []Domain{DomainHTTP, DomainJob, DomainMessage, DomainCLI, "custom"}
	var wg sync.WaitGroup
	for d := range domains {
		wg.Go(func() {
			for i := range 50 {
				op := Start(context.Background(), rt, OperationStart{Domain: domains[d], Name: "x"})
				Add(op.Context(), "i", i)
				if i%7 == 0 {
					Error(op.Context(), fmt.Errorf("boom %d", i))
				}
				_ = op.End(nil)
			}
		})
	}
	wg.Wait()
	evs := ts.Events()
	if len(evs) != len(domains)*50 {
		t.Fatalf("events = %d, want %d (errors bypass sampling)", len(evs), len(domains)*50)
	}
}

func ptrRate(r float64) *float64 { return &r }

// Concurrent first-End callers against a slow sink: exactly one
// commit, every caller observes the same result.
func TestCrashConcurrentEndSlowSink(t *testing.T) {
	slow := &slowSink{d: 3 * time.Millisecond}
	rt := MustCompile(Config{Sink: slow, SamplingRate: 1})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	const racers = 16
	res := make([]bool, racers)
	var wg sync.WaitGroup
	for i := range racers {
		wg.Go(func() {
			res[i] = op.End(nil)
		})
	}
	wg.Wait()
	for i, r := range res {
		if i > 0 && r != res[0] {
			t.Fatalf("racer %d observed %v, racer 0 observed %v", i, r, res[0])
		}
	}
	if slow.writes != 1 {
		t.Fatalf("sink writes = %d, want 1", slow.writes)
	}
}

type slowSink struct {
	d      time.Duration
	writes int
}

func (s *slowSink) Write(_ context.Context, rec *Record) {
	time.Sleep(s.d)
	s.writes++
}

// A context handed to a spawned goroutine keeps writing to ITS OWN
// operation (innermost attach wins) — the sibling operation must stay
// untouched.
func TestCrashForeignCtxIsolation(t *testing.T) {
	rt, ts := testRT(t, nil)
	opA := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	opB := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})

	done := make(chan struct{})
	go func() {
		defer close(done)
		Add(opA.Context(), "fromGoroutine", "A")
	}()
	<-done
	Add(opB.Context(), "belongs", "B")

	_ = opA.End(nil)
	_ = opB.End(nil)
	for _, ev := range ts.Events() {
		if v, ok := ev.Lookup("fromGoroutine"); ok {
			if _, also := ev.Lookup("belongs"); also {
				t.Fatal("field bled across operations")
			}
			if v != "A" {
				t.Fatalf("fromGoroutine = %v", v)
			}
		}
	}
}

// ════════════════════════════════════════════════════════════════════
// Agent C — encoder abuse
// ════════════════════════════════════════════════════════════════════

func mustParseLine(t *testing.T, line []byte) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatalf("line not parseable: %v: %s", err, line)
	}
	return m
}

func TestCrashHostileStrings(t *testing.T) {
	cases := []string{
		"\xff\xfe raw high bytes",               // invalid UTF-8
		"\x00\x01\x1f control",                  // control characters
		"quote\"backslash\\newline\n\t\r",       // JSON metacharacters
		"\x7f\x80\xef\xbb\xbf bom",              // DEL, lone continuation, real BOM bytes
		"emoji \U0001F6A8 unicode \u2028\u2029", // line/para separators
		strings.Repeat("x", 1<<16),              // 64KB value
	}
	for _, tc := range cases {
		rec := recOf(LevelInfo, tc, fieldOf("v", tc), fieldOf("k"+tc, "key with hostile bytes"))
		line := rec.Encoded()
		m := mustParseLine(t, line)
		if m["message"] == nil {
			t.Fatalf("message missing for %q", tc[:min(len(tc), 20)])
		}
		// Round-trip: Go's decoder replaces invalid UTF-8 with U+FFFD;
		// anything valid must come back byte-identical.
		got, _ := m["v"].(string)
		if !strings.ContainsRune(tc, 0xFFFD) && got != tc && utf8.ValidString(tc) {
			t.Fatalf("round-trip mismatch: got %q want %q", got, tc)
		}
	}
}

func TestCrashCyclicAnyValue(t *testing.T) {
	m := map[string]any{}
	m["self"] = m
	rec := recOf(LevelWarn, "cycle", fieldOf("cyc", m))
	line := rec.Encoded() // must not hang
	parsed := mustParseLine(t, line)
	s, _ := parsed["cyc"].(string)
	if !strings.Contains(s, "cycle") && !strings.Contains(s, "error") {
		t.Fatalf("cyclic value = %#v, want a marshaling-error string", parsed["cyc"])
	}

	// Capture path (TestSink deep-copy) must also survive the cycle.
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Add(op.Context(), "cyc", m)
	_ = op.End(nil)
	if len(ts.Events()) != 1 {
		t.Fatal("cyclic value dropped the event")
	}
}

func TestCrashFloatEdgeValues(t *testing.T) {
	// Untyped constant -0.0 is +0 (constants carry no sign) — Copysign
	// is the only way to obtain a real negative zero.
	negZero := math.Copysign(0, -1)
	vals := []float64{math.NaN(), math.Inf(1), math.Inf(-1), math.SmallestNonzeroFloat64, math.MaxFloat64, negZero}
	for _, v := range vals {
		rec := recOf(LevelInfo, "f", fieldOf("f", v))
		m := mustParseLine(t, rec.Encoded())
		if m["f"] == nil {
			t.Fatalf("float %v dropped from line", v)
		}
	}
	rec32 := recOf(LevelInfo, "f32", fieldOf("f32", float32(math.NaN())))
	m := mustParseLine(t, rec32.Encoded())
	if s, _ := m["f32"].(string); s != "NaN" {
		t.Fatalf("float32 NaN = %#v, want \"NaN\"", m["f32"])
	}
}

func TestCrashExtremeFieldCounts(t *testing.T) {
	for _, n := range []int{0, 1, 23, 24, 25, 1023, 1024, 1025, 5000} {
		fields := make([]Field, n)
		for i := range n {
			fields[i] = fieldOf(fmt.Sprintf("k%d", i), i)
		}
		rec := recOf(LevelInfo, "wide", fields...)
		m := mustParseLine(t, rec.Encoded())
		// Every key present exactly once.
		for i := range min(n, 30) {
			key := fmt.Sprintf("k%d", i)
			if m[key] == nil {
				t.Fatalf("n=%d: key %s missing", n, key)
			}
		}
		if got := len(m) - 3; n <= 1024 && got != n { // minus level/time/message
			t.Fatalf("n=%d: members = %d", n, got)
		}
	}
}

func TestCrashEnvelopeCollisions(t *testing.T) {
	cases := [][]string{
		{"time"},
		{"message"},
		{"level"},
		{"time", "message", "level"},
		{"fields.time"},
		{"fields.message", "message"},
		{"time", "time", "fields.time"},
		{"message", "fields.message", "fields.fields.message"},
	}
	for _, keys := range cases {
		var fields []Field
		for i, k := range keys {
			fields = append(fields, fieldOf(k, fmt.Sprintf("v%d", i)))
		}
		rec := recOf(LevelInfo, "real message", fields...)
		m := mustParseLine(t, rec.Encoded())
		// Envelope survives intact.
		if m["message"] != "real message" {
			t.Fatalf("keys %v: envelope message = %#v", keys, m["message"])
		}
		if m["level"] != "info" {
			t.Fatalf("keys %v: envelope level = %#v", keys, m["level"])
		}
		if _, ok := m["time"].(string); !ok {
			t.Fatalf("keys %v: envelope time missing", keys)
		}
		// Last aliased write wins.
		last := fmt.Sprintf("v%d", len(keys)-1)
		if keys[len(keys)-1] == "time" || keys[len(keys)-1] == "message" || keys[len(keys)-1] == "level" {
			aliased := "fields." + keys[len(keys)-1]
			if m[aliased] != last {
				t.Fatalf("keys %v: %s = %#v, want %s", keys, aliased, m[aliased], last)
			}
		}
	}
}

func TestCrashEmptyAndUnicodeKeys(t *testing.T) {
	rec := recOf(LevelInfo, "m",
		fieldOf("", "empty key"),
		fieldOf("日本語キー", "unicode"),
		fieldOf("k\x00with\xffjunk", "hostile key"),
	)
	m := mustParseLine(t, rec.Encoded())
	if v, _ := m["日本語キー"].(string); v != "unicode" {
		t.Fatalf("unicode key lost: %#v", m)
	}
}

func TestCrashTypedNilAndWeirdAny(t *testing.T) {
	type customErr struct{}
	var nilTypedErr *customErr
	rec := recOf(LevelInfo, "m",
		fieldOf("nilAny", nil),
		fieldOf("typedNil", nilTypedErr), // non-nil interface, nil pointer
		fieldOf("chan", make(chan int)),  // json.Marshal errors
		fieldOf("func", func() {}),
	)
	line := rec.Encoded()
	m := mustParseLine(t, line)
	if m["nilAny"] != nil {
		t.Fatalf("nilAny = %#v", m["nilAny"])
	}
	if m["chan"] == nil && m["func"] == nil {
		t.Fatal("both unmarshalable values vanished without a trace")
	}
}

// TestTestSinkNestedEmptySlices pins the deep-copy identity fix: empty
// slices of different types all report Pointer() == 0, and the old
// uintptr visited key aliased them into one cache entry, then panicked
// inside End on reflect.Set. The capture must preserve both values.
func TestTestSinkNestedEmptySlices(t *testing.T) {
	ts := NewTestSink()
	rt := MustCompile(Config{Sink: ts, SamplingRate: 1})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "x"})
	Add(op.Context(), "mixed", [2]any{[]string(nil), []int(nil)})
	Add(op.Context(), "empty", []string{})
	if !op.End(nil) {
		t.Fatal("event dropped")
	}

	mixed, ok := ts.Events()[0].Lookup("mixed")
	if !ok {
		t.Fatal("mixed field missing")
	}
	arr, ok := mixed.([2]any)
	if !ok {
		t.Fatalf("mixed = %#v, want [2]any", mixed)
	}
	if s, ok := arr[0].([]string); !ok || s != nil {
		t.Fatalf("arr[0] = %#v, want a nil []string", arr[0])
	}
	if i, ok := arr[1].([]int); !ok || i != nil {
		t.Fatalf("arr[1] = %#v, want a nil []int", arr[1])
	}
	if empty, ok := ts.Events()[0].Lookup("empty"); !ok {
		t.Fatal("empty field missing")
	} else if s, ok := empty.([]string); !ok || s == nil {
		t.Fatalf("empty = %#v, want a non-nil empty []string", empty)
	}
}

// ════════════════════════════════════════════════════════════════════
// Agent D — pool and memory lifecycle
// ════════════════════════════════════════════════════════════════════

// Sequential heavy reuse: with a tiny pool the same events serve
// thousands of requests; no field may survive its request.
func TestCrashPoolReuseNoBleed(t *testing.T) {
	rt, ts := testRT(t, nil)
	const n = 2000
	for i := range n {
		op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
		Add(op.Context(), "seq", fmt.Sprintf("s%d", i), "padding", strings.Repeat("p", 32))
		if !op.End(nil) {
			t.Fatalf("request %d dropped", i)
		}
	}
	evs := ts.Events()
	if len(evs) != n {
		t.Fatalf("events = %d, want %d", len(evs), n)
	}
	seen := map[string]bool{}
	for _, ev := range evs {
		v, _ := ev.Lookup("seq")
		s, _ := v.(string)
		if seen[s] {
			t.Fatalf("duplicate seq %s — pool bleed", s)
		}
		seen[s] = true
	}
}

// Operations that are started and abandoned (never Ended) must not
// leak goroutines (goleak gates the suite) or poison the pool.
func TestCrashAbandonedOperations(t *testing.T) {
	rt, _ := testRT(t, nil)
	ops := make([]*Operation, 500)
	for i := range ops {
		ops[i] = Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
		Add(ops[i].Context(), "abandoned", i)
	}
	// One late End on an arbitrary abandoned op still works.
	if !ops[len(ops)-1].End(nil) {
		t.Fatal("late End on abandoned op failed")
	}
	// Pool serves fresh requests afterwards.
	rt2, ts2 := testRT(t, nil)
	op := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "fresh"})
	Add(op.Context(), "fresh", true)
	if !op.End(nil) || len(ts2.Events()) != 1 {
		t.Fatal("pool unusable after abandonment storm")
	}
}

// Events whose backing array grows past the 1024-field pool cap are
// dropped from the pool at release — behavior, not corruption, is the
// contract: the wide event itself must still be complete on the wire.
func TestCrashWideEventPoolCapBoundary(t *testing.T) {
	rt, ts := testRT(t, nil)
	for _, n := range []int{1023, 1024, 1025, 1100} {
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "wide"})
		for i := range n {
			Add(op.Context(), fmt.Sprintf("f%d", i), i)
		}
		if !op.End(nil) {
			t.Fatalf("n=%d: dropped", n)
		}
	}
	evs := ts.Events()
	if len(evs) != 4 {
		t.Fatalf("events = %d, want 4", len(evs))
	}
	for i, ev := range evs {
		want := []int{1023, 1024, 1025, 1100}[i]
		last := fmt.Sprintf("f%d", want-1)
		v, ok := ev.Lookup(last)
		n, isInt := v.(int64)
		if !ok || !isInt || n != int64(want-1) {
			t.Fatalf("event %d (n=%d): last field = %#v", i, want, v)
		}
	}
}

// The copy-out contract, demonstrated: a sink that retains the RAW
// Fields() slice across subsequent requests observes recycling (the
// documented hazard), while the deep-copying TestSink does not. Pins
// why amendment 9 says "copy anything you retain".
func TestCrashRetainRawSliceVsCopy(t *testing.T) {
	raw := &rawRetainingSink{}
	rtRaw := MustCompile(Config{Sink: raw, SamplingRate: 1})

	op := Start(context.Background(), rtRaw, OperationStart{Domain: DomainJob, Name: "first"})
	Add(op.Context(), "who", "first")
	_ = op.End(nil)
	retained := raw.lastFields

	// Recycle the same events with different payloads.
	for i := range 50 {
		op := Start(context.Background(), rtRaw, OperationStart{Domain: DomainJob, Name: "later"})
		Add(op.Context(), "who", fmt.Sprintf("later-%d", i))
		_ = op.End(nil)
	}

	// The copied view (TestSink semantics) is stable regardless.
	rt2, ts := testRT(t, nil)
	op2 := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "stable"})
	Add(op2.Context(), "who", "stable")
	_ = op2.End(nil)
	if v, _ := ts.Events()[0].Lookup("who"); v != "stable" {
		t.Fatalf("copied view unstable: %v", v)
	}

	// The raw retained slice is only valid until the event is reused;
	// with a same-shape successor it may already show foreign data.
	// This is the hazard — assert it is BOUNDED: the slice still has
	// the fields-shape (never out-of-bounds memory or a panic).
	if len(retained) == 0 {
		t.Fatal("retained raw slice empty")
	}
	for _, f := range retained {
		_ = f.Key() // must not panic
	}
}

type rawRetainingSink struct {
	lastFields []Field
}

func (r *rawRetainingSink) Write(_ context.Context, rec *Record) {
	r.lastFields = rec.Fields()
}

// ════════════════════════════════════════════════════════════════════
// Agent E — configuration and API misuse
// ════════════════════════════════════════════════════════════════════

// Plain out-of-range rates and NaN are pinned by TestCompileSentinels;
// this covers the infinities and negative zero it omits.
func TestCrashHostileRates(t *testing.T) {
	for _, r := range []float64{math.Inf(1), math.Inf(-1)} {
		if _, err := Compile(Config{SamplingRate: r}); err == nil {
			t.Fatalf("rate %v accepted", r)
		}
	}
	for _, r := range []float64{0, 1, 0.5, math.Copysign(0, -1)} {
		if _, err := Compile(Config{SamplingRate: r}); err != nil {
			t.Fatalf("rate %v rejected: %v", r, err)
		}
	}
	inf := math.Inf(1)
	if _, err := Compile(Config{OperationPolicies: map[Domain]OperationPolicy{"job": {SamplingRate: &inf}}}); err == nil {
		t.Fatal("+Inf policy rate accepted")
	}
	// Nested NaN in the level map: TestCompileSentinels covers only the
	// global rate and plain out-of-range map values — keep this one
	// deterministic rather than fuzz-only.
	if _, err := Compile(Config{LevelSamplingRates: map[Level]float64{LevelInfo: math.NaN()}}); err == nil {
		t.Fatal("NaN level rate accepted")
	}
}

// The compiled Runtime shares no mutable state with the caller's
// Config: mutating every map (and rate pointers) after Compile must
// not change behavior.
func TestCrashConfigMutatedAfterCompile(t *testing.T) {
	levelRates := map[Level]float64{LevelInfo: 1.0}
	polRate := 0.0
	cfg := Config{
		Sink:               NewTestSink(),
		SamplingRate:       1.0,
		LevelSamplingRates: levelRates,
		OperationPolicies: map[Domain]OperationPolicy{
			DomainJob: {SamplingRate: &polRate, OutcomeLevels: map[Outcome]Level{OutcomeSuccess: LevelInfo}},
		},
	}
	rt, err := Compile(cfg)
	if err != nil {
		t.Fatal(err)
	}

	// Hostile post-compile mutation.
	levelRates[LevelInfo] = -5
	polRate = 7
	cfg.OperationPolicies[DomainJob].OutcomeLevels[OutcomeSuccess] = Level(-99)
	cfg.SamplingRate = -1

	// Job domain keeps its policy (rate ptr cloned at 0.0): dropped.
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	if op.End(nil) {
		t.Fatal("mutated policy leaked into compiled runtime (job should drop at rate 0)")
	}
	// Default domain unaffected by the job policy: rate 1 keeps.
	op2 := Start(context.Background(), rt, OperationStart{Domain: DomainCLI, Name: "c"})
	if !op2.End(nil) {
		t.Fatal("default domain dropped after config mutation")
	}
}

func TestCrashNilReceiversAndArgs(t *testing.T) {
	// Operations.
	var nilOp *Operation
	if nilOp.End(nil) {
		t.Fatal("nil op End emitted")
	}
	if nilOp.Context() != nil {
		t.Fatal("nil op Context not nil")
	}
	// Start with nil runtime and nil ctx.
	//lint:ignore SA1012 nil contexts are this test's charter
	op := Start(nil, nil, OperationStart{})
	if op.End(nil) {
		t.Fatal("nil runtime emitted")
	}
	// Context helpers with nil/no-event ctx: silent no-ops.
	//lint:ignore SA1012 nil contexts are this test's charter
	Add(nil, "k", "v")
	Add(context.Background(), "k", "v")
	//lint:ignore SA1012 nil contexts are this test's charter
	Error(nil, nil)
	Error(context.Background(), nil)
	//lint:ignore SA1012 nil contexts are this test's charter
	SetMessage(nil, "")
	//lint:ignore SA1012 nil contexts are this test's charter
	SetRoute(nil, "")
	//lint:ignore SA1012 nil contexts are this test's charter
	SetLevel(nil, Level(999))
	// Sinks.
	(*JSONSink)(nil).Write(context.Background(), nil)
	NewJSONSink(nil).Write(context.Background(), nil)
	var nilTest *TestSink
	nilTest.Write(context.Background(), nil)
	if len(nilTest.Events()) != 0 {
		t.Fatal("nil TestSink has events")
	}
	nilTest.Reset()
	// Record accessors on zero records.
	var zeroRec Record
	_ = zeroRec.Encoded()
	_ = zeroRec.Fields()
	_, _ = zeroRec.Lookup("k")
	var zeroField Field
	_ = zeroField.Key()
	_ = zeroField.WireKey()
	// MustCompile panics loudly on garbage.
	func() {
		defer func() { _ = recover() }()
		MustCompile(Config{SamplingRate: math.NaN()})
		t.Fatal("MustCompile accepted NaN")
	}()
}

// Malformed variadic tails: non-string keys, empty keys, odd tails —
// skipped silently, well-formed pairs kept.
func TestCrashAddMalformedTails(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Add(op.Context(),
		"good", 1,
		42, "non-string key",
		"", "empty key",
		"also_good", 2,
		"dangling",
	)
	_ = op.End(nil)
	ev := ts.Events()[0]
	good, _ := ev.Lookup("good")
	if v, _ := good.(int64); v != 1 {
		t.Fatalf("good = %#v", good)
	}
	also, _ := ev.Lookup("also_good")
	if v, _ := also.(int64); v != 2 {
		t.Fatalf("also_good = %#v", also)
	}
	if _, ok := ev.Lookup("dangling"); ok {
		t.Fatal("dangling tail became a field")
	}
	if n := len(ev.Fields()); n < 2 {
		t.Fatalf("fields = %d", n)
	}
}

func TestCrashSetterGarbage(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	SetLevel(op.Context(), LevelWarn)  // floor can only raise severity
	SetLevel(op.Context(), Level(100)) // invalid: ignored
	SetRoute(op.Context(), "")
	SetMessage(op.Context(), "")
	Add(op.Context(), "took", -1*time.Hour) // negative duration
	_ = op.End(nil)
	ev := ts.Events()[0]
	if ev.Level() != LevelWarn {
		t.Fatalf("level = %v, want warn (floor raised)", ev.Level())
	}
	if _, ok := ev.Lookup("http.route"); ok {
		t.Fatal("empty route became a field")
	}
}

// ════════════════════════════════════════════════════════════════════
// Agent G — time and metadata abuse
// ════════════════════════════════════════════════════════════════════

func TestCrashExtremeTimes(t *testing.T) {
	times := []time.Time{
		{}, // zero
		time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC),      // negative year
		time.Date(0, 1, 1, 0, 0, 0, 0, time.UTC),       // year 0
		time.Date(12345, 6, 7, 8, 9, 10, 11, time.UTC), // five-digit year
		time.Date(-100000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Unix(math.MinInt64, 0),                         // wall-clock wrapped
		time.Unix(math.MaxInt64, math.MaxInt32),             // max nano
		time.Now().Add(math.MaxInt64),                       // overflowed Add
		time.Date(2024, 13, 45, 30, 70, 70, 1e10, time.UTC), // normalized
	}
	for i, ts := range times {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("time[%d] %v panicked: %v", i, ts, r)
				}
			}()
			rec := recOf(LevelInfo, "t", fieldOf("t", ts), fieldOf("t2", ts))
			_ = mustParseLine(t, rec.Encoded())

			rt, ts2 := testRT(t, nil)
			op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
			Add(op.Context(), "when", ts)
			_ = op.End(nil)
			if len(ts2.Events()) != 1 {
				t.Fatalf("time[%d] dropped", i)
			}
		}()
	}
}

func TestCrashExtremeDurations(t *testing.T) {
	durs := []time.Duration{
		0,
		-1,
		time.Nanosecond,
		math.MaxInt64,
		math.MinInt64,
		time.Duration(math.MaxInt64 / 2), // near-max without const overflow
		-24 * time.Hour,
	}
	for i, d := range durs {
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("dur[%d] %v panicked: %v", i, d, r)
				}
			}()
			rec := recOf(LevelInfo, "d", fieldOf("d", d))
			m := mustParseLine(t, rec.Encoded())
			if m["d"] == nil {
				t.Fatalf("dur[%d] vanished", i)
			}
		}()
	}
	// duration_ms annotation of an extremely long operation clock read
	// is bounded by real time; the field constructor path above covers
	// the value extremes.
}

func TestCrashHostileOperationStart(t *testing.T) {
	starts := []OperationStart{
		{Domain: DomainHTTP, Name: strings.Repeat("n", 1<<16)}, // 64KB name
		{Domain: "custom-domain\nwith\"hostile\x00bytes", Name: "x"},
		{Domain: Domain(strings.Repeat("d", 4096)), Name: "x"},
		{Domain: DomainJob, Name: "j", ID: "id\xff\xfe", Source: "src\x00"},
		{Domain: "MIXEDcase", Name: "x"}, // normalizeDomain case handling
		{Domain: DomainJob, Name: "j", Attempt: -5, MaxAttempts: -5},
		{Domain: DomainJob, Name: "j", Attempt: math.MaxInt32, MaxAttempts: math.MinInt32},
	}
	for i, st := range starts {
		rt, ts := testRT(t, nil)
		op := Start(context.Background(), rt, st)
		Add(op.Context(), "i", i)
		if !op.End(nil) {
			t.Fatalf("start[%d] dropped", i)
		}
		m := mustParseLine(t, []byte(encodedOf(t, ts)))
		if m["op.outcome"] == nil {
			t.Fatalf("start[%d]: outcome missing", i)
		}
	}
}

// encodedOf renders the single captured event via the JSON sink shape.
func encodedOf(t *testing.T, ts *TestSink) string {
	t.Helper()
	rec := recOf(LevelInfo, "m", evs2Fields(t, ts)...)
	return string(rec.Encoded())
}

// Domain policy lookup semantics, pinned: the "" alias installs the
// default-domain ("operation") policy and applies ONLY there; domains
// are exact-match (case-sensitive); explicit keys beat the alias for
// the same normalized slot.
func TestCrashDomainPolicyLookupShapes(t *testing.T) {
	mk := func(pol map[Domain]OperationPolicy) (*Runtime, *TestSink) {
		sink := NewTestSink()
		return MustCompile(Config{Sink: sink, SamplingRate: 1, OperationPolicies: pol}), sink
	}

	// Alias applies to the default domain only.
	rtAlias, sink := mk(map[Domain]OperationPolicy{"": {SuccessLevel: LevelWarn}})
	for _, d := range []Domain{"", "operation"} {
		op := Start(context.Background(), rtAlias, OperationStart{Domain: d, Name: "x"})
		_ = op.End(nil)
	}
	if got := len(sink.Events()); got != 2 {
		t.Fatalf("events = %d", got)
	}
	for _, ev := range sink.Events() {
		if ev.Level() != LevelWarn {
			t.Fatalf("alias domain level = %v, want warn", ev.Level())
		}
	}
	// Non-default domains do NOT inherit the alias.
	op := Start(context.Background(), rtAlias, OperationStart{Domain: DomainJob, Name: "x"})
	_ = op.End(nil)
	if got := sink.Events()[2].Level(); got != LevelInfo {
		t.Fatalf("job inherited alias: level = %v, want info", got)
	}

	// Domains are exact-match, case-sensitive.
	rtCase, sinkCase := mk(map[Domain]OperationPolicy{"JoB": {SuccessLevel: LevelError}})
	op = Start(context.Background(), rtCase, OperationStart{Domain: "JoB", Name: "x"})
	_ = op.End(nil)
	if got := sinkCase.Events()[0].Level(); got != LevelError {
		t.Fatalf("JoB level = %v, want error", got)
	}
	op = Start(context.Background(), rtCase, OperationStart{Domain: DomainJob, Name: "x"})
	_ = op.End(nil)
	if got := sinkCase.Events()[1].Level(); got != LevelInfo {
		t.Fatalf("job matched JoB policy: level = %v, want info", got)
	}

	// Explicit default key beats the alias deterministically.
	rtBoth, sinkBoth := mk(map[Domain]OperationPolicy{"": {SuccessLevel: LevelInfo}, "operation": {SuccessLevel: LevelError}})
	op = Start(context.Background(), rtBoth, OperationStart{Domain: "", Name: "x"})
	_ = op.End(nil)
	if got := sinkBoth.Events()[0].Level(); got != LevelError {
		t.Fatalf("explicit operation level = %v, want error (explicit beats alias)", got)
	}
}

func TestCrashSetRouteHostile(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	SetRoute(op.Context(), "/x/\xff\xfe\"\nroute")
	Add(op.Context(), "ok", 1)
	_ = op.End(nil)
	m := mustParseLine(t, []byte(encodedOf(t, ts)))
	if m["ok"] != float64(1) {
		t.Fatalf("sibling lost: %v", m)
	}
}

// ════════════════════════════════════════════════════════════════════
// Agent H — deep and wide payloads
// ════════════════════════════════════════════════════════════════════

func nest(depth int, leaf any) map[string]any {
	m := map[string]any{"leaf": leaf}
	for range depth {
		m = map[string]any{"n": m}
	}
	return m
}

func TestCrashDeepNesting(t *testing.T) {
	// Depth stays under encoding/json's decoder maxNestingDepth (10000)
	// so the parseability oracle works on every Go version; a separate
	// encode-only pass goes far past it.
	for _, depth := range []int{100, 1000, 9000} {
		v := nest(depth, "bottom")
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("depth %d panicked: %v", depth, r)
				}
			}()
			rec := recOf(LevelInfo, "deep", fieldOf("nest", v))
			_ = mustParseLine(t, rec.Encoded())

			// The TestSink deep-copy path must also survive.
			rt, ts := testRT(t, nil)
			op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
			Add(op.Context(), "nest", v)
			_ = op.End(nil)
			if len(ts.Events()) != 1 {
				t.Fatalf("depth %d dropped", depth)
			}
		}()
	}

	// Past the stdlib decoder limit (which differs across Go versions):
	// the contract is only no-panic on encode and capture — the line's
	// parseability is a decoder property, not ours.
	v := nest(50000, "bottom")
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("depth 50000 panicked: %v", r)
			}
		}()
		rec := recOf(LevelInfo, "deep", fieldOf("nest", v))
		if len(rec.Encoded()) == 0 {
			t.Fatal("empty line")
		}
		rt, ts := testRT(t, nil)
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
		Add(op.Context(), "nest", v)
		_ = op.End(nil)
		if len(ts.Events()) != 1 {
			t.Fatal("depth 50000 dropped")
		}
	}()
}

func TestCrashMegaFields(t *testing.T) {
	const n = 100000
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "mega"})
	for i := range n {
		Add(op.Context(), fmt.Sprintf("f%d", i), i)
	}
	if !op.End(nil) {
		t.Fatal("mega event dropped")
	}
	evs := ts.Events()
	if len(evs) != 1 {
		t.Fatalf("events = %d", len(evs))
	}
	// Spot check first/last fields through the encoder.
	rec := recOf(LevelInfo, "mega", evs[0].Fields()...)
	line := rec.Encoded()
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatalf("mega line unparseable: %v", err)
	}
	if v, _ := m["f0"].(float64); v != 0 {
		t.Fatalf("f0 = %v", m["f0"])
	}
	if v, _ := m[fmt.Sprintf("f%d", n-1)].(float64); v != float64(n-1) {
		t.Fatalf("f%d = %v", n-1, m[fmt.Sprintf("f%d", n-1)])
	}
}

func TestCrashHugeString(t *testing.T) {
	big := strings.Repeat("payload\xef\xbb\xbf", 1<<20) // ~5MB
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "big"})
	Add(op.Context(), "big", big, "sib", 1)
	_ = op.End(nil)
	rec := recOf(LevelInfo, "big", evs2Fields(t, ts)...)
	line := rec.Encoded()
	var m map[string]any
	if err := json.Unmarshal(line, &m); err != nil {
		t.Fatalf("huge line unparseable: %v", err)
	}
	if got, _ := m["big"].(string); len(got) != len(big) {
		t.Fatalf("huge string truncated: got %d want %d", len(got), len(big))
	}
}

func evs2Fields(t *testing.T, ts *TestSink) []Field {
	t.Helper()
	evs := ts.Events()
	if len(evs) != 1 {
		t.Fatalf("events = %d", len(evs))
	}
	return evs[0].Fields()
}

// A few concurrent mega events: memory-bounded stress through the
// pool, exact isolation.
func TestCrashConcurrentMegaEvents(t *testing.T) {
	rt, ts := testRT(t, nil)
	done := make(chan int, 4)
	for w := range 4 {
		go func(w int) {
			defer func() { done <- w }()
			for i := range 3 {
				op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "m"})
				for j := range 20000 {
					Add(op.Context(), fmt.Sprintf("w%d-i%d-f%d", w, i, j), j)
				}
				_ = op.End(nil)
			}
		}(w)
	}
	for range 4 {
		<-done
	}
	if got := len(ts.Events()); got != 12 {
		t.Fatalf("events = %d, want 12", got)
	}
	for _, ev := range ts.Events() {
		if len(ev.Fields()) < 20000 {
			t.Fatalf("event shrank: %d fields", len(ev.Fields()))
		}
	}
}

// ════════════════════════════════════════════════════════════════════
// Agent I — lifecycle and context misuse
// ════════════════════════════════════════════════════════════════════

// 50 nested operations sharing the chain of contexts; ends run in
// reverse order — every event must land with its own fields.
func TestCrashDeepOperationChain(t *testing.T) {
	rt, ts := testRT(t, nil)
	const depth = 50
	ops := make([]*Operation, depth)
	ctx := context.Background()
	for i := range depth {
		ops[i] = Start(ctx, rt, OperationStart{Domain: DomainJob, Name: fmt.Sprintf("op%d", i)})
		ctx = ops[i].Context()
		Add(ctx, "depth", i)
	}
	for i := depth - 1; i >= 0; i-- {
		if !ops[i].End(nil) {
			t.Fatalf("op%d dropped", i)
		}
	}
	evs := ts.Events()
	if len(evs) != depth {
		t.Fatalf("events = %d, want %d", len(evs), depth)
	}
	seen := map[string]int{}
	for _, ev := range evs {
		name, _ := ev.Lookup("op.name")
		seen[name.(string)]++
	}
	for i := range depth {
		if seen[fmt.Sprintf("op%d", i)] != 1 {
			t.Fatalf("op%d seen %d times", i, seen[fmt.Sprintf("op%d", i)])
		}
	}
}

// Ends in scrambled order: an inner op may outlive its parent's End —
// the parent's seal does not gate the child (separate events).
func TestCrashScrambledEndOrder(t *testing.T) {
	rt, ts := testRT(t, nil)
	outer := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	inner := Start(outer.Context(), rt, OperationStart{Domain: DomainJob, Name: "job"})
	Add(inner.Context(), "inner", 1)

	_ = outer.End(nil)                    // parent commits first
	Add(outer.Context(), "late-outer", 1) // stale: no-op
	if !inner.End(nil) {
		t.Fatal("inner dropped after parent end")
	}
	Add(inner.Context(), "late-inner", 1) // stale: no-op

	if got := len(ts.Events()); got != 2 {
		t.Fatalf("events = %d, want 2", got)
	}
	for _, ev := range ts.Events() {
		if _, ok := ev.Lookup("late-outer"); ok {
			t.Fatal("late-outer write landed")
		}
		if _, ok := ev.Lookup("late-inner"); ok {
			t.Fatal("late-inner write landed")
		}
	}
}

// Concurrent first-End callers each hand a DIFFERENT error pointer:
// the winner's error is the one recorded — exactly once.
func TestCrashConcurrentEndDistinctErrPtrs(t *testing.T) {
	for attempt := range 20 {
		rt, ts := testRT(t, nil)
		op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
		errs := make([]*error, 8)
		for i := range errs {
			e := fmt.Errorf("err-%d", i)
			errs[i] = &e
		}
		var wg sync.WaitGroup
		for i := range errs {
			wg.Go(func() {
				_ = op.End(errs[i])
			})
		}
		wg.Wait()
		evs := ts.Events()
		if len(evs) != 1 {
			t.Fatalf("attempt %d: events = %d, want 1", attempt, len(evs))
		}
		count := 0
		for _, f := range evs[0].Fields() {
			if f.key == "error" {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("attempt %d: error fields = %d, want 1", attempt, count)
		}
	}
}

// A finished operation's context used as the parent for a fresh
// request: the new request gets its own WAL; writes through the old
// ctx stay no-ops.
func TestCrashStaleCtxAsParent(t *testing.T) {
	rt, ts := testRT(t, nil)
	first := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	Add(first.Context(), "first", 1)
	_ = first.End(nil)

	second := Start(first.Context(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	Add(second.Context(), "second", 2)
	Add(first.Context(), "ghost", 3) // stale ctx write
	_ = second.End(nil)

	for _, ev := range ts.Events() {
		if _, ok := ev.Lookup("ghost"); ok {
			t.Fatal("ghost field leaked into an event")
		}
	}
	if len(ts.Events()) != 2 {
		t.Fatalf("events = %d, want 2", len(ts.Events()))
	}
}

// Start after Start on the same base ctx: both valid, independent.
func TestCrashSiblingOpsSameBase(t *testing.T) {
	rt, ts := testRT(t, nil)
	base := context.Background()
	a := Start(base, rt, OperationStart{Domain: DomainCLI, Name: "a"})
	b := Start(base, rt, OperationStart{Domain: DomainCLI, Name: "b"})
	Add(a.Context(), "who", "a")
	Add(b.Context(), "who", "b")
	_ = a.End(nil)
	_ = b.End(nil)
	evs := ts.Events()
	if len(evs) != 2 {
		t.Fatalf("events = %d, want 2", len(evs))
	}
	whos := map[string]bool{}
	for _, ev := range evs {
		v, _ := ev.Lookup("who")
		whos[v.(string)] = true
	}
	if !whos["a"] || !whos["b"] {
		t.Fatalf("siblings crossed: %v", whos)
	}
}

// Start on an already-canceled context: the operation must still run,
// commit, and emit — the WAL is ctx-independent (cancellation surfaces
// through the caller's error, not the context).
func TestCrashStartOnCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	rt, ts := testRT(t, nil)
	op := Start(ctx, rt, OperationStart{Domain: DomainJob, Name: "j"})
	Add(op.Context(), "k", "v")
	if !op.End(nil) || len(ts.Events()) != 1 {
		t.Fatal("canceled parent context broke the lifecycle")
	}
}

// ════════════════════════════════════════════════════════════════════
// Agent J — sink contract edges
// ════════════════════════════════════════════════════════════════════

// The Encoded() contract: concurrent callers race benignly and share
// the winner's buffer. Pin it hard under -race: 16 goroutines, all
// must return the same bytes.
func TestCrashConcurrentEncoded(t *testing.T) {
	fields := make([]Field, 64)
	for i := range fields {
		fields[i] = fieldOf(fmt.Sprintf("k%d", i), i)
	}
	rec := recOf(LevelInfo, "m", fields...)

	const n = 16
	const iters = 50
	var wg sync.WaitGroup
	lines := make([][]byte, n)
	for g := range n {
		wg.Go(func() {
			for range iters {
				lines[g] = rec.Encoded()
			}
		})
	}
	wg.Wait()
	for g := 1; g < n; g++ {
		if string(lines[g]) != string(lines[0]) {
			t.Fatalf("goroutine %d got different bytes", g)
		}
	}
	if !json.Valid(lines[0]) {
		t.Fatalf("line invalid: %s", lines[0])
	}
}

// A sink that encodes for the first time AFTER the event was recycled
// violates the retention contract — characterize: no panic, and the
// result is either the original line or another request's line (the
// documented hazard), never memory unsafety.
func TestCrashFirstEncodeAfterRecycle(t *testing.T) {
	rt := MustCompile(Config{Sink: NewJSONSink(&strings.Builder{}), SamplingRate: 1})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "first"})
	Add(op.Context(), "who", "first")
	// Do NOT End through a capturing sink; grab the record another way.
	sink := &recordGrabber{}
	rt2 := MustCompile(Config{Sink: sink, SamplingRate: 1})
	op2 := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "first"})
	Add(op2.Context(), "who", "first")
	_ = op2.End(nil)
	_ = op.End(nil)

	// Recycle a few events through the pool.
	for i := range 8 {
		o := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "later"})
		Add(o.Context(), "who", fmt.Sprintf("later-%d", i))
		_ = o.End(nil)
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("late Encoded panicked: %v", r)
			}
		}()
		line := sink.rec.Encoded()
		_ = json.Valid(line) // may be another request's line: the hazard
	}()
}

type recordGrabber struct {
	rec *Record
}

func (g *recordGrabber) Write(_ context.Context, rec *Record) { g.rec = rec }

// Fanout: one sink fanning out to several downstream sinks
// concurrently — the Sink contract requires the fanout itself to be
// safe, and the first-party sinks it calls must be too.
type concurrentFanoutSink struct {
	inner []Sink
}

func (f *concurrentFanoutSink) Write(ctx context.Context, rec *Record) {
	var wg sync.WaitGroup
	for _, s := range f.inner {
		wg.Go(func() {
			s.Write(ctx, rec)
		})
	}
	wg.Wait()
}

func TestCrashFanoutSinks(t *testing.T) {
	var buf strings.Builder
	ts := NewTestSink()
	fan := &concurrentFanoutSink{inner: []Sink{NewJSONSink(&buf), ts, NewJSONSink(&strings.Builder{})}}
	rt := MustCompile(Config{Sink: fan, SamplingRate: 1})

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 25 {
				op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
				Add(op.Context(), "w", w, "i", i)
				_ = op.End(nil)
			}
		})
	}
	wg.Wait()

	if got, want := len(ts.Events()), 200; got != want {
		t.Fatalf("captured = %d, want %d", got, want)
	}
	for ln := range strings.SplitSeq(strings.TrimRight(buf.String(), "\n"), "\n") {
		if ln == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(ln), &m); err != nil {
			t.Fatalf("fanout line unparseable: %v: %s", err, ln)
		}
	}
}

// Encoding inside Write while stragglers hammer the (sealed) WAL:
// post-seal writes are no-ops, so the encoded line is stable.
func TestCrashEncodeDuringStragglerFire(t *testing.T) {
	sink := &encodingStragglerSink{}
	rt := MustCompile(Config{Sink: sink, SamplingRate: 1})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	Add(op.Context(), "stable", "v")
	_ = op.End(nil)
	before := string(sink.rec.Encoded()) // cached now

	// Stragglers fire after the seal: no-op writes, cached line stable.
	for i := range 100 {
		Add(op.Context(), fmt.Sprintf("straggler-%d", i), i)
	}
	if after := string(sink.rec.Encoded()); after != before {
		t.Fatalf("encoded line changed across straggler fire:\n%s\n%s", before, after)
	}
}

type encodingStragglerSink struct {
	rec *Record
}

func (s *encodingStragglerSink) Write(_ context.Context, rec *Record) {
	s.rec = rec
	rec.Encoded() // cache the line while the record is valid
}

// ════════════════════════════════════════════════════════════════════
// Agent L — guarded-mode deterministic stress
// ════════════════════════════════════════════════════════════════════

// Mixed writers on one guarded event plus one End: no race (pinned by
// -race), exactly one event, line valid.
func TestCrashMixedWritersSingleEnd(t *testing.T) {
	for round := range 10 {
		rt, ts := testRT(t, nil)
		op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})

		var wg sync.WaitGroup
		stop := make(chan struct{})

		for w := range 6 {
			wg.Go(func() {
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					Add(op.Context(), fmt.Sprintf("w%d", w), i)
				}
			})
		}
		for s := range 3 {
			wg.Go(func() {
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					switch s % 3 {
					case 0:
						Error(op.Context(), fmt.Errorf("soft-%d", i))
					case 1:
						SetLevel(op.Context(), LevelWarn)
					case 2:
						SetMessage(op.Context(), fmt.Sprintf("m-%d", i))
					}
				}
			})
		}
		// Watchdog-style snapshotter.
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = op.ev.snapshotFields(genOf(op.ev))
				}
			}
		})

		_ = op.End(nil)
		close(stop)
		wg.Wait()

		evs := ts.Events()
		if len(evs) != 1 {
			t.Fatalf("round %d: events = %d, want 1", round, len(evs))
		}
		rec := recOf(evs[0].Level(), evs[0].Message(), evs[0].Fields()...)
		if !json.Valid(rec.Encoded()) {
			t.Fatalf("round %d: line invalid", round)
		}
	}
}

// A watchdog snapshot racing End's seal: both take the same mutex, so
// every interleaving must be safe and the committed line complete.
func TestCrashSnapshotRacingSeal(t *testing.T) {
	for round := range 20 {
		rt, ts := testRT(t, nil)
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
		Add(op.Context(), "k", round)

		stop := make(chan struct{})
		var wg sync.WaitGroup
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = op.ev.snapshotFields(genOf(op.ev))
				}
			}
		})
		_ = op.End(nil)
		close(stop)
		wg.Wait()

		if len(ts.Events()) != 1 {
			t.Fatalf("round %d: events = %d", round, len(ts.Events()))
		}
	}
}

// setError racing seal on a guarded event: the P5 discipline (append +
// hasErr latch under the same mutex as the seal) must keep the latch
// consistent — an error that landed is never lost, a straggler error
// post-seal never latches.
func TestCrashErrorVsSeal(t *testing.T) {
	for round := range 20 {
		rt, ts := testRT(t, nil)
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})

		var wg sync.WaitGroup
		wg.Go(func() {
			Error(op.Context(), fmt.Errorf("racing-%d", round))
		})
		_ = op.End(nil)
		wg.Wait()

		evs := ts.Events()
		if len(evs) != 1 {
			t.Fatalf("round %d: events = %d", round, len(evs))
		}
		// With End(nil) the outcome can only be success: outcome derives
		// from the deferred error pointer, not the WAL error field, so a
		// racing Error() that lands pre-seal yields success WITH an error
		// field — a legitimate state. The oracle is the latch itself: if
		// an error field is present it must be the racing error, and a
		// post-seal Error() must never latch.
		o, _ := evs[0].Lookup("op.outcome")
		if o != string(OutcomeSuccess) {
			t.Fatalf("round %d: outcome = %v, want success (outcome is errp-derived)", round, o)
		}
		if errVal, hasErr := evs[0].Lookup("error"); hasErr {
			m, _ := errVal.(map[string]any)
			msg, _ := m["message"].(string)
			if msg != fmt.Sprintf("racing-%d", round) {
				t.Fatalf("round %d: foreign error field latched: %#v", round, errVal)
			}
		}
	}
}

// Snapshot racing the owner's post-seal appends (annotatePostSeal):
// snapshots are copies, so the committed line is unaffected.
func TestCrashSnapshotVsPostSealAppends(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_, _ = op.ev.snapshotFields(genOf(op.ev))
			}
		}
	})
	Add(op.Context(), "k", "v")
	_ = op.End(nil)
	close(stop)
	wg.Wait()

	evs := ts.Events()
	if len(evs) != 1 {
		t.Fatalf("events = %d", len(evs))
	}
	rec := recOf(evs[0].Level(), evs[0].Message(), evs[0].Fields()...)
	line := string(rec.Encoded())
	// http.status is middleware-written (not core): core completion
	// fields only.
	for _, want := range []string{`"op.outcome"`, `"duration_ms"`, `"op.domain"`} {
		if !strings.Contains(line, want) {
			t.Fatalf("completion fields missing (%s): %s", want, line)
		}
	}
}

// ════════════════════════════════════════════════════════════════════
// Agent K — adversarial fuzz targets
// ════════════════════════════════════════════════════════════════════

func FuzzCrashAdversarialValues(f *testing.F) {
	f.Add("plain", 0)
	f.Add("\xff\xfe\x00\x1f\"\n\\", 1)
	f.Add(strings.Repeat("x", 100000), 2)
	f.Add("time", 3)
	f.Add("message", 4)
	f.Add("fields.time", 5)
	f.Add("", 6)
	f.Add("日本語\xef\xbb\xbf", 7)

	f.Fuzz(func(t *testing.T, key string, variant int) {
		var val any
		switch variant % 12 {
		case 0:
			val = t.Name()
		case 1:
			val = math.NaN()
		case 2:
			val = math.Inf(-1)
		case 3:
			m := map[string]any{"self": nil}
			m["self"] = m // cycle
			val = m
		case 4:
			val = nest(200, "leaf")
		case 5:
			val = func() {} // unmarshalable
		case 6:
			val = make(chan int)
		case 7:
			var nilPtr *strings.Builder
			val = nilPtr
		case 8:
			val = strings.Repeat("\xff\x00", 5000)
		case 9:
			val = []any{math.MaxFloat64, math.SmallestNonzeroFloat64, math.Copysign(0, -1), nil, true}
		case 10:
			val = time.Date(-5, 13, 40, 25, 61, 61, -1, time.UTC)
		case 11:
			val = map[string]any{"": map[string]any{"nested": []any{map[string]any{"deep": 1}}}}
		}

		rt, ts := testRT(t, nil)
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
		Add(op.Context(), key, val, "sib", 1)
		emitted := op.End(nil)
		if !emitted || len(ts.Events()) != 1 {
			t.Fatalf("event dropped: variant %d key %q", variant, key)
		}
		rec := recOf(LevelInfo, "m", evs2Fields(t, ts)...)
		line := rec.Encoded()
		if !json.Valid(line) {
			t.Fatalf("invalid line (variant %d): %s", variant, truncate(line, 200))
		}
	})
}

func truncate(b []byte, n int) string {
	if len(b) <= n {
		return string(b)
	}
	return string(b[:n]) + "..."
}

// FuzzCrashGuardedInterleavings: random interleavings of guarded writes,
// setters, and a single End on a guarded event. Oracle: exactly one
// event, valid line, no lost completion fields.
func FuzzCrashGuardedInterleavings(f *testing.F) {
	f.Add(0, 0, 0, 0)
	f.Add(1, 10, 3, 7)
	f.Add(2, 0, 0, 1)
	f.Fuzz(func(t *testing.T, variant, writes, setters, errorsN int) {
		if writes > 64 {
			writes = 64
		}
		if setters > 16 {
			setters = 16
		}
		if errorsN > 16 {
			errorsN = 16
		}
		rt, ts := testRT(t, nil)
		op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
		for i := range writes {
			Add(op.Context(), key2(i), i)
		}
		for i := range setters {
			if i%2 == 0 {
				SetLevel(op.Context(), LevelWarn)
			} else {
				SetMessage(op.Context(), "override")
			}
		}
		for range errorsN {
			Error(op.Context(), errBoom2{})
		}
		_ = op.End(nil)
		if variant%3 == 0 {
			Add(op.Context(), "straggler", 1) // post-seal no-op
		}
		evs := ts.Events()
		if len(evs) != 1 {
			t.Fatalf("events = %d, want 1", len(evs))
		}
		if _, ok := evs[0].Lookup("straggler"); ok {
			t.Fatal("post-seal write landed")
		}
		rec := recOf(evs[0].Level(), evs[0].Message(), evs[0].Fields()...)
		line := string(rec.Encoded())
		for _, want := range []string{`"op.outcome"`, `"duration_ms"`} {
			if !strings.Contains(line, want) {
				t.Fatalf("missing %s: %s", want, truncate([]byte(line), 200))
			}
		}
		if !json.Valid([]byte(line)) {
			t.Fatalf("invalid line: %s", truncate([]byte(line), 200))
		}
	})
}

func key2(i int) string { return "k" + strings.Repeat("p", i%8) + string(rune('a'+i%26)) }

type errBoom2 struct{}

func (errBoom2) Error() string { return "boom2" }

// ════════════════════════════════════════════════════════════════════
// Agent N — testing/synctest bubbles (per-test goroutine hygiene)
// ════════════════════════════════════════════════════════════════════
//
// goleak gates the whole suite at TestMain exit; synctest bubbles make
// "every goroutine this test started has exited" a per-test assertion
// (Test waits for bubble goroutines before returning) and replace real
// sleeps with a virtual clock. Stable since go1.25 — the module floor —
// so no build tags. Constraints respected: no t.Run/t.Parallel inside
// a bubble, bubble channels stay inside.

// Concurrent End against a gated sink: the winner durably blocks on a
// channel inside the bubble (no wall or virtual time involved), the
// losers spin in End's one-shot wait, every racer provably exits with
// the bubble, and exactly one write lands.
//
// NOTE — virtual-clock incompatibility, pinned here: the original
// variant used a slowSink (time.Sleep) and hangs forever in a bubble.
// End's one-shot wait spins with runtime.Gosched, and spinners are
// never durably blocked, so synctest's clock cannot advance while the
// winner sleeps — a live lock, not a detected deadlock: a regressed
// publish defer would HANG the bubble until the test timeout, never
// fail it, because spinners are never durably blocked. Any future
// library path that sleeps while another goroutine spins cannot be
// bubble-tested with real time.Sleep in the winner's path — including
// the v1.1 watchdog itself, which would sleep on real timers while
// request writers spin in the guarded-add path: the same livelock,
// mirrored. Use channel gates instead.
func TestSynctestConcurrentEndGatedSink(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		gate := make(chan struct{})
		gated := &gateSink{gate: gate}
		rt := MustCompile(Config{Sink: gated, SamplingRate: 1})
		op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
		const racers = 16
		var wg sync.WaitGroup
		first := make([]bool, racers)
		for i := range racers {
			wg.Go(func() {
				first[i] = op.End(nil)
			})
		}
		// The gate usually closes before the winner reaches Write, so
		// the durable block is scheduling-dependent — the assertions
		// that matter are the publication result and exactly one write.
		close(gate)
		wg.Wait()
		if !first[0] {
			t.Fatal("first racer observed not-emitted (publication must report the winner's result)")
		}
		for i := 1; i < racers; i++ {
			if first[i] != first[0] {
				t.Fatalf("racer %d observed %v, racer 0 %v", i, first[i], first[0])
			}
		}
		if gated.writes != 1 {
			t.Fatalf("sink writes = %d, want 1", gated.writes)
		}
	})
}

type gateSink struct {
	gate   chan struct{}
	writes int
}

func (s *gateSink) Write(_ context.Context, _ *Record) {
	<-s.gate // durably blocks inside the bubble
	s.writes++
}

// Guarded-mode mixed writers plus a snapshotter against a single End:
// the bubble returning proves every goroutine exited (a per-test leak
// assertion), and the committed line is complete and valid.
func TestSynctestGuardedWritersAllExit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt, ts := testRT(t, nil)
		op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
		stop := make(chan struct{})
		var wg sync.WaitGroup
		for w := range 6 {
			wg.Go(func() {
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
						Add(op.Context(), fmt.Sprintf("w%d", w), i)
					}
				}
			})
		}
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = op.ev.snapshotFields(genOf(op.ev))
				}
			}
		})
		Add(op.Context(), "owner", "final")
		_ = op.End(nil)
		close(stop)
		wg.Wait()

		evs := ts.Events()
		if len(evs) != 1 {
			t.Fatalf("events = %d, want 1", len(evs))
		}
		if v, ok := evs[0].Lookup("owner"); !ok || v != "final" {
			t.Fatalf("owner field lost: %#v", v)
		}
	})
}

// The v1.1 watchdog shape, simulated deterministically under the
// virtual clock: a stalled request is guarded (what the watchdog will
// do), the watchdog takes a mid-flight snapshot after its stall
// threshold, the request resumes writing, then commits. The snapshot
// is a stable copy of a strict prefix of the committed WAL.
func TestSynctestWatchdogShape(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt, ts := testRT(t, nil)
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "stalled"})
		Add(op.Context(), "phase", "before-stall")

		// The watchdog snapshots a stalled request.
		time.Sleep(500 * time.Millisecond) // virtual: instant, deterministic
		snap, ok := op.ev.snapshotFields(genOf(op.ev))
		if !ok {
			t.Fatal("snapshot refused")
		}

		// The request wakes, REWRITES a snapshotted key, adds a new
		// one, then commits. The rewrite is what makes the stability
		// assertion real: the snapshot must still show the pre-rewrite
		// value (a snapshot aliasing the live slice would fail here).
		Add(op.Context(), "phase", "rewritten", "phase2", "after-stall", "k", "v")
		_ = op.End(nil)

		if len(snap) == 0 || len(snap) >= len(ts.Events()[0].Fields()) {
			t.Fatalf("snapshot = %d fields, committed = %d (want a strict prefix)",
				len(snap), len(ts.Events()[0].Fields()))
		}
		// Snapshot stability, made real by the rewrite: the snapshot
		// still shows the pre-rewrite value...
		snapPhase, ok := snapshotLookup(snap, "phase")
		if !ok || snapPhase != "before-stall" {
			t.Fatalf("snapshot phase = %v, want pre-rewrite \"before-stall\" (snapshot aliased the live slice?)", snapPhase)
		}
		// ...while the committed WAL carries the rewrite, and every
		// snapshotted key still exists.
		committed := ts.Events()[0]
		if v, _ := committed.Lookup("phase"); v != "rewritten" {
			t.Fatalf("committed phase = %v, want the rewrite", v)
		}
		for _, f := range snap {
			if _, ok := committed.Lookup(f.Key()); !ok {
				t.Fatalf("snapshotted key %q missing from committed event", f.Key())
			}
		}
		if v, _ := committed.Lookup("phase2"); v != "after-stall" {
			t.Fatalf("post-snapshot field lost: %v", v)
		}
	})
}

// Straggler storm in a bubble: the request commits, stragglers fire
// after, and every one of them exits before the bubble returns.
func TestSynctestStragglerStormBubble(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rt, ts := testRT(t, nil)
		const requests = 25
		for i := range requests {
			op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
			Add(op.Context(), "mine", i)
			_ = op.End(nil)
			var wg sync.WaitGroup
			for s := range 8 {
				wg.Go(func() {
					Add(op.Context(), "straggler", s)
				})
			}
			wg.Wait()
		}
		evs := ts.Events()
		if len(evs) != requests {
			t.Fatalf("events = %d, want %d", len(evs), requests)
		}
		for _, ev := range evs {
			if _, ok := ev.Lookup("straggler"); ok {
				t.Fatal("post-seal straggler field reached the wire")
			}
		}
	})
}

func snapshotLookup(fields []Field, key string) (any, bool) {
	for _, f := range fields {
		if f.key == key {
			return valueOf(f), true
		}
	}
	return nil, false
}

// TestCrashWrappedTypedNilContained pins the containment against
// %w-wrapped typed-nils: isTypedNilError inspects only the outermost
// error, so the wrap chain's inner nil deref used to panic inside End
// AFTER its recover — losing the event and masking any in-flight
// panic. safeErrorMessage fences it (found by the GLM-5.3 core audit).
type wrappedNilErr struct{ msg string }

func (e *wrappedNilErr) Error() string { return e.msg }

func TestCrashWrappedTypedNilContained(t *testing.T) {
	var pe *wrappedNilErr
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Error(op.Context(), fmt.Errorf("job failed: %w", pe))
	Add(op.Context(), "e2", fmt.Errorf("join: %w", pe))
	if !op.End(nil) {
		t.Fatal("wrapped typed-nil dropped the event")
	}
	if len(ts.Events()) != 1 {
		t.Fatalf("events = %d", len(ts.Events()))
	}
	rec := recOf(LevelInfo, "m", ts.Events()[0].Fields()...)
	if !json.Valid(rec.Encoded()) {
		t.Fatalf("line invalid: %s", rec.Encoded())
	}
}

// TestCrashPanickingErrorContained fences arbitrary panicking Error()
// implementations through the error-metainfo path.
type boomErr struct{}

func (boomErr) Error() string { panic("boom") }

func TestCrashPanickingErrorContained(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Error(op.Context(), boomErr{})
	if !op.End(nil) || len(ts.Events()) != 1 {
		t.Fatal("panicking Error() implementation lost the event")
	}
	rec := recOf(LevelInfo, "m", ts.Events()[0].Fields()...)
	if !json.Valid(rec.Encoded()) {
		t.Fatalf("line invalid: %s", rec.Encoded())
	}
}
