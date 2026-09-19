package unolog

// Operation lifecycle, concurrent-End, and panic tests

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func testRT(t *testing.T, mut func(*Config)) (*Runtime, *TestSink) {
	t.Helper()
	cfg := Config{Sink: NewTestSink(), SamplingRate: 1}
	if mut != nil {
		mut(&cfg)
	}
	rt, err := Compile(cfg)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ts, _ := rt.sink.(*TestSink)
	return rt, ts
}

func TestLifecycleBasicEmit(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "import", ID: "j-1"})
	ctx := op.Context()
	Add(ctx, "user_id", "u_1", "attempt_no", 2)
	Add(ctx, "took", 1500*time.Millisecond)
	err := errors.New("db down")
	if !op.End(&err) {
		t.Fatal("event not emitted")
	}

	events := ts.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d", len(events))
	}
	ev := events[0]
	if ev.Level() != LevelError {
		t.Errorf("level = %v", ev.Level())
	}
	if ev.Message() != "operation_completed" {
		t.Errorf("message = %q", ev.Message())
	}
	checks := map[string]any{
		"op.domain":   "job",
		"op.name":     "import",
		"op.id":       "j-1",
		"user_id":     "u_1",
		"attempt_no":  int64(2),
		"took":        1500 * time.Millisecond,
		"op.outcome":  "failure",
		"duration_ms": int64(0), // overwritten below with presence check
	}
	for k, want := range checks {
		got, ok := ev.Lookup(k)
		if !ok {
			t.Errorf("missing field %q", k)
			continue
		}
		if k == "duration_ms" {
			if _, isInt := got.(int64); !isInt {
				t.Errorf("duration_ms = %T", got)
			}
			continue
		}
		if got != want {
			t.Errorf("%q = %v, want %v", k, got, want)
		}
	}
	if errField, ok := ev.Lookup("error"); !ok {
		t.Error("missing structured error field")
	} else if m, ok := errField.(map[string]any); !ok || m["message"] != "db down" {
		t.Errorf("error.message = %v", errField)
	}
}

func TestLifecycleHTTPDefaults(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "GET /x"})
	Add(op.Context(), "http.method", "GET", "http.path", "/x", "http.status", 204)
	op.End(nil)

	ev := ts.Events()[0]
	if ev.Message() != "request_completed" {
		t.Errorf("message = %q, want request_completed", ev.Message())
	}
	if v, _ := ev.Lookup("op.outcome"); v != "success" {
		t.Errorf("outcome = %v", v)
	}
	if _, hasOpCode := ev.Lookup("op.code"); hasOpCode {
		t.Error("op.code must not be emitted for HTTP operations (http.status carries it)")
	}
	if v, _ := ev.Lookup("http.status"); v != int64(204) {
		t.Errorf("http.status = %v", v)
	}
	if ev.Level() != LevelInfo {
		t.Errorf("level = %v", ev.Level())
	}
}

func TestLifecycleOneShotEnd(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{})
	first := op.End(nil)
	second := op.End(nil)
	third := op.End(nil)
	if !first || second != first || third != first {
		t.Fatalf("one-shot violated: %v %v %v", first, second, third)
	}
	if len(ts.Events()) != 1 {
		t.Fatalf("events = %d, want 1", len(ts.Events()))
	}
	// pool state intact: a fresh request still works
	op2 := Start(context.Background(), rt, OperationStart{})
	if !op2.End(nil) {
		t.Fatal("second request dropped")
	}
	if len(ts.Events()) != 2 {
		t.Fatalf("events = %d, want 2", len(ts.Events()))
	}
}

func TestLifecyclePanicCapturedAndRepanicked(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "boom"})

	func() {
		defer func() { _ = recover() }() // swallows End's re-panic
		defer op.End(nil)                // direct defer: observes the panic
		panic("kaboom")
	}()

	if len(ts.Events()) != 1 {
		t.Fatal("panic event not emitted")
	}
	ev := ts.Events()[0]
	if v, _ := ev.Lookup("op.outcome"); v != "panic" {
		t.Errorf("outcome = %v", v)
	}
	if p, ok := ev.Lookup("panic"); !ok {
		t.Error("missing panic field")
	} else if pm, ok := p.(map[string]any); !ok || pm["value"] != "kaboom" {
		t.Errorf("panic.value = %v", p)
	}
	if ev.Level() != LevelError {
		t.Errorf("level = %v", ev.Level())
	}
}

// deferred End observes the panic itself and must re-panic after commit
func TestLifecycleRepanic(t *testing.T) {
	rt, _ := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{})
	repanicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				repanicked = true
				if r != "original" {
					t.Errorf("re-panic value = %v", r)
				}
			}
		}()
		defer op.End(nil)
		panic("original")
	}()
	if !repanicked {
		t.Fatal("End swallowed the panic")
	}
}

func TestOutcomePrecedence(t *testing.T) {
	cases := []struct {
		name     string
		setup    func(ctx context.Context)
		err      error
		panicked any
		want     Outcome
	}{
		{"success", func(context.Context) {}, nil, nil, OutcomeSuccess},
		{"panic beats error", func(context.Context) {}, errors.New("e"), "p", OutcomePanic},
		{"error", func(context.Context) {}, errors.New("e"), nil, OutcomeFailure},
		{"canceled", func(context.Context) {}, context.Canceled, nil, OutcomeCanceled},
		{"timeout", func(context.Context) {}, context.DeadlineExceeded, nil, OutcomeTimeout},
		{"explicit beats 5xx", func(ctx context.Context) {
			Add(ctx, "op.outcome", "retry")
			Add(ctx, "http.status", 503)
		}, nil, nil, OutcomeRetry},
		{"error beats explicit", func(ctx context.Context) { Add(ctx, "op.outcome", "retry") }, errors.New("e"), nil, OutcomeFailure},
		{"5xx status", func(ctx context.Context) { Add(ctx, "http.status", 500) }, nil, nil, OutcomeFailure},
		{"4xx is success", func(ctx context.Context) { Add(ctx, "http.status", 404) }, nil, nil, OutcomeSuccess},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rt, ts := testRT(t, nil)
			op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "x"})
			c.setup(op.Context())
			if c.panicked != nil {
				func() {
					defer func() { _ = recover() }()
					defer op.End(nil)
					panic(c.panicked)
				}()
			} else {
				op.End(&c.err)
			}
			got, _ := ts.Events()[0].Lookup("op.outcome")
			outcome, ok := got.(string)
			if !ok || Outcome(outcome) != c.want {
				t.Fatalf("outcome = %v, want %v", got, c.want)
			}
		})
	}
}

// TestPanicBeatsErrorWhenCoDelivered pins the panic > error precedence
// for the shape the table test cannot reach: the error pointer is
// already set when End runs AND a panic is in flight (defer op.End(&err)
// with err non-nil, then panic). resolveOutcome must still resolve
// OutcomePanic — a swap to error-first precedence silently turns the
// event into OutcomeFailure. Found as a gap by mutation testing (M7):
// every panic test deferred op.End(nil), so the error was never
// co-delivered and the precedence row in TestOutcomePrecedence carried
// an err it never passed to End.
func TestPanicBeatsErrorWhenCoDelivered(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "both"})

	err := errors.New("original error")
	func() {
		defer func() { _ = recover() }() // swallow End's re-panic
		defer op.End(&err)               // direct defer: observes the panic
		panic("panic value")
	}()

	events := ts.Events()
	if len(events) != 1 {
		t.Fatalf("captured %d events, want 1", len(events))
	}
	ev := events[0]

	if v, _ := ev.Lookup("op.outcome"); v != string(OutcomePanic) {
		t.Fatalf("op.outcome = %v, want %q — panic must beat the co-delivered error", v, OutcomePanic)
	}
	if p, ok := ev.Lookup("panic"); !ok {
		t.Fatal("missing panic field")
	} else if pm, ok := p.(map[string]any); !ok || pm["value"] != "panic value" {
		t.Fatalf("panic field = %v, want value %q", p, "panic value")
	}
	if e, ok := ev.Lookup("error"); !ok {
		t.Fatal("missing error field")
	} else if em, ok := e.(map[string]any); !ok || em["message"] != "original error" {
		t.Fatalf("error.message = %v, want %q — the real error must survive, not the synthetic \"panic: …\" error", em["message"], "original error")
	}
}

// TestErrorsBypassSampling is amendment 4: failures are never sampled
// away, structurally, before any custom sampler runs.
func TestErrorsBypassSampling(t *testing.T) {
	samplerCalls := 0
	rt, ts := testRT(t, func(c *Config) {
		c.Sampler = func(in SampleInput) bool {
			samplerCalls++
			return false
		}
	})
	_ = rt

	// failing request still emits; the sampler never ran
	op := Start(context.Background(), rt, OperationStart{})
	err := errors.New("boom")
	if !op.End(&err) {
		t.Fatal("error event was sampled away")
	}
	if samplerCalls != 0 {
		t.Fatalf("custom sampler ran %d times on an error event", samplerCalls)
	}
	if len(ts.Events()) != 1 {
		t.Fatal("error event not written")
	}

	// healthy request is dropped by the sampler
	op2 := Start(context.Background(), rt, OperationStart{})
	if op2.End(nil) {
		t.Fatal("healthy event survived NeverSampler")
	}
	if samplerCalls != 1 {
		t.Fatalf("sampler calls = %d, want 1", samplerCalls)
	}
}

// TestRetryDoesNotBypassSampling pins that an explicit non-error
// outcome is not structurally kept: SampleInput.HasError is documented
// as error-or-panic, so a retry at rate 0 drops like any healthy event.
func TestRetryDoesNotBypassSampling(t *testing.T) {
	ts := NewTestSink()
	rt := MustCompile(Config{Sink: ts, SamplingRate: 0})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "retry"})
	Add(op.Context(), KeyOpOutcome, string(OutcomeRetry))
	if op.End(nil) {
		t.Fatal("retry event bypassed sampling at rate 0")
	}
	if got := len(ts.Events()); got != 0 {
		t.Fatalf("emitted %d events at rate 0, want 0", got)
	}

	// Control: the same outcome at rate 1 is kept.
	ts.Reset()
	rt = MustCompile(Config{Sink: ts, SamplingRate: 1})
	op = Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "retry"})
	Add(op.Context(), KeyOpOutcome, string(OutcomeRetry))
	if !op.End(nil) {
		t.Fatal("retry event was dropped at rate 1")
	}
	if got, _ := ts.Events()[0].Lookup(KeyOpOutcome); got != string(OutcomeRetry) {
		t.Fatalf("outcome = %v, want retry", got)
	}
}

// TestCanonicalStatusKeepsLastUsableWrite pins the scanWAL kind rule: a
// later write of another kind is not a usable canonical value and does
// not erase an earlier usable one, so a stray string write cannot hide
// a 5xx.
func TestCanonicalStatusKeepsLastUsableWrite(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "x"})
	Add(op.Context(), KeyHTTPStatus, 500)
	Add(op.Context(), KeyHTTPStatus, "503") // wrong kind: not canonical
	op.End(nil)

	ev := ts.Events()[0]
	if got, _ := ev.Lookup(KeyOpOutcome); got != string(OutcomeFailure) {
		t.Fatalf("outcome = %v, want failure from the usable int status", got)
	}
	// Lookup keeps the last-write-wins view: the user's string write.
	if got, _ := ev.Lookup(KeyHTTPStatus); got != "503" {
		t.Fatalf("wire status = %v, want the user's last write (503)", got)
	}
}

// TestSampleInputUsesWALOpName pins v0 HTTP parity: a last-write
// op.name on the WAL (the route template) is what Sampler sees as
// Operation, not the original Start name ("request").
func TestSampleInputUsesWALOpName(t *testing.T) {
	var got string
	rt, ts := testRT(t, func(c *Config) {
		c.Sampler = func(in SampleInput) bool {
			got = in.Operation
			return true
		}
	})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	Add(op.Context(), "op.name", "GET /orders/{id}")
	if !op.End(nil) {
		t.Fatal("event dropped")
	}
	if got != "GET /orders/{id}" {
		t.Fatalf("SampleInput.Operation = %q, want the WAL op.name", got)
	}
	if v, ok := ts.Events()[0].Lookup("op.name"); !ok || v != "GET /orders/{id}" {
		t.Fatalf("wire op.name = %v", v)
	}
}

// TestSampleInputCodeSurfacesOpCode pins the sampler contract: for
// non-HTTP operations Code carries the canonical op.code (README:
// "for non-HTTP operations use Domain, Operation, Outcome, and Code"),
// while StatusCode stays the http.status view. HTTP samplers keep
// seeing http.status even if a stray op.code was written.
func TestSampleInputCodeSurfacesOpCode(t *testing.T) {
	var jobCode, jobStatus, httpCode int
	rt, _ := testRT(t, func(c *Config) {
		c.Sampler = func(in SampleInput) bool {
			switch in.Domain {
			case DomainJob:
				jobCode, jobStatus = in.Code, in.StatusCode
			case DomainHTTP:
				httpCode = in.Code
			default:
				t.Fatalf("unexpected domain %q", in.Domain)
			}
			return true
		}
	})

	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "job"})
	Add(op.Context(), "op.code", 42)
	_ = op.End(nil)
	if jobCode != 42 {
		t.Fatalf("job Code = %d, want 42 (canonical op.code)", jobCode)
	}
	if jobStatus != 0 {
		t.Fatalf("job StatusCode = %d, want 0 (http.status view)", jobStatus)
	}

	op = Start(t.Context(), rt, OperationStart{Domain: DomainJob, Name: "job"})
	Add(op.Context(), KeyHTTPStatus, 201)
	op.End(nil)
	if jobCode != 0 || jobStatus != 201 {
		t.Fatalf("job Code/StatusCode = %d/%d, want 0/201 without op.code", jobCode, jobStatus)
	}

	// 2xx, not 5xx: error events bypass the sampler structurally.
	hop := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "request"})
	Add(hop.Context(), "http.status", 201, "op.code", 7)
	_ = hop.End(nil)
	if httpCode != 201 {
		t.Fatalf("http Code = %d, want 201 (http.status wins on HTTP)", httpCode)
	}
}

func TestSampleInputEmptyStringOverrides(t *testing.T) {
	var got SampleInput
	rt, _ := testRT(t, func(c *Config) {
		c.Sampler = func(in SampleInput) bool {
			got = in
			return true
		}
	})
	op := Start(t.Context(), rt, OperationStart{Domain: DomainHTTP, Name: "original"})
	Add(op.Context(), KeyHTTPMethod, "GET", KeyHTTPPath, "/keep", KeyOpName, "old")
	Add(op.Context(), KeyHTTPMethod, "", KeyHTTPPath, "", KeyOpName, "")
	// Wrong kinds do not replace the last usable typed value, including empty strings.
	Add(op.Context(), KeyHTTPMethod, 1, KeyHTTPPath, 2, KeyOpName, 3)
	op.End(nil)
	if got.Method != "" || got.Path != "" || got.Operation != "" {
		t.Fatalf("sampler got method=%q path=%q operation=%q; want empty overrides", got.Method, got.Path, got.Operation)
	}
}

func TestCustomSamplerSeesCompletionFields(t *testing.T) {
	for _, keep := range []bool{false, true} {
		calls := 0
		rt, _ := testRT(t, func(c *Config) {
			c.Sampler = func(in SampleInput) bool {
				calls++
				for key, want := range map[string]any{
					KeyOpDomain: string(DomainJob), KeyOpName: "job", KeyOpID: "j1",
					KeyOpCode: int64(42), KeyOpOutcome: string(OutcomeSuccess),
				} {
					if got, ok := in.Lookup(key); !ok || got != want {
						t.Fatalf("sampler lookup %s = %v (%v), want %v", key, got, ok, want)
					}
				}
				if _, ok := in.Lookup(KeyDurationMS); !ok {
					t.Fatal("sampler cannot see completion duration")
				}
				return keep
			}
		})
		op := Start(t.Context(), rt, OperationStart{Domain: DomainJob, Name: "job", ID: "j1"})
		Add(op.Context(), KeyOpCode, 42)
		if emitted := op.End(nil); emitted != keep || calls != 1 {
			t.Fatalf("emitted=%v calls=%d, want %v and 1", emitted, calls, keep)
		}
	}
}

// TestNonHTTPOpCode pins the canonical-field rule: op.code is non-HTTP
// only, surfaced from the explicit op.code field the caller wrote.
func TestNonHTTPOpCode(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "import"})
	Add(op.Context(), "op.code", 42)
	op.End(nil)
	ev := ts.Events()[0]
	if v, _ := ev.Lookup("op.code"); v != int64(42) {
		t.Fatalf("op.code = %v, want 42", v)
	}

	// absent when not set
	op2 := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "x"})
	op2.End(nil)
	if _, has := ts.Events()[1].Lookup("op.code"); has {
		t.Fatal("op.code emitted without an explicit field")
	}
}

// TestExplicitOpCodeOnHTTPIsUserData pins the boundary: the core never
// SYNTHESIZES op.code for HTTP operations, but an explicit user write
// is their data and survives.
func TestExplicitOpCodeOnHTTPIsUserData(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "GET /x"})
	Add(op.Context(), "http.status", 200)
	op.End(nil)
	if _, has := ts.Events()[0].Lookup("op.code"); has {
		t.Fatal("core synthesized op.code for an HTTP operation")
	}

	op2 := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP, Name: "GET /x"})
	Add(op2.Context(), "http.status", 200, "op.code", 777)
	op2.End(nil)
	if v, has := ts.Events()[1].Lookup("op.code"); !has || v != int64(777) {
		t.Fatalf("explicit user op.code dropped: %v %v", v, has)
	}
}

func TestRequestedLevelFloor(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{})
	SetLevel(op.Context(), LevelWarn) // success auto-level is info
	op.End(nil)
	if ev := ts.Events()[0]; ev.Level() != LevelWarn {
		t.Fatalf("level = %v, want warn floor", ev.Level())
	}
}

func TestSetMessageOverride(t *testing.T) {
	rt, ts := testRT(t, func(c *Config) { c.Message = "configured" })
	op := Start(context.Background(), rt, OperationStart{})
	SetMessage(op.Context(), "handler message")
	op.End(nil)
	if ev := ts.Events()[0]; ev.Message() != "handler message" {
		t.Fatalf("message = %q", ev.Message())
	}

	op2 := Start(context.Background(), rt, OperationStart{})
	op2.End(nil)
	if ev := ts.Events()[1]; ev.Message() != "configured" {
		t.Fatalf("configured message = %q", ev.Message())
	}
}

func TestNilRuntimeNoop(t *testing.T) {
	var rt *Runtime
	op := Start(context.Background(), rt, OperationStart{})
	Add(op.Context(), "k", 1) // requests run
	if op.End(nil) {
		t.Fatal("nil runtime emitted")
	}
	// and nothing panicked
}

func TestAddNoEventNoop(t *testing.T) {
	Add(context.Background(), "k", 1) // no panic
	//lint:ignore SA1012 intentional: pin the nil-context no-op contract
	Add(nil, "k", 1) //nolint:staticcheck // intentional: pin the nil-context no-op contract
	Error(context.Background(), errors.New("x"))
	SetMessage(context.Background(), "m")
	SetRoute(context.Background(), "/r")
	SetLevel(context.Background(), LevelWarn)
}

func TestLevelSamplingRates(t *testing.T) {
	rt, ts := testRT(t, func(c *Config) {
		c.SamplingRate = 0
		c.LevelSamplingRates = map[Level]float64{LevelWarn: 1}
	})
	// info events dropped (rate 0), warn kept via level rate
	op := Start(context.Background(), rt, OperationStart{})
	if op.End(nil) {
		t.Fatal("info event kept despite rate 0")
	}
	op2 := Start(context.Background(), rt, OperationStart{})
	SetLevel(op2.Context(), LevelWarn)
	if !op2.End(nil) {
		t.Fatal("warn event dropped despite level rate 1")
	}
	if len(ts.Events()) != 1 {
		t.Fatalf("events = %d", len(ts.Events()))
	}
}

func TestDomainPolicySamplingRate(t *testing.T) {
	rate := 1.0
	rt, ts := testRT(t, func(c *Config) {
		c.SamplingRate = 0
		c.OperationPolicies = map[Domain]OperationPolicy{
			DomainJob: {SamplingRate: &rate},
		}
	})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob})
	if !op.End(nil) {
		t.Fatal("job event dropped despite domain rate 1")
	}
	op2 := Start(context.Background(), rt, OperationStart{Domain: DomainHTTP})
	if op2.End(nil) {
		t.Fatal("http event kept despite generic rate 0")
	}
	if len(ts.Events()) != 1 {
		t.Fatalf("events = %d", len(ts.Events()))
	}
}

func TestOperationContextReachesSink(t *testing.T) {
	type ctxKey struct{}
	var gotCtx context.Context
	rt := MustCompile(Config{
		Sink:         sinkFunc(func(ctx context.Context, rec *Record) { gotCtx = ctx }), //nolint:fatcontext // the sink captures the request ctx and derives nothing
		SamplingRate: 1,
	})
	parent := context.WithValue(context.Background(), ctxKey{}, "request-value")
	op := Start(parent, rt, OperationStart{})
	op.End(nil)
	if gotCtx.Value(ctxKey{}) != "request-value" {
		t.Fatal("sink did not receive the request context")
	}
}

type sinkFunc func(context.Context, *Record)

func (f sinkFunc) Write(ctx context.Context, rec *Record) { f(ctx, rec) }

func TestSinkPanicDoesNotLeakPool(t *testing.T) {
	rt, _ := testRT(t, func(c *Config) {
		c.Sink = sinkFunc(func(context.Context, *Record) { panic("sink exploded") })
	})
	func() {
		defer func() { _ = recover() }()
		op := Start(context.Background(), rt, OperationStart{})
		defer op.End(nil)
	}()
	// pool must still be usable
	rt2, ts := testRT(t, nil)
	op := Start(context.Background(), rt2, OperationStart{})
	if !op.End(nil) || len(ts.Events()) != 1 {
		t.Fatal("pool corrupted after sink panic")
	}
}

func TestConcurrentRequests(t *testing.T) {
	rt, ts := testRT(t, nil)
	const goroutines = 16
	const each = 50
	done := make(chan struct{}, goroutines)
	for g := range goroutines {
		go func(g int) {
			defer func() { done <- struct{}{} }()
			for i := range each {
				op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j", ID: "g"})
				ctx := op.Context()
				Add(ctx, "g", g, "i", i)
				Add(ctx, strings.Repeat("f", 32), i)
				var err error
				if i%7 == 0 {
					err = errors.New("periodic failure")
				}
				op.End(&err)
			}
		}(g)
	}
	for range goroutines {
		<-done
	}
	events := ts.Events()
	if len(events) != goroutines*each {
		t.Fatalf("events = %d, want %d", len(events), goroutines*each)
	}
	for _, ev := range events {
		if _, ok := ev.Lookup("g"); !ok {
			t.Fatal("event lost its fields — pool corruption")
		}
	}
}

// concurrentEnd races n goroutines over one End on a fresh runtime and
// returns their results plus the emitted event count.
func concurrentEnd(t *testing.T, rate float64, n int) ([]bool, int) {
	t.Helper()
	ts := NewTestSink()
	rt := MustCompile(Config{Sink: ts, SamplingRate: rate})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "race"})

	results := make([]bool, n)
	var mu sync.Mutex
	start := sync.NewCond(&mu)
	released, ready := false, 0
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			mu.Lock()
			ready++
			start.Broadcast()
			for !released {
				start.Wait()
			}
			mu.Unlock()
			results[i] = op.End(nil)
		})
	}
	mu.Lock()
	for ready != n {
		start.Wait()
	}
	released = true
	start.Broadcast()
	mu.Unlock()
	wg.Wait()

	return results, len(ts.Events())
}

// TestConcurrentEndCharacterization races End on one operation: the
// winner commits exactly one event and every caller observes the same
// emitted result (the losers wait for the winner's publication).
func TestConcurrentEndCharacterization(t *testing.T) {
	// Emitted case (rate 1): exactly one event, every caller sees true.
	for round := range 25 {
		results, events := concurrentEnd(t, 1, 16)
		if events != 1 {
			t.Fatalf("round %d: %d events, want exactly 1", round, events)
		}
		for i, r := range results {
			if !r {
				t.Fatalf("round %d: caller %d returned false although the event was emitted", round, i)
			}
		}
	}
	// Sampled-away case (rate 0): no event, every caller sees false.
	for round := range 25 {
		results, events := concurrentEnd(t, 0, 16)
		if events != 0 {
			t.Fatalf("round %d: %d events at rate 0", round, events)
		}
		for i, r := range results {
			if r {
				t.Fatalf("round %d: caller %d saw an emission that never happened", round, i)
			}
		}
	}
}

// TestConcurrentEndGuarded races End on a guarded event: the seal mutex
// and the claim word must keep the whole race single-emission and
// race-free (concurrent guarded appends interleave with the End
// callers).
func TestConcurrentEndGuarded(t *testing.T) {
	for round := range 25 {
		ts := NewTestSink()
		rt := MustCompile(Config{Sink: ts, SamplingRate: 1})
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "guarded-race"})
		ctx := op.Context()

		var mu sync.Mutex
		start := sync.NewCond(&mu)
		released, ready := false, 0
		const n = 8
		var wg sync.WaitGroup
		results := make([]bool, n)
		for i := range n {
			wg.Go(func() {
				mu.Lock()
				ready++
				start.Broadcast()
				for !released {
					start.Wait()
				}
				mu.Unlock()
				if i%2 == 0 {
					results[i] = op.End(nil)
				} else {
					Add(ctx, "async", i) // guarded append racing the seal
					results[i] = false
				}
			})
		}
		mu.Lock()
		for ready != n {
			start.Wait()
		}
		released = true
		start.Broadcast()
		mu.Unlock()
		wg.Wait()

		if got := len(ts.Events()); got != 1 {
			t.Fatalf("round %d: %d events, want exactly 1", round, got)
		}
		for i, r := range results {
			if r != (i%2 == 0) {
				t.Fatalf("round %d: caller %d returned %v", round, i, r)
			}
		}
		// Pool is clean for the next request.
		ok := &matrixSink{}
		rt2 := MustCompile(Config{Sink: ok, SamplingRate: 1})
		op2 := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "after"})
		Add(op2.Context(), "clean", true)
		op2.End(nil)
		if len(ok.capture) != 1 || !bytes.Contains(ok.capture[0], []byte(`"clean":true`)) {
			t.Fatalf("round %d: pool corrupted after the guarded race", round)
		}
	}
}

// TestConcurrentEndErrorPointer races End with per-caller error
// pointers: the winner's error is the one recorded; the event commits
// exactly once and every caller sees the same result.
func TestConcurrentEndErrorPointer(t *testing.T) {
	for round := range 25 {
		ts := NewTestSink()
		rt := MustCompile(Config{Sink: ts, SamplingRate: 1})
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "err-race"})
		results := make([]bool, 8)
		var wg sync.WaitGroup
		for i := range 8 {
			wg.Go(func() {
				err := fmt.Errorf("caller-%d", i)
				results[i] = op.End(&err)
			})
		}
		wg.Wait()
		if len(ts.Events()) != 1 {
			t.Fatalf("round %d: %d events", round, len(ts.Events()))
		}
		for i := 1; i < len(results); i++ {
			if results[i] != results[0] {
				t.Fatalf("round %d: inconsistent results %v", round, results)
			}
		}
	}
}

// panicPayload is one panic value shape.
type panicPayload struct {
	name  string
	value any
}

// typedNilErr is a typed-nil panic payload (the interface is non-nil,
// the pointer is nil — the classic typed-nil trap).
type typedNilErr struct{}

func (p *typedNilErr) Error() string { return "typed-nil" }

// runtimeStyleError implements runtime.Error (a real runtime panic
// payload shape without reaching into the runtime package).
type runtimeStyleError string

func (e runtimeStyleError) Error() string        { return string(e) }
func (e runtimeStyleError) RuntimeError() string { return string(e) }

func panicPayloads() []panicPayload {
	return []panicPayload{
		{"string", "boom"},
		{"error", errors.New("boom-err")},
		{"int", 42},
		{"struct", struct{ X int }{7}},
		{"nil-interface", nil}, // panic(nil): *runtime.PanicNilError
		{"typed-nil", (*typedNilErr)(nil)},
		{"runtime-error", runtimeStyleError("runtime-style")},
	}
}

// capturePanicEvent runs a panicking lifecycle and returns the capture
// plus the re-panicked value.
func capturePanicEvent(t *testing.T, run func(op *Operation)) (lifeCapture, any) {
	t.Helper()
	sink := &lifeSink{}
	cfg := rtConfigFor(sink)
	op := Start(context.Background(), MustCompile(cfg), OperationStart{Domain: DomainJob, Name: "panic"})
	var rePanicked any
	func() {
		defer func() { rePanicked = recover() }()
		run(op)
	}()
	if len(sink.events) != 1 {
		t.Fatalf("captured %d events, want 1", len(sink.events))
	}
	return sink.events[0], rePanicked
}

func rtConfigFor(s Sink) Config { return Config{Sink: s, SamplingRate: 1} }

// TestPanicValuePermutations covers every payload through the direct-
// defer source (the documented usage) and the deferred-function source.
func TestPanicValuePermutations(t *testing.T) {
	for _, p := range panicPayloads() {
		t.Run("direct-"+p.name, func(t *testing.T) {
			ev, rePanicked := capturePanicEvent(t, func(op *Operation) {
				defer op.End(nil) // direct defer: observes the panic
				panic(p.value)
			})
			verifyPanicEvent(t, ev, rePanicked, p.value, nil)
		})
		t.Run("deferred-func-"+p.name, func(t *testing.T) {
			ev, rePanicked := capturePanicEvent(t, func(op *Operation) {
				defer op.End(nil) // registered first → runs last, sees the panic
				defer func() { panic(p.value) }()
				Add(op.Context(), "before", "work")
			})
			verifyPanicEvent(t, ev, rePanicked, p.value, nil)
		})
		t.Run("co-delivered-"+p.name, func(t *testing.T) {
			err := errors.New("co-err")
			ev, rePanicked := capturePanicEvent(t, func(op *Operation) {
				defer op.End(&err) // error pointer set when the panic hits
				panic(p.value)
			})
			verifyPanicEvent(t, ev, rePanicked, p.value, err)
		})
	}
}

// verifyPanicEvent checks the captured event and the re-panicked value
// against the payload. coErr nil means the error field must carry the
// synthetic "panic: <value>" error; non-nil means the real error.
func verifyPanicEvent(t *testing.T, ev lifeCapture, rePanicked, payload any, coErr error) {
	t.Helper()

	// The re-panic propagates the original value — except panic(nil),
	// which Go ≥1.21 surfaces as *runtime.PanicNilError.
	if payload == nil {
		if _, ok := rePanicked.(error); !ok {
			t.Fatalf("panic(nil) re-panicked %T (%v), want an error", rePanicked, rePanicked)
		}
	} else if rePanicked != payload {
		t.Fatalf("re-panicked %#v, want the original %#v", rePanicked, payload)
	}

	if ev.level != LevelError {
		t.Fatalf("level = %v, want ERROR (panic policy)", ev.level)
	}

	// Panic field shape: {type, value} derived from the payload. For
	// panic(nil) the observed value is the *runtime.PanicNilError
	// wrapper Go ≥1.21 recovers, so expectations derive from it.
	observed := payload
	if payload == nil {
		observed = rePanicked
	}
	line := string(ev.line)
	panicField, ok := eventMember(t, ev, "panic")
	if !ok {
		t.Fatalf("missing panic field in %s", line)
	}
	wantType := fmt.Sprintf("%T", observed)
	if got := panicField["type"]; got != wantType {
		t.Fatalf("panic.type = %v, want %s", got, wantType)
	}
	wantValue := fmt.Sprint(observed)
	if got := panicField["value"]; got != wantValue {
		t.Fatalf("panic.value = %q, want %q", got, wantValue)
	}

	// Error field: the real co-delivered error or the synthetic panic
	// fallback.
	ef, ok := eventMember(t, ev, "error")
	if !ok {
		t.Fatalf("missing error field in %s", line)
	}
	if coErr != nil {
		if ef["message"] != coErr.Error() {
			t.Fatalf("error.message = %v, want %q", ef["message"], coErr.Error())
		}
	} else {
		want := "panic: " + fmt.Sprint(observed)
		if ef["message"] != want {
			t.Fatalf("error.message = %v, want %q", ef["message"], want)
		}
	}
}

// eventMember parses the captured line and returns one member's value
// as a decoded map.
func eventMember(t *testing.T, ev lifeCapture, key string) (map[string]any, bool) {
	t.Helper()
	got, _ := decodeLineStrict(t, ev.line)
	raw, ok := got[key]
	if !ok {
		return nil, false
	}
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("member %s not an object: %v", key, err)
	}
	return m, true
}

// TestSinkPanicPermutations: a panicking sink (payload from the table)
// with and without an in-flight handler panic. The sink panic replaces
// the in-flight panic (documented) and must not corrupt the pool.
func TestSinkPanicPermutations(t *testing.T) {
	for _, p := range panicPayloads() {
		run := func(t *testing.T, handlerPanic any) {
			t.Helper()
			sink := &captureThenPanicSink{payload: p.value}
			rt := MustCompile(Config{Sink: sink, SamplingRate: 1})
			op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "sink-panic"})
			Add(op.Context(), "k", "v")

			var escaped any
			func() {
				defer func() { escaped = recover() }()
				if handlerPanic != nil {
					// An in-flight handler panic: the sink panic replaces
					// it (documented in End's comment) — the sink payload
					// escapes, not the handler's.
					defer op.End(nil)
					panic(handlerPanic)
				}
				op.End(nil)
			}()
			if escaped == nil {
				t.Fatal("sink panic did not escape End")
			}
			if p.value == nil {
				// sink panics with panic(nil) → PanicNilError wrapper
				if _, ok := escaped.(error); !ok {
					t.Fatalf("escaped %T", escaped)
				}
			} else if escaped != p.value {
				t.Fatalf("escaped %#v, want sink payload %#v (handler panic %#v replaced)", escaped, p.value, handlerPanic)
			}
			if len(sink.captured) != 1 {
				t.Fatalf("sink captured %d records", len(sink.captured))
			}
			if handlerPanic != nil {
				// The captured record reflects the HANDLER panic (the
				// event was built before the sink panicked).
				if !bytes.Contains(sink.captured[0], []byte(fmt.Sprintf(`"value":%q`, fmt.Sprint(handlerPanic)))) {
					t.Fatalf("record does not reflect the handler panic: %s", sink.captured[0])
				}
			}
			// pool integrity after the sink panic
			ok := &matrixSink{}
			rt2 := MustCompile(Config{Sink: ok, SamplingRate: 1})
			op2 := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "after"})
			Add(op2.Context(), "clean", true)
			op2.End(nil)
			if len(ok.capture) != 1 || !bytes.Contains(ok.capture[0], []byte(`"clean":true`)) {
				t.Fatalf("pool corrupted after sink panic: %v", ok.capture)
			}
		}
		t.Run(p.name, func(t *testing.T) { run(t, nil) })
		t.Run(p.name+"-replaces-handler-panic", func(t *testing.T) { run(t, "handler-panic") })
	}
}

// captureThenPanicSink records the record, then panics with the
// configured payload.
type captureThenPanicSink struct {
	payload  any
	captured [][]byte
}

func (s *captureThenPanicSink) Write(_ context.Context, rec *Record) {
	s.captured = append(s.captured, bytes.Clone(rec.Encoded()))
	panic(s.payload)
}

// TestPanicAfterEndRan documents the defer-order requirement: when End
// is deferred LAST it runs first — a panic in a later deferred
// function is NOT captured (the event commits as a success) and the
// panic propagates.
func TestPanicAfterEndRan(t *testing.T) {
	sink := &lifeSink{}
	op := Start(context.Background(), MustCompile(rtConfigFor(sink)), OperationStart{Domain: DomainJob, Name: "late"})
	var escaped any
	func() {
		defer func() { escaped = recover() }()
		defer func() { panic("late-panic") }() // runs first
		defer op.End(nil)                      // runs last: no panic in flight
	}()
	if escaped != "late-panic" {
		t.Fatalf("escaped %v", escaped)
	}
	if len(sink.events) != 1 {
		t.Fatalf("events = %d", len(sink.events))
	}
	if v := sink.events[0].level; v != LevelInfo {
		t.Fatalf("level = %v, want INFO (End ran before the panic)", v)
	}
	if got := sink.events[0].line; bytes.Contains(got, []byte(`"panic"`)) {
		t.Fatalf("late panic leaked into the event: %s", got)
	}
}

// TestNonHTTPFiveHundredOpCodeDrivesFailure pins the coherent canonical
// code rule: a job surfacing op.code >= 500 resolves failure (and
// bypasses sampling) like its HTTP twin, not a self-contradictory
// op.code=503 + op.outcome=success line that healthy-rate sampling can
// drop. Sub-500 codes stay success.
func TestNonHTTPFiveHundredOpCodeDrivesFailure(t *testing.T) {
	samplerCalls := 0
	rt, ts := testRT(t, func(c *Config) {
		c.SamplingRate = 0
		c.Sampler = func(in SampleInput) bool {
			samplerCalls++
			return true
		}
	})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "job"})
	Add(op.Context(), "op.code", 503)
	if !op.End(nil) {
		t.Fatal("5xx op.code dropped at rate 0 (must bypass sampling as a failure)")
	}
	ev := ts.Events()[0]
	if o, _ := ev.Lookup("op.outcome"); o != string(OutcomeFailure) {
		t.Fatalf("outcome = %v, want failure", o)
	}
	if samplerCalls != 0 {
		t.Fatalf("sampler consulted %d times for a 5xx op.code event", samplerCalls)
	}
	if ev.Level() != LevelError {
		t.Fatalf("level = %v, want error", ev.Level())
	}

	op2 := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "job"})
	Add(op2.Context(), "op.code", 404)
	_ = op2.End(nil)
	if o, _ := ts.Events()[1].Lookup("op.outcome"); o != string(OutcomeSuccess) {
		t.Fatalf("4xx op.code outcome = %v, want success (only 5xx imply failure)", o)
	}
}

// TestTypedNilErrorsContained pins the containment rule: a typed-nil
// error (non-nil interface, nil pointer) reaches finalization and
// encode through every path without its nil-dereference panic.
func TestTypedNilErrorsContained(t *testing.T) {
	var pe *os.PathError

	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Error(op.Context(), pe) // unolog.Error path
	_ = op.End(nil)
	ev := ts.Events()[0]
	errField, ok := ev.Lookup("error")
	if !ok {
		t.Fatal("typed-nil error dropped the field")
	}
	m, _ := errField.(map[string]any)
	if msg, _ := m["message"].(string); msg != "<nil>" {
		t.Fatalf("error.message = %q, want \"<nil>\"", msg)
	}

	// KindErr encode path: Add with a typed-nil error value.
	rec := recOf(LevelInfo, "m", fieldOf("e", pe))
	line := rec.Encoded()
	var parsed map[string]any
	if err := json.Unmarshal(line, &parsed); err != nil {
		t.Fatalf("line unparseable: %v: %s", err, line)
	}
	if parsed["e"] != "<nil>" {
		t.Fatalf("typed-nil field = %#v, want \"<nil>\"", parsed["e"])
	}
}

// TestPanicWireShapeParity pins the two hand-maintained copies of the
// canonical panic shape against each other: integration/flow's
// public PanicField/FinalizeRequest path (middleware-recovered
// panics) and the core's own End path. They live in different
// packages by design; this test fails if either drifts.
func TestPanicWireShapeParity(t *testing.T) {
	rt, ts := testRT(t, nil)
	recovered := any("wire-parity boom")

	// Core path: End's own panic handling.
	func() {
		defer func() { _ = recover() }()
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "core"})
		defer op.End(nil)
		panic(recovered)
	}()

	// common path: middleware-style recover + explicit writes + End(nil).
	op2 := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "common"})
	Add(op2.Context(), "panic", structuredPanicField(recovered))
	Error(op2.Context(), fmt.Errorf("panic: %v", recovered))
	Add(op2.Context(), "op.outcome", string(OutcomePanic))
	_ = op2.End(nil)

	evs := ts.Events()
	if len(evs) != 2 {
		t.Fatalf("events = %d, want 2", len(evs))
	}
	for _, key := range []string{"panic", "error", "op.outcome"} {
		a, okA := evs[0].Lookup(key)
		b, okB := evs[1].Lookup(key)
		if okA != okB {
			t.Fatalf("key %q: core=%v common=%v", key, okA, okB)
		}
		if !okA {
			t.Fatalf("key %q missing", key)
		}
		if fmt.Sprint(a) != fmt.Sprint(b) {
			t.Fatalf("key %q diverges: core=%v common=%v", key, a, b)
		}
	}
	if evs[0].Level() != evs[1].Level() || evs[0].Level() != LevelError {
		t.Fatalf("levels = %v/%v, want error/error", evs[0].Level(), evs[1].Level())
	}
}

// TestAddJSONRawMessage pins the supported raw-JSON path: json.RawMessage
// through regular Add embeds verbatim on the canonical line (its
// MarshalJSON contract), no re-marshaling of the decoded value.
func TestAddJSONRawMessage(t *testing.T) {
	rt, ts := testRT(t, nil)
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
	Add(op.Context(), "meta", json.RawMessage(`{"batch":true}`), "sib", 1)
	if !op.End(nil) {
		t.Fatal("event dropped")
	}
	ev := ts.Events()[0]
	v, ok := ev.Lookup("meta")
	b, isBytes := v.(json.RawMessage)
	if !ok || !isBytes || string(b) != `{"batch":true}` {
		t.Fatalf("meta = %#v, want the verbatim RawMessage", v)
	}
	rec := recOf(LevelInfo, "m", ev.Fields()...)
	var m map[string]any
	if err := json.Unmarshal(rec.Encoded(), &m); err != nil {
		t.Fatalf("line unparseable: %v: %s", err, rec.Encoded())
	}
	nested, _ := m["meta"].(map[string]any)
	if nested["batch"] != true || m["sib"] != float64(1) {
		t.Fatalf("line = %s", rec.Encoded())
	}
}

// TestZeroOperationEndIsNoop pins the zero-value contract: End on a
// zero Operation (never Start-ed) is a no-op returning false, matching
// the nil-*Operation guard instead of dereferencing the nil event.
func TestZeroOperationEndIsNoop(t *testing.T) {
	var op Operation
	if op.End(nil) {
		t.Fatal("zero Operation End reported an emission")
	}
	if op.Context() != nil {
		t.Fatalf("zero Operation context = %v, want nil", op.Context())
	}
}

// TestAddSkipsEmptyKeysUniformly pins the key-skipping contract: the
// empty key is skipped in the leading pair exactly as in the variadic
// tail (the doc's "non-empty string" rule).
func TestAddSkipsEmptyKeysUniformly(t *testing.T) {
	ts := NewTestSink()
	rt := MustCompile(Config{Sink: ts, SamplingRate: 1})
	op := Start(context.Background(), rt, OperationStart{Name: "skip"})
	Add(op.Context(), "", 1)
	Add(op.Context(), "", 2, "kept", 3)
	Add(op.Context(), "also", 4)
	op.End(nil)

	events := ts.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	for _, f := range events[0].Fields() {
		if f.Key() == "" {
			t.Fatal("an empty-key field reached the record")
		}
	}
	if v, _ := events[0].Lookup("kept"); v != int64(3) {
		t.Fatalf("kept = %v, want 3", v)
	}
	if v, _ := events[0].Lookup("also"); v != int64(4) {
		t.Fatalf("also = %v, want 4", v)
	}
}
