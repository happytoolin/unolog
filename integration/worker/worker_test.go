package worker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/happytoolin/unolog"
	"go.uber.org/goleak"
)

func TestStartAddsWorkerFields(t *testing.T) {
	scheduledAt := time.Date(2026, 2, 10, 8, 30, 0, 0, time.UTC)
	op := Start(context.Background(), nil, JobMeta{Name: "cleanup"}) // nil rt: valid no-op handle
	if op == nil || op.Context() == nil {
		t.Fatal("expected operation handle and context")
	}

	// in-flight fields are observable through the sampler view
	var fields map[string]any
	capture := unolog.MustCompile(unolog.Config{
		Sink:         unolog.NewTestSink(),
		SamplingRate: 1,
		Sampler: func(in unolog.SampleInput) bool {
			fields = map[string]any{}
			for _, f := range in.Fields() {
				if v, ok := in.Lookup(f.Key()); ok {
					fields[f.Key()] = v
				}
			}
			return true
		},
	})
	op2 := Start(context.Background(), capture, JobMeta{
		Name:        "cleanup",
		ID:          "job_1",
		Queue:       "nightly",
		Attempt:     2,
		MaxAttempts: 5,
		ScheduledAt: scheduledAt,
	})
	op2.End(nil)
	if fields == nil {
		t.Fatal("sampler never saw the fields")
	}
	if fields["op.domain"] != string(unolog.DomainJob) {
		t.Fatalf("op.domain = %v", fields["op.domain"])
	}
	if fields["op.name"] != "cleanup" {
		t.Fatalf("op.name = %v", fields["op.name"])
	}
	// job.* mirrors dropped with the canonical-field pass: op.name and
	// op.source (queue) carry them
	if fields["op.source"] != "nightly" {
		t.Fatalf("op.source = %v", fields["op.source"])
	}
	if _, hasMirror := fields["job.name"]; hasMirror {
		t.Fatal("job.name mirror still emitted")
	}
	if got, ok := fields["job.scheduled_at"].(time.Time); !ok || !got.Equal(scheduledAt) {
		t.Fatalf("job.scheduled_at = %v", fields["job.scheduled_at"])
	}
}

func TestEndSuccessDefaultMessage(t *testing.T) {
	sink := unolog.NewTestSink()
	rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
	op := Start(context.Background(), rt, JobMeta{Name: "cleanup", ID: "job_1", Queue: "nightly"})
	var err error

	if !op.End(&err) {
		t.Fatal("expected End to write")
	}

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Message() != "operation_completed" {
		t.Fatalf("message = %q", events[0].Message())
	}
	if v, _ := events[0].Lookup("op.outcome"); v != string(unolog.OutcomeSuccess) {
		t.Fatalf("op.outcome = %v", v)
	}
}

func TestEndErrorAndPanic(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		sink := unolog.NewTestSink()
		rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 0})
		op := Start(context.Background(), rt, JobMeta{Name: "cleanup"})
		err := errors.New("boom")
		if !op.End(&err) {
			t.Fatal("expected error to bypass sampling")
		}
		ev := sink.Events()[0]
		if ev.Level() != unolog.LevelError {
			t.Fatalf("level = %v, want ERROR", ev.Level())
		}
		if v, _ := ev.Lookup("op.outcome"); v != string(unolog.OutcomeFailure) {
			t.Fatalf("outcome = %v", v)
		}
	})

	t.Run("panic", func(t *testing.T) {
		sink := unolog.NewTestSink()
		rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 0})
		op := Start(context.Background(), rt, JobMeta{Name: "cleanup"})
		func() {
			var err error
			defer func() {
				recovered := recover()
				if recovered != "panic-value" {
					t.Fatalf("recovered = %v, want panic-value", recovered)
				}
			}()
			defer op.End(&err) // direct defer: observes the panic
			panic("panic-value")
		}()
		ev := sink.Events()[0]
		if v, _ := ev.Lookup("op.outcome"); v != string(unolog.OutcomePanic) {
			t.Fatalf("outcome = %v", v)
		}
		if p, ok := ev.Lookup("panic"); !ok {
			t.Fatal("expected panic metadata")
		} else if _, isMap := p.(map[string]any); !isMap {
			t.Fatalf("panic metadata = %T", p)
		}
	})
}

func TestEndGuards(t *testing.T) {
	rt := unolog.MustCompile(unolog.Config{})
	op := Start(context.Background(), rt, JobMeta{Name: "cleanup"})
	var err error
	if op.End(&err) {
		t.Fatal("expected false without sink")
	}
	var nilOp *unolog.Operation
	if nilOp.End(&err) {
		t.Fatal("expected false with nil operation")
	}
}

// P8.2 worker lifecycle scenario battery (dst-research §8.2): the
// job-shaped scenarios assembled as end-to-end flows with real
// contexts — retries with attempt metadata, cancellation, deadlines,
// retryable errors, panics, nil runtimes, scheduled_at preservation,
// and consecutive jobs over the same pool. Each scenario drives the
// real worker Start + a deferred End(&err) closure (the documented
// usage shape) and asserts the captured event.

// TestWorkerRetryMetadata: attempt/max_attempts survive to the wire
// through op.*, and a retryable error produces a failure outcome with
// the error level.
func TestWorkerRetryMetadata(t *testing.T) {
	ts := unolog.NewTestSink()
	rt := unolog.MustCompile(unolog.Config{Sink: ts, SamplingRate: 1})
	ctx := t.Context()

	err := errors.New("retryable")
	op := Start(ctx, rt, JobMeta{Name: "sync", Attempt: 3, MaxAttempts: 5})
	op.End(&err)

	ev := ts.Events()[0]
	for _, tc := range []struct {
		key  string
		want any
	}{
		{"op.attempt", int64(3)},
		{"op.max_attempts", int64(5)},
	} {
		if v, ok := ev.Lookup(tc.key); !ok || v != tc.want {
			t.Fatalf("%s = %v (%v), want %v", tc.key, v, ok, tc.want)
		}
	}
	if v, _ := ev.Lookup("op.outcome"); v != string(unolog.OutcomeFailure) {
		t.Fatalf("outcome = %v, want failure", v)
	}
	if ev.Level() != unolog.LevelError {
		t.Fatalf("level = %v, want error", ev.Level())
	}
	if e, ok := ev.Lookup("error"); !ok {
		t.Fatal("missing error field")
	} else if em, ok := e.(map[string]any); !ok || em["message"] != "retryable" {
		t.Fatalf("error.message = %v", e)
	}
}

// TestWorkerCancellation: a canceled context yields OutcomeCanceled
// with the error bypass (never sampled away, even at rate 0).
func TestWorkerCancellation(t *testing.T) {
	for _, rate := range []float64{1, 0} { // error bypass at any rate
		ts := unolog.NewTestSink()
		rt := unolog.MustCompile(unolog.Config{Sink: ts, SamplingRate: rate})
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // canceled before the job starts
		err := context.Canceled
		op := Start(ctx, rt, JobMeta{Name: "cancel"})
		op.End(&err)
		if len(ts.Events()) != 1 {
			t.Fatalf("rate %v: canceled event dropped", rate)
		}
		ev := ts.Events()[0]
		if v, _ := ev.Lookup("op.outcome"); v != string(unolog.OutcomeCanceled) {
			t.Fatalf("rate %v: outcome = %v, want canceled", rate, v)
		}
		if ev.Level() != unolog.LevelError {
			t.Fatalf("rate %v: level = %v", rate, ev.Level())
		}
	}
}

// TestWorkerDeadline: an expired context deadline yields
// OutcomeTimeout with the error bypass.
func TestWorkerDeadline(t *testing.T) {
	ts := unolog.NewTestSink()
	rt := unolog.MustCompile(unolog.Config{Sink: ts, SamplingRate: 1})
	ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond) // let the deadline expire
	err := context.DeadlineExceeded
	op := Start(ctx, rt, JobMeta{Name: "slow"})
	op.End(&err)
	ev := ts.Events()[0]
	if v, _ := ev.Lookup("op.outcome"); v != string(unolog.OutcomeTimeout) {
		t.Fatalf("outcome = %v, want timeout", v)
	}
}

// TestWorkerJobPanic: a panic in the job function yields
// OutcomePanic + the panic field, re-panics the original value, and
// records error level.
func TestWorkerJobPanic(t *testing.T) {
	ts := unolog.NewTestSink()
	rt := unolog.MustCompile(unolog.Config{Sink: ts, SamplingRate: 1})

	repanicked := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				repanicked = r == "worker-boom"
			}
		}()
		op := Start(context.Background(), rt, JobMeta{Name: "boom-job"})
		defer op.End(nil) // direct defer observes the panic
		panic("worker-boom")
	}()
	if !repanicked {
		t.Fatal("the panic did not propagate")
	}
	ev := ts.Events()[0]
	if v, _ := ev.Lookup("op.outcome"); v != string(unolog.OutcomePanic) {
		t.Fatalf("outcome = %v", v)
	}
	if ev.Level() != unolog.LevelError {
		t.Fatalf("level = %v", ev.Level())
	}
	if p, ok := ev.Lookup("panic"); !ok {
		t.Fatal("missing panic field")
	} else if pm, ok := p.(map[string]any); !ok || pm["value"] != "worker-boom" {
		t.Fatalf("panic.value = %v", p)
	}
}

// TestWorkerNilRuntime: with a nil runtime the job still runs; nothing
// emits.
func TestWorkerNilRuntime(t *testing.T) {
	ran := false
	op := Start(context.Background(), nil, JobMeta{Name: "noop"})
	var err error
	func() {
		defer op.End(&err)
		ran = true
	}()
	if !ran {
		t.Fatal("job did not run")
	}
	if op.End(&err) {
		t.Fatal("nil runtime emitted")
	}
}

// TestWorkerScheduledAtPreserved: job.scheduled_at survives to the
// wire as the RFC3339 string of the UTC instant.
func TestWorkerScheduledAtPreserved(t *testing.T) {
	ts := unolog.NewTestSink()
	rt := unolog.MustCompile(unolog.Config{Sink: ts, SamplingRate: 1})
	scheduled := time.Date(2026, 2, 10, 8, 30, 0, 0, time.FixedZone("+05:30", 5*3600+1800))
	op := Start(context.Background(), rt, JobMeta{Name: "sched", ScheduledAt: scheduled})
	op.End(nil)
	ev := ts.Events()[0]
	// Lookup returns the typed value (KindTime); the worker stores the
	// UTC instant.
	if v, ok := ev.Lookup("job.scheduled_at"); !ok {
		t.Fatal("missing job.scheduled_at")
	} else if tm, isTime := v.(time.Time); !isTime || !tm.Equal(scheduled.UTC()) {
		t.Fatalf("job.scheduled_at = %v (%T), want the UTC instant %v", v, v, scheduled.UTC())
	}
	// the wire rendering of KindTime fields is RFC3339 (pinned by the
	// core round-trip suites); the typed value is what survives here
	_ = ev
}

// TestWorkerConsecutiveJobs: consecutive jobs on one runtime recycle
// the pooled event between them; each event carries only its own
// fields.
func TestWorkerConsecutiveJobs(t *testing.T) {
	ts := unolog.NewTestSink()
	rt := unolog.MustCompile(unolog.Config{Sink: ts, SamplingRate: 1})
	const jobs = 32
	for i := range jobs {
		op := Start(context.Background(), rt, JobMeta{Name: "job", ID: string(rune('a' + i))})
		unolog.Add(op.Context(), "index", i)
		var err error
		op.End(&err)
	}
	events := ts.Events()
	if len(events) != jobs {
		t.Fatalf("captured %d events, want %d", len(events), jobs)
	}
	for i, ev := range events {
		if v, _ := ev.Lookup("index"); v != int64(i) {
			t.Fatalf("event %d index = %v — pool cross-contamination", i, v)
		}
		if v, _ := ev.Lookup("op.name"); v != "job" {
			t.Fatalf("event %d name = %v", i, v)
		}
	}
}

// TestWorkerConcurrentJobs: many workers over one runtime, each with
// its own request-confined event (the -race pin for the pooled WAL).
func TestWorkerConcurrentJobs(t *testing.T) {
	ts := unolog.NewTestSink()
	rt := unolog.MustCompile(unolog.Config{Sink: ts, SamplingRate: 1})
	var wg sync.WaitGroup
	for w := range 12 {
		wg.Go(func() {
			for i := range 100 {
				op := Start(context.Background(), rt, JobMeta{Name: "worker", ID: "w"})
				unolog.Add(op.Context(), "worker", w, "seq", i)
				var err error
				op.End(&err)
			}
		})
	}
	wg.Wait()
	if got := len(ts.Events()); got != 1200 {
		t.Fatalf("captured %d events, want 1200", got)
	}
	for _, ev := range ts.Events() {
		if _, ok := ev.Lookup("worker"); !ok {
			t.Fatal("event lost its worker field")
		}
		if _, ok := ev.Lookup("seq"); !ok {
			t.Fatal("event lost its seq field")
		}
	}
}

// goleak integration: every test in this module runs under
// goleak.VerifyTestMain, failing the suite on any leaked goroutine.
// goleak is test-only: nothing outside _test.go imports it.

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
