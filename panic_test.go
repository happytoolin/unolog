package unolog

import (
	"bytes"
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// The default reporter writes the first report with its stack, then
// suppresses reports for reportInterval and counts them.
func TestDefaultReporterRateLimits(t *testing.T) {
	var buf bytes.Buffer
	r := &rateLimitedReporter{w: &buf}
	r.reportPanic(ErrSinkPanic, "first")
	for range 5 {
		r.reportPanic(ErrSinkPanic, "suppressed")
	}
	out := buf.String()
	if !strings.HasPrefix(out, "unolog: sink panic: first\n") {
		t.Fatalf("first report = %q", out)
	}
	if !strings.Contains(out, "goroutine ") {
		t.Fatal("first report has no stack")
	}
	if strings.Contains(out, "suppressed") {
		t.Fatal("reports inside the interval were written")
	}
	if got := r.suppressed.Load(); got != 5 {
		t.Fatalf("suppressed = %d, want 5", got)
	}

	// Open the window: the next report carries the suppressed count.
	r.next.Store(0)
	buf.Reset()
	r.reportPanic(ErrSamplerPanic, "later")
	if !strings.HasPrefix(buf.String(), "unolog: sampler panic: later (5 reports suppressed since the last report)\n") {
		t.Fatalf("report after the interval = %q", buf.String())
	}
	if r.suppressed.Load() != 0 {
		t.Fatal("suppressed count not reset")
	}

	// One suppressed report is also counted.
	r.reportError(errors.New("in window"))
	r.next.Store(0)
	buf.Reset()
	r.reportError(errors.New("next"))
	if got := buf.String(); got != "next (1 reports suppressed since the last report)\n" {
		t.Fatalf("report after one suppressed = %q", got)
	}
}

func TestDefaultReporterConcurrent(t *testing.T) {
	var buf bytes.Buffer
	r := &rateLimitedReporter{w: &buf}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				r.reportPanic(ErrSinkPanic, "boom")
			}
		})
	}
	wg.Wait()
	if n := strings.Count(buf.String(), "unolog: sink panic: boom"); n != 1 {
		t.Fatalf("wrote %d reports inside one interval, want 1", n)
	}
	// Suppressions racing the winner may land in its own count; the
	// total is exact.
	var inReport int64
	if _, after, ok := strings.Cut(buf.String(), "boom ("); ok {
		inReport, _ = strconv.ParseInt(strings.Fields(after)[0], 10, 64)
	}
	if total := inReport + r.suppressed.Load(); total != 799 {
		t.Fatalf("suppressed total = %d, want 799", total)
	}
}

// Compile always installs the default reporter: it is the default and
// the fallback for a panicking ErrorHandler. End routes a sink panic to
// it without escaping.
func TestCompileSelectsReporter(t *testing.T) {
	rt := MustCompile(Config{Sink: sinkFunc(func(context.Context, *Record) { panic("x") }), SamplingRate: 1})
	if rt.reporter == nil || rt.onError != nil {
		t.Fatal("default reporter not installed")
	}
	var buf bytes.Buffer
	rt.reporter.w = &buf
	op := Start(context.Background(), rt, OperationStart{})
	if op.End(nil) {
		t.Fatal("End reported an emission through a panicking sink")
	}
	if !strings.HasPrefix(buf.String(), "unolog: sink panic: x\n") {
		t.Fatalf("stderr report = %q", buf.String())
	}

	rt = MustCompile(Config{ErrorHandler: func(error) {}})
	if rt.reporter == nil {
		t.Fatal("no fallback reporter next to a custom ErrorHandler")
	}
}

// A working ErrorHandler takes every report: stderr stays empty.
func TestErrorHandlerReplacesStderr(t *testing.T) {
	var reports []error
	rt := MustCompile(Config{
		Sink:         NewJSONSink(jsonWriterFunc(func([]byte) (int, error) { return 0, errors.New("disk full") })),
		SamplingRate: 1,
		Sampler:      func(SampleInput) bool { panic("sampler bug") },
		ErrorHandler: func(err error) { reports = append(reports, err) },
	})
	var buf bytes.Buffer
	rt.reporter.w = &buf
	// The sampler panics, the event is kept, and the sink write fails.
	Start(context.Background(), rt, OperationStart{}).End(nil)
	if len(reports) != 2 || !errors.Is(reports[0], ErrSamplerPanic) || !errors.Is(reports[1], ErrSinkWrite) {
		t.Fatalf("reports = %v", reports)
	}
	if buf.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", buf.String())
	}
}

func TestPanicErrorMatchesSentinels(t *testing.T) {
	err := error(&PanicError{Value: 1, kind: ErrSinkPanic})
	if !errors.Is(err, ErrSinkPanic) || errors.Is(err, ErrSamplerPanic) {
		t.Fatal("PanicError does not match its sentinel")
	}
	if err.Error() != "unolog: sink panic: 1" {
		t.Fatalf("Error() = %q", err.Error())
	}
	// A value whose String method panics must still render.
	err = &PanicError{Value: panickyStringer{}, kind: ErrSamplerPanic}
	if !strings.HasPrefix(err.Error(), "unolog: sampler panic: ") {
		t.Fatalf("Error() = %q", err.Error())
	}
}

type panickyStringer struct{}

func (panickyStringer) String() string { panic("stringer boom") }

// A panicking ErrorHandler must not escape End: the report falls back
// to stderr, and an application panic keeps its own value.
func TestPanickingErrorHandlerFallsBackToStderr(t *testing.T) {
	rt := MustCompile(Config{
		Sink:         sinkFunc(func(context.Context, *Record) { panic("sink") }),
		SamplingRate: 1,
		ErrorHandler: func(err error) { panic(err) },
	})
	var buf bytes.Buffer
	rt.reporter.w = &buf
	op := Start(context.Background(), rt, OperationStart{})
	if op.End(nil) {
		t.Fatal("End reported an emission through a panicking sink")
	}
	if !strings.HasPrefix(buf.String(), "unolog: sink panic: sink\n") {
		t.Fatalf("fallback report = %q", buf.String())
	}

	var escaped any
	func() {
		defer func() { escaped = recover() }()
		func() {
			op := Start(context.Background(), rt, OperationStart{})
			defer op.End(nil)
			panic("app")
		}()
	}()
	if escaped != "app" {
		t.Fatalf("End re-panicked with %#v, want the application's own value", escaped)
	}
}
