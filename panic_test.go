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
	r.report(ErrSinkPanic, "first")
	for range 5 {
		r.report(ErrSinkPanic, "suppressed")
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
	r.report(ErrSamplerPanic, "later")
	if !strings.HasPrefix(buf.String(), "unolog: sampler panic: later (5 reports suppressed since the last report)\n") {
		t.Fatalf("report after the interval = %q", buf.String())
	}
	if r.suppressed.Load() != 0 {
		t.Fatal("suppressed count not reset")
	}
}

func TestDefaultReporterConcurrent(t *testing.T) {
	var buf bytes.Buffer
	r := &rateLimitedReporter{w: &buf}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 100 {
				r.report(ErrSinkPanic, "boom")
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

// Compile installs the default reporter only when no handler is set,
// and End routes a sink panic to it without escaping.
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
	if rt.reporter != nil {
		t.Fatal("reporter installed next to a custom ErrorHandler")
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

// A panicking ErrorHandler is the caller's own code: it propagates, and
// the operation still publishes so a concurrent End cannot hang.
func TestPanickingErrorHandlerPropagates(t *testing.T) {
	rt := MustCompile(Config{
		Sink:         sinkFunc(func(context.Context, *Record) { panic("sink") }),
		SamplingRate: 1,
		ErrorHandler: func(err error) { panic(err) },
	})
	op := Start(context.Background(), rt, OperationStart{})
	var escaped any
	func() {
		defer func() { escaped = recover() }()
		op.End(nil)
	}()
	if err, ok := escaped.(error); !ok || !errors.Is(err, ErrSinkPanic) {
		t.Fatalf("escaped %#v", escaped)
	}
	if op.End(nil) {
		t.Fatal("second End must report the unpublished emission as false")
	}
}
