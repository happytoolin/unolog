package unolog

import (
	"errors"
	"fmt"
	"io"
	"os"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrSinkPanic matches a *PanicError from a panicking Sink.Write.
	ErrSinkPanic = errors.New("sink panic")
	// ErrSamplerPanic matches a *PanicError from a panicking Sampler.
	ErrSamplerPanic = errors.New("sampler panic")
)

// PanicError reports a panic that unolog contained in a Sink or a
// Sampler. errors.Is matches ErrSinkPanic or ErrSamplerPanic.
type PanicError struct {
	// Value is the recovered panic value.
	Value any
	// Stack is the panicking goroutine's stack at recovery.
	Stack []byte

	kind error
}

// Error renders the source and the panic value.
func (e *PanicError) Error() string {
	// fmt contains a panicking String or Error method on Value.
	return "unolog: " + e.kind.Error() + ": " + fmt.Sprint(e.Value)
}

// Unwrap returns ErrSinkPanic or ErrSamplerPanic.
func (e *PanicError) Unwrap() error { return e.kind }

// reportPanic hands a contained panic to the configured ErrorHandler,
// or to the default stderr reporter. It runs inside the deferred
// recover, so debug.Stack still shows the panicking frames.
func (rt *Runtime) reportPanic(kind error, recovered any) {
	if rt.onError != nil {
		rt.onError(&PanicError{Value: recovered, Stack: debug.Stack(), kind: kind})
		return
	}
	if rt.reporter != nil {
		rt.reporter.report(kind, recovered)
	}
}

// reportInterval is the minimum gap between two default reports, so a
// sink that panics on every request cannot flood stderr.
const reportInterval = time.Second

// rateLimitedReporter is the default ErrorHandler: it writes at most
// one report per reportInterval and counts the reports it suppressed.
// It never blocks a request that loses the race to report: losers only
// increment the counter, so the count is exact in total but a report
// can include suppressions that happened while it was being written.
type rateLimitedReporter struct {
	mu         sync.Mutex
	w          io.Writer
	next       atomic.Int64 // UnixNano before which reports are suppressed
	suppressed atomic.Int64
}

func newStderrReporter() *rateLimitedReporter {
	return &rateLimitedReporter{w: os.Stderr}
}

func (r *rateLimitedReporter) report(kind error, recovered any) {
	now := time.Now().UnixNano()
	next := r.next.Load()
	if now < next || !r.next.CompareAndSwap(next, now+int64(reportInterval)) {
		r.suppressed.Add(1)
		return
	}
	err := &PanicError{Value: recovered, Stack: debug.Stack(), kind: kind}
	suppressed := r.suppressed.Swap(0)
	r.mu.Lock()
	defer r.mu.Unlock()
	if suppressed > 0 {
		_, _ = fmt.Fprintf(r.w, "%v (%d reports suppressed since the last report)\n%s", err, suppressed, err.Stack)
		return
	}
	_, _ = fmt.Fprintf(r.w, "%v\n%s", err, err.Stack)
}
