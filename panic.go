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
	// ErrSinkWrite matches the error reported when a sink fails to
	// write a line. errors.Is also matches the writer's own error.
	ErrSinkWrite = errors.New("sink write")
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
// or to the default stderr reporter when there is none or it panics. It
// runs inside the deferred recover, so debug.Stack still shows the
// panicking frames. It never panics.
func (rt *Runtime) reportPanic(kind error, recovered any) {
	if rt.onError != nil && callErrorHandler(rt.onError, &PanicError{Value: recovered, Stack: debug.Stack(), kind: kind}) {
		return
	}
	if rt.reporter != nil {
		rt.reporter.reportPanic(kind, recovered)
	}
}

// reportError hands a contained error to the configured ErrorHandler,
// or to the default stderr reporter when there is none or it panics.
func (rt *Runtime) reportError(err error) {
	if rt.onError != nil && callErrorHandler(rt.onError, err) {
		return
	}
	if rt.reporter != nil {
		rt.reporter.reportError(err)
	}
}

// callErrorHandler reports whether onError returned. It recovers a
// panic in the handler: escaping End, that panic would replace the
// application's own panic when End re-panics.
func callErrorHandler(onError func(error), err error) (returned bool) {
	defer func() { _ = recover() }()
	onError(err)
	return true
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

// reportPanic writes a contained panic with its stack. The stack is
// captured only for a report that is written.
func (r *rateLimitedReporter) reportPanic(kind error, recovered any) {
	if r.acquire() {
		r.write(&PanicError{Value: recovered, Stack: debug.Stack(), kind: kind})
	}
}

// reportError writes err, with the stack of a *PanicError.
func (r *rateLimitedReporter) reportError(err error) {
	if r.acquire() {
		r.write(err)
	}
}

// acquire reports whether a report may be written now. A caller that
// loses only counts the suppressed report.
func (r *rateLimitedReporter) acquire() bool {
	now := time.Now().UnixNano()
	next := r.next.Load()
	if now < next || !r.next.CompareAndSwap(next, now+int64(reportInterval)) {
		r.suppressed.Add(1)
		return false
	}
	return true
}

func (r *rateLimitedReporter) write(err error) {
	var stack []byte
	if pe, ok := err.(*PanicError); ok { //nolint:errorlint // only the reporter's own, unwrapped *PanicError carries a stack to print
		stack = pe.Stack
	}
	suppressed := r.suppressed.Swap(0)
	r.mu.Lock()
	defer r.mu.Unlock()
	if suppressed > 0 {
		_, _ = fmt.Fprintf(r.w, "%v (%d reports suppressed since the last report)\n%s", err, suppressed, stack)
		return
	}
	_, _ = fmt.Fprintf(r.w, "%v\n%s", err, stack)
}
