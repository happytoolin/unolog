package unolog

import (
	"context"
	"errors"
	"fmt"
	"io"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

// DefaultAsyncBufferBytes is the AsyncJSONSink buffer size when
// AsyncJSONSinkConfig.BufferBytes is zero.
const DefaultAsyncBufferBytes = 1 << 20

// ErrSinkFull matches the error an AsyncJSONSink reports when it drops
// lines because its buffer is full.
var ErrSinkFull = errors.New("sink buffer full")

// AsyncJSONSinkConfig configures NewAsyncJSONSink.
type AsyncJSONSinkConfig struct {
	// BufferBytes bounds the lines the sink holds while its writer
	// goroutine is busy. A line that does not fit is dropped and
	// counted, so a slow writer never blocks a request; a line longer
	// than BufferBytes never fits. The sink uses at most twice this
	// memory: one buffer fills while the other is written. Zero or a
	// negative value selects DefaultAsyncBufferBytes.
	BufferBytes int

	// ErrorHandler receives write errors (errors.Is matches
	// ErrSinkWrite), writer panics (*PanicError, ErrSinkPanic), and drop
	// reports (ErrSinkFull). It runs on the writer goroutine, so it must
	// not call Close, or Flush with a context that is never done: both
	// wait for that goroutine and can block forever. Nil selects the
	// default, which writes to os.Stderr at most once per second; a
	// panicking handler also falls back to it.
	ErrorHandler func(error)
}

// AsyncJSONSink writes the canonical lines JSONSink writes, but off the
// request path: Write copies the line into a buffer, and one goroutine
// writes the buffered lines to the writer in batches. Use it when the
// writer can be slow, such as a pipe, a network connection, or a busy
// disk. Use JSONSink when every line must be written: AsyncJSONSink
// drops lines while its buffer is full.
//
// Write returns once the line is buffered, so End reports true for a
// buffered line. Lines from one goroutine keep their order. Call Close
// at shutdown to write the remaining lines and stop the goroutine.
type AsyncJSONSink struct {
	w        io.Writer
	max      int
	onError  func(error)
	reporter *rateLimitedReporter

	mu      sync.Mutex
	cond    *sync.Cond // broadcast after each batch, for Flush
	buf     []byte     // lines waiting for the writer
	spare   []byte     // the last batch, reused as the next buffer
	queued  uint64     // bytes accepted since the start
	written uint64     // bytes the writer finished since the start
	err     error      // last write error, returned by Flush and Close
	closed  bool

	// wake holds one token while lines wait, or after Close. Write sends
	// it when the buffer turns non-empty, and the writer drains the
	// buffer before it waits again, so a buffered line always has a
	// token or a running writer.
	wake chan struct{}
	done chan struct{} // closed when the writer goroutine returns

	dropped    atomic.Uint64
	unreported atomic.Uint64 // drops the writer has not reported yet
}

// NewAsyncJSONSink returns an AsyncJSONSink writing newline-delimited
// canonical events to w, and starts its writer goroutine. A nil writer
// discards the lines.
func NewAsyncJSONSink(w io.Writer, cfg AsyncJSONSinkConfig) *AsyncJSONSink {
	if w == nil {
		w = io.Discard
	}
	maxBytes := cfg.BufferBytes
	if maxBytes <= 0 {
		maxBytes = DefaultAsyncBufferBytes
	}
	s := &AsyncJSONSink{
		w:        w,
		max:      maxBytes,
		onError:  cfg.ErrorHandler,
		reporter: newStderrReporter(),
		wake:     make(chan struct{}, 1),
		done:     make(chan struct{}),
	}
	s.cond = sync.NewCond(&s.mu)
	go s.run()
	return s
}

// Write implements Sink. It encodes the line on the caller's goroutine,
// copies it into the buffer, and returns without waiting for the
// writer. A line that does not fit, and any line after Close, is
// dropped and counted.
func (s *AsyncJSONSink) Write(_ context.Context, rec *Record) {
	if s == nil || rec == nil {
		return
	}
	line := rec.Encoded()
	s.mu.Lock()
	if s.closed || len(s.buf)+len(line) > s.max {
		s.mu.Unlock()
		s.dropped.Add(1)
		s.unreported.Add(1)
		s.signal() // the writer reports drops even with nothing to write
		return
	}
	if need := len(s.buf) + len(line); need > cap(s.buf) {
		// Grow by hand: append could reserve more than max.
		grown := make([]byte, len(s.buf), min(max(2*cap(s.buf), need, 4<<10), s.max))
		copy(grown, s.buf)
		s.buf = grown
	}
	wasEmpty := len(s.buf) == 0
	s.buf = append(s.buf, line...)
	s.queued += uint64(len(line))
	s.mu.Unlock()
	if wasEmpty {
		s.signal()
	}
}

// Flush waits until the writer has written every line accepted before
// the call, or until ctx is done. It returns the context's error, or
// the last write error since the previous Flush or Close.
func (s *AsyncJSONSink) Flush(ctx context.Context) error {
	if s == nil {
		return nil
	}
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for target := s.queued; s.written < target; {
		if err := ctx.Err(); err != nil {
			return err
		}
		s.cond.Wait()
	}
	err := s.err
	s.err = nil
	return err
}

// Close writes the remaining lines, stops the writer goroutine, and
// returns the last write error since the previous Flush. It waits for
// the writer, and it does not close w. Close is idempotent.
func (s *AsyncJSONSink) Close() error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.signal()
	<-s.done
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.err
	s.err = nil
	return err
}

// Dropped returns the number of lines dropped since the start: lines
// that did not fit in the buffer, and lines written after Close.
func (s *AsyncJSONSink) Dropped() uint64 {
	if s == nil {
		return 0
	}
	return s.dropped.Load()
}

func (s *AsyncJSONSink) signal() {
	select {
	case s.wake <- struct{}{}:
	default: // a token is already waiting
	}
}

// run is the writer goroutine: it swaps the buffers under the lock and
// writes the full one without it, so Write waits only for a copy.
func (s *AsyncJSONSink) run() {
	defer close(s.done)
	for range s.wake {
		for {
			s.mu.Lock()
			batch, closed := s.buf, s.closed
			if len(batch) > 0 {
				s.buf = s.spare[:0]
			}
			s.mu.Unlock()
			if len(batch) == 0 {
				s.reportDrops()
				if closed {
					return
				}
				break
			}

			err := s.writeBatch(batch)

			s.mu.Lock()
			s.spare = batch
			s.written += uint64(len(batch))
			if err != nil {
				s.err = err
			}
			s.cond.Broadcast()
			s.mu.Unlock()

			if err != nil {
				s.report(err)
			}
			s.reportDrops()
		}
	}
}

// writeBatch writes one batch. A panicking writer must not end the
// goroutine, or every later line would wait forever.
func (s *AsyncJSONSink) writeBatch(batch []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = &PanicError{Value: r, Stack: debug.Stack(), kind: ErrSinkPanic}
		}
	}()
	n, err := s.w.Write(batch)
	endCutLine(s.w, batch, n)
	if err == nil && n < len(batch) {
		err = io.ErrShortWrite
	}
	if err != nil {
		err = fmt.Errorf("unolog: %w: %w", ErrSinkWrite, err)
	}
	return err
}

func (s *AsyncJSONSink) reportDrops() {
	if n := s.unreported.Swap(0); n > 0 {
		s.report(fmt.Errorf("unolog: %w: %d lines dropped", ErrSinkFull, n))
	}
}

func (s *AsyncJSONSink) report(err error) {
	if s.onError != nil && callErrorHandler(s.onError, err) {
		return
	}
	s.reporter.reportError(err)
}

var _ Sink = (*AsyncJSONSink)(nil)
