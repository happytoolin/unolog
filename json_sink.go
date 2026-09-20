package unolog

import (
	"bufio"
	"context"
	"io"
	"sync"
)

// JSONSink is a first-party sink that writes one canonical JSON line per
// event with a single Write call to the underlying writer — no logger
// dependency required. The line is the record's Encoded() form: the
// canonical unolog line (level, deduped fields, RFC3339 completion time,
// message). A JSONSink is safe for concurrent use: encoding happens
// without a lock, then Write is serialized so unsynchronized writers
// (bytes.Buffer) stay race-clean. Write errors are ignored (events are
// best-effort, like the bridges).
type JSONSink struct {
	mu sync.Mutex
	w  io.Writer
	// bw is the buffering layer when the sink was built buffered; w is
	// then bw itself. Flush drives it; nil means unbuffered.
	bw *bufio.Writer
	// scratch pools the encode buffer. Encoding into a pooled buffer
	// keeps the line off the heap and never touches the record's own
	// per-record Encoded() cache — so a sink that retains the record
	// still gets stable bytes from a later Encoded() call.
	scratch sync.Pool // *[]byte
}

// NewJSONSink returns a JSON sink writing newline-delimited canonical
// events to w. A nil writer yields a no-op sink.
func NewJSONSink(w io.Writer) *JSONSink {
	return &JSONSink{w: w}
}

// NewBufferedJSONSink returns a JSON sink that batches events through a
// size-byte buffer: lines accumulate in memory and reach the underlying
// writer in one syscall per full buffer instead of one syscall per
// event. That is the difference that matters in production — a syscall
// per request dominates the encode.
//
// The caller MUST call Flush before the process exits; events still in
// the buffer are lost otherwise. Use the default size for line-oriented
// log writers: NewBufferedJSONSink(w, 0) picks bufio's 4096.
func NewBufferedJSONSink(w io.Writer, size int) *JSONSink {
	if w == nil {
		return &JSONSink{} // same nil-writer no-op contract as NewJSONSink
	}
	if size <= 0 {
		size = 4096
	}
	bw := bufio.NewWriterSize(w, size)
	return &JSONSink{w: bw, bw: bw}
}

// Flush writes any buffered events to the underlying writer. It is a
// no-op on a sink that is not buffered.
func (s *JSONSink) Flush() error {
	if s == nil || s.bw == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.bw.Flush()
}

// Write implements Sink.
func (s *JSONSink) Write(_ context.Context, rec *Record) {
	if s == nil || s.w == nil || rec == nil {
		return
	}
	buf := s.acquire()
	line := rec.encodeInto((*buf)[:0])
	s.mu.Lock()
	defer s.mu.Unlock()
	_, _ = s.w.Write(line)
	*buf = line // keep any grown capacity for the next event
	s.scratch.Put(buf)
}

// acquire returns a pooled encode buffer (a fresh one on a cold pool).
func (s *JSONSink) acquire() *[]byte {
	if v := s.scratch.Get(); v != nil {
		return v.(*[]byte) //nolint:forcetypeassert // only *[]byte is stored
	}
	return new([]byte)
}

var _ Sink = (*JSONSink)(nil)
