package unolog

import (
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
// (bytes.Buffer) stay race-clean. As Config.Sink, a *JSONSink reports a
// failed or short write to Config.ErrorHandler, and End returns false.
// A type that embeds *JSONSink is an ordinary Sink: End calls its Write,
// and write errors are not reported. Use AsyncJSONSink when the writer
// can be slow.
type JSONSink struct {
	mu sync.Mutex
	w  io.Writer
}

// NewJSONSink returns a JSON sink writing newline-delimited canonical
// events to w. A nil writer yields a no-op sink.
func NewJSONSink(w io.Writer) *JSONSink {
	return &JSONSink{w: w}
}

// Write implements Sink. It ignores write errors.
func (s *JSONSink) Write(ctx context.Context, rec *Record) {
	_ = s.writeErr(ctx, rec)
}

// writeErr writes the canonical line and returns the writer's error, or
// io.ErrShortWrite for a short write. It is not the exported WriteErr of
// FallibleSink: a type that embeds *JSONSink and overrides Write would
// get WriteErr by promotion, and End would call it instead of the
// override. Compile finds it by the exact type *JSONSink.
func (s *JSONSink) writeErr(_ context.Context, rec *Record) error {
	if s == nil || s.w == nil || rec == nil {
		return nil
	}
	b := rec.Encoded()
	n, err := s.write(b)
	if err == nil && n < len(b) {
		err = io.ErrShortWrite
	}
	return err
}

func (s *JSONSink) write(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock() // a panicking writer must not leave the sink locked
	n, err := s.w.Write(b)
	endCutLine(s.w, b, n)
	return n, err
}

// lineEnd is the newline endCutLine writes. A package variable, so the
// write does not allocate.
var lineEnd = []byte{'\n'}

// endCutLine ends a line that a short write cut: w took only n bytes of
// b. Without the newline, the next line would continue the cut one, and
// a log reader would reject both. A write error here is ignored: the
// short write is already reported.
func endCutLine(w io.Writer, b []byte, n int) {
	if n > 0 && n < len(b) && b[n-1] != '\n' {
		_, _ = w.Write(lineEnd)
	}
}
