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
// (bytes.Buffer) stay race-clean. It is a FallibleSink: as Config.Sink,
// a failed or short write goes to Config.ErrorHandler and End returns
// false. Use AsyncJSONSink when the writer can be slow.
type JSONSink struct {
	mu sync.Mutex
	w  io.Writer
}

// NewJSONSink returns a JSON sink writing newline-delimited canonical
// events to w. A nil writer yields a no-op sink.
func NewJSONSink(w io.Writer) *JSONSink {
	return &JSONSink{w: w}
}

// Write implements Sink. It ignores write errors; WriteErr returns them.
func (s *JSONSink) Write(ctx context.Context, rec *Record) {
	_ = s.WriteErr(ctx, rec)
}

// WriteErr implements FallibleSink: it writes the canonical line and
// returns the writer's error, or io.ErrShortWrite for a short write.
func (s *JSONSink) WriteErr(_ context.Context, rec *Record) error {
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
	return s.w.Write(b)
}

var _ FallibleSink = (*JSONSink)(nil)
