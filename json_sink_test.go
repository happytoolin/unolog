package unolog

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestJSONSinkRemainsUsableAfterWriterPanic(t *testing.T) {
	var buf bytes.Buffer
	first := true
	sink := NewJSONSink(jsonWriterFunc(func(p []byte) (int, error) {
		if first {
			first = false
			panic("writer failed")
		}
		return buf.Write(p)
	}))
	rec := recOf(LevelInfo, "after panic")
	func() {
		defer func() {
			if got := recover(); got != "writer failed" {
				t.Fatalf("panic = %v, want writer failed", got)
			}
		}()
		sink.Write(t.Context(), rec)
	}()
	// Check the lock before another Write so a regression fails without hanging.
	if !sink.mu.TryLock() {
		t.Fatal("writer panic left the sink locked")
	}
	sink.mu.Unlock()
	sink.Write(t.Context(), rec)
	if !bytes.Equal(buf.Bytes(), rec.Encoded()) {
		t.Fatalf("next write = %q, want %q", buf.Bytes(), rec.Encoded())
	}
}

type jsonWriterFunc func([]byte) (int, error)

func (f jsonWriterFunc) Write(p []byte) (int, error) { return f(p) }

// A JSONSink write error reaches ErrorHandler matching both ErrSinkWrite
// and the writer's error, and End reports that nothing was emitted.
func TestJSONSinkReportsWriteErrors(t *testing.T) {
	errDisk := errors.New("disk full")
	for name, tc := range map[string]struct {
		write func([]byte) (int, error)
		want  error
	}{
		"error":       {func([]byte) (int, error) { return 0, errDisk }, errDisk},
		"short write": {func(p []byte) (int, error) { return len(p) - 1, nil }, io.ErrShortWrite},
	} {
		t.Run(name, func(t *testing.T) {
			var reported []error
			rt := MustCompile(Config{
				Sink:         NewJSONSink(jsonWriterFunc(tc.write)),
				SamplingRate: 1,
				ErrorHandler: func(err error) { reported = append(reported, err) },
			})
			op := Start(context.Background(), rt, OperationStart{})
			if op.End(nil) {
				t.Fatal("End reported an emission through a failed write")
			}
			if len(reported) != 1 || !errors.Is(reported[0], ErrSinkWrite) || !errors.Is(reported[0], tc.want) {
				t.Fatalf("reported %v, want one error matching %v and %v", reported, ErrSinkWrite, tc.want)
			}
		})
	}
}

// With no ErrorHandler, a write error goes to the stderr reporter.
func TestJSONSinkWriteErrorDefaultReport(t *testing.T) {
	rt := MustCompile(Config{
		Sink:         NewJSONSink(jsonWriterFunc(func([]byte) (int, error) { return 0, errors.New("pipe closed") })),
		SamplingRate: 1,
	})
	var buf bytes.Buffer
	rt.reporter.w = &buf
	Start(context.Background(), rt, OperationStart{}).End(nil)
	if got := buf.String(); got != "unolog: sink write: pipe closed\n" {
		t.Fatalf("stderr report = %q", got)
	}
}

// Write keeps the Sink contract: it returns nothing and swallows the
// error that writeErr returns.
func TestJSONSinkWriteIgnoresErrors(t *testing.T) {
	sink := NewJSONSink(jsonWriterFunc(func([]byte) (int, error) { return 0, errors.New("x") }))
	sink.Write(t.Context(), recOf(LevelInfo, "m"))
	if err := sink.writeErr(t.Context(), recOf(LevelInfo, "m")); err == nil || !strings.Contains(err.Error(), "x") {
		t.Fatalf("writeErr = %v", err)
	}
	var nilSink *JSONSink
	if err := nilSink.writeErr(t.Context(), recOf(LevelInfo, "m")); err != nil {
		t.Fatalf("nil sink writeErr = %v", err)
	}
	if err := sink.writeErr(t.Context(), nil); err != nil {
		t.Fatalf("nil record writeErr = %v", err)
	}
}

// filterSink embeds *JSONSink and overrides Write, as a sink that
// filters or redacts lines does.
type filterSink struct {
	*JSONSink
	calls int
}

func (s *filterSink) Write(ctx context.Context, rec *Record) {
	s.calls++
	s.JSONSink.Write(ctx, rec)
}

// Embedding *JSONSink promotes no WriteErr, so End calls the Write
// override instead of skipping it.
func TestJSONSinkEmbeddedWriteOverrideRuns(t *testing.T) {
	var buf bytes.Buffer
	sink := &filterSink{JSONSink: NewJSONSink(&buf)}
	if _, ok := Sink(sink).(FallibleSink); ok {
		t.Fatal("a type that embeds *JSONSink is a FallibleSink")
	}
	rt := MustCompile(Config{Sink: sink, SamplingRate: 1})
	if !Start(context.Background(), rt, OperationStart{}).End(nil) || sink.calls != 1 {
		t.Fatalf("End made %d calls to the Write override, want 1", sink.calls)
	}
	if buf.Len() == 0 {
		t.Fatal("the override wrote no line")
	}
}

// fallibleSink is a third-party sink that reports its write errors.
type fallibleSink struct {
	err    error
	writes int
}

func (s *fallibleSink) Write(context.Context, *Record) { s.writes++ }

func (s *fallibleSink) WriteErr(context.Context, *Record) error { return s.err }

// End calls WriteErr on a FallibleSink, not Write, and reports its error.
func TestFallibleSinkReportsWriteErrors(t *testing.T) {
	errRemote := errors.New("remote down")
	var reported error
	sink := &fallibleSink{err: errRemote}
	rt := MustCompile(Config{Sink: sink, SamplingRate: 1, ErrorHandler: func(err error) { reported = err }})
	if Start(context.Background(), rt, OperationStart{}).End(nil) {
		t.Fatal("End reported an emission through a failed write")
	}
	if !errors.Is(reported, ErrSinkWrite) || !errors.Is(reported, errRemote) || sink.writes != 0 {
		t.Fatalf("reported %v after %d Write calls, want the WriteErr error and no Write call", reported, sink.writes)
	}
}

// After a short write, the sink ends the cut line, so the next line
// starts on its own line.
func TestJSONSinkEndsCutLine(t *testing.T) {
	var buf bytes.Buffer
	first := true
	sink := NewJSONSink(jsonWriterFunc(func(p []byte) (int, error) {
		if first {
			first = false
			n, _ := buf.Write(p[:len(p)/2])
			return n, errors.New("disk full")
		}
		return buf.Write(p)
	}))
	cut, next := recOf(LevelInfo, "cut"), recOf(LevelInfo, "next")
	sink.Write(t.Context(), cut)
	sink.Write(t.Context(), next)
	want := string(cut.Encoded()[:len(cut.Encoded())/2]) + "\n" + string(next.Encoded())
	if buf.String() != want {
		t.Fatalf("output = %q, want %q", buf.String(), want)
	}
}

func TestEndCutLine(t *testing.T) {
	b := []byte("{\"a\":1}\n{\"b\":2}\n")
	for _, tc := range []struct {
		n    int
		want string
	}{
		{0, ""},            // nothing written: no line to end
		{3, "\n"},          // cut inside the first line
		{8, ""},            // cut after the first newline
		{len(b) - 1, "\n"}, // only the last newline is missing
		{len(b), ""},       // a full write
	} {
		var buf bytes.Buffer
		endCutLine(&buf, b, tc.n)
		if buf.String() != tc.want {
			t.Fatalf("n=%d: wrote %q, want %q", tc.n, buf.String(), tc.want)
		}
	}
}
