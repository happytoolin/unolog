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
// error that WriteErr returns.
func TestJSONSinkWriteIgnoresErrors(t *testing.T) {
	sink := NewJSONSink(jsonWriterFunc(func([]byte) (int, error) { return 0, errors.New("x") }))
	sink.Write(t.Context(), recOf(LevelInfo, "m"))
	if err := sink.WriteErr(t.Context(), recOf(LevelInfo, "m")); err == nil || !strings.Contains(err.Error(), "x") {
		t.Fatalf("WriteErr = %v", err)
	}
	var nilSink *JSONSink
	if err := nilSink.WriteErr(t.Context(), recOf(LevelInfo, "m")); err != nil {
		t.Fatalf("nil sink WriteErr = %v", err)
	}
	if err := sink.WriteErr(t.Context(), nil); err != nil {
		t.Fatalf("nil record WriteErr = %v", err)
	}
}
