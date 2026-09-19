package unolog

import (
	"bytes"
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
