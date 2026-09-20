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

// TestBufferedJSONSinkBatchesUntilFlush pins the batching contract: a
// line smaller than the buffer stays in memory (the underlying writer
// sees nothing), and Flush delivers it verbatim.
func TestBufferedJSONSinkBatchesUntilFlush(t *testing.T) {
	var buf bytes.Buffer
	sink := NewBufferedJSONSink(&buf, 4096)
	rec := recOf(LevelInfo, "batched")
	sink.Write(t.Context(), rec)
	if buf.Len() != 0 {
		t.Fatalf("writer saw %q before Flush, want nothing buffered", buf.Bytes())
	}
	if err := sink.Flush(); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if !bytes.Equal(buf.Bytes(), rec.Encoded()) {
		t.Fatalf("after Flush = %q, want %q", buf.Bytes(), rec.Encoded())
	}
}
