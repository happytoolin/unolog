package unolog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// lockedBuffer is a concurrency-safe writer for reading what the
// writer goroutine wrote.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// The async sink writes the same lines as JSONSink, in order, and Close
// writes every buffered line before it returns.
func TestAsyncJSONSinkWritesCanonicalLines(t *testing.T) {
	var out lockedBuffer
	var want bytes.Buffer
	sink := NewAsyncJSONSink(&out, AsyncJSONSinkConfig{})
	for i := range 100 {
		rec := recOf(LevelInfo, "m", Int("i", i))
		want.Write(rec.Encoded())
		sink.Write(t.Context(), rec)
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if out.String() != want.String() {
		t.Fatalf("async lines differ from JSONSink lines:\n%s\nwant\n%s", out.String(), want.String())
	}
	if sink.Dropped() != 0 {
		t.Fatalf("dropped %d lines with room in the buffer", sink.Dropped())
	}
}

// Many request goroutines: every line is written or counted as dropped.
func TestAsyncJSONSinkConcurrentWriters(t *testing.T) {
	var out lockedBuffer
	sink := NewAsyncJSONSink(&out, AsyncJSONSinkConfig{BufferBytes: 4 << 10, ErrorHandler: func(error) {}})
	rt := MustCompile(Config{Sink: sink, SamplingRate: 1})
	var wg sync.WaitGroup
	const goroutines, perGoroutine = 16, 500
	for g := range goroutines {
		wg.Go(func() {
			for i := range perGoroutine {
				op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "j"})
				Add(op.Context(), "g", g, "i", i)
				op.End(nil)
			}
		})
	}
	wg.Wait()
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	lines := strings.Count(out.String(), "\n")
	if total := uint64(lines) + sink.Dropped(); total != goroutines*perGoroutine {
		t.Fatalf("written %d + dropped %d = %d, want %d", lines, sink.Dropped(), total, goroutines*perGoroutine)
	}
}

// A blocked writer must not block requests: lines that do not fit are
// dropped, counted, and reported once the writer returns.
func TestAsyncJSONSinkDropsWhenFull(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{}, 1)
	var out lockedBuffer
	var reports []error
	var mu sync.Mutex
	sink := NewAsyncJSONSink(jsonWriterFunc(func(p []byte) (int, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-release
		return out.Write(p)
	}), AsyncJSONSinkConfig{
		BufferBytes: 512,
		ErrorHandler: func(err error) {
			mu.Lock()
			reports = append(reports, err)
			mu.Unlock()
		},
	})
	rec := recOf(LevelInfo, "m", String("pad", strings.Repeat("x", 100)))
	sink.Write(t.Context(), rec) // the writer takes this batch and blocks
	<-started
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 50 {
			sink.Write(t.Context(), rec)
		}
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Write blocked on a blocked writer")
	}
	if sink.Dropped() == 0 {
		t.Fatal("no line dropped with a full buffer")
	}
	close(release)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	var reported uint64
	for _, err := range reports {
		if !errors.Is(err, ErrSinkFull) {
			t.Fatalf("unexpected report %v", err)
		}
		var n uint64
		if _, scanErr := fmt.Sscanf(err.Error(), "unolog: sink buffer full: %d lines dropped", &n); scanErr != nil {
			t.Fatalf("report %q: %v", err, scanErr)
		}
		reported += n
	}
	if reported != sink.Dropped() {
		t.Fatalf("reported %d drops, counted %d", reported, sink.Dropped())
	}
	written := uint64(strings.Count(out.String(), "\n"))
	if written+sink.Dropped() != 51 {
		t.Fatalf("written %d + dropped %d, want 51", written, sink.Dropped())
	}
}

// A line longer than the whole buffer is dropped and reported even
// though the writer has nothing else to write.
func TestAsyncJSONSinkReportsOversizeLine(t *testing.T) {
	reported := make(chan error, 1)
	sink := NewAsyncJSONSink(&lockedBuffer{}, AsyncJSONSinkConfig{BufferBytes: 64, ErrorHandler: func(err error) { reported <- err }})
	defer sink.Close()
	sink.Write(t.Context(), recOf(LevelInfo, strings.Repeat("m", 100)))
	select {
	case err := <-reported:
		if !errors.Is(err, ErrSinkFull) {
			t.Fatalf("report = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("oversize drop never reported")
	}
}

// Write errors and writer panics are reported, Flush returns the error,
// and the writer goroutine keeps writing later lines.
func TestAsyncJSONSinkWriterFailures(t *testing.T) {
	errDisk := errors.New("disk full")
	var calls atomic.Int32
	var out lockedBuffer
	var mu sync.Mutex
	var reports []error
	sink := NewAsyncJSONSink(jsonWriterFunc(func(p []byte) (int, error) {
		switch calls.Add(1) {
		case 1:
			return 0, errDisk
		case 2:
			panic("writer bug")
		default:
			return out.Write(p)
		}
	}), AsyncJSONSinkConfig{ErrorHandler: func(err error) {
		mu.Lock()
		reports = append(reports, err)
		mu.Unlock()
	}})
	defer sink.Close()
	var stderr lockedBuffer
	sink.reporter.w = &stderr

	sink.Write(t.Context(), recOf(LevelInfo, "first"))
	if err := sink.Flush(t.Context()); !errors.Is(err, ErrSinkWrite) || !errors.Is(err, errDisk) {
		t.Fatalf("Flush = %v, want the write error", err)
	}
	sink.Write(t.Context(), recOf(LevelInfo, "second"))
	var pe *PanicError
	if err := sink.Flush(t.Context()); !errors.As(err, &pe) || !errors.Is(err, ErrSinkPanic) {
		t.Fatalf("Flush = %v, want the writer panic", err)
	}
	sink.Write(t.Context(), recOf(LevelInfo, "third"))
	if err := sink.Flush(t.Context()); err != nil {
		t.Fatalf("Flush = %v after the writer recovered", err)
	}
	if !strings.Contains(out.String(), `"message":"third"`) {
		t.Fatalf("output %q lacks the line after the failures", out.String())
	}
	mu.Lock()
	defer mu.Unlock()
	if len(reports) != 2 || !errors.Is(reports[0], errDisk) || !errors.Is(reports[1], ErrSinkPanic) {
		t.Fatalf("reports = %v", reports)
	}
	if stderr.String() != "" {
		t.Fatalf("the handler took the reports, but stderr has %q", stderr.String())
	}
}

// Flush stops waiting when its context ends, even if the writer hangs.
// A short write is an error. With no ErrorHandler, the error goes to the
// stderr reporter.
func TestAsyncJSONSinkShortWriteDefaultReport(t *testing.T) {
	sink := NewAsyncJSONSink(jsonWriterFunc(func(p []byte) (int, error) {
		return len(p) - 1, nil
	}), AsyncJSONSinkConfig{})
	var report lockedBuffer
	sink.reporter.w = &report
	sink.Write(t.Context(), recOf(LevelInfo, "m"))
	if err := sink.Flush(t.Context()); !errors.Is(err, ErrSinkWrite) || !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("Flush = %v, want a short write", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("Close = %v, want nil after Flush returned the error", err)
	}
	if got := report.String(); got != "unolog: sink write: short write\n" {
		t.Fatalf("stderr report = %q", got)
	}
}

// Close returns a write error that no Flush returned, once.
func TestAsyncJSONSinkCloseReturnsErrorOnce(t *testing.T) {
	sink := NewAsyncJSONSink(jsonWriterFunc(func([]byte) (int, error) {
		return 0, errors.New("disk full")
	}), AsyncJSONSinkConfig{ErrorHandler: func(error) {}})
	sink.Write(t.Context(), recOf(LevelInfo, "m"))
	if err := sink.Close(); !errors.Is(err, ErrSinkWrite) {
		t.Fatalf("Close = %v, want the write error", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("second Close = %v, want nil", err)
	}
}

// A nil writer discards the lines.
func TestAsyncJSONSinkNilWriter(t *testing.T) {
	sink := NewAsyncJSONSink(nil, AsyncJSONSinkConfig{})
	sink.Write(t.Context(), recOf(LevelInfo, "m"))
	if err := sink.Close(); err != nil || sink.Dropped() != 0 {
		t.Fatalf("Close = %v, dropped %d; want nil and 0", err, sink.Dropped())
	}
}

// waitWriterIdle waits until every AsyncJSONSink writer goroutine waits
// on its wake channel. The tests close their sinks, so only the sink of
// the running test has a writer goroutine.
func waitWriterIdle(t *testing.T) {
	t.Helper()
	buf := make([]byte, 1<<20)
	for deadline := time.Now().Add(5 * time.Second); ; {
		stacks := strings.Split(string(buf[:runtime.Stack(buf, true)]), "\n\n")
		writers, idle := 0, 0
		for _, g := range stacks {
			if !strings.Contains(g, ".(*AsyncJSONSink).run(") {
				continue
			}
			writers++
			// Idle: blocked on a channel, with run itself as the top frame.
			if lines := strings.SplitN(g, "\n", 3); len(lines) > 1 &&
				strings.Contains(lines[0], "[chan receive") && strings.Contains(lines[1], ".(*AsyncJSONSink).run(") {
				idle++
			}
		}
		if writers > 0 && idle == writers {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%d of %d writer goroutines are not idle", writers-idle, writers)
		}
		time.Sleep(time.Millisecond)
	}
}

// With nothing to write, the writer goroutine waits on its channel. It
// does not poll the buffer in a loop.
func TestAsyncJSONSinkWriterIdlesWhenEmpty(t *testing.T) {
	sink := NewAsyncJSONSink(io.Discard, AsyncJSONSinkConfig{})
	defer sink.Close()
	waitWriterIdle(t)
	sink.Write(t.Context(), recOf(LevelInfo, "m"))
	if err := sink.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitWriterIdle(t)
}

// A line written while the writer writes a batch goes to the other
// buffer: it does not change the batch. The two buffers must never share
// memory, also after the writer found the buffer empty.
func TestAsyncJSONSinkWriteDuringBatchKeepsBatch(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	var out lockedBuffer
	sink := NewAsyncJSONSink(jsonWriterFunc(func(p []byte) (int, error) {
		if calls.Add(1) == 2 {
			close(entered)
			<-release
		}
		return out.Write(p)
	}), AsyncJSONSinkConfig{ErrorHandler: func(error) {}})
	a, b, c := recOf(LevelInfo, "aaaa"), recOf(LevelInfo, "bbbb"), recOf(LevelInfo, "cccc")
	sink.Write(t.Context(), a)
	if err := sink.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	waitWriterIdle(t) // the writer saw the empty buffer
	sink.Write(t.Context(), b)
	<-entered // the writer holds the batch with b
	sink.Write(t.Context(), c)
	close(release)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if want := string(a.Encoded()) + string(b.Encoded()) + string(c.Encoded()); out.String() != want {
		t.Fatalf("output %q, want %q", out.String(), want)
	}
}

// The writer reports drops after each batch, also while more lines wait.
// A writer that is always busy must not delay the report forever.
func TestAsyncJSONSinkReportsDropsWhileBusy(t *testing.T) {
	entered := make(chan int, 2)
	release := make(chan struct{})
	var calls atomic.Int32
	reports := make(chan error, 4)
	line := recOf(LevelInfo, "m")
	sink := NewAsyncJSONSink(jsonWriterFunc(func(p []byte) (int, error) {
		entered <- int(calls.Add(1))
		<-release
		return len(p), nil
	}), AsyncJSONSinkConfig{
		BufferBytes:  len(line.Encoded()), // one line fits
		ErrorHandler: func(err error) { reports <- err },
	})
	defer func() {
		close(release)
		_ = sink.Close()
	}()
	next := func() {
		t.Helper()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("the writer did not start the next batch")
		}
	}
	sink.Write(t.Context(), line)
	next()                        // the writer holds the first batch
	sink.Write(t.Context(), line) // fills the buffer
	sink.Write(t.Context(), line) // dropped
	release <- struct{}{}         // finish the first batch only
	next()                        // the writer holds the second batch
	select {
	case err := <-reports:
		if !errors.Is(err, ErrSinkFull) {
			t.Fatalf("report %v, want ErrSinkFull", err)
		}
	default:
		t.Fatal("the drop was not reported before the next batch")
	}
}

// A line that fills the buffer to the limit fits. When it fits the
// capacity, the buffer does not move to new memory.
func TestAsyncJSONSinkLineFillsBufferExactly(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	line := recOf(LevelInfo, "m")
	n := len(line.Encoded())
	sink := NewAsyncJSONSink(jsonWriterFunc(func(p []byte) (int, error) {
		once.Do(func() { close(entered) })
		<-release
		return len(p), nil
	}), AsyncJSONSinkConfig{BufferBytes: 2 * n})
	sink.Write(t.Context(), line)
	<-entered                     // the writer holds the first batch
	sink.Write(t.Context(), line) // allocates the buffer: capacity 2n
	sink.mu.Lock()
	first := &sink.buf[0]
	sink.mu.Unlock()
	sink.Write(t.Context(), line) // fills the buffer to the limit
	sink.mu.Lock()
	moved := &sink.buf[0] != first
	sink.mu.Unlock()
	close(release)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if sink.Dropped() != 0 {
		t.Fatalf("dropped %d lines that fit the buffer", sink.Dropped())
	}
	if moved {
		t.Fatal("the buffer moved although the line fit its capacity")
	}
}

// Zero and negative sizes select the documented 1 MiB default. Any
// positive size, also 1, is the limit.
func TestAsyncJSONSinkBufferBytesDefault(t *testing.T) {
	if DefaultAsyncBufferBytes != 1<<20 {
		t.Fatalf("DefaultAsyncBufferBytes = %d, the README documents 1 MiB", DefaultAsyncBufferBytes)
	}
	for size, want := range map[int]int{0: DefaultAsyncBufferBytes, -1: DefaultAsyncBufferBytes, 1: 1, 4096: 4096} {
		sink := NewAsyncJSONSink(io.Discard, AsyncJSONSinkConfig{BufferBytes: size})
		if sink.max != want {
			t.Errorf("BufferBytes %d: limit %d, want %d", size, sink.max, want)
		}
		_ = sink.Close()
	}
}

func TestAsyncJSONSinkFlushHonorsContext(t *testing.T) {
	release := make(chan struct{})
	sink := NewAsyncJSONSink(jsonWriterFunc(func(p []byte) (int, error) {
		<-release
		return len(p), nil
	}), AsyncJSONSinkConfig{})
	sink.Write(t.Context(), recOf(LevelInfo, "m"))
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := sink.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Flush = %v, want the deadline error", err)
	}
	close(release)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

// After Close, Write drops and counts the line; Close is idempotent;
// a nil sink is a no-op.
func TestAsyncJSONSinkAfterClose(t *testing.T) {
	var out lockedBuffer
	sink := NewAsyncJSONSink(&out, AsyncJSONSinkConfig{})
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	sink.Write(t.Context(), recOf(LevelInfo, "late"))
	if sink.Dropped() != 1 || out.String() != "" {
		t.Fatalf("late line: dropped %d, output %q", sink.Dropped(), out.String())
	}
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
	if err := sink.Flush(t.Context()); err != nil {
		t.Fatal(err)
	}
	sink.Write(t.Context(), nil) // a nil record is a no-op, not a drop
	if sink.Dropped() != 1 {
		t.Fatalf("nil record: dropped %d, want 1", sink.Dropped())
	}
	var nilSink *AsyncJSONSink
	nilSink.Write(t.Context(), recOf(LevelInfo, "m"))
	if nilSink.Close() != nil || nilSink.Flush(t.Context()) != nil || nilSink.Dropped() != 0 {
		t.Fatal("nil sink is not a no-op")
	}
}

// The buffer never grows past BufferBytes.
func TestAsyncJSONSinkBufferStaysBounded(t *testing.T) {
	release := make(chan struct{})
	sink := NewAsyncJSONSink(jsonWriterFunc(func(p []byte) (int, error) {
		<-release
		return len(p), nil
	}), AsyncJSONSinkConfig{BufferBytes: 10 << 10, ErrorHandler: func(error) {}})
	for i := range 1000 {
		sink.Write(t.Context(), recOf(LevelInfo, "m", Int("i", i)))
	}
	sink.mu.Lock()
	bufCap, spareCap := cap(sink.buf), cap(sink.spare)
	sink.mu.Unlock()
	if bufCap > 10<<10 || spareCap > 10<<10 {
		t.Fatalf("buffer caps %d and %d exceed the 10 KiB limit", bufCap, spareCap)
	}
	close(release)
	if err := sink.Close(); err != nil {
		t.Fatal(err)
	}
}

// FuzzAsyncJSONSink drives random writes, flushes, and buffer sizes
// against a writer that sometimes fails, writes short, or panics. Every
// line must be written, dropped, or lost in a failed batch, and the
// written lines keep their order.
func FuzzAsyncJSONSink(f *testing.F) {
	f.Add(uint16(256), []byte("abc\x11def"))
	f.Add(uint16(64), []byte{200, 0, 17, 255, 3})
	f.Fuzz(func(t *testing.T, bufBytes uint16, program []byte) {
		var out lockedBuffer
		var calls, lost atomic.Int64
		sink := NewAsyncJSONSink(jsonWriterFunc(func(p []byte) (int, error) {
			lines := int64(bytes.Count(p, []byte("\n")))
			switch calls.Add(1) % 7 {
			case 2:
				lost.Add(lines)
				return 0, errors.New("write failed")
			case 4:
				lost.Add(lines)
				return len(p) - 1, nil
			case 6:
				lost.Add(lines)
				panic("writer panic")
			default:
				return out.Write(p)
			}
		}), AsyncJSONSinkConfig{BufferBytes: int(bufBytes)%4096 + 1, ErrorHandler: func(error) {}})

		want := make([]string, 0, len(program))
		for i, b := range program {
			rec := recOf(LevelInfo, "m", Int("i", i), String("pad", strings.Repeat("x", int(b))))
			want = append(want, string(rec.Encoded()))
			sink.Write(t.Context(), rec)
			if b%17 == 0 {
				_ = sink.Flush(t.Context())
			}
		}
		_ = sink.Close()

		got := strings.SplitAfter(out.String(), "\n")
		got = got[:len(got)-1] // SplitAfter leaves an empty tail
		if total := int64(len(got)) + int64(sink.Dropped()) + lost.Load(); total != int64(len(program)) {
			t.Fatalf("written %d + dropped %d + lost %d != %d lines", len(got), sink.Dropped(), lost.Load(), len(program))
		}
		next := 0
		for _, line := range got {
			for next < len(want) && want[next] != line {
				next++
			}
			if next == len(want) {
				t.Fatalf("line %q is out of order or unknown", line)
			}
			next++
		}
	})
}
