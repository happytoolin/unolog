package slog

import (
	"bytes"
	"context"
	"encoding/json"
	stdslog "log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/happytoolin/unolog"
	"go.uber.org/goleak"
)

func emit(t *testing.T, sink unolog.Sink, level unolog.Level, kv ...any) {
	t.Helper()
	rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
	unolog.SetLevel(op.Context(), level)
	if len(kv) > 0 {
		unolog.Add(op.Context(), kv[0].(string), kv[1], kv[2:]...)
	}
	op.End(nil)
}

func TestSinkWriteMapsLevelAndMessage(t *testing.T) {
	h := &captureSlogHandler{}
	emit(t, New(stdslog.New(h)), unolog.LevelWarn, "user_id", "u_1")

	if len(h.records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(h.records))
	}
	if h.records[0].Message != unolog.DefaultOperationMessage {
		t.Fatalf("expected default message, got %q", h.records[0].Message)
	}
	if h.records[0].Level != stdslog.LevelWarn {
		t.Fatalf("expected warn level, got %v", h.records[0].Level)
	}
	if h.records[0].Attrs["user_id"] != "u_1" {
		t.Fatalf("missing user_id attr")
	}
}

func TestSinkWriteMapsAllKnownLevels(t *testing.T) {
	// debug arrives via a domain policy, warn via the requested floor,
	// error via the error outcome — each must map to its slog level.
	t.Run("debug", func(t *testing.T) {
		h := &captureSlogHandler{}
		rt := unolog.MustCompile(unolog.Config{
			Sink:         New(stdslog.New(h)),
			SamplingRate: 1,
			OperationPolicies: map[unolog.Domain]unolog.OperationPolicy{
				unolog.DomainJob: {SuccessLevel: unolog.LevelDebug},
			},
		})
		unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"}).End(nil)
		if h.records[0].Level != stdslog.LevelDebug {
			t.Fatalf("level = %v, want DEBUG", h.records[0].Level)
		}
	})
	t.Run("warn", func(t *testing.T) {
		h := &captureSlogHandler{}
		op := unolog.Start(context.Background(), mustRT(t, New(stdslog.New(h))), unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
		unolog.SetLevel(op.Context(), unolog.LevelWarn)
		op.End(nil)
		if h.records[0].Level != stdslog.LevelWarn {
			t.Fatalf("level = %v, want WARN", h.records[0].Level)
		}
	})
	t.Run("error", func(t *testing.T) {
		h := &captureSlogHandler{}
		var err error = errBoom{}
		op := unolog.Start(context.Background(), mustRT(t, New(stdslog.New(h))), unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
		op.End(&err)
		if h.records[0].Level != stdslog.LevelError {
			t.Fatalf("level = %v, want ERROR", h.records[0].Level)
		}
	})
}

func mustRT(t *testing.T, sink unolog.Sink) *unolog.Runtime {
	t.Helper()
	return unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }

// TestSinkTypedAttrs pins the typed-constructor mapping: insertion
// order is preserved and kinds survive (int64, bool, duration).
func TestSinkTypedAttrs(t *testing.T) {
	h := &captureSlogHandler{}
	emit(t, New(stdslog.New(h)), unolog.LevelInfo,
		"s", "v", "i", 7, "b", true, "d", 1500*int64(1000000))

	rec := h.records[0]
	if got := rec.Attrs["i"]; got != int64(7) {
		t.Fatalf("i = %v (%T)", got, got)
	}
	if got := rec.Attrs["b"]; got != true {
		t.Fatalf("b = %v", got)
	}
	if _, ok := rec.Attrs["s"]; !ok {
		t.Fatal("missing s")
	}
	// insertion order preserved: s, i, b, d, then the lazy start
	// metadata (op.domain, op.name, appended at commit) and completion
	// fields
	if rec.Order[0] != "s" || rec.Order[1] != "i" {
		t.Fatalf("order = %v", rec.Order)
	}
}

// TestSinkDedupesLastWriteWins mirrors the core encode-side resolution.
func TestSinkDedupesLastWriteWins(t *testing.T) {
	h := &captureSlogHandler{}
	rt := unolog.MustCompile(unolog.Config{Sink: New(stdslog.New(h)), SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
	unolog.Add(op.Context(), "k", "first", "k", "second")
	op.End(nil)

	if got := h.records[0].Attrs["k"]; got != "second" {
		t.Fatalf("k = %v", got)
	}
	count := 0
	for _, k := range h.records[0].Order {
		if k == "k" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("k emitted %d times", count)
	}
}

// TestSinkErrorAndRawWireFidelity pins the v0 shapes: error fields
// render the message string (never null); raw bytes render via stdslog.Any.
func TestSinkErrorAndRawWireFidelity(t *testing.T) {
	h := &captureSlogHandler{}
	rt := unolog.MustCompile(unolog.Config{Sink: New(stdslog.New(h)), SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
	unolog.Add(op.Context(), "e", errBoom{})
	unolog.Add(op.Context(), "meta", json.RawMessage(`{"raw":true}`))
	op.End(nil)

	if got := h.records[0].Attrs["e"]; got != "boom" {
		t.Fatalf("error field = %v (%T), want \"boom\"", got, got)
	}
	if got, ok := h.records[0].Attrs["meta"]; !ok || got == nil {
		t.Fatalf("raw field = %v, want non-nil (stdslog.Any bytes)", got)
	}
}

func TestSinkWriteNilSafety(t *testing.T) {
	var nilSink *Sink
	nilSink.Write(context.Background(), nil)

	sink := New(nil)
	sink.Write(context.Background(), nil)
}

type disabledSlogHandler struct {
	handled bool
}

func (*disabledSlogHandler) Enabled(context.Context, stdslog.Level) bool { return false }
func (h *disabledSlogHandler) Handle(context.Context, stdslog.Record) error {
	h.handled = true
	return nil
}
func (h *disabledSlogHandler) WithAttrs([]stdslog.Attr) stdslog.Handler { return h }
func (h *disabledSlogHandler) WithGroup(string) stdslog.Handler         { return h }

func TestSinkSkipsDisabledEvent(t *testing.T) {
	handler := &disabledSlogHandler{}
	emit(t, New(stdslog.New(handler)), unolog.LevelDebug)
	if handler.handled {
		t.Fatal("disabled debug reached handler")
	}
}

type captureSlogRecord struct {
	Level   stdslog.Level
	Message string
	Attrs   map[string]any
	Order   []string
}

type captureSlogHandler struct {
	records []captureSlogRecord
}

func (h *captureSlogHandler) Enabled(context.Context, stdslog.Level) bool {
	return true
}

func (h *captureSlogHandler) Handle(_ context.Context, r stdslog.Record) error {
	rec := captureSlogRecord{
		Level:   r.Level,
		Message: r.Message,
		Attrs:   make(map[string]any),
	}
	r.Attrs(func(attr stdslog.Attr) bool {
		rec.Attrs[attr.Key] = attr.Value.Any()
		rec.Order = append(rec.Order, attr.Key)
		return true
	})
	h.records = append(h.records, rec)
	return nil
}

func (h *captureSlogHandler) WithAttrs([]stdslog.Attr) stdslog.Handler { return h }
func (h *captureSlogHandler) WithGroup(string) stdslog.Handler         { return h }

// Bridge robustness tests: nil/garbage abuse and typed-nil error
// containment.

type recSink struct{ rec *unolog.Record }

func (s *recSink) Write(_ context.Context, rec *unolog.Record) { s.rec = rec }

func crashRecord(t *testing.T) *unolog.Record {
	t.Helper()
	s := &recSink{}
	rt := unolog.MustCompile(unolog.Config{Sink: s, SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "j"})
	unolog.Add(op.Context(), "k", "v")
	if !op.End(nil) || s.rec == nil {
		t.Fatal("no record captured")
	}
	return s.rec
}

func TestCrashNilAbuse(t *testing.T) {
	rec := crashRecord(t)
	New(nil).Write(context.Background(), rec)
	New(nil).Write(context.Background(), nil)
	var nilSink *Sink
	nilSink.Write(context.Background(), rec)
	New(stdslog.New(stdslog.DiscardHandler)).Write(context.Background(), rec)
	New(stdslog.Default()).Write(context.Background(), rec)
}

func TestCrashTypedNilErrorField(t *testing.T) {
	var pe *os.PathError
	var buf bytes.Buffer
	s := &recSink{}
	rt := unolog.MustCompile(unolog.Config{Sink: s, SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "j"})
	unolog.Add(op.Context(), "e", pe)
	unolog.Error(op.Context(), pe)
	_ = op.End(nil)
	rec := s.rec
	New(stdslog.New(stdslog.NewTextHandler(&buf, nil))).Write(context.Background(), rec)
	if !strings.Contains(buf.String(), "<nil>") {
		t.Fatalf("typed-nil error not rendered as <nil>: %s", buf.String())
	}
}

type retainingHandler struct {
	mu      sync.Mutex
	records []stdslog.Record
}

func (h *retainingHandler) Enabled(context.Context, stdslog.Level) bool { return true }

func (h *retainingHandler) Handle(_ context.Context, r stdslog.Record) error {
	h.mu.Lock()
	h.records = append(h.records, r)
	h.mu.Unlock()
	return nil
}

func (h *retainingHandler) WithAttrs([]stdslog.Attr) stdslog.Handler { return h }
func (h *retainingHandler) WithGroup(string) stdslog.Handler         { return h }

// TestSinkRecordsSurviveRetainingHandler drives many lifecycles
// through the adapter; a handler that retains records must never see
// corrupted or cross-request data.
func TestSinkRecordsSurviveRetainingHandler(t *testing.T) {
	h := &retainingHandler{}
	rt := unolog.MustCompile(unolog.Config{Sink: New(stdslog.New(h)), SamplingRate: 1})

	for range 100 {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
		unolog.Add(op.Context(), "a", 1, "b", "two", "c", true)
		op.End(nil)
	}

	if len(h.records) != 100 {
		t.Fatalf("got %d records, want 100", len(h.records))
	}
	for i, r := range h.records {
		count := 0
		r.Attrs(func(a stdslog.Attr) bool {
			count++
			switch a.Key {
			case "a":
				if a.Value.Int64() != 1 {
					t.Fatalf("record %d: a = %v", i, a.Value)
				}
			case "b":
				if a.Value.String() != "two" {
					t.Fatalf("record %d: b = %v", i, a.Value)
				}
			case "c":
				if !a.Value.Bool() {
					t.Fatalf("record %d: c = %v", i, a.Value)
				}
			}
			return true
		})
		if count < 3 {
			t.Fatalf("record %d: %d attrs, want >= 3", i, count)
		}
	}
}

// TestSinkConcurrentWrites drives concurrent lifecycles with distinct
// field payloads through one adapter instance.
func TestSinkConcurrentWrites(t *testing.T) {
	h := &retainingHandler{}
	rt := unolog.MustCompile(unolog.Config{Sink: New(stdslog.New(h)), SamplingRate: 1})

	const writers = 8
	const writes = 50

	var wg sync.WaitGroup
	for w := range writers {
		wg.Go(func() {
			tag := "w" + strconv.Itoa(w)
			for range writes {
				op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: tag})
				ctx := op.Context()
				for i := range 10 {
					unolog.Add(ctx, "k"+strconv.Itoa(i), tag+":"+strconv.Itoa(i))
				}
				op.End(nil)
			}
		})
	}
	wg.Wait()

	total := writers * writes
	if len(h.records) != total {
		t.Fatalf("got %d records, want %d", len(h.records), total)
	}
}

// TestStressSlogSustainedCorrectness hammers the adapter with
// sustained concurrent traffic; every record must stay intact.
func TestStressSlogSustainedCorrectness(t *testing.T) {
	h := &retainingHandler{}
	rt := unolog.MustCompile(unolog.Config{Sink: New(stdslog.New(h)), SamplingRate: 1})

	const goroutines = 8
	const writes = 2_000

	var wg sync.WaitGroup
	for range goroutines {
		wg.Go(func() {
			for range writes {
				op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "stress"})
				unolog.Add(op.Context(), "a", 1, "b", "two", "c", true)
				op.End(nil)
			}
		})
	}
	wg.Wait()

	if len(h.records) != goroutines*writes {
		t.Fatalf("got %d records, want %d", len(h.records), goroutines*writes)
	}
}

// goleak integration: every test in this module runs under
// goleak.VerifyTestMain, failing the suite on any leaked goroutine.
// goleak is test-only: nothing outside _test.go imports it.

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
