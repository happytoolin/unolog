package zap

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happytoolin/unolog"
	"go.uber.org/goleak"
	gozap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func emit(t *testing.T, sink unolog.Sink, mutate func(ctx context.Context)) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	rt := unolog.MustCompile(unolog.Config{Sink: New(gozap.New(core)), SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
	if mutate != nil {
		mutate(op.Context())
	}
	op.End(nil)
	return logs
}

func emitErr(t *testing.T, err error) *observer.ObservedLogs {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	rt := unolog.MustCompile(unolog.Config{Sink: New(gozap.New(core)), SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
	op.End(&err)
	return logs
}

func TestSinkWriteMapsLevelAndMessage(t *testing.T) {
	logs := emit(t, nil, func(ctx context.Context) {
		unolog.Add(ctx, "http.status", 500, "user_id", "u_1")
		unolog.SetLevel(ctx, unolog.LevelError)
	})

	if logs.Len() != 1 {
		t.Fatalf("expected one log entry, got %d", logs.Len())
	}
	entry := logs.All()[0]
	if entry.Level != zapcore.ErrorLevel {
		t.Fatalf("expected error level, got %v", entry.Level)
	}
	if entry.Message != unolog.DefaultOperationMessage {
		t.Fatalf("expected default message, got %q", entry.Message)
	}
	if got := entry.ContextMap()["http.status"]; got != int64(500) {
		t.Fatalf("expected status field, got %v", got)
	}
	if got := entry.ContextMap()["user_id"]; got != "u_1" {
		t.Fatalf("expected user_id field, got %v", got)
	}
}

func TestSinkWriteMapsAllKnownLevels(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(ctx context.Context)
		err    error
		want   zapcore.Level
	}{
		{name: "debug", mutate: func(ctx context.Context) { unolog.SetLevel(ctx, unolog.LevelDebug) }, want: zapcore.InfoLevel},
		{name: "warn", mutate: func(ctx context.Context) { unolog.SetLevel(ctx, unolog.LevelWarn) }, want: zapcore.WarnLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var logs *observer.ObservedLogs
			if tt.err != nil {
				logs = emitErr(t, tt.err)
			} else {
				logs = emit(t, nil, tt.mutate)
			}
			entry := logs.All()[0]
			if entry.Level != tt.want {
				t.Fatalf("level = %v, want %v", entry.Level, tt.want)
			}
		})
	}
}

func TestSinkTypedFieldsAndOrder(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	rt := unolog.MustCompile(unolog.Config{Sink: New(gozap.New(core)), SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
	unolog.Add(op.Context(), "s", "v", "i", 7, "b", true, "d", time.Second)
	unolog.Add(op.Context(), "k", "first", "k", "second")
	op.End(nil)

	entry := logs.All()[0]
	ctxMap := entry.ContextMap()
	if ctxMap["i"] != int64(7) {
		t.Fatalf("i = %v (%T)", ctxMap["i"], ctxMap["i"])
	}
	if ctxMap["b"] != true || ctxMap["s"] != "v" {
		t.Fatalf("s/b = %v/%v", ctxMap["s"], ctxMap["b"])
	}
	// duration: zap stores time.Duration natively in the field; the
	// observer's ContextMap renders it as time.Duration
	if d, ok := ctxMap["d"].(time.Duration); !ok || d != time.Second {
		t.Fatalf("d = %v (%T)", ctxMap["d"], ctxMap["d"])
	}
	if ctxMap["k"] != "second" {
		t.Fatalf("k = %v, want last write", ctxMap["k"])
	}
}

// TestSinkFloat32AndRawWireFidelity pins the v0 shapes: float32 renders
// 32-bit precision (0.1, not the widened double digits); raw bytes
// render via gozap.Any (base64), never null.
func TestSinkFloat32AndRawWireFidelity(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	rt := unolog.MustCompile(unolog.Config{Sink: New(gozap.New(core)), SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
	unolog.Add(op.Context(), "f", float32(0.1))
	unolog.Add(op.Context(), "meta", json.RawMessage(`{"raw":true}`))
	op.End(nil)

	ctxMap := logs.All()[0].ContextMap()
	switch f := ctxMap["f"].(type) {
	case float32:
		if fmtFloat(float64(f)) != "0.1" {
			t.Fatalf("float32 field = %v, want 0.1", f)
		}
	case float64:
		if fmtFloat(f) != "0.1" {
			t.Fatalf("float32 field = %v, want 0.1 (32-bit rendering)", f)
		}
	default:
		t.Fatalf("float32 field = %v (%T)", ctxMap["f"], ctxMap["f"])
	}
	if got, ok := ctxMap["meta"]; !ok || got == nil {
		t.Fatalf("raw field = %v, want non-nil", got)
	}
}

func fmtFloat(f float64) string { return strconv.FormatFloat(f, 'g', -1, 32) }

func TestSinkWriteNilSafety(t *testing.T) {
	var nilSink *Sink
	nilSink.Write(context.Background(), nil)

	New(nil).Write(context.Background(), nil)
}

func TestSinkSkipsDisabledEvent(t *testing.T) {
	core, logs := observer.New(zapcore.WarnLevel) // debug/info disabled
	rt := unolog.MustCompile(unolog.Config{Sink: New(gozap.New(core)), SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
	op.End(nil) // success -> info -> filtered

	if logs.Len() != 0 {
		t.Fatal("disabled info event reached the core")
	}
}

func TestSinkConcurrentWrites(t *testing.T) {
	core, logs := observer.New(zapcore.DebugLevel)
	rt := unolog.MustCompile(unolog.Config{Sink: New(gozap.New(core)), SamplingRate: 1})

	var wg sync.WaitGroup
	for w := range 8 {
		wg.Go(func() {
			for i := range 100 {
				op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "w"})
				unolog.Add(op.Context(), "w", w, "i", i)
				op.End(nil)
			}
		})
	}
	wg.Wait()

	if logs.Len() != 800 {
		t.Fatalf("entries = %d, want 800", logs.Len())
	}
}

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
	New(gozap.NewNop()).Write(context.Background(), rec)
	New(gozap.NewExample()).Write(context.Background(), rec)
}

func TestCrashTypedNilErrorField(t *testing.T) {
	var pe *os.PathError
	s := &recSink{}
	rt := unolog.MustCompile(unolog.Config{Sink: s, SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "j"})
	unolog.Add(op.Context(), "e", pe)
	unolog.Error(op.Context(), pe)
	_ = op.End(nil)
	rec := s.rec
	var out bytes.Buffer
	zl := gozap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zapcore.EncoderConfig{TimeKey: "ts", LevelKey: "level", MessageKey: "msg"}), zapcore.Lock(zapcore.AddSync(&out)), zapcore.DebugLevel))
	New(zl).Write(context.Background(), rec)
	if !strings.Contains(out.String(), "<nil>") {
		t.Fatalf("typed-nil error not rendered as <nil>: %s", out.String())
	}
}

// goleak integration: every test in this module runs under
// goleak.VerifyTestMain, failing the suite on any leaked goroutine.
// goleak is test-only: nothing outside _test.go imports it.

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
