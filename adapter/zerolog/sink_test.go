package zerolog

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/happytoolin/unolog"
	gozerolog "github.com/rs/zerolog"
	"go.uber.org/goleak"
)

func emit(t *testing.T, buf *bytes.Buffer, mutate func(ctx context.Context)) {
	t.Helper()
	logger := gozerolog.New(buf)
	rt := unolog.MustCompile(unolog.Config{Sink: New(&logger), SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
	if mutate != nil {
		mutate(op.Context())
	}
	op.End(nil)
}

func lastPayload(t *testing.T, buf *bytes.Buffer) map[string]any {
	t.Helper()
	line := bytes.TrimRight(buf.Bytes(), "\n")
	lines := strings.Split(string(line), "\n")
	var payload map[string]any
	if err := json.Unmarshal([]byte(lines[len(lines)-1]), &payload); err != nil {
		t.Fatalf("invalid JSON %q: %v", line, err)
	}
	return payload
}

func TestSinkWriteMapsLevelAndFields(t *testing.T) {
	var buf bytes.Buffer
	emit(t, &buf, func(ctx context.Context) {
		unolog.Add(ctx, "http.status", 500, "user_id", "u_1")
		unolog.SetLevel(ctx, unolog.LevelError)
	})

	payload := lastPayload(t, &buf)
	if payload["level"] != "error" {
		t.Fatalf("level = %v", payload["level"])
	}
	if payload["message"] != unolog.DefaultOperationMessage {
		t.Fatalf("message = %q", payload["message"])
	}
	if payload["user_id"] != "u_1" {
		t.Fatalf("user_id = %v", payload["user_id"])
	}
	if payload["http.status"] != float64(500) {
		t.Fatalf("http.status = %v", payload["http.status"])
	}
	tm, ok := payload["time"].(string)
	if !ok {
		t.Fatalf("time is not a string: %v", payload["time"])
	}
	if _, err := time.Parse(time.RFC3339, tm); err != nil {
		t.Fatalf("time not RFC3339: %v", payload["time"])
	}
}

func TestSinkWriteMapsAllKnownLevels(t *testing.T) {
	cases := map[string]struct {
		mutate func(ctx context.Context)
		err    error
		want   string
	}{
		"debug": {mutate: func(ctx context.Context) { unolog.SetLevel(ctx, unolog.LevelDebug) }, want: "info"}, // floor never lowers
		"warn":  {mutate: func(ctx context.Context) { unolog.SetLevel(ctx, unolog.LevelWarn) }, want: "warn"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			var buf bytes.Buffer
			emit(t, &buf, c.mutate)
			if payload := lastPayload(t, &buf); payload["level"] != c.want {
				t.Fatalf("level = %v, want %v", payload["level"], c.want)
			}
		})
	}
	t.Run("error", func(t *testing.T) {
		var buf bytes.Buffer
		var err error = errBoom{}
		logger := gozerolog.New(&buf)
		rt := unolog.MustCompile(unolog.Config{Sink: New(&logger), SamplingRate: 1})
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
		op.End(&err)
		if payload := lastPayload(t, &buf); payload["level"] != "error" {
			t.Fatalf("level = %v", payload["level"])
		}
	})
}

func TestSinkTypedFieldsAndDedupe(t *testing.T) {
	var buf bytes.Buffer
	now := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	emit(t, &buf, func(ctx context.Context) {
		unolog.Add(ctx, "s", "v", "i", 7, "b", true, "t", now, "d", 1500*time.Millisecond)
		unolog.Add(ctx, "k", "first", "k", "second")
		unolog.Add(ctx, "meta", json.RawMessage(`{"raw":true}`))
	})

	payload := lastPayload(t, &buf)
	if payload["i"] != float64(7) || payload["b"] != true || payload["s"] != "v" {
		t.Fatalf("scalars = %v %v %v", payload["i"], payload["b"], payload["s"])
	}
	if payload["t"] != "2026-09-01T10:00:00Z" {
		t.Fatalf("t = %v", payload["t"])
	}
	if payload["d"] != 1500.0 { // zerolog Dur default: float ms
		t.Fatalf("d = %v", payload["d"])
	}
	if payload["k"] != "second" || strings.Count(buf.String(), `"k":`) != 1 {
		t.Fatalf("dedupe broken: k = %v", payload["k"])
	}
	if raw, ok := payload["meta"].(map[string]any); !ok || raw["raw"] != true {
		t.Fatalf("raw json = %v", payload["meta"])
	}
}

// TestSinkFloat32WireFidelity pins the 32-bit rendering (0.1, not the
// widened double digits).
func TestSinkFloat32WireFidelity(t *testing.T) {
	var buf bytes.Buffer
	emit(t, &buf, func(ctx context.Context) {
		unolog.Add(ctx, "f", float32(0.1))
	})
	payload := lastPayload(t, &buf)
	if payload["f"] != 0.1 {
		t.Fatalf("float32 = %v, want 0.1", payload["f"])
	}
	if !strings.Contains(buf.String(), `"f":0.1`) {
		t.Fatalf("wire shows widened digits: %s", buf.String())
	}
}

type redactedObject struct {
	Secret string
}

func (redactedObject) MarshalZerologObject(event *gozerolog.Event) {
	event.Str("token", "redacted")
}

func TestSinkPlainLoggerHonorsObjectMarshaller(t *testing.T) {
	var buf bytes.Buffer
	emit(t, &buf, func(ctx context.Context) {
		unolog.Add(ctx, "secret", redactedObject{Secret: "private"})
	})
	got, ok := lastPayload(t, &buf)["secret"].(map[string]any)
	if !ok || len(got) != 1 || got["token"] != "redacted" {
		t.Fatalf("secret = %v, want only the redacted token", got)
	}
}

func TestSinkWriteNilSafety(t *testing.T) {
	var nilSink *Sink
	nilSink.Write(context.Background(), nil)

	New(nil).Write(context.Background(), nil)

	var zl gozerolog.Logger // zero-value logger: no writer, nothing emits
	New(&zl).Write(context.Background(), nil)
}

// captureSink holds Write open until test cleanup so the record stays valid.
type captureSink struct {
	records chan *unolog.Record
	release chan struct{}
}

func (c *captureSink) Write(_ context.Context, rec *unolog.Record) {
	c.records <- rec
	<-c.release
}

func bridgeRecord(t *testing.T, mutate func(ctx context.Context)) *unolog.Record {
	t.Helper()
	cap := &captureSink{records: make(chan *unolog.Record), release: make(chan struct{})}
	rt := unolog.MustCompile(unolog.Config{Sink: cap, SamplingRate: 1})
	op := unolog.Start(t.Context(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "t"})
	if mutate != nil {
		mutate(op.Context())
	}
	ended := make(chan struct{})
	t.Cleanup(func() {
		close(cap.release)
		<-ended
	})
	go func() {
		defer close(ended)
		op.End(nil)
	}()
	return <-cap.records
}

// TestSinkDefaultRenderingMatchesCanonicalLine pins the default wire shape.
func TestSinkDefaultRenderingMatchesCanonicalLine(t *testing.T) {
	rec := bridgeRecord(t, func(ctx context.Context) {
		unolog.Add(ctx, "s", "v", "i", 7, "b", true, "d", 1500*time.Millisecond)
	})

	var buf bytes.Buffer
	logger := gozerolog.New(&buf)
	New(&logger).Write(context.Background(), rec)

	if got, want := buf.Bytes(), rec.Encoded(); !bytes.Equal(got, want) {
		t.Fatalf("bridge output differs from the canonical line:\ngot  %q\nwant %q", got, want)
	}
	var payload map[string]any
	if err := json.Unmarshal(bytes.TrimRight(buf.Bytes(), "\n"), &payload); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	for k, want := range map[string]any{"level": "info", "s": "v", "i": float64(7), "b": true, "d": float64(1500)} {
		if payload[k] != want {
			t.Fatalf("%s = %v, want %v", k, payload[k], want)
		}
	}
}

func TestCanonicalSinkServesCanonicalLine(t *testing.T) {
	rec := bridgeRecord(t, func(ctx context.Context) {
		unolog.Add(ctx, "s", "v", "i", 7, "b", true, "d", 1500*time.Millisecond)
	})
	var buf bytes.Buffer
	NewCanonical(&buf).Write(context.Background(), rec)
	if got, want := buf.Bytes(), rec.Encoded(); !bytes.Equal(got, want) {
		t.Fatalf("canonical output differs:\ngot  %q\nwant %q", got, want)
	}
}

// TestSinkRespectsLoggerLevel pins the native zerolog level gate.
func TestSinkRespectsLoggerLevel(t *testing.T) {
	info := bridgeRecord(t, nil) // success → info
	errRec := bridgeRecord(t, func(ctx context.Context) { unolog.SetLevel(ctx, unolog.LevelError) })

	var buf bytes.Buffer
	logger := gozerolog.New(&buf).Level(gozerolog.WarnLevel)
	sink := New(&logger)

	sink.Write(context.Background(), info)
	if buf.Len() != 0 {
		t.Fatalf("info record crossed a warn threshold: %q", buf.String())
	}
	sink.Write(context.Background(), errRec)
	if buf.Len() == 0 {
		t.Fatal("error record filtered by a warn threshold")
	}
	var payload map[string]any
	if err := json.Unmarshal(bytes.TrimRight(buf.Bytes(), "\n"), &payload); err != nil {
		t.Fatalf("not valid JSON: %v", err)
	}
	if payload["level"] != "error" {
		t.Fatalf("level = %v", payload["level"])
	}
}

// TestSinkDisabledLoggerEmitsNothing verifies the native disabled level.
func TestSinkDisabledLoggerEmitsNothing(t *testing.T) {
	rec := bridgeRecord(t, func(ctx context.Context) { unolog.SetLevel(ctx, unolog.LevelError) })
	var buf bytes.Buffer
	logger := gozerolog.New(&buf).Level(gozerolog.Disabled)
	New(&logger).Write(context.Background(), rec)
	if buf.Len() != 0 {
		t.Fatalf("disabled logger wrote %q", buf.String())
	}
}

// TestSinkZeroValueLoggerEmitsNothing: a zero-value *gozerolog.Logger has
// no writer; the sink must drop records without panicking.
func TestSinkZeroValueLoggerEmitsNothing(t *testing.T) {
	rec := bridgeRecord(t, nil)
	var logger gozerolog.Logger
	New(&logger).Write(context.Background(), rec) // no observable output: must not panic
}

// TestSinkEnrichedLoggerKeepsNativeAugmentation verifies that zerolog
// context fields, hooks, and samplers remain active.
func TestSinkEnrichedLoggerKeepsNativeAugmentation(t *testing.T) {
	rec := bridgeRecord(t, func(ctx context.Context) {
		unolog.Add(ctx, "user_id", "u_1")
	})

	t.Run("context_fields", func(t *testing.T) {
		var buf bytes.Buffer
		logger := gozerolog.New(&buf).With().Str("svc", "payments").Logger()
		New(&logger).Write(context.Background(), rec)
		payload := lastPayload(t, &buf)
		if payload["svc"] != "payments" {
			t.Fatalf("context field dropped: %v", payload)
		}
		if payload["user_id"] != "u_1" {
			t.Fatalf("record field missing: %v", payload)
		}
	})

	t.Run("sampler", func(t *testing.T) {
		var buf bytes.Buffer
		logger := gozerolog.New(&buf).Sample(&gozerolog.BasicSampler{N: 1}) // keeps everything
		New(&logger).Write(context.Background(), rec)
		if buf.Len() == 0 {
			t.Fatal("sampled logger dropped the record")
		}
	})

	t.Run("hook", func(t *testing.T) {
		var buf bytes.Buffer
		var ran bool
		logger := gozerolog.New(&buf).Hook(gozerolog.HookFunc(func(*gozerolog.Event, gozerolog.Level, string) {
			ran = true
		}))
		New(&logger).Write(context.Background(), rec)
		if !ran {
			t.Fatal("hook did not run")
		}
	})
}

// TestSinkCustomizedFieldNames verifies that the adapter uses zerolog's
// live public configuration.
func TestSinkCustomizedFieldNames(t *testing.T) {
	levelName, timeName, messageName := gozerolog.LevelFieldName, gozerolog.TimestampFieldName, gozerolog.MessageFieldName
	gozerolog.LevelFieldName, gozerolog.TimestampFieldName, gozerolog.MessageFieldName = "lvl", "ts", "msg"
	t.Cleanup(func() {
		gozerolog.LevelFieldName, gozerolog.TimestampFieldName, gozerolog.MessageFieldName = levelName, timeName, messageName
	})

	rec := bridgeRecord(t, func(ctx context.Context) { unolog.Add(ctx, "k", "v") })
	var buf bytes.Buffer
	logger := gozerolog.New(&buf)
	New(&logger).Write(context.Background(), rec)

	if buf.Len() == 0 {
		t.Fatal("record dropped")
	}
	payload := lastPayload(t, &buf)
	if payload["lvl"] != "info" {
		t.Fatalf("lvl = %v, want the customized level member", payload["lvl"])
	}
	if payload["msg"] != rec.Message() {
		t.Fatalf("msg = %v, want the customized message member", payload["msg"])
	}
	if _, ok := payload["ts"]; !ok {
		t.Fatalf("ts missing: customized TimestampFieldName was ignored: %v", payload)
	}
	if _, ok := payload["level"]; ok {
		t.Fatalf("canonical %q member leaked onto a customized pipeline: %v", "level", payload)
	}
	if _, ok := payload["time"]; ok {
		t.Fatalf("canonical %q member leaked onto a customized pipeline: %v", "time", payload)
	}
	if _, ok := payload["message"]; ok {
		t.Fatalf("canonical %q member leaked onto a customized pipeline: %v", "message", payload)
	}
	if payload["k"] != "v" {
		t.Fatalf("record field missing: %v", payload)
	}
}

// TestSinkCustomizedRendering verifies every supported zerolog rendering
// global through its public typed constructors.
func TestSinkCustomizedRendering(t *testing.T) {
	t.Run("time format", func(t *testing.T) {
		old := gozerolog.TimeFieldFormat
		gozerolog.TimeFieldFormat = gozerolog.TimeFormatUnix
		t.Cleanup(func() { gozerolog.TimeFieldFormat = old })

		rec := bridgeRecord(t, func(ctx context.Context) { unolog.Add(ctx, "k", "v") })
		var buf bytes.Buffer
		logger := gozerolog.New(&buf)
		New(&logger).Write(context.Background(), rec)

		payload := lastPayload(t, &buf)
		if _, ok := payload["time"].(float64); !ok {
			t.Fatalf("time = %v (%T), want the Unix number from TimeFieldFormat", payload["time"], payload["time"])
		}
	})

	t.Run("level marshaller", func(t *testing.T) {
		old := gozerolog.LevelFieldMarshalFunc
		gozerolog.LevelFieldMarshalFunc = func(l gozerolog.Level) string { return strings.ToUpper(l.String()) }
		t.Cleanup(func() { gozerolog.LevelFieldMarshalFunc = old })

		rec := bridgeRecord(t, func(ctx context.Context) { unolog.Add(ctx, "k", "v") })
		var buf bytes.Buffer
		logger := gozerolog.New(&buf)
		New(&logger).Write(context.Background(), rec)

		payload := lastPayload(t, &buf)
		if payload["level"] != "INFO" {
			t.Fatalf("level = %v, want the marshalled INFO", payload["level"])
		}
	})

	t.Run("level value", func(t *testing.T) {
		// The default LevelFieldMarshalFunc calls Level.String, which
		// reads the exported Level*Value vars: a customized value must
		// not slip past the gate.
		old := gozerolog.LevelInfoValue
		gozerolog.LevelInfoValue = "informational"
		t.Cleanup(func() { gozerolog.LevelInfoValue = old })

		rec := bridgeRecord(t, func(ctx context.Context) { unolog.Add(ctx, "k", "v") })
		var buf bytes.Buffer
		logger := gozerolog.New(&buf)
		New(&logger).Write(context.Background(), rec)

		payload := lastPayload(t, &buf)
		if payload["level"] != "informational" {
			t.Fatalf("level = %v, want the customized LevelInfoValue", payload["level"])
		}
	})

	t.Run("duration unit", func(t *testing.T) {
		oldUnit, oldInt := gozerolog.DurationFieldUnit, gozerolog.DurationFieldInteger
		gozerolog.DurationFieldUnit, gozerolog.DurationFieldInteger = time.Second, true
		t.Cleanup(func() { gozerolog.DurationFieldUnit, gozerolog.DurationFieldInteger = oldUnit, oldInt })

		rec := bridgeRecord(t, func(ctx context.Context) { unolog.Add(ctx, "d", 2500*time.Millisecond) })
		var buf bytes.Buffer
		logger := gozerolog.New(&buf)
		New(&logger).Write(context.Background(), rec)

		payload := lastPayload(t, &buf)
		if payload["d"] != float64(2) {
			t.Fatalf("d = %v, want 2 seconds from the customized unit", payload["d"])
		}
	})

	t.Run("duration format", func(t *testing.T) {
		old := gozerolog.DurationFieldFormat
		gozerolog.DurationFieldFormat = gozerolog.DurationFormatString
		t.Cleanup(func() { gozerolog.DurationFieldFormat = old })

		var buf bytes.Buffer
		emit(t, &buf, func(ctx context.Context) { unolog.Add(ctx, "d", 2500*time.Millisecond) })
		if got := lastPayload(t, &buf)["d"]; got != "2.5s" {
			t.Fatalf("d = %v, want the duration string 2.5s", got)
		}
	})

	t.Run("float precision", func(t *testing.T) {
		old := gozerolog.FloatingPointPrecision
		gozerolog.FloatingPointPrecision = 2
		t.Cleanup(func() { gozerolog.FloatingPointPrecision = old })

		var buf bytes.Buffer
		emit(t, &buf, func(ctx context.Context) {
			unolog.Add(ctx, "f32", float32(1.2345), "f64", 1.2345, "d", 1234567*time.Nanosecond)
		})
		payload := lastPayload(t, &buf)
		for _, key := range []string{"f32", "f64", "d"} {
			if got := payload[key]; got != 1.23 {
				t.Errorf("%s = %v, want 1.23", key, got)
			}
		}
	})

	t.Run("interface marshaller", func(t *testing.T) {
		old := gozerolog.InterfaceMarshalFunc
		gozerolog.InterfaceMarshalFunc = func(any) ([]byte, error) { return []byte(`"redacted"`), nil }
		t.Cleanup(func() { gozerolog.InterfaceMarshalFunc = old })

		var buf bytes.Buffer
		emit(t, &buf, func(ctx context.Context) {
			unolog.Add(ctx, "secret", map[string]string{"token": "private"})
		})
		if got := lastPayload(t, &buf)["secret"]; got != "redacted" {
			t.Fatalf("secret = %v, want redacted", got)
		}
	})

	t.Run("timestamp function", func(t *testing.T) {
		old := gozerolog.TimestampFunc
		gozerolog.TimestampFunc = func() time.Time { return time.Date(1999, 1, 1, 0, 0, 0, 0, time.UTC) }
		t.Cleanup(func() { gozerolog.TimestampFunc = old })

		rec := bridgeRecord(t, func(ctx context.Context) { unolog.Add(ctx, "k", "v") })
		var buf bytes.Buffer
		logger := gozerolog.New(&buf).With().Timestamp().Logger()
		NewWithLoggerTimestamp(&logger).Write(context.Background(), rec)

		payload := lastPayload(t, &buf)
		if payload["time"] != "1999-01-01T00:00:00Z" {
			t.Fatalf("time = %v, want the customized TimestampFunc value", payload["time"])
		}
	})
}

// TestSinkStampsRecordCompletionTime verifies that New uses the record's
// completion time rather than a fresh write-time read.
func TestSinkStampsRecordCompletionTime(t *testing.T) {
	rec := bridgeRecord(t, func(ctx context.Context) { unolog.Add(ctx, "k", "v") })

	var buf bytes.Buffer
	logger := gozerolog.New(&buf).With().Str("svc", "payments").Logger()
	New(&logger).Write(context.Background(), rec)

	payload := lastPayload(t, &buf)
	s, ok := payload["time"].(string)
	if !ok {
		t.Fatalf("time = %v, want RFC3339 string", payload["time"])
	}
	got, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatalf("time %q not RFC3339: %v", s, err)
	}
	// Both the canonical line and zerolog's TimeFieldFormat render
	// RFC3339 seconds precision, so compare at that granularity.
	if want := rec.Time().Truncate(time.Second); !got.Equal(want) {
		t.Fatalf("adapter stamped %v, want the record's completion time %v", got, want)
	}
}

// TestSinkAliasesEnvelopeKeys pins Encoded() parity: user keys that collide with the envelope are renamed to
// fields.* so encoding/json last-wins cannot drop them.
func TestSinkAliasesEnvelopeKeys(t *testing.T) {
	rec := bridgeRecord(t, func(ctx context.Context) {
		unolog.Add(ctx, "time", "user-time", "message", "user-msg", "level", "user-level")
	})

	var buf bytes.Buffer
	logger := gozerolog.New(&buf).With().Str("svc", "payments").Logger()
	New(&logger).Write(context.Background(), rec)

	payload := lastPayload(t, &buf)
	if payload["fields.time"] != "user-time" {
		t.Fatalf("fields.time = %v, user time was dropped or not aliased", payload["fields.time"])
	}
	if payload["fields.message"] != "user-msg" {
		t.Fatalf("fields.message = %v", payload["fields.message"])
	}
	if payload["fields.level"] != "user-level" {
		t.Fatalf("fields.level = %v", payload["fields.level"])
	}
	if _, ok := payload["time"].(string); !ok {
		t.Fatalf("envelope time missing: %v", payload["time"])
	}
	if payload["message"] != rec.Message() {
		t.Fatalf("envelope message = %v", payload["message"])
	}
}

func TestSinkConcurrentWrites(t *testing.T) {
	var mu sync.Mutex
	var buf bytes.Buffer
	logger := gozerolog.New(lockedWriter{mu: &mu, buf: &buf})
	rt := unolog.MustCompile(unolog.Config{Sink: New(&logger), SamplingRate: 1})

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

	mu.Lock()
	defer mu.Unlock()
	lines := strings.Split(strings.TrimRight(buf.String(), "\n"), "\n")
	if len(lines) != 800 {
		t.Fatalf("lines = %d, want 800", len(lines))
	}
	for _, ln := range lines {
		var payload map[string]any
		if err := json.Unmarshal([]byte(ln), &payload); err != nil {
			t.Fatalf("corrupt line: %q", ln)
		}
	}
}

type lockedWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

type errBoom struct{}

func (errBoom) Error() string { return "boom" }

// TestSinkTimestampHookDoesNotDuplicateTime pins the explicit constructor
// for a logger configured with .With().Timestamp().
func TestSinkTimestampHookDoesNotDuplicateTime(t *testing.T) {
	rec := bridgeRecord(t, func(ctx context.Context) {
		unolog.Add(ctx, "k", "v")
	})

	t.Run("plain", func(t *testing.T) {
		var buf bytes.Buffer
		logger := gozerolog.New(&buf).With().Timestamp().Logger()
		NewWithLoggerTimestamp(&logger).Write(context.Background(), rec)
		line := bytes.TrimRight(buf.Bytes(), "\n")
		if n := bytes.Count(line, []byte(`"time":`)); n != 1 {
			t.Fatalf("time members = %d, want 1: %s", n, line)
		}
	})

	t.Run("context fields", func(t *testing.T) {
		var buf bytes.Buffer
		logger := gozerolog.New(&buf).With().Timestamp().Str("svc", "payments").Logger()
		NewWithLoggerTimestamp(&logger).Write(context.Background(), rec)
		line := bytes.TrimRight(buf.Bytes(), "\n")
		if n := bytes.Count(line, []byte(`"time":`)); n != 1 {
			t.Fatalf("time members = %d, want 1: %s", n, line)
		}
		payload := lastPayload(t, &buf)
		if payload["svc"] != "payments" {
			t.Fatalf("context field missing: %v", payload["svc"])
		}
	})
}

// Bridge robustness tests: nil/garbage abuse and typed-nil error
// containment.

func crashRecord(t *testing.T) *unolog.Record {
	t.Helper()
	return bridgeRecord(t, func(ctx context.Context) { unolog.Add(ctx, "k", "v") })
}

func TestCrashNilAbuse(t *testing.T) {
	rec := crashRecord(t)
	New(nil).Write(context.Background(), rec)
	New(nil).Write(context.Background(), nil)
	var nilSink *Sink
	nilSink.Write(context.Background(), rec)

	disabled := gozerolog.New(nil).Level(gozerolog.Disabled)
	New(&disabled).Write(context.Background(), rec)

	ts := gozerolog.New(nil).With().Timestamp().Str("svc", "x").Logger()
	NewWithLoggerTimestamp(&ts).Write(context.Background(), rec)

	sampled := ts.Sample(&gozerolog.BurstSampler{Burst: 1, Period: 1e9})
	NewWithLoggerTimestamp(&sampled).Write(context.Background(), rec)
}

func TestCrashTypedNilErrorField(t *testing.T) {
	var pe *os.PathError
	var buf bytes.Buffer
	emit(t, &buf, func(ctx context.Context) {
		unolog.Add(ctx, "e", pe)
		unolog.Error(ctx, pe)
	})
	if !strings.Contains(buf.String(), `"<nil>"`) {
		t.Fatalf("typed-nil error not rendered as <nil>: %s", buf.String())
	}
}

// goleak integration: every test in this module runs under
// goleak.VerifyTestMain, failing the suite on any leaked goroutine.
// goleak is test-only: nothing outside _test.go imports it.

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
