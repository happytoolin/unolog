package zap

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/happytoolin/unolog"
	gozap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// FuzzSinkWritesValidJSON: for any keys and values, the bridge must not
// panic, and zap's JSON encoder must write one valid line in which the
// last write of each key wins.
func FuzzSinkWritesValidJSON(f *testing.F) {
	f.Add("s", "v", int64(7), 1.5)
	f.Add("\x00\"", " \xff\\", int64(math.MinInt64), math.Inf(1))
	f.Add("", "é😀", int64(-1), math.NaN())
	f.Fuzz(func(t *testing.T, key, value string, i int64, fl float64) {
		var buf bytes.Buffer
		core := zapcore.NewCore(zapcore.NewJSONEncoder(gozap.NewProductionEncoderConfig()), zapcore.AddSync(&buf), zapcore.DebugLevel)
		rt := unolog.MustCompile(unolog.Config{Sink: New(gozap.New(core)), SamplingRate: 1})
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "fuzz"})
		ctx := op.Context()
		// The "u." prefix keeps fuzzed keys off zap's own level/ts/msg.
		unolog.Add(ctx, "u."+key, "first", "u.i", i, "u.f", fl)
		unolog.AddFields(ctx, unolog.String("u."+key, value))
		op.End(nil)

		line := buf.Bytes()
		if bytes.Count(line, []byte("\n")) != 1 || !json.Valid(line) {
			t.Fatalf("not one valid JSON line: %q", line)
		}
		var payload map[string]any
		if err := json.Unmarshal(line, &payload); err != nil {
			t.Fatal(err)
		}
		if got, want := payload["u."+sanitize(key)], sanitize(value); got != want {
			t.Fatalf("last write lost: got %q, want %q", got, want)
		}
	})
}

// sanitize is the JSON round trip of s: each invalid UTF-8 byte becomes
// U+FFFD, as the encoders write it.
func sanitize(s string) string {
	var b strings.Builder
	for _, r := range s {
		b.WriteRune(r)
	}
	return b.String()
}
