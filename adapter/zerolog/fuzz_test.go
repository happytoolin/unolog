package zerolog

import (
	"bytes"
	"context"
	"math"
	"testing"

	"github.com/happytoolin/unolog"
	gozerolog "github.com/rs/zerolog"
)

// paritySink renders each record through the zerolog field API and
// compares it with the canonical line while the record is valid.
type paritySink struct {
	got, want []byte
}

func (s *paritySink) Write(ctx context.Context, rec *unolog.Record) {
	var buf bytes.Buffer
	logger := gozerolog.New(&buf)
	New(&logger).Write(ctx, rec)
	s.got = buf.Bytes()
	s.want = bytes.Clone(rec.Encoded())
}

// FuzzSinkMatchesCanonicalLine: for any keys and values, the zerolog
// field-API bridge must write exactly the canonical line, the contract
// TestSinkDefaultRenderingMatchesCanonicalLine pins for fixed inputs.
// It covers escaping, envelope-key aliasing, duplicate resolution, and
// float rendering in one property.
func FuzzSinkMatchesCanonicalLine(f *testing.F) {
	f.Add("s", "v", int64(7), 1.5, true)
	f.Add("message", "\x00 \xff\"\\", int64(math.MinInt64), math.Inf(-1), false)
	f.Add("level", "", int64(0), math.NaN(), true)
	f.Add("time", "é😀", int64(-1), 1e-7, false)
	f.Fuzz(func(t *testing.T, key, value string, i int64, fl float64, b bool) {
		sink := &paritySink{}
		rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "fuzz"})
		ctx := op.Context()
		unolog.Add(ctx, key, value, key+".i", i, "f", fl, "f32", float32(fl), "b", b)
		unolog.AddFields(ctx, unolog.String(key, value+"-last"), unolog.Uint64("u", uint64(i)))
		op.End(nil)
		if !bytes.Equal(sink.got, sink.want) {
			t.Fatalf("bridge line differs from the canonical line:\ngot  %q\nwant %q", sink.got, sink.want)
		}
	})
}
