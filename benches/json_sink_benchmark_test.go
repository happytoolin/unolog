//go:build go1.27

package benches_test

import (
	"context"
	"encoding/json"
	"encoding/json/jsontext"
	"io"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/happytoolin/unolog"
)

// BenchmarkJSONSink measures the sink write path with pre-encoded
// records (Encoded() cached after the first call — the loop measures
// Write of the canonical line).
func BenchmarkJSONSink(b *testing.B) {
	sink := unolog.NewJSONSink(io.Discard)
	b.Run("write_12_fields", func(b *testing.B) {
		withRecord(12, func(rec *unolog.Record) {
			_ = rec.Encoded()
			b.ReportAllocs()
			for b.Loop() {
				sink.Write(context.Background(), rec)
			}
		})
	})
}

// BenchmarkJSONSinkLifecycle is the honest end-to-end sink gate:
// Start + 12 fields + End through NewJSONSink (encode once + single
// Write). The §4 sink gate (≤ 400 ns / ≤ 2 allocs) was stated for the
// v0.6 sink-write shape; this is the v2 lifecycle inclusive of it.
func BenchmarkJSONSinkLifecycle(b *testing.B) {
	rt := unolog.MustCompile(unolog.Config{Sink: unolog.NewJSONSink(io.Discard), SamplingRate: 1})
	b.Run("12_fields", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
			benchmarkFields(op.Context(), 12)
			op.End(nil)
		}
	})
	b.Run("0_fields", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			op := unolog.Start(context.Background(), rt, unolog.OperationStart{})
			op.End(nil)
		}
	})
}

// BenchmarkJSONSinkEscaping isolates escape-scan cost at the sink level.
func BenchmarkJSONSinkEscaping(b *testing.B) {
	rt := unolog.MustCompile(unolog.Config{Sink: unolog.NewJSONSink(io.Discard), SamplingRate: 1})
	b.Run("escape_heavy", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			op := unolog.Start(context.Background(), rt, unolog.OperationStart{})
			ctx := op.Context()
			unolog.Add(ctx, "url", "/search?q="+strings.Repeat("héllo☃", 8))
			unolog.Add(ctx, "agent", `Mozilla/5.0 (X11; "quote" back\slash) Engine/1.0`)
			op.End(nil)
		}
	})
}

// jsontextEncode builds the same canonical line the first-party encoder
// produces, but with Go 1.27's encoding/json/jsontext appends — the
// maintenance-free comparator backend kept in benches/ per amendment 10,
// so the fork can be re-raced against the stdlib per Go release.
func jsontextEncode(rec *unolog.Record) []byte {
	dst := make([]byte, 0, 512)
	dst = append(dst, `{"level":"`...)
	dst = append(dst, jsonLevelText(rec.Level())...)
	dst = append(dst, '"')

	fields := rec.Fields()
	if len(fields) <= 24 { // mirror the encoder's allocation-free narrow path
		for i := range fields {
			f := fields[i]
			last := true
			for j := i + 1; j < len(fields); j++ {
				if fields[j].Key() == f.Key() {
					last = false
					break
				}
			}
			if last {
				dst = append(dst, ',')
				dst, _ = jsontext.AppendQuote(dst, f.Key())
				dst = append(dst, ':')
				dst = appendJSONTextField(dst, f)
			}
		}
	} else {
		seen := map[string]struct{}{}
		kept := make([]int, 0, 16)
		for i, field := range slices.Backward(fields) {
			if _, dup := seen[field.Key()]; dup {
				continue
			}
			seen[field.Key()] = struct{}{}
			kept = append(kept, i)
		}
		for _, k := range slices.Backward(kept) {
			f := fields[k]
			dst = append(dst, ',')
			dst, _ = jsontext.AppendQuote(dst, f.Key())
			dst = append(dst, ':')
			dst = appendJSONTextField(dst, f)
		}
	}

	dst = append(dst, ',')
	dst, _ = jsontext.AppendQuote(dst, "time")
	dst = append(dst, ':', '"')
	dst = rec.Time().AppendFormat(dst, time.RFC3339)
	dst = append(dst, '"', ',')
	dst, _ = jsontext.AppendQuote(dst, "message")
	dst = append(dst, ':')
	dst, _ = jsontext.AppendQuote(dst, rec.Message())
	dst = append(dst, '}', '\n')
	return dst
}

// appendJSONTextField covers the canonical field shapes (strings, ints,
// bools, durations) — the bench corpus subset, not the full type set.
func appendJSONTextField(dst []byte, f unolog.Field) []byte {
	if s, ok := f.Str(); ok {
		dst, _ = jsontext.AppendQuote(dst, s)
		return dst
	}
	if i, ok := f.Int(); ok {
		return strconv.AppendInt(dst, i, 10)
	}
	if b, ok := f.Bool(); ok {
		return strconv.AppendBool(dst, b)
	}
	if d, ok := f.Duration(); ok {
		return jsontext.AppendFloat(dst, float64(d)/float64(time.Millisecond), 64)
	}
	blob, err := json.Marshal(f.Any())
	if err != nil {
		dst, _ = jsontext.AppendQuote(dst, err.Error())
		return dst
	}
	return append(dst, blob...)
}

func jsonLevelText(level unolog.Level) string {
	switch level {
	case unolog.LevelDebug:
		return "debug"
	case unolog.LevelWarn:
		return "warn"
	case unolog.LevelError:
		return "error"
	default:
		return "info"
	}
}

type jsontextSink struct{ w io.Writer }

func (s *jsontextSink) Write(_ context.Context, rec *unolog.Record) {
	_, _ = s.w.Write(jsontextEncode(rec))
}

// BenchmarkJSONSinkJsontextComparator races the jsontext backend against
// the first-party encoder on the same event shape.
var rtFork = unolog.MustCompile(unolog.Config{Sink: unolog.NewJSONSink(io.Discard), SamplingRate: 1})

func BenchmarkJSONSinkJsontextComparator(b *testing.B) {
	js := &jsontextSink{w: io.Discard}
	fork := unolog.NewJSONSink(io.Discard)

	b.Run("jsontext_12_fields", func(b *testing.B) {
		withRecord(12, func(rec *unolog.Record) {
			b.ReportAllocs()
			for b.Loop() {
				js.Write(context.Background(), rec)
			}
		})
	})
	b.Run("fork_12_fields_preencoded", func(b *testing.B) {
		withRecord(12, func(rec *unolog.Record) {
			_ = rec.Encoded()
			b.ReportAllocs()
			for b.Loop() {
				fork.Write(context.Background(), rec)
			}
		})
	})
	b.Run("fork_12_fields_full_encode", func(b *testing.B) {
		// fresh records each iteration: measures encode-once + write
		b.ReportAllocs()
		for b.Loop() {
			op := unolog.Start(context.Background(), rtFork, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
			benchmarkFields(op.Context(), 12)
			op.End(nil)
		}
	})
}

func TestJSONTextComparatorMatchesCanonicalLine(t *testing.T) {
	for _, fields := range []int{12, 32} {
		withRecord(fields, func(rec *unolog.Record) {
			var got, want map[string]any
			if err := json.Unmarshal(jsontextEncode(rec), &got); err != nil {
				t.Fatalf("jsontext with %d fields: %v", fields, err)
			}
			if err := json.Unmarshal(rec.Encoded(), &want); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("jsontext with %d fields = %v, want %v", fields, got, want)
			}
		})
	}
}
