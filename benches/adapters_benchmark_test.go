package benches_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/happytoolin/unolog"
	uslog "github.com/happytoolin/unolog/adapter/slog"
	uzap "github.com/happytoolin/unolog/adapter/zap"
	uzerolog "github.com/happytoolin/unolog/adapter/zerolog"
	"github.com/rs/zerolog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// adapterEvent runs one 12-field lifecycle through the sink under test.
func adapterEvent(ctx context.Context, rt *unolog.Runtime) {
	op := unolog.Start(ctx, rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
	benchmarkFields(op.Context(), 12)
	op.End(nil)
}

// withRecord runs use while the sink owns a valid record. Retaining a
// record after Write returns would let pool reuse corrupt benchmark inputs.
type recordSinkFunc func(*unolog.Record)

func (f recordSinkFunc) Write(_ context.Context, rec *unolog.Record) { f(rec) }

func withRecord(fields int, use func(*unolog.Record)) {
	rt := unolog.MustCompile(unolog.Config{Sink: recordSinkFunc(use), SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
	benchmarkFields(op.Context(), fields)
	op.End(nil)
}

func withAnyRecord(use func(*unolog.Record)) {
	rt := unolog.MustCompile(unolog.Config{Sink: recordSinkFunc(use), SamplingRate: 1})
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "job"})
	unolog.Add(op.Context(), "payload", map[string]any{"id": 7, "valid": true})
	op.End(nil)
}

func BenchmarkHostFloors(b *testing.B) {
	b.Run("slog_json_12_fields", func(b *testing.B) {
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		ctx := context.Background()
		b.ReportAllocs()
		for b.Loop() {
			logger.LogAttrs(ctx, slog.LevelInfo, "request_completed",
				slog.String("http.method", "GET"),
				slog.String("http.path", "/api/v1/orders/12345"),
				slog.String("http.route", "/api/v1/orders/:id"),
				slog.Int64("http.status", 200),
				slog.String("op.domain", "http"),
				slog.String("op.name", "GET /api/v1/orders/:id"),
				slog.String("op.outcome", "success"),
				slog.Int64("op.code", 200),
				slog.Int64("duration_ms", 12),
				slog.String("request_id", "req_01HZX4T7W8Y3N2M1K0J9Z8X7V6"),
				slog.String("user_id", "usr_77451"),
				slog.Bool("cache.hit", true))
		}
	})
	b.Run("zap_json_12_fields", func(b *testing.B) {
		core := zapcore.NewCore(
			zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
			zapcore.AddSync(io.Discard),
			zapcore.DebugLevel,
		)
		logger := zap.New(core)
		b.ReportAllocs()
		for b.Loop() {
			logger.Info("request_completed",
				zap.String("http.method", "GET"),
				zap.String("http.path", "/api/v1/orders/12345"),
				zap.String("http.route", "/api/v1/orders/:id"),
				zap.Int64("http.status", 200),
				zap.String("op.domain", "http"),
				zap.String("op.name", "GET /api/v1/orders/:id"),
				zap.String("op.outcome", "success"),
				zap.Int64("op.code", 200),
				zap.Int64("duration_ms", 12),
				zap.String("request_id", "req_01HZX4T7W8Y3N2M1K0J9Z8X7V6"),
				zap.String("user_id", "usr_77451"),
				zap.Bool("cache.hit", true))
		}
	})
	b.Run("zerolog_12_fields", func(b *testing.B) {
		logger := zerolog.New(io.Discard)
		b.ReportAllocs()
		for b.Loop() {
			logger.Info().
				Str("http.method", "GET").
				Str("http.path", "/api/v1/orders/12345").
				Str("http.route", "/api/v1/orders/:id").
				Int64("http.status", 200).
				Str("op.domain", "http").
				Str("op.name", "GET /api/v1/orders/:id").
				Str("op.outcome", "success").
				Int64("op.code", 200).
				Int64("duration_ms", 12).
				Str("request_id", "req_01HZX4T7W8Y3N2M1K0J9Z8X7V6").
				Str("user_id", "usr_77451").
				Bool("cache.hit", true).
				Msg("request_completed")
		}
	})
}

func BenchmarkAdapterSlog(b *testing.B) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	sink := uslog.New(logger)
	rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
	ctx := context.Background()

	b.Run("bridge_only_12_fields", func(b *testing.B) {
		withRecord(12, func(rec *unolog.Record) {
			_ = rec.Encoded() // bridge-only rows exclude canonical encoding
			b.ReportAllocs()
			for b.Loop() {
				sink.Write(ctx, rec)
			}
		})
	})

	b.Run("write_12_fields", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			adapterEvent(ctx, rt)
		}
	})
	b.Run("write_empty", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			unolog.Start(ctx, rt, unolog.OperationStart{}).End(nil)
		}
	})
}

func BenchmarkAdapterZap(b *testing.B) {
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(io.Discard),
		zapcore.DebugLevel,
	)
	sink := uzap.New(zap.New(core))
	rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
	ctx := context.Background()

	b.Run("bridge_only_12_fields", func(b *testing.B) {
		withRecord(12, func(rec *unolog.Record) {
			_ = rec.Encoded() // bridge-only rows exclude canonical encoding
			b.ReportAllocs()
			for b.Loop() {
				sink.Write(ctx, rec)
			}
		})
	})

	b.Run("write_12_fields", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			adapterEvent(ctx, rt)
		}
	})
	b.Run("write_empty", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			unolog.Start(ctx, rt, unolog.OperationStart{}).End(nil)
		}
	})
}

func BenchmarkAdapterZerolog(b *testing.B) {
	logger := zerolog.New(io.Discard)
	sink := uzerolog.New(&logger)
	canonicalSink := uzerolog.NewCanonical(io.Discard)
	rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
	canonicalRT := unolog.MustCompile(unolog.Config{Sink: canonicalSink, SamplingRate: 1})
	ctx := context.Background()

	b.Run("bridge_only_12_fields", func(b *testing.B) {
		withRecord(12, func(rec *unolog.Record) {
			_ = rec.Encoded() // bridge-only rows exclude canonical encoding
			b.ReportAllocs()
			for b.Loop() {
				sink.Write(ctx, rec)
			}
		})
	})
	b.Run("bridge_only_any", func(b *testing.B) {
		withAnyRecord(func(rec *unolog.Record) {
			b.ReportAllocs()
			for b.Loop() {
				sink.Write(ctx, rec)
			}
		})
	})
	b.Run("bridge_only_canonical_12_fields", func(b *testing.B) {
		withRecord(12, func(rec *unolog.Record) {
			_ = rec.Encoded()
			b.ReportAllocs()
			for b.Loop() {
				canonicalSink.Write(ctx, rec)
			}
		})
	})

	b.Run("write_12_fields", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			adapterEvent(ctx, rt)
		}
	})
	b.Run("write_canonical_12_fields", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			adapterEvent(ctx, canonicalRT)
		}
	})
	b.Run("write_empty", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			unolog.Start(ctx, rt, unolog.OperationStart{}).End(nil)
		}
	})
}
