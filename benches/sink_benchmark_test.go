//go:build go1.27

package benches_test

import (
	"context"
	"os"
	"testing"

	"github.com/happytoolin/unolog"
)

// BenchmarkSinkRealFile is the production-shaped sink gate: an *os.File
// target, so every unbuffered Write is a real syscall. Buffering is the
// difference between one syscall per request and one per buffer.
func BenchmarkSinkRealFile(b *testing.B) {
	cases := []struct {
		name string
		sink func(*os.File) (unolog.Sink, func())
	}{
		{"unbuffered", func(f *os.File) (unolog.Sink, func()) { return unolog.NewJSONSink(f), func() {} }},
		{"buffered_4k", func(f *os.File) (unolog.Sink, func()) {
			s := unolog.NewBufferedJSONSink(f, 4096)
			return s, func() { _ = s.Flush() }
		}},
		{"buffered_64k", func(f *os.File) (unolog.Sink, func()) {
			s := unolog.NewBufferedJSONSink(f, 65536)
			return s, func() { _ = s.Flush() }
		}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			f, err := os.CreateTemp(b.TempDir(), "unolog-*.log")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = f.Close() })
			sink, flush := tc.sink(f)
			rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})

			b.SetBytes(260) // canonical 12-field line is ~260 bytes
			b.ReportAllocs()
			for b.Loop() {
				op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
				benchmarkFields(op.Context(), 12)
				op.End(nil)
			}
			flush()
		})
	}
}

// BenchmarkSinkRealFileParallel is the same gate under concurrent
// request goroutines: the sink mutex serializes the actual Write.
func BenchmarkSinkRealFileParallel(b *testing.B) {
	cases := []struct {
		name string
		sink func(*os.File) (unolog.Sink, func())
	}{
		{"unbuffered", func(f *os.File) (unolog.Sink, func()) { return unolog.NewJSONSink(f), func() {} }},
		{"buffered_64k", func(f *os.File) (unolog.Sink, func()) {
			s := unolog.NewBufferedJSONSink(f, 65536)
			return s, func() { _ = s.Flush() }
		}},
	}
	for _, tc := range cases {
		b.Run(tc.name, func(b *testing.B) {
			f, err := os.CreateTemp(b.TempDir(), "unolog-*.log")
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = f.Close() })
			sink, flush := tc.sink(f)
			rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})

			b.SetBytes(260)
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
					benchmarkFields(op.Context(), 12)
					op.End(nil)
				}
			})
			flush()
		})
	}
}
