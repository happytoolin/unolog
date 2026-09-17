package unolog

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"
)

// Benchmarks for the sampling gate and the record encode/write path —
// the repo-wide root-module benchmark file (the cross-adapter and host
// comparisons live in the benches module).

// These benches measure the gate segments that the external benches
// cannot isolate (they run inside package unolog with unexported access).

// BenchmarkEndDropPath times the complete field-less End on pre-built
// operations — claim, recover, clock read, seal, scan, post-seal
// annotations, commit, and release. The §4 100 ns gate names only the
// sampler + release + pool segment of this path; that segment is
// measured separately by the WAL micro-benchmarks (RateSampler ≈ 5 ns
// plus EventReleaseRecycle ≈ 38 ns).
func BenchmarkEndDropPath(b *testing.B) {
	rt := MustCompile(Config{Sink: dropCountSink{}, SamplingRate: 0})
	const pre = 4096
	ops := make([]*Operation, pre)
	rebuild := func() {
		for i := range ops {
			ops[i] = Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "cleanup"})
		}
	}
	rebuild()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if n&(pre-1) == 0 && n > 0 {
			b.StopTimer()
			rebuild()
			b.StartTimer()
		}
		ops[n&(pre-1)].End(nil)
	}
}

// BenchmarkEndDropPathCustomSampler is the same segment with a custom
// sampler (the amendment-4 path: one closure call before the drop).
func BenchmarkEndDropPathCustomSampler(b *testing.B) {
	rt := MustCompile(Config{Sink: dropCountSink{}, Sampler: func(SampleInput) bool { return false }})
	const pre = 4096
	ops := make([]*Operation, pre)
	rebuild := func() {
		for i := range ops {
			ops[i] = Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "cleanup"})
		}
	}
	rebuild()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if n&(pre-1) == 0 && n > 0 {
			b.StopTimer()
			rebuild()
			b.StartTimer()
		}
		ops[n&(pre-1)].End(nil)
	}
}

type dropCountSink struct{}

func (dropCountSink) Write(context.Context, *Record) {}

// BenchmarkRecordEncodeWrite12 is the fresh-record sink gate shape:
// encode a 12-field record once and write it (the lifecycle itself is
// benched separately in benches).
func BenchmarkRecordEncodeWrite12(b *testing.B) {
	fields := make([]Field, 12)
	keys := []string{"http.method", "http.path", "http.route", "http.status", "op.domain", "op.name", "op.outcome", "op.code", "duration_ms", "request_id", "user_id", "cache.hit"}
	for i, k := range keys {
		fields[i] = fieldStr(k, "v")
	}
	completedAt := time.Now()
	b.ReportAllocs()
	for b.Loop() {
		rec := &Record{level: LevelInfo, msg: DefaultMessage, fields: fields, completedAt: completedAt}
		_, _ = io.Discard.Write(rec.Encoded())
	}
}

// Benchmarks for the WAL (event) state machine itself — the guarded
// append path, sealing, straggler no-ops, the watchdog snapshot, the
// setters, and pool recycling. The lifecycle-level gates live in
// bench_test.go and the benches module; these isolate the wal.go
// primitives so the watchdog protocol has a perf record.

// benchPre is the round-robin pool size for the rebuild pattern (same
// shape as BenchmarkEndDropPath): mutations that grow fields or flip
// one-shot state run against pre-built events, rebuilt with the timer
// stopped, so the timed region is only the operation under test.
const benchPre = 4096

func benchField() Field { return fieldStr("user_id", "u_8472") }

// BenchmarkEventAppendGuarded is the append hot path: one atomic load,
// one mutex round trip, one slice append. Every open event is guarded,
// so this is the per-field cost the request pays.
func BenchmarkEventAppendGuarded(b *testing.B) {
	ev := newEvent()
	ev.fields = make([]Field, 0, benchPre)
	gen := ev.state.Load() >> walStateBits
	f := benchField()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if n&(benchPre-1) == 0 && n > 0 {
			b.StopTimer()
			ev.fields = ev.fields[:0]
			b.StartTimer()
		}
		ev.append(gen, f)
	}
	ev.release()
}

// BenchmarkEventAppendStaleGen is the straggler no-op for a recycled
// event: generation mismatch, one load and return.
func BenchmarkEventAppendStaleGen(b *testing.B) {
	ev := newEvent()
	gen := ev.state.Load() >> walStateBits
	ev.reset() // bump the generation: the held gen is now stale
	f := benchField()
	b.ReportAllocs()
	for b.Loop() {
		ev.append(gen, f)
	}
	ev.release()
}

// BenchmarkEventAppendSealed is the straggler no-op for a sealed event:
// generation matches, state says sealed — the write must not land.
func BenchmarkEventAppendSealed(b *testing.B) {
	ev := newEvent()
	gen := ev.state.Load() >> walStateBits
	ev.seal()
	f := benchField()
	b.ReportAllocs()
	for b.Loop() {
		ev.append(gen, f)
	}
	ev.release()
}

// BenchmarkEventSeal is the seal path End and release run: lock, set
// the sealed bit, unlock.
func BenchmarkEventSeal(b *testing.B) {
	events := make([]*event, benchPre)
	rebuild := func() {
		for i := range events {
			events[i] = newEvent()
		}
	}
	rebuild()
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if n&(benchPre-1) == 0 && n > 0 {
			b.StopTimer()
			rebuild()
			b.StartTimer()
		}
		events[n&(benchPre-1)].seal()
	}
	for _, ev := range events {
		ev.release()
	}
}

// BenchmarkEventSnapshotFields is the watchdog's read of a WAL: copy of
// the current tail under the append mutex.
func BenchmarkEventSnapshotFields(b *testing.B) {
	ev := newEvent()
	gen := ev.state.Load() >> walStateBits
	for range 12 {
		ev.append(gen, fieldStr("k", "v"))
	}
	b.ReportAllocs()
	for b.Loop() {
		_, _ = ev.snapshotFields(gen)
	}
	ev.release()
}

// BenchmarkEventSetError is the failure-path setter on an open event:
// structuredErrorField builds the map (message, type, cause) that every
// failing request pays at End.
func BenchmarkEventSetError(b *testing.B) {
	ev := newEvent()
	ev.fields = make([]Field, 0, benchPre)
	ref := &walRef{ev: ev, gen: ev.state.Load() >> walStateBits}
	err := errors.New("bench failure")
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		if n&(benchPre-1) == 0 && n > 0 {
			b.StopTimer()
			ev.fields = ev.fields[:0]
			b.StartTimer()
		}
		ev.setError(ref, err)
	}
	ev.release()
}

// BenchmarkEventSetMessage is the message override on an open event.
func BenchmarkEventSetMessage(b *testing.B) {
	ev := newEvent()
	ref := &walRef{ev: ev, gen: ev.state.Load() >> walStateBits}
	b.ReportAllocs()
	for b.Loop() {
		ev.setMessage(ref, "override")
	}
	ev.release()
}

// BenchmarkEventSetLevel is the requested-level floor on an open event —
// the per-request write the middleware shapes make.
func BenchmarkEventSetLevel(b *testing.B) {
	ev := newEvent()
	ref := &walRef{ev: ev, gen: ev.state.Load() >> walStateBits}
	b.ReportAllocs()
	for b.Loop() {
		ev.setLevel(ref, LevelWarn)
	}
	ev.release()
}

// BenchmarkEventReleaseRecycle is the pool round trip End performs:
// release (seal + Put) then newEvent (Get + reset, one time.Now).
func BenchmarkEventReleaseRecycle(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		ev := newEvent()
		ev.release()
	}
}
