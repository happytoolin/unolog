package benches_test

import (
	"context"
	"strconv"
	"testing"

	"github.com/happytoolin/unolog"
)

type discardSink struct{}

func (discardSink) Write(context.Context, *unolog.Record) {}

// runtimeFor returns a compiled runtime with the given sink and full
// sampling (kept events), as the gates measure the kept path.
func runtimeFor() *unolog.Runtime {
	return unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 1})
}

// BenchmarkWALAddStableKeys measures the single-pair Add gate by
// delta: Start+Add minus Start-only (constant key/value, the gate
// shape). Fresh operations per iteration keep the WAL at steady state —
// no unbounded growth, no pool return noise.
func BenchmarkWALAddStableKeys(b *testing.B) {
	rt := runtimeFor()
	ctx := context.Background()
	b.Run("start_only", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = unolog.Start(ctx, rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "bench"})
		}
	})
	b.Run("start_add_pair", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			op := unolog.Start(ctx, rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "bench"})
			unolog.Add(op.Context(), "user_id", "u_8472")
		}
	})
}

// BenchmarkWALAddMany measures the variadic multi-pair Add shape.
func BenchmarkWALAddMany(b *testing.B) {
	rt := runtimeFor()
	op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "bench"})
	ctx := op.Context()
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		unolog.Add(ctx, "a", i, "b", i, "c", i, "d", i, "e", i, "f", i)
		i++
	}
	op.End(nil)
}

// benchmarkFields appends n typed fields to the operation context.
func benchmarkFields(ctx context.Context, n int) {
	unolog.Add(ctx, "http.method", "GET")
	unolog.Add(ctx, "http.path", "/api/v1/orders/12345")
	unolog.Add(ctx, "http.route", "/api/v1/orders/:id")
	unolog.Add(ctx, "http.status", 200)
	unolog.Add(ctx, "op.domain", "http")
	unolog.Add(ctx, "op.name", "GET /api/v1/orders/:id")
	unolog.Add(ctx, "op.outcome", "success")
	unolog.Add(ctx, "op.code", 200)
	unolog.Add(ctx, "duration_ms", 12)
	unolog.Add(ctx, "request_id", "req_01HZX4T7W8Y3N2M1K0J9Z8X7V6")
	unolog.Add(ctx, "user_id", "usr_77451")
	unolog.Add(ctx, "cache.hit", true)
	for i := 12; i < n; i++ {
		unolog.Add(ctx, "k"+strconv.Itoa(i), i)
	}
}

// BenchmarkOperationLifecycle is the §4 kept-lifecycle gate
// (≤ 250 ns / ≤ 4 allocs), measured on the same corpus as the v0.4.0
// baseline: full OperationStart metadata plus one multi-pair Add.
func BenchmarkOperationLifecycle(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		var err error
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{
			Domain:      unolog.DomainJob,
			Name:        "cleanup",
			ID:          "job_8472",
			Source:      "nightly",
			Attempt:     1,
			MaxAttempts: 3,
		})
		unolog.Add(op.Context(), "worker", "payments", "tenant", "enterprise")
		op.End(&err)
	}
}

// BenchmarkOperationLifecycle12Fields is the same kept lifecycle with
// the 12-field medium corpus (the sink-axis event shape).
func BenchmarkOperationLifecycle12Fields(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
		benchmarkFields(op.Context(), 12)
		op.End(nil)
	}
}

// BenchmarkOperationLifecycleSparse is the kept lifecycle with no user
// fields — the floor of the core machinery.
func BenchmarkOperationLifecycleSparse(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{})
		op.End(nil)
	}
}

// BenchmarkOperationLifecycleDropped is the §4 full-dropped-lifecycle
// gate (≤ 300 ns), on the v0-parity corpus: N fields then a sampled-out
// End. "End-drop path ≤ 100 ns / ≤ 2 al" is the no-fields variant below.
func BenchmarkOperationLifecycleDropped(b *testing.B) {
	rt := unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 0})
	for _, count := range []int{8, 32} {
		b.Run(strconv.Itoa(count)+"_fields", func(b *testing.B) {
			keys := make([]string, count)
			for i := range keys {
				keys[i] = "k" + strconv.Itoa(i)
			}
			b.ReportAllocs()
			for b.Loop() {
				op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "cleanup"})
				for _, k := range keys {
					unolog.Add(op.Context(), k, 7)
				}
				op.End(nil)
			}
		})
	}
}

// BenchmarkOperationLifecycleDroppedNoFields approaches the end-drop
// floor (≤ 100 ns): sampler + release + pool with no field appends.
func BenchmarkOperationLifecycleDroppedNoFields(b *testing.B) {
	rt := unolog.MustCompile(unolog.Config{Sink: discardSink{}, Sampler: func(unolog.SampleInput) bool { return false }})
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{})
		op.End(nil)
	}
}

// BenchmarkOperationLifecycleWithPolicies runs the kept lifecycle under
// a policy table, as the v0 bench did.
func BenchmarkOperationLifecycleWithPolicies(b *testing.B) {
	policies := map[unolog.Domain]unolog.OperationPolicy{
		unolog.DomainHTTP: {SuccessLevel: unolog.LevelInfo, FailureLevel: unolog.LevelError},
		unolog.DomainJob:  {SuccessLevel: unolog.LevelDebug, FailureLevel: unolog.LevelWarn},
	}
	rt := unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 1, OperationPolicies: policies})
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "job"})
		benchmarkFields(op.Context(), 12)
		op.End(nil)
	}
}

// BenchmarkOperationPolicyScale runs the kept lifecycle with a wide
// policy table (the scale axis of the v0 quality matrix).
func BenchmarkOperationPolicyScale(b *testing.B) {
	policies := make(map[unolog.Domain]unolog.OperationPolicy, 128)
	for i := range 128 {
		policies[unolog.Domain("svc"+strconv.Itoa(i))] = unolog.OperationPolicy{SuccessLevel: unolog.LevelInfo}
	}
	rt := unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 1, OperationPolicies: policies})
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /x"})
		op.End(nil)
	}
}

// BenchmarkNonHTTPManualLifecycle is the background-job shape with an
// error result: explicit domain/name/id plus a few fields.
func BenchmarkNonHTTPManualLifecycle(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		var err error = errBench
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "import", ID: "j-1", Attempt: 1})
		unolog.Add(op.Context(), "rows", 42)
		op.End(&err)
	}
}

var errBench = &benchError{}

type benchError struct{}

func (*benchError) Error() string { return "bench failure" }

// BenchmarkNonHTTPBackgroundJob is the success job shape.
func BenchmarkNonHTTPBackgroundJob(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainJob, Name: "digest", ID: "d-9"})
		unolog.Add(op.Context(), "batch", 100, "source", "queue")
		op.End(nil)
	}
}

// BenchmarkRateSampler measures the built-in probabilistic sampler.
func BenchmarkRateSampler(b *testing.B) {
	in := unolog.SampleInput{Domain: unolog.DomainHTTP, Operation: "GET /x", Outcome: unolog.OutcomeSuccess, StatusCode: 200, Level: unolog.LevelInfo}
	sampler := unolog.RateSampler(0.5)
	b.ReportAllocs()
	for b.Loop() {
		_ = sampler(in)
	}
}

// BenchmarkDisabledLevelWrite measures the level-sampling drop for
// always-sampled-out debug traffic (no regression axis).
func BenchmarkDisabledLevelWrite(b *testing.B) {
	rt := unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 0})
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{})
		unolog.SetLevel(op.Context(), unolog.LevelDebug)
		op.End(nil)
	}
}
