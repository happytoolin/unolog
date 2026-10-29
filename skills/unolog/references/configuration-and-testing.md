# Configuration, sampling, and testing

## Compile configuration once

Use `Compile` for environment, file, flag, or remote configuration:

```go
rt, err := unolog.Compile(unolog.Config{
	Sink:         sink,
	SamplingRate: cfg.Logging.HealthyRequestRate,
	Message:      cfg.Logging.RequestMessage,
})
if err != nil {
	return fmt.Errorf("configure unolog: %w", err)
}
```

Do not clamp an invalid rate. `Compile` validates rates, levels, outcomes, and policies. Return its error so startup fails with the bad value visible.

Use `MustCompile` only for constants:

```go
rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
```

The runtime is immutable. Build it once and share it.

## Sampling

`SamplingRate` controls healthy traffic from `0` to `1`. Start with `1` unless the user provides a traffic or cost requirement.

Use per-level rates only when the policy is explicitly level-based:

```go
LevelSamplingRates: map[unolog.Level]float64{
	unolog.LevelDebug: 0.01,
	unolog.LevelWarn:  1,
},
```

Use built-in sampler middleware for business rules:

```go
Sampler: unolog.ChainSampler(
	unolog.RateSampler(0.05),
	unolog.KeepErrors(),
	unolog.KeepPathPrefix("/admin", "/checkout"),
	unolog.KeepSlowerThan(500*time.Millisecond),
),
```

Errors, panics, and server failures bypass sampling structurally. Do not add a second failure-sampling guard around the sink.

Use `OperationPolicies` only when domains need different levels or rates. Do not add policy maps for one global rate.

## Limits and failure handling

The defaults bound every event: `DefaultMaxFields` (256) fields and `DefaultMaxValueBytes` (32 KiB) per string value. Keep them unless the user has a measured need. Set a larger value for an event that is wide by design. Set `-1` to remove a limit; do this only for trusted, bounded input.

```go
rt, err := unolog.Compile(unolog.Config{
	Sink:          sink,
	SamplingRate:  1,
	MaxFields:     512,     // a deliberately wide event
	MaxValueBytes: 8 << 10, // a pipeline with an 8 KiB field limit
	ErrorHandler: func(err error) {
		metrics.Inc("unolog_internal_errors")
	},
})
```

At the field limit, a write to an existing key replaces its value, so a progress counter stays correct. A new key is dropped and counted in `unolog.dropped_fields`. Canonical keys are never dropped. A cut value is counted in `unolog.truncated_values`. Treat either field in production output as a signal to fix the code or raise the limit.

A panicking sink or sampler does not escape `End`. unolog reports it to `ErrorHandler` as a `*unolog.PanicError` (with the stack), keeps the event on a sampler panic, and drops it on a sink panic. The default handler writes to stderr at most once per second. Do not wrap sinks in your own `recover`.

## Test with TestSink

Test observable event behavior without parsing logger output:

```go
func TestHandlerEmitsOneEvent(t *testing.T) {
	sink := unolog.NewTestSink()
	rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
	handler := std.Middleware(rt)(http.HandlerFunc(handler))

	req := httptest.NewRequest(http.MethodGet, "/orders/42", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if got, ok := events[0].Lookup("order_id"); !ok || got != "42" {
		t.Fatalf("order_id = %v, %v", got, ok)
	}
}
```

Check the fields that define the requested behavior. For failures, also check the level, outcome, status or code, and error field. Do not assert every automatic field in every test.

`TestSink.Events` returns captured snapshots. Use `Reset` only when one test intentionally reuses a sink.

Run tests for each changed module. This repository uses separate Go modules for the root, adapters, and integrations, so a root-only `go test ./...` does not cover every nested module.
