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
