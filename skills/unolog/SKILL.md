---
name: unolog
description: Integrate, configure, or review unolog in Go HTTP services and background jobs. Use for canonical one-event-per-operation logging with slog, zap, zerolog, net/http, Gin, Echo, Fiber, or worker jobs.
---

# unolog

Add unolog without replacing the application's router or logger.

## Choose the existing stack

Inspect `go.mod` and the application bootstrap before editing. Reuse the logger and framework already in use. Add only the required modules:

| Existing stack | Package |
| --- | --- |
| `log/slog` | `github.com/happytoolin/unolog/adapter/slog` |
| zap | `github.com/happytoolin/unolog/adapter/zap` |
| zerolog | `github.com/happytoolin/unolog/adapter/zerolog` |
| No logger | `unolog.NewJSONSink(io.Writer)` |
| `net/http` | `github.com/happytoolin/unolog/integration/std` |
| Gin | `github.com/happytoolin/unolog/integration/gin` |
| Echo v4 | `github.com/happytoolin/unolog/integration/echo` |
| Fiber v2 | `github.com/happytoolin/unolog/integration/fiber` |
| Fiber v3 | `github.com/happytoolin/unolog/integration/fiberv3` |
| Background jobs | `github.com/happytoolin/unolog/integration/worker` |

The root package is `github.com/happytoolin/unolog`. Run `go get` only for the selected root, adapter, and integration packages.

## Integrate

Create one sink and one immutable `*unolog.Runtime` during application startup. Use `unolog.Compile` when configuration comes from input and return its error. Use `unolog.MustCompile` only for constants.

Wrap the router with the matching `Middleware`. In handlers, add business context through the request context:

```go
unolog.Add(r.Context(), "user_id", userID, "feature", "checkout")
```

Use `unolog.Error(ctx, err)` for a handled error that the framework cannot observe. Use `unolog.SetMessage` only when the final event needs a more specific name. Keep startup, shutdown, and other process logs in the existing logger, but remove per-request log lines that duplicate the final unolog event.

For a background job, use the operation context and defer `End` directly so returned errors and panics are recorded:

```go
func run(ctx context.Context, rt *unolog.Runtime, meta worker.JobMeta) (err error) {
	op := worker.Start(ctx, rt, meta)
	defer op.End(&err)

	ctx = op.Context()
	unolog.Add(ctx, "rows", 42)
	return doWork(ctx)
}
```

Pass the unolog context to downstream functions. Calling `unolog.Add` with the original context is a no-op.

## Configure and verify

Default to `SamplingRate: 1` unless the user gives a sampling policy. Do not sample away failures; unolog preserves errors and server failures automatically.

For a code change, add one focused test with `unolog.NewTestSink()`. Exercise the handler or job, require exactly one captured event, and use `CapturedEvent.Lookup` to check the important fields. Run the tests for every changed Go module.
