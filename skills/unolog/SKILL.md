---
name: unolog
description: Integrate, configure, extend, or review unolog in Go HTTP services and background operations. Use for one-event-per-operation logging with slog, zap, zerolog, canonical JSON, net/http, Gin, Echo, Fiber, worker jobs, or custom sinks.
---

# unolog

Add unolog around the application's existing logger and framework. Keep startup, shutdown, and process logs in the host logger. Replace fragmented request or job logs with fields on one final unolog event.

## Inspect before editing

Read `go.mod`, the application bootstrap, router construction, logger construction, and one representative handler or job. Determine:

- the existing logger or output writer;
- the HTTP framework or operation type;
- where one shared runtime can be created;
- which context reaches the business logic;
- which existing log lines describe the same request or operation.

Do not replace the logger, router, configuration system, or dependency injection pattern.

## Read only the relevant guides

Select one sink guide:

- For standard-library `log/slog`, read [references/slog.md](references/slog.md).
- For zap, read [references/zap.md](references/zap.md).
- For zerolog, read [references/zerolog.md](references/zerolog.md).
- For dependency-free JSON or a new `unolog.Sink`, read [references/json-and-custom-sinks.md](references/json-and-custom-sinks.md).

Then select the operation guide:

- For `net/http`, Gin, Echo, Fiber v2, or Fiber v3, read [references/http-integrations.md](references/http-integrations.md).
- For worker jobs, message consumers, CLI commands, or other operations, read [references/operations.md](references/operations.md).

For configuration, sampling, and verification, read [references/configuration-and-testing.md](references/configuration-and-testing.md).

Do not load unrelated logger or framework guides.

## Common integration contract

Install only the root module, one adapter when needed, and one integration package when available. Create one sink and one immutable `*unolog.Runtime` during startup. Share the runtime across requests or jobs.

Use `unolog.Compile` for values from configuration and return the error. Use `unolog.MustCompile` only for literals that should stop startup when invalid.

Add business fields through the operation context:

```go
unolog.Add(ctx, "user_id", userID, "feature", "checkout")
```

Use `unolog.Error(ctx, err)` only when an error is handled and will not reach the integration. Use `unolog.SetMessage` for a useful final event name. Use `unolog.SetLevel` only when the operation needs a higher severity than its resolved outcome.

Always pass the enriched context to downstream functions. An original or background context has no attached event, so unolog writes through it are no-ops.

Remove per-request or per-job log lines only when their information is now present in the final event. Keep security, audit, and independently meaningful events separate.

## Finish

Add one focused `unolog.NewTestSink` test for changed behavior. Require one event and check its important fields. Run tests for each changed Go module.
