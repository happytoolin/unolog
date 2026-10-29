# Upgrading from unolog v1.1 to v1.2

v1.2 adds three fields to `Config`: `MaxFields`, `MaxValueBytes`, and
`ErrorHandler`. A `Config` literal without field names no longer compiles.
Add the field names before you upgrade. The other API changes only add
names. These defaults change what an event holds and what reaches the
output. Check them before you upgrade.

| Change | Effect | To keep the v1.1 behavior |
|---|---|---|
| Field limit | An event holds at most 256 distinct keys (`DefaultMaxFields`). A new key past the limit is dropped and counted in `unolog.dropped_fields`. Canonical keys are never dropped. | Set `Config.MaxFields` to `-1`. |
| Value limit | A string value longer than 32 KiB (`DefaultMaxValueBytes`) is cut and counted in `unolog.truncated_values`. The canonical line also cuts error text. | Set `Config.MaxValueBytes` to `-1`. |
| Panic containment | A panicking `Sink` or `Sampler` no longer crashes the request. unolog reports a `*unolog.PanicError` to `Config.ErrorHandler`, which writes to stderr by default. | Not available. Set `ErrorHandler` to send the reports elsewhere. |
| `JSONSink` write errors | A failed or short write goes to `Config.ErrorHandler`, and `End` returns `false`. | Set `ErrorHandler` to a function that ignores the report. `End` still returns `false`. |
| Adapter key renames | A user field named after a key the logger writes itself becomes `fields.<key>`: `time`, `level`, and `msg` for slog, and `ts`, `level`, `logger`, `caller`, `msg`, and `stacktrace` for zap's `New`. | Not available. Rename the field, or use `uzap.NewWithEncoderConfig` with your encoder keys. |
| Go version | `integration/gin`, `integration/echo`, and `integration/fiberv3` need Go 1.26, because their dependencies do. The other modules still need Go 1.25. | Keep those integrations at v1.1. |

See [Limits and failure handling](README.md#limits-and-failure-handling)
for the details.

# Migrating from happycontext v0.x to unolog v1.0.0

v1.0.0 is the v2 record core — and the release that renames the project.
The module is now `github.com/happytoolin/unolog` and the root package is
`unolog` (was `hc`). The same one-canonical-event-per-request philosophy
runs on a typed, insertion-ordered WAL instead of a map. This guide maps
every removed or changed symbol. The
[wire format](#wire-format-changes) section covers output differences.

## Module and package rename

Published v0.x versions stay downloadable from the Go module proxy under
the old path, but `github.com/happytoolin/happycontext` receives no
further releases. Import paths and package names changed as follows:

| Area | v0 | v1 |
|---|---|---|
| Root | `github.com/happytoolin/happycontext`, package `hc` | `github.com/happytoolin/unolog`, package `unolog` |
| slog adapter | package `slogadapter` | package `slog` (alias it — `log/slog` collides) |
| zap adapter | package `zapadapter` | package `zap` (alias it) |
| zerolog adapter | package `zerologadapter` | package `zerolog` (alias it) |
| `integration/std` | package `stdhappycontext` | package `std` |
| `integration/gin` | package `ginhappycontext` | package `gin` (alias it) |
| `integration/echo` | package `echohappycontext` | package `echo` (alias it) |
| `integration/fiber` | package `fiberhappycontext` | package `fiber` (alias it) |
| `integration/fiberv3` | package `fiberv3happycontext` | package `fiberv3` |
| `integration/worker` | package `workerhappycontext` | package `worker` |

The nested module paths keep their directories; only the root prefix
changes. The v0 examples below deliberately keep the old `hc.` qualifier
because that is what v0 code looks like; the v1 column is `unolog.`.

## The mental model

- **v0**: configure per call sites (`hc.Config` handed to middleware or
  `op.End`), fields in a `map[string]any` (random order), sinks receive
  `(level, message, map)`.
- **v1**: `unolog.Compile`/`unolog.MustCompile` once at startup → an immutable
  `*Runtime` shared by everything; fields are typed `unolog.Field` records
  in insertion order; sinks receive `(ctx, *unolog.Record)`.

## Construction

| v0 | v1 |
|---|---|
| `hc.Config{...}` passed to middleware / `op.End(cfg, &err)` | `rt := unolog.MustCompile(unolog.Config{...})` once; middleware takes `rt`; `op.End(&err)` |
| `hc.NormalizeConfig(cfg)` | internal to `Compile`; invalid config is now a **construction-time error** (`ErrInvalidRate`, `ErrInvalidLevel`, `ErrInvalidOutcome`, wrapped with `%w`) |
| — | `unolog.Compile(cfg) (*Runtime, error)` for config from files/flags |

```go
// v0
mw := stdhc.Middleware(hc.Config{Sink: sink, SamplingRate: 1})

// v1
rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
mw := std.Middleware(rt)
```

## Lifecycle

| v0 | v1 |
|---|---|
| `hc.BeginOperation(ctx, start)` → `(ctx, *Event)` | `op := unolog.Start(ctx, rt, start)`; pass `op.Context()` down |
| `hc.StartOperation(ctx, start)` → `*Operation` | `unolog.Start(ctx, rt, start)` |
| `op.End(cfg, &err)` | `op.End(&err)` — one-shot, returns *emitted* |
| `hc.FinishOperation(cfg, OperationFinish{...})` | gone; integrations call `op.End` |
| `hc.Commit(ctx, sink, level)` | gone; the event commits once at `op.End` |
| `op.Event()` | gone; the WAL is request-confined (use the sampler view or sinks) |

`End` must be **deferred directly** — `defer op.End(&err)`. The closure
form (`defer func() { op.End(&err) }()`) compiles but silently disables
panic capture.

## Fields

| v0 | v1 |
|---|---|
| `hc.Add(ctx, k, v)` returning `bool` | same call, returns nothing (silent no-op without an event — the `slog` helper idiom) |
| `hc.EventFields(e)` / `EventMessage` / `EventHasMessage` / `EventHasError` / `EventStartTime` | gone; sinks receive `*unolog.Record` (`Fields()`, `Lookup`, `Message()`, `Level()`); samplers receive `SampleInput.Fields()`/`Lookup`. HTTP `SampleInput.Operation` is the last-write `op.name` (the route template), not the Start name `"request"`. Non-HTTP `SampleInput.Code` surfaces the canonical `op.code` (`StatusCode` stays the `http.status` view); HTTP `Code` remains `http.status`. |
| `hc.NewContext(ctx)` / `hc.FromContext(ctx)` | gone (internal) |
| `hc.AddRaw(ctx, k, raw)` | gone; pass `json.RawMessage` to `hc.Add` — it embeds verbatim through the regular path with bridge parity |
| — | `unolog.SetLevel(ctx, level)` unchanged in shape; `GetLevel` removed |

## Levels

`unolog.Level` is now int-backed with slog-compatible ranks
(`LevelDebug = -4`, `LevelInfo = 0`, `LevelWarn = 4`, `LevelError = 8`).
Source that references the constants compiles unchanged;
`hc.LevelRank` and `hc.MergeLevelWithFloor` are gone (internal).

## Sinks

```go
// v0
type Sink interface {
    Write(level hc.Level, message string, fields map[string]any)
}

// v1 — slog.Handler.Handle shape
type Sink interface {
    Write(ctx context.Context, rec *unolog.Record)
}
```

- `rec.Fields()` is the insertion-ordered typed slice; `rec.Lookup(key)`
  resolves last-write-wins; `rec.Encoded()` is the canonical JSON line,
  computed once (copy it if you retain it).
- The Record is valid only inside `Write`; sinks must be safe for
  concurrent use.

## Bridges

`NewWithOptions` and `SinkOptions` are removed from all three adapters
(`adapter/slog`, `adapter/zap`, `adapter/zerolog`) — constructors are
`New` only. Bridges iterate `rec.Fields()` in order with typed
constructors.

## Outcome precedence

v0 resolved `explicit outcome > panic > error` in some paths; v1 is
strictly **panic > error > explicit > 5xx > success**. Errors and
panics also **bypass sampling structurally** — before any custom
`Sampler` runs — so `NeverSampler()` can no longer hide failures.

## Wire format changes

- **Field order**: insertion order (deterministic), replacing Go's
  random map order.
- **`op.code`**: emitted only for non-HTTP operations (from the explicit
  `op.code` field); HTTP events carry `http.status`.
- **Worker `job.*` mirrors dropped**: `job.name`, `job.id`,
  `job.queue`, `job.attempt`, `job.max_attempts` are gone — `op.name`,
  `op.id`, `op.source`, `op.attempt`, `op.max_attempts` carry them.
  `job.scheduled_at` remains.
- **Duplicate keys**: last write wins (v0 maps did the same); encoding
  emits each key once.
- **Canonical-key collisions** (the logrus precedent): user fields
  named `message`, `time`, or `level` collide with the canonical
  envelope members, which used to produce duplicate JSON keys on the
  line. They are now renamed to `fields.message`, `fields.time`,
  `fields.level` on the wire (dedupe runs over the renamed keys, so a
  user `message` and a user `fields.message` fold into one last-write-
  wins member). The rename is wire-only: `Record.Fields()`/`Lookup`
  keep the original keys; adapters that consume `Fields()` still see
  them unrenamed.
- **Durations** through bridges render each host's native shape
  (unchanged from v0 adapters); the first-party JSON sink renders float
  milliseconds.
- **float32 values**: the JSON sink and the zap/zerolog bridges render
  32-bit precision (`0.1`); the slog host widens on Go ≥ 1.24
  (`slog.AnyValue` converts to `Float64Value`) — same as the v0
  adapter.
- **Failure levels on default policies**: unchanged (INFO success,
  ERROR failures/panics).

## Removed symbols (complete list)

`BeginOperation`, `FinishOperation`, `OperationFinish`, `StartOperation`,
`Commit`, `GetLevel`, `LevelRank`, `MergeLevelWithFloor`, `EventFields`,
`EventMessage`, `EventHasMessage`, `EventHasError`, `EventStartTime`,
`NewContext`, `FromContext`, `NormalizeConfig`, `AddRaw`/`AddRawJSON` (kind `KindRaw` and `Field.Raw()` went with them), plus the
`Event` type itself, `SinkOptions`/`NewWithOptions` in the bridges, and
`operation/common.NormalizeConfig`-style helpers that moved into
`Compile`.

## Nothing to configure?

A `nil` `*Runtime` is a valid no-op: requests run, nothing emits — the
v0 zero-config behavior, kept deliberately.
