# zerolog adapter

Use this adapter when the application already uses zerolog or needs zerolog-specific hooks, context, sampling, or global behavior.

## Install

```bash
go get github.com/happytoolin/unolog
go get github.com/happytoolin/unolog/adapter/zerolog
```

Also install the selected integration package.

## Choose one constructor

Use `New` for an existing logger that does not add its own timestamp:

```go
logger := zerolog.New(os.Stdout)
sink := uzerolog.New(&logger)
```

`New` uses zerolog's public typed field API. It preserves logger context, hooks, sampling, levels, and rendering behavior. The adapter adds the unolog record completion time.

Use `NewWithLoggerTimestamp` when the logger is configured with `.With().Timestamp()`:

```go
logger := zerolog.New(os.Stdout).With().
	Str("service", serviceName).
	Timestamp().
	Logger()
sink := uzerolog.NewWithLoggerTimestamp(&logger)
```

This avoids two timestamp fields. Do not use plain `New` with a timestamped logger.

Use `NewCanonical` only when the output needs canonical unolog JSON and does not need logger context, hooks, sampling, or rendering customization:

```go
sink := uzerolog.NewCanonical(os.Stdout)
```

`NewCanonical` accepts an `io.Writer` or zerolog `LevelWriter`. It honors zerolog's global level. For canonical JSON without any zerolog dependency, use `unolog.NewJSONSink` instead.

## Build the runtime

```go
rt, err := unolog.Compile(unolog.Config{
	Sink:         sink,
	SamplingRate: samplingRate,
})
if err != nil {
	return err
}
```

Use `MustCompile` only when the configuration is literal.

## Adapter behavior

All constructors map unolog levels to zerolog debug, info, warn, and error levels. `New` and `NewWithLoggerTimestamp` use the existing logger's public API. `NewCanonical` writes the already encoded canonical record.

Keep zerolog calls for process events. Use unolog fields inside an operation so the same request does not produce fragmented zerolog lines and a duplicate final event.
