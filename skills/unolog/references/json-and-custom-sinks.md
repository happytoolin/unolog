# JSON and custom sinks

Prefer a built-in sink. Write a custom sink only when an existing backend cannot accept canonical JSON or one of the logger adapters.

## Dependency-free canonical JSON

The root module includes a concurrency-safe newline-delimited JSON sink:

```go
sink := unolog.NewJSONSink(os.Stdout)
rt := unolog.MustCompile(unolog.Config{
	Sink:         sink,
	SamplingRate: 1,
})
```

`NewJSONSink` writes one canonical line per event. It serializes writes, so ordinary writers such as `bytes.Buffer` are safe. A nil writer creates a no-op sink. Use this before adding a logger dependency only to format JSON. A failed or short write goes to `Config.ErrorHandler` (`unolog.ErrSinkWrite`), and `End` returns `false`. After a short write, the sink writes a newline, so the next line starts on its own line.

## Slow writers

`NewJSONSink` writes on the request goroutine. When the writer can be slow (a pipe, a network connection, a busy disk), use the asynchronous sink:

```go
sink := unolog.NewAsyncJSONSink(os.Stdout, unolog.AsyncJSONSinkConfig{})
defer sink.Close()
```

It copies each line into a bounded buffer (`BufferBytes`, default 1 MiB) and writes from one goroutine. It never blocks a request: when the buffer is full, it drops the line, counts it in `Dropped()`, and reports `unolog.ErrSinkFull` to its own `ErrorHandler`. Call `Close` at shutdown, and `Flush(ctx)` when lines must be written before a step continues. The `ErrorHandler` runs on the writer goroutine: do not call `Close`, or `Flush` with a context that is never done, from it, because both wait for that goroutine. Keep `NewJSONSink` when every line must be written.

## Sink contract

A custom sink implements one method:

```go
type Sink interface {
	Write(context.Context, *unolog.Record)
}
```

The method runs synchronously when the operation ends. Follow these rules:

- Make `Write` safe for concurrent calls.
- Do not retain `*unolog.Record`, `Record.Fields()`, or `Record.Encoded()` after `Write` returns.
- Copy bytes or values that the sink keeps.
- Return promptly. A blocked sink blocks request completion.
- Treat the context as the operation context at end time. Background work started with it inherits request cancellation.
- To report write errors, also implement `unolog.FallibleSink` (`WriteErr(ctx, rec) error`). `End` then calls `WriteErr`, reports a non-nil error to `Config.ErrorHandler`, and returns `false`. Otherwise, handle errors inside the sink.
- A type that embeds a `FallibleSink` also gets its `WriteErr`. If the type overrides `Write`, override `WriteErr` too, or `End` skips the override. A type that embeds `*unolog.JSONSink` is an ordinary sink: `End` calls its `Write`, and write errors are not reported.

## Retain canonical lines safely

This example copies the encoded bytes before it stores them:

```go
type memorySink struct {
	mu    sync.Mutex
	lines [][]byte
}

func (s *memorySink) Write(_ context.Context, rec *unolog.Record) {
	if s == nil || rec == nil {
		return
	}
	line := bytes.Clone(rec.Encoded())
	s.mu.Lock()
	s.lines = append(s.lines, line)
	s.mu.Unlock()
}

var _ unolog.Sink = (*memorySink)(nil)
```

Do not use this sink when `unolog.NewTestSink` already meets a test's needs.

## Build a typed logger adapter

Use these record methods inside `Write`:

- `rec.Level()` for severity;
- `rec.Message()` for the final message;
- `rec.Time()` for completion time;
- `rec.Fields()` for insertion-ordered typed fields;
- `rec.Lookup(key)` for one last-write-wins value;
- `rec.Encoded()` for canonical JSON.

`Fields()` can contain duplicate keys. When translating all fields, resolve duplicates with `github.com/happytoolin/unolog/wire`:

```go
fields := rec.Fields()
for _, i := range wire.LastIndices(fields, unolog.Field.Key) {
	f := fields[i]
	// Map f through Str, Int, Uint, Float, Bool, Time, Duration, Err, or Any.
}
```

When the host logger writes its own envelope, rename a field whose key the logger writes itself (the slog and zap adapters use `fields.<key>`), and resolve duplicates over the renamed keys. Use `Field.WireKey` for output that uses the canonical envelope, where user fields named `level`, `time`, or `message` must not collide with it. Use `wire.ErrorMessage` when an error field must become text safely.

Add one focused test for concurrency, retained-data copying, and the important type mappings. Do not reproduce every first-party adapter test.
