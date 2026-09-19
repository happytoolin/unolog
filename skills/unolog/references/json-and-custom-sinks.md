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

`NewJSONSink` writes one canonical line per event. It serializes writes, so ordinary writers such as `bytes.Buffer` are safe. A nil writer creates a no-op sink. Use this before adding a logger dependency only to format JSON.

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
- Define error handling inside the sink because `Write` does not return an error.

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

Use `Field.Key` when the host logger keeps its own envelope. Use `Field.WireKey` for JSON output where user fields named `level`, `time`, or `message` must not collide with the canonical envelope. Use `wire.ErrorMessage` when an error field must become text safely.

Add one focused test for concurrency, retained-data copying, and the important type mappings. Do not reproduce every first-party adapter test.
