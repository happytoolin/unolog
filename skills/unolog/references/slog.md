# slog adapter

Use this adapter when the application already uses `log/slog`. Keep the existing `*slog.Logger`, handler, level configuration, groups, and bound attributes.

## Install

```bash
go get github.com/happytoolin/unolog
go get github.com/happytoolin/unolog/adapter/slog
```

Also install the selected integration package. Do not add another logging library.

## Build the sink and runtime

Pass the application's logger to the adapter:

```go
import (
	"log/slog"

	"github.com/happytoolin/unolog"
	uslog "github.com/happytoolin/unolog/adapter/slog"
)

func newRuntime(logger *slog.Logger, samplingRate float64) (*unolog.Runtime, error) {
	return unolog.Compile(unolog.Config{
		Sink:         uslog.New(logger),
		SamplingRate: samplingRate,
	})
}
```

If all values are literals in `main`, the short form is valid:

```go
rt := unolog.MustCompile(unolog.Config{
	Sink:         uslog.New(logger),
	SamplingRate: 1,
})
```

Create the logger first. Create the runtime once after it. Inject or close over the runtime in the same way the application shares its logger.

## Adapter behavior

The adapter maps unolog levels to `slog.LevelDebug`, `Info`, `Warn`, and `Error`. It calls the existing handler's `Enabled` method before it builds attributes. It forwards fields as typed `slog.Attr` values and preserves the logger's handler behavior.

Bound logger attributes still apply:

```go
logger := baseLogger.With("service", serviceName, "environment", environment)
sink := uslog.New(logger)
```

Do not add the same bound attributes with `unolog.Add` on every request unless a request can override them.

Use the existing handler to choose JSON or text output:

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, handlerOptions))
```

The adapter does not require a special handler. Do not build a second handler only for unolog.

## Avoid duplicate logs

Keep `logger.Info` or `logger.Error` for process events such as startup failure or shutdown. Inside a request or job, prefer `unolog.Add`, `unolog.Error`, and one final event instead of logging the same facts through both APIs.
