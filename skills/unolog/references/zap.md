# zap adapter

Use this adapter when the application already owns a `*zap.Logger`. Keep its core, level enabler, encoder, outputs, sampling, hooks, and bound fields.

## Install

```bash
go get github.com/happytoolin/unolog
go get github.com/happytoolin/unolog/adapter/zap
```

Also install the selected integration package. Do not create a new zap logger when the application already provides one.

## Build the sink and runtime

```go
import (
	"github.com/happytoolin/unolog"
	uzap "github.com/happytoolin/unolog/adapter/zap"
	"go.uber.org/zap"
)

func newRuntime(logger *zap.Logger, samplingRate float64) (*unolog.Runtime, error) {
	return unolog.Compile(unolog.Config{
		Sink:         uzap.New(logger),
		SamplingRate: samplingRate,
	})
}
```

For literal configuration in `main`:

```go
rt := unolog.MustCompile(unolog.Config{
	Sink:         uzap.New(logger),
	SamplingRate: 1,
})
```

Use a logger with existing service fields when they must appear on every event:

```go
logger := baseLogger.With(
	zap.String("service", serviceName),
	zap.String("environment", environment),
)
sink := uzap.New(logger)
```

## Adapter behavior

The adapter maps unolog levels to zap debug, info, warn, and error levels. It uses zap's checked-entry path, so the existing core decides whether the event is enabled. It forwards unolog values through typed zap field constructors.

The application still owns logger lifecycle. Keep its existing `Sync` policy. Do not call `Sync` from a sink or on every event.

## Avoid duplicate logs

Keep zap calls for process events. Inside one request or job, attach context with `unolog.Add`. Record a handled terminal error with `unolog.Error`. Do not emit a zap line and a unolog event for the same failure unless they serve different audit or security purposes.
