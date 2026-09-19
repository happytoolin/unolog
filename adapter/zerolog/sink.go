// Package zerolog bridges unolog records into zerolog through its public API.
package zerolog

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/happytoolin/unolog"
	"github.com/happytoolin/unolog/wire"
	gozerolog "github.com/rs/zerolog"
)

// Sink writes unolog records to the zerolog logger.
type Sink struct {
	logger          *gozerolog.Logger
	canonical       gozerolog.LevelWriter
	loggerTimestamp bool
}

// New creates a zerolog-backed sink.
func New(l *gozerolog.Logger) *Sink {
	return &Sink{logger: l}
}

// NewWithLoggerTimestamp creates a sink for a logger configured with
// logger.With().Timestamp(). The logger supplies the timestamp, so the sink
// does not add the record completion time as a second member.
func NewWithLoggerTimestamp(l *gozerolog.Logger) *Sink {
	return &Sink{logger: l, loggerTimestamp: true}
}

// NewCanonical creates a sink that writes unolog's canonical JSON directly.
// Use New when zerolog context, hooks, sampling, or rendering globals apply.
func NewCanonical(w io.Writer) *Sink {
	if w == nil {
		w = io.Discard
	}
	lw, ok := w.(gozerolog.LevelWriter)
	if !ok {
		lw = gozerolog.LevelWriterAdapter{Writer: w}
	}
	return &Sink{canonical: lw}
}

// Write implements unolog.Sink. Fields are appended in insertion order
// through zerolog's public typed constructors.
func (s *Sink) Write(ctx context.Context, rec *unolog.Record) {
	if s == nil || rec == nil {
		return
	}
	if s.canonical != nil {
		level := zerologLevel(rec.Level())
		if level < gozerolog.GlobalLevel() {
			return
		}
		if _, err := s.canonical.WriteLevel(level, rec.Encoded()); err != nil {
			if gozerolog.ErrorHandler != nil {
				gozerolog.ErrorHandler(err)
			} else {
				fmt.Fprintf(os.Stderr, "zerolog: could not write event: %v\n", err)
			}
		}
		return
	}
	if s.logger == nil {
		return
	}

	event := s.eventFor(rec.Level())
	if !event.Enabled() {
		return
	}

	fields := rec.Fields()
	var indexStorage [wire.NarrowLimit]int
	for _, i := range wire.AppendLastIndices(indexStorage[:0], fields, unolog.Field.WireKey) {
		event = appendField(event, fields[i])
	}
	// New stamps the record completion time. NewWithLoggerTimestamp lets a
	// configured zerolog timestamp hook stamp the event once instead.
	if !s.loggerTimestamp {
		event.Time(gozerolog.TimestampFieldName, rec.Time())
	}
	event.Msg(rec.Message())
}

func zerologLevel(level unolog.Level) gozerolog.Level {
	switch level {
	case unolog.LevelDebug:
		return gozerolog.DebugLevel
	case unolog.LevelWarn:
		return gozerolog.WarnLevel
	case unolog.LevelError:
		return gozerolog.ErrorLevel
	default:
		return gozerolog.InfoLevel
	}
}

func (s *Sink) eventFor(level unolog.Level) *gozerolog.Event {
	switch level {
	case unolog.LevelDebug:
		return s.logger.Debug()
	case unolog.LevelWarn:
		return s.logger.Warn()
	case unolog.LevelError:
		return s.logger.Error()
	default:
		return s.logger.Info()
	}
}

// appendField maps a typed record field to zerolog's constructor — the
// mapping the v0 adapter used (error → message string, duration →
// float milliseconds via zerolog defaults, time → RFC3339 string).
func appendField(event *gozerolog.Event, f unolog.Field) *gozerolog.Event {
	// WireKey matches Encoded(): colliding envelope keys become fields.*.
	key := f.WireKey()
	if str, ok := f.Str(); ok {
		return event.Str(key, str)
	}
	if i, ok := f.Int(); ok {
		return event.Int64(key, i)
	}
	if u, ok := f.Uint(); ok {
		return event.Uint64(key, u)
	}
	if fl, ok := f.Float(); ok {
		if f.Kind() == unolog.KindFloat32 {
			return event.Float32(key, float32(fl))
		}
		return event.Float64(key, fl)
	}
	if b, ok := f.Bool(); ok {
		return event.Bool(key, b)
	}
	if tm, ok := f.Time(); ok {
		return event.Time(key, tm)
	}
	if d, ok := f.Duration(); ok {
		return event.Dur(key, d)
	}
	if err, ok := f.Err(); ok {
		return event.Str(key, wire.ErrorMessage(err))
	}
	return event.Interface(key, f.Any())
}

var _ unolog.Sink = (*Sink)(nil)
