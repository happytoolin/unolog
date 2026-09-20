// Package zap bridges unolog records into zap: a Sink that
// forwards each finalized record as typed zap fields through the
// logger's CheckedEntry path.
package zap

import (
	"context"
	"sync"

	"github.com/happytoolin/unolog"
	"github.com/happytoolin/unolog/wire"
	gozap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	zapFieldPoolCapacity    = 32
	zapFieldPoolMaxCapacity = 160
)

var zapFieldPool = sync.Pool{
	New: func() any {
		buf := make([]gozap.Field, 0, zapFieldPoolCapacity)
		return &buf
	},
}

func recycleZapFields(bufPtr *[]gozap.Field, buf []gozap.Field) {
	if cap(buf) > zapFieldPoolMaxCapacity {
		return
	}
	clear(buf) // gozap.Field carries an any: do not retain a record's values
	*bufPtr = buf[:0]
	zapFieldPool.Put(bufPtr)
}

// Sink writes unolog records to the zap logger.
type Sink struct {
	logger *gozap.Logger
}

// New creates a zap-backed sink.
func New(l *gozap.Logger) *Sink {
	return &Sink{logger: l}
}

// Write implements unolog.Sink: the record's fields are appended in
// insertion order (last-write-wins duplicates resolved) as typed zap
// fields.
func (s *Sink) Write(ctx context.Context, rec *unolog.Record) {
	if s == nil || s.logger == nil || rec == nil {
		return
	}
	checked := s.check(rec.Level(), rec.Message())
	if checked == nil {
		return
	}
	fields := rec.Fields()
	if len(fields) == 0 {
		checked.Write()
		return
	}

	bufPtr := zapFieldPool.Get().(*[]gozap.Field) //nolint:forcetypeassert // the pool's New stores exactly *[]gozap.Field
	zapFields := (*bufPtr)[:0]
	defer func() { recycleZapFields(bufPtr, zapFields) }()
	var indexStorage [wire.NarrowLimit]int
	for _, i := range rec.AppendLastIndices(indexStorage[:0]) {
		zapFields = append(zapFields, fieldOf(fields[i]))
	}
	checked.Write(zapFields...)
}

// fieldOf maps a typed record field to the matching zap constructor.
// Error fields render the message string; everything without a typed
// slot goes through gozap.Any.
func fieldOf(f unolog.Field) gozap.Field {
	if err, ok := f.Err(); ok {
		return gozap.String(f.Key(), wire.ErrorMessage(err))
	}
	if str, ok := f.Str(); ok {
		return gozap.String(f.Key(), str)
	}
	if i, ok := f.Int(); ok {
		return gozap.Int64(f.Key(), i)
	}
	if u, ok := f.Uint(); ok {
		return gozap.Uint64(f.Key(), u)
	}
	if fl, ok := f.Float(); ok {
		if f.Kind() == unolog.KindFloat32 {
			return gozap.Float32(f.Key(), float32(fl))
		}
		return gozap.Float64(f.Key(), fl)
	}
	if b, ok := f.Bool(); ok {
		return gozap.Bool(f.Key(), b)
	}
	if tm, ok := f.Time(); ok {
		return gozap.Time(f.Key(), tm)
	}
	if d, ok := f.Duration(); ok {
		return gozap.Duration(f.Key(), d)
	}
	return gozap.Any(f.Key(), f.Any())
}

func (s *Sink) check(level unolog.Level, message string) *zapcore.CheckedEntry {
	switch level {
	case unolog.LevelDebug:
		return s.logger.Check(zapcore.DebugLevel, message)
	case unolog.LevelWarn:
		return s.logger.Check(zapcore.WarnLevel, message)
	case unolog.LevelError:
		return s.logger.Check(zapcore.ErrorLevel, message)
	default:
		return s.logger.Check(zapcore.InfoLevel, message)
	}
}

var _ unolog.Sink = (*Sink)(nil)
