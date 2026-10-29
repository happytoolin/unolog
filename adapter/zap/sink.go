// Package zap bridges unolog records into zap: a Sink that
// forwards each finalized record as typed zap fields through the
// logger's CheckedEntry path.
package zap

import (
	"context"

	"github.com/happytoolin/unolog"
	"github.com/happytoolin/unolog/wire"
	gozap "go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Sink writes unolog records to the zap logger.
type Sink struct {
	logger   *gozap.Logger
	reserved []reservedKey // keys the logger's encoder writes itself
	lengths  uint64        // bit min(len(key), 63) of each reserved key
}

type reservedKey struct{ key, alias string }

// New creates a zap-backed sink for a logger whose encoder uses the keys
// of zap.NewProductionEncoderConfig (ts, level, logger, caller, msg,
// stacktrace). Use NewWithEncoderConfig for an encoder with other keys.
func New(l *gozap.Logger) *Sink {
	return NewWithEncoderConfig(l, gozap.NewProductionEncoderConfig())
}

// NewWithEncoderConfig creates a zap-backed sink for a logger whose
// encoder uses the keys in cfg. A field with one of these keys is
// renamed to "fields.<key>", as the canonical line renames "message",
// "time", and "level": two members with one key are parsed differently
// by different tools.
func NewWithEncoderConfig(l *gozap.Logger, cfg zapcore.EncoderConfig) *Sink {
	s := &Sink{logger: l}
	for _, key := range []string{cfg.TimeKey, cfg.LevelKey, cfg.NameKey, cfg.CallerKey, cfg.FunctionKey, cfg.MessageKey, cfg.StacktraceKey} {
		if key != zapcore.OmitKey && s.alias(key) == key {
			s.reserved = append(s.reserved, reservedKey{key: key, alias: "fields." + key})
			s.lengths |= lengthBit(key)
		}
	}
	return s
}

// Write implements unolog.Sink: the record's fields are appended in
// insertion order (last-write-wins duplicates resolved) as typed zap
// fields. The dedupe runs over the renamed keys, so a user "msg" and a
// user "fields.msg" resolve as one field.
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

	zapFields := make([]gozap.Field, 0, len(fields))
	for _, i := range wire.LastIndices(fields, s.wireKey) {
		zapFields = append(zapFields, fieldOf(s.wireKey(fields[i]), fields[i]))
	}
	checked.Write(zapFields...)
}

func (s *Sink) wireKey(f unolog.Field) string { return s.alias(f.Key()) }

// alias returns the renamed key for a reserved key, and key otherwise.
// Most keys differ in length from every reserved key and skip the loop.
func (s *Sink) alias(key string) string {
	if s.lengths&lengthBit(key) == 0 {
		return key
	}
	for _, r := range s.reserved {
		if r.key == key {
			return r.alias
		}
	}
	return key
}

func lengthBit(key string) uint64 { return 1 << min(len(key), 63) }

// fieldOf maps a typed record field to the matching zap constructor.
// Error fields render the message string; everything without a typed
// slot goes through gozap.Any.
func fieldOf(key string, f unolog.Field) gozap.Field {
	if err, ok := f.Err(); ok {
		return gozap.String(key, wire.ErrorMessage(err))
	}
	if str, ok := f.Str(); ok {
		return gozap.String(key, str)
	}
	if i, ok := f.Int(); ok {
		return gozap.Int64(key, i)
	}
	if u, ok := f.Uint(); ok {
		return gozap.Uint64(key, u)
	}
	if fl, ok := f.Float(); ok {
		if f.Kind() == unolog.KindFloat32 {
			return gozap.Float32(key, float32(fl))
		}
		return gozap.Float64(key, fl)
	}
	if b, ok := f.Bool(); ok {
		return gozap.Bool(key, b)
	}
	if tm, ok := f.Time(); ok {
		return gozap.Time(key, tm)
	}
	if d, ok := f.Duration(); ok {
		return gozap.Duration(key, d)
	}
	return gozap.Any(key, f.Any())
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
