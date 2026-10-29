// Package slog bridges unolog records into log/slog: a
// Sink that forwards each finalized record as typed slog attributes on
// the logger's own level threshold.
package slog

import (
	"context"
	stdslog "log/slog"
	"sync"

	"github.com/happytoolin/unolog"
	"github.com/happytoolin/unolog/wire"
)

const (
	slogPoolCapacity    = 32
	slogPoolMaxCapacity = 160
)

var slogAttrPool = sync.Pool{
	New: func() any {
		buf := make([]stdslog.Attr, 0, slogPoolCapacity)
		return &buf
	},
}

func recycleAttrs(bufPtr *[]stdslog.Attr, buf []stdslog.Attr) {
	if cap(buf) > slogPoolMaxCapacity {
		return
	}
	clear(buf)
	*bufPtr = buf[:0]
	slogAttrPool.Put(bufPtr)
}

// Sink writes unolog records to the standard library slog logger.
type Sink struct {
	logger *stdslog.Logger
}

// New creates a slog-backed sink.
func New(l *stdslog.Logger) *Sink {
	return &Sink{logger: l}
}

// Write implements unolog.Sink: the record's fields are appended in
// insertion order (last-write-wins duplicates resolved) as typed slog
// attributes. A field whose key is one the handler writes on every
// record (slog.TimeKey, LevelKey, MessageKey) is renamed to
// "fields.<key>", as the canonical line renames "message", "time", and
// "level". A "source" field keeps its key: the handler writes its own
// "source" only with HandlerOptions.AddSource, which the sink cannot
// see.
func (s *Sink) Write(ctx context.Context, rec *unolog.Record) {
	if s == nil || s.logger == nil || rec == nil {
		return
	}

	var slogLevel stdslog.Level
	switch rec.Level() {
	case unolog.LevelDebug:
		slogLevel = stdslog.LevelDebug
	case unolog.LevelWarn:
		slogLevel = stdslog.LevelWarn
	case unolog.LevelError:
		slogLevel = stdslog.LevelError
	default:
		slogLevel = stdslog.LevelInfo
	}
	if !s.logger.Enabled(ctx, slogLevel) {
		return
	}

	fields := rec.Fields()
	if len(fields) == 0 {
		s.logger.LogAttrs(ctx, slogLevel, rec.Message())
		return
	}

	bufPtr := slogAttrPool.Get().(*[]stdslog.Attr) //nolint:forcetypeassert // the pool's New stores exactly *[]stdslog.Attr
	attrs := (*bufPtr)[:0]
	defer func() { recycleAttrs(bufPtr, attrs) }()
	for _, i := range wire.LastIndices(fields, wireKey) {
		attrs = append(attrs, attrOf(wireKey(fields[i]), fields[i]))
	}
	s.logger.LogAttrs(ctx, slogLevel, rec.Message(), attrs...)
}

// wireKey returns the attribute key for f. Two members with one key
// are parsed differently by different tools, so a key the handler
// writes itself gets the "fields." prefix. The dedupe runs over these
// keys: a user "msg" and a user "fields.msg" resolve as one attribute.
func wireKey(f unolog.Field) string {
	switch key := f.Key(); key {
	case stdslog.TimeKey:
		return "fields." + stdslog.TimeKey
	case stdslog.LevelKey:
		return "fields." + stdslog.LevelKey
	case stdslog.MessageKey:
		return "fields." + stdslog.MessageKey
	default:
		return key
	}
}

// attrOf maps a typed field to the matching slog constructor. Error
// fields render the message string; everything without a typed slot
// goes through stdslog.Any.
func attrOf(key string, f unolog.Field) stdslog.Attr {
	if err, ok := f.Err(); ok {
		return stdslog.String(key, wire.ErrorMessage(err))
	}
	if str, ok := f.Str(); ok {
		return stdslog.String(key, str)
	}
	if i, ok := f.Int(); ok {
		return stdslog.Int64(key, i)
	}
	if u, ok := f.Uint(); ok {
		return stdslog.Uint64(key, u)
	}
	if fl, ok := f.Float(); ok {
		// slog widens float32 (no Float32 constructor) — the v0 adapter's
		// shape; the JSON sink and zap/zerolog bridges keep 32-bit
		// precision.
		return stdslog.Float64(key, fl)
	}
	if b, ok := f.Bool(); ok {
		return stdslog.Bool(key, b)
	}
	if tm, ok := f.Time(); ok {
		return stdslog.Time(key, tm)
	}
	if d, ok := f.Duration(); ok {
		return stdslog.Duration(key, d)
	}
	return stdslog.Any(key, f.Any())
}

var _ unolog.Sink = (*Sink)(nil)
