// Package std provides the net/http unolog
// middleware: one canonical event per request with optional-interface
// response-writer fidelity (Flusher/Hijacker/Pusher/ReaderFrom).
package std

import (
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/happytoolin/unolog"
	"github.com/happytoolin/unolog/integration/flow"
)

// Middleware wraps an http.Handler with unolog request lifecycle
// logging. rt comes from unolog.Compile/MustCompile; a nil *unolog.Runtime is a
// passthrough (the no-op runtime semantics).
//
// The handler must finish all response writes before it returns, as
// net/http requires. The middleware returns the pooled response writer
// to the pool when the handler returns; a write after that point can
// touch the writer of a different request.
func Middleware(rt *unolog.Runtime) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if rt == nil {
				next.ServeHTTP(w, r)
				return
			}

			op := flow.StartRequest(r.Context(), rt, r.Method, r.URL.Path)

			req := r.WithContext(op.Context())
			core := getTracker(w)
			ww := promoteOptional(w, core)

			defer func() {
				// Snapshot the tracker state before it returns to the pool:
				// another request's getTracker may reset it the moment
				// release() lands.
				statusCode, wroteHeader := core.statusCode, core.wroteHeader
				core.release()
				recovered := recover()
				status := flow.ResolveStatus(flow.StatusInput{
					Committed:       statusCode,
					Recovered:       recovered,
					ResponseStarted: wroteHeader,
				})
				flow.FinalizeRequest(op, req.Pattern, status, nil, recovered)

				if recovered != nil {
					panic(recovered)
				}
			}()

			next.ServeHTTP(ww, req)
		})
	}
}

// responseWriter tracks the first committed status while delegating
// everything else to the wrapped writer. It replaces the httpsnoop
// wrapper the v0 middleware used: one pooled allocation instead of the
// hook-closure chain.
//
// Optional-interface fidelity: the wrappers make Flusher, Hijacker,
// Pusher, CloseNotifier, and ReaderFrom assertable whenever the tracker
// is promoted; methods are safe no-ops (or io.Copy fallbacks) when the
// underlying writer lacks the capability.
type responseWriter struct {
	http.ResponseWriter
	statusCode  int
	wroteHeader bool
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.ResponseWriter.WriteHeader(code)
	// Informational responses leave the final status open, except a protocol switch.
	informational := code >= 100 && code <= 199 && code != http.StatusSwitchingProtocols
	if !rw.wroteHeader && !informational {
		rw.statusCode = code
		rw.wroteHeader = true
	}
}

func (rw *responseWriter) Write(p []byte) (int, error) {
	if !rw.wroteHeader {
		rw.statusCode = http.StatusOK
		rw.wroteHeader = true
	}
	return rw.ResponseWriter.Write(p)
}

func (rw *responseWriter) ReadFrom(src io.Reader) (int64, error) {
	if rf, ok := rw.ResponseWriter.(io.ReaderFrom); ok {
		var n int64
		if !rw.wroteHeader {
			// Match net/http's 512-byte content sniff before its zero-copy path.
			// An empty or failed read commits nothing; a later panic keeps
			// the status recorded by the writes that already succeeded.
			var err error
			n, err = rw.writeSniff(src)
			if err != nil || n < 512 {
				return n, err
			}
		}
		remaining, err := rf.ReadFrom(src)
		return n + remaining, err
	}
	return io.Copy(onlyWriter{rw}, src)
}

func (rw *responseWriter) writeSniff(src io.Reader) (int64, error) {
	buf := sniffBufferPool.Get().(*[512]byte) //nolint:forcetypeassert // the pool stores exactly *[512]byte
	defer sniffBufferPool.Put(buf)
	var written int64
	for written < int64(len(buf)) {
		chunk := buf[:int64(len(buf))-written]
		n, readErr := src.Read(chunk)
		if n < 0 || n > len(chunk) {
			if readErr != nil {
				return written, readErr
			}
			return written, errInvalidRead
		}
		if n > 0 {
			nw, writeErr := rw.Write(chunk[:n])
			if nw < 0 || nw > n {
				if writeErr != nil {
					return written, writeErr
				}
				return written, errInvalidWrite
			}
			written += int64(nw)
			if writeErr != nil {
				return written, writeErr
			}
			if nw != n {
				return written, io.ErrShortWrite
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				readErr = nil
			}
			return written, readErr
		}
	}
	return written, nil
}

var (
	errInvalidRead  = errors.New("invalid Read result")
	errInvalidWrite = errors.New("invalid Write result")
)

type onlyWriter struct{ rw *responseWriter }

func (ow onlyWriter) Write(p []byte) (int, error) { return ow.rw.Write(p) }

// CloseNotify keeps the deprecated interface assertable for v0 users;
// it is a no-op when the underlying writer does not implement it.
func (rw *responseWriter) CloseNotify() <-chan bool {
	//lint:ignore SA1019 v0 parity: keep the deprecated interface assertable for existing users
	if cn, ok := rw.ResponseWriter.(http.CloseNotifier); ok { //nolint:staticcheck // v0 parity: keep the deprecated interface assertable
		return cn.CloseNotify()
	}
	return nil
}

var trackerPool = sync.Pool{
	New: func() any { return &responseWriter{} },
}

var sniffBufferPool = sync.Pool{
	New: func() any { return new([512]byte) },
}

func getTracker(w http.ResponseWriter) *responseWriter {
	tracker := trackerPool.Get().(*responseWriter) //nolint:forcetypeassert // the pool's New stores exactly *responseWriter
	tracker.ResponseWriter = w
	tracker.statusCode = 0
	tracker.wroteHeader = false
	return tracker
}

// promoteOptional wraps the pooled tracker so the optional interfaces
// the underlying writer supports stay assertable downstream (the
// httpsnoop behavior v0 provided). Plain writers return the tracker
// itself — the common case stays allocation-free.
func promoteOptional(w http.ResponseWriter, core *responseWriter) http.ResponseWriter {
	flusher, hasFlush := w.(http.Flusher)
	hijacker, hasHijack := w.(http.Hijacker)
	pusher, hasPush := w.(http.Pusher)
	switch {
	case hasFlush && hasHijack && hasPush:
		return &fullTracker{core, flushGuard{core, flusher}, hijacker, pusher}
	case hasFlush && hasHijack:
		return &flushHijackTracker{core, flushGuard{core, flusher}, hijacker}
	case hasFlush && hasPush:
		return &flushPushTracker{core, flushGuard{core, flusher}, pusher}
	case hasFlush:
		return &flushTracker{core, flushGuard{core, flusher}}
	case hasHijack && hasPush:
		return &hijackPushTracker{core, hijacker, pusher}
	case hasHijack:
		return &hijackTracker{core, hijacker}
	case hasPush:
		return &pushTracker{core, pusher}
	default:
		return core
	}
}

// flushGuard is embedded by every wrapper that promotes a Flusher: it
// records the implicit commit before flushing, because net/http sends
// the header (status 200 if unset) on the first Flush — without it, a
// panic after the first flush resolves to 500 against a 200 the
// client already received.
type flushGuard struct {
	rw *responseWriter
	f  http.Flusher
}

func (g flushGuard) Flush() {
	g.rw.markFlushed()
	g.f.Flush()
}

func (rw *responseWriter) markFlushed() {
	if !rw.wroteHeader {
		rw.statusCode = http.StatusOK
		rw.wroteHeader = true
	}
}

type flushTracker struct {
	*responseWriter
	flushGuard
}

type hijackTracker struct {
	*responseWriter
	http.Hijacker
}

type pushTracker struct {
	*responseWriter
	http.Pusher
}

type flushHijackTracker struct {
	*responseWriter
	flushGuard
	http.Hijacker
}

type flushPushTracker struct {
	*responseWriter
	flushGuard
	http.Pusher
}

type hijackPushTracker struct {
	*responseWriter
	http.Hijacker
	http.Pusher
}

type fullTracker struct {
	*responseWriter
	flushGuard
	http.Hijacker
	http.Pusher
}

// Unwrap lets http.ResponseController discover deadline/duplex controls
// on the underlying writer (the httpsnoop fidelity v0 provided).
func (rw *responseWriter) Unwrap() http.ResponseWriter { return rw.ResponseWriter }

func (rw *responseWriter) release() {
	rw.ResponseWriter = nil
	trackerPool.Put(rw)
}
