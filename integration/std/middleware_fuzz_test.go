package std

// FuzzMiddlewareRequest (dst-research §6.5): fuzz bytes decode into a
// request-behavior script (handler actions: Add fields, SetMessage,
// SetLevel, WriteHeader with a status, body writes, ReadFrom streams,
// flush, hijack, panic) plus an interface mask selecting which optional
// ResponseWriter capabilities the underlying writer exposes (Flusher,
// Hijacker, Pusher — the interfaces the middleware promotes). Each
// request runs through the real middleware and the emitted event is
// checked against an INDEPENDENT model that mirrors the documented
// middleware semantics: exactly one event, http.status == the
// first-committed status (ResolveStatus's rules: 500 on a
// pre-response panic, 200 for an implicit status), the outcome/level/
// message resolution of the lifecycle core, and last-write-wins user
// fields. Flush commits an implicit 200 when no final header exists;
// failed hijacks only exercise interface delegation and -race cleanliness.

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/happytoolin/unolog"
)

// Interface-masked ResponseWriter

// fuzzBaseWriter is the fake network: it records what the middleware
// forwarded. It implements io.ReaderFrom and http.CloseNotifier so the
// tracker's own delegation paths are exercised (the middleware tracker
// always exposes ReaderFrom/CloseNotify itself).
type fuzzBaseWriter struct {
	header    http.Header
	code      int
	body      bytes.Buffer
	flushed   bool
	hijacked  bool
	pushed    bool
	readFromN int64
}

func (w *fuzzBaseWriter) Header() http.Header { return w.header }
func (w *fuzzBaseWriter) Write(p []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	return w.body.Write(p)
}
func (w *fuzzBaseWriter) WriteHeader(code int) {
	if w.code != 0 {
		return
	}
	if code < 100 || code > 999 {
		panic(fmt.Sprintf("invalid WriteHeader code %v", code))
	}
	if code >= 200 || code == http.StatusSwitchingProtocols {
		w.code = code
	}
}
func (w *fuzzBaseWriter) ReadFrom(r io.Reader) (int64, error) {
	n, err := io.Copy(struct{ io.Writer }{Writer: w}, r)
	w.readFromN = n
	return n, err
}
func (w *fuzzBaseWriter) CloseNotify() <-chan bool { return nil }

func (w *fuzzBaseWriter) flush() {
	w.WriteHeader(http.StatusOK)
	w.flushed = true
}

// fuzzBase exposes the shared base writer through any wrapper type.
func (w *fuzzBaseWriter) fuzzBase() *fuzzBaseWriter { return w }

// The optional-interface wrappers mirror the middleware's promotion
// switch: one concrete type per (Flusher, Hijacker, Pusher) combo, so
// the fuzz exercises every promotion shape and the mask semantics are
// exact (a writer only exposes what its mask selected).
type fwFlush struct{ *fuzzBaseWriter }

func (w *fwFlush) Flush() { w.flush() }

type fwHijack struct{ *fuzzBaseWriter }

func (w *fwHijack) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	return nil, nil, errHijackUnavailable
}

type fwPush struct{ *fuzzBaseWriter }

func (w *fwPush) Push(string, *http.PushOptions) error {
	w.pushed = true
	return nil
}

type fwFlushHijack struct {
	*fuzzBaseWriter
}

func (w *fwFlushHijack) Flush() { w.flush() }
func (w *fwFlushHijack) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	return nil, nil, errHijackUnavailable
}

type fwFlushPush struct {
	*fuzzBaseWriter
}

func (w *fwFlushPush) Flush() { w.flush() }
func (w *fwFlushPush) Push(string, *http.PushOptions) error {
	w.pushed = true
	return nil
}

type fwHijackPush struct {
	*fuzzBaseWriter
}

func (w *fwHijackPush) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	return nil, nil, errHijackUnavailable
}

func (w *fwHijackPush) Push(string, *http.PushOptions) error {
	w.pushed = true
	return nil
}

type fwFull struct {
	*fuzzBaseWriter
}

func (w *fwFull) Flush() { w.flush() }
func (w *fwFull) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijacked = true
	return nil, nil, errHijackUnavailable
}

func (w *fwFull) Push(string, *http.PushOptions) error {
	w.pushed = true
	return nil
}

// maskedWriterFor builds a writer whose concrete type implements the
// mask-selected optional interfaces (bit 0x1 Flusher, 0x2 Hijacker,
// 0x4 Pusher) — the mirror of the middleware's own promotion switch.
func maskedWriterFor(mask byte) http.ResponseWriter {
	base := &fuzzBaseWriter{header: make(http.Header)}
	switch mask & 0x7 {
	case 0:
		return base
	case 0x1:
		return &fwFlush{base}
	case 0x2:
		return &fwHijack{base}
	case 0x3:
		return &fwFlushHijack{base}
	case 0x4:
		return &fwPush{base}
	case 0x5:
		return &fwFlushPush{base}
	case 0x6:
		return &fwHijackPush{base}
	default:
		return &fwFull{base}
	}
}

var errHijackUnavailable = errors.New("hijack unavailable")

// Script decoding

// mwAction is one handler behavior.
type mwAction struct {
	kind     byte
	key      string
	value    any
	msg      string
	level    unolog.Level
	status   int
	body     string
	panic    any
	readMode byte
	readSize int
}

const (
	mwAdd byte = iota
	mwSetMsg
	mwSetLevel
	mwWriteHeader
	mwWrite
	mwFlush
	mwPanic
	mwHijack
	mwReadFrom
)

var (
	mwKeys      = []string{"ua", "ub", "uc"}
	mwStatuses  = []int{0, 200, 201, 204, 301, 400, 404, 418, 500, 503, 100, 101, 102, 103, 99, 1000}
	mwLevels    = []unolog.Level{unolog.LevelDebug, unolog.LevelInfo, unolog.LevelWarn, unolog.LevelError, unolog.Level(99)}
	mwReadSizes = []int{0, 1, 2, 511, 512, 513, 1024}
)

const (
	mwReadOK byte = iota
	mwReadError
	mwReadTailError
	mwReadTailPanic
	mwReadInvalidNegative
	mwReadInvalidOversized
)

// decodeScript parses fuzz bytes into an action list (total: any byte
// stream decodes; truncated streams stop cleanly).
func decodeScript(b []byte) (mask byte, actions []mwAction) {
	if len(b) == 0 {
		return 0, nil // empty program: no mask, no actions
	}
	mask = b[0]
	rest := b[1:]
	for len(rest) > 0 && len(actions) < 24 {
		cmd := rest[0] % 9
		rest = rest[1:]
		next := func() byte {
			if len(rest) == 0 {
				return 0
			}
			v := rest[0]
			rest = rest[1:]
			return v
		}
		window := func(n int) string {
			if len(rest) < n {
				n = len(rest)
			}
			s := string(rest[:n])
			rest = rest[n:]
			return s
		}
		a := mwAction{kind: cmd}
		switch cmd {
		case mwAdd:
			a.key = mwKeys[int(next())%len(mwKeys)]
			switch next() % 3 {
			case 0:
				a.value = fmt.Sprintf("v%d", next())
			case 1:
				a.value = int64(int8(next()))
			default:
				a.value = next()&1 == 0
			}
		case mwSetMsg:
			switch next() % 3 {
			case 0:
				a.msg = ""
			case 1:
				a.msg = fmt.Sprintf("msg-%d", next())
			default:
				a.msg = window(int(next() % 9))
			}
		case mwSetLevel:
			a.level = mwLevels[int(next())%len(mwLevels)]
		case mwWriteHeader:
			a.status = mwStatuses[int(next())%len(mwStatuses)]
		case mwWrite:
			a.body = window(1 + int(next()%5))
		case mwPanic:
			if next()%2 == 0 {
				a.panic = fmt.Sprintf("boom-%d", next())
			} else {
				a.panic = int64(int8(next()))
			}
		case mwReadFrom:
			a.readMode = next() % 6
			a.readSize = mwReadSizes[int(next())%len(mwReadSizes)]
		}
		actions = append(actions, a)
	}
	return mask, actions
}

// The model — independent re-derivation of the middleware contract

// mwModel is the expected event state for one executed request.
type mwModel struct {
	msg            string
	requested      unolog.Level
	hasRequested   bool
	committed      int  // first committed status (0 = nothing committed)
	started        bool // final header or implicit 200 committed
	userFields     map[string]any
	panicValue     any
	hasPanic       bool
	panicAny       bool
	flushExecuted  bool
	hijackExecuted bool
	bodyBytes      int
}

// apply mirrors the handler + tracker semantics for one action.
func (m *mwModel) apply(a mwAction, mask byte) {
	switch a.kind {
	case mwAdd:
		m.userFields[a.key] = a.value // last write wins
	case mwSetMsg:
		if a.msg != "" {
			m.msg = a.msg
		}
	case mwSetLevel:
		if validMWLevel(a.level) {
			m.requested = a.level
			m.hasRequested = true
		}
	case mwWriteHeader:
		if m.started {
			return
		}
		if a.status < 100 || a.status > 999 {
			m.hasPanic = true
			m.panicValue = fmt.Sprintf("invalid WriteHeader code %v", a.status)
			return
		}
		if a.status < 200 && a.status != http.StatusSwitchingProtocols {
			return
		}
		m.committed = a.status
		m.started = true
	case mwWrite:
		m.bodyBytes += len(a.body)
		if !m.started {
			m.committed = http.StatusOK
			m.started = true
		}
	case mwFlush:
		if mask&0x1 != 0 { // the flush only reaches the writer when promoted
			m.flushExecuted = true
			if !m.started {
				m.committed = http.StatusOK
				m.started = true
			}
		}
	case mwHijack:
		if mask&0x2 != 0 {
			m.hijackExecuted = true
		}
	case mwPanic:
		m.hasPanic = true
		m.panicValue = a.panic
	case mwReadFrom:
		wasStarted := m.started
		switch a.readMode {
		case mwReadOK, mwReadTailError, mwReadTailPanic:
			m.bodyBytes += a.readSize
			if a.readSize > 0 && !m.started {
				m.committed = http.StatusOK
				m.started = true
			}
		}
		if a.readMode == mwReadTailPanic {
			m.hasPanic = true
			m.panicValue = fuzzReadPanic
		} else if a.readMode == mwReadInvalidOversized && wasStarted {
			// Once committed, the wrapper delegates directly to the
			// underlying ReaderFrom. io.Copy panics when a broken Reader
			// reports more bytes than fit in its buffer.
			m.hasPanic = true
			m.panicAny = true
		}
	}
}

var errFuzzRead = errors.New("fuzz read failure")

const fuzzReadPanic = "fuzz read panic"

type fuzzReaderFunc func([]byte) (int, error)

func (f fuzzReaderFunc) Read(p []byte) (int, error) { return f(p) }

func fuzzReadSource(a mwAction) io.Reader {
	payload := bytes.NewReader(bytes.Repeat([]byte{'x'}, a.readSize))
	switch a.readMode {
	case mwReadError:
		return fuzzReaderFunc(func([]byte) (int, error) { return 0, errFuzzRead })
	case mwReadTailError:
		return io.MultiReader(payload, fuzzReaderFunc(func([]byte) (int, error) { return 0, errFuzzRead }))
	case mwReadTailPanic:
		return io.MultiReader(payload, fuzzReaderFunc(func([]byte) (int, error) { panic(fuzzReadPanic) }))
	case mwReadInvalidNegative:
		first := true
		return fuzzReaderFunc(func([]byte) (int, error) {
			if first {
				first = false
				return -1, nil
			}
			return 0, errFuzzRead
		})
	case mwReadInvalidOversized:
		return fuzzReaderFunc(func(p []byte) (int, error) { return len(p) + 1, nil })
	default:
		return payload
	}
}

func validMWLevel(l unolog.Level) bool {
	switch l {
	case unolog.LevelDebug, unolog.LevelInfo, unolog.LevelWarn, unolog.LevelError:
		return true
	}
	return false
}

// resolveStatus mirrors flow.ResolveStatus.
func (m *mwModel) resolveStatus() int {
	if m.hasPanic && !m.started {
		return http.StatusInternalServerError
	}
	if m.committed == 0 {
		return http.StatusOK
	}
	return m.committed
}

// finalize mirrors flow.FinalizeRequest + End's resolution.
func (m *mwModel) finalize() (outcome unolog.Outcome, level unolog.Level, status int) {
	status = m.resolveStatus()
	switch {
	case m.hasPanic:
		outcome = unolog.OutcomePanic
		level = unolog.LevelError
	case status >= 500:
		outcome = unolog.OutcomeFailure
		level = unolog.LevelError
	default:
		outcome = unolog.OutcomeSuccess
		level = unolog.LevelInfo
	}
	if m.hasRequested && m.requested > level {
		level = m.requested
	}
	if m.msg == "" {
		m.msg = unolog.DefaultMessage
	}
	return outcome, level, status
}

// Fuzz target

func FuzzMiddlewareRequest(f *testing.F) {
	// Seeds: happy path, panic-before-write, panic-after-write,
	// flush-then-write, double-write-header, hijack, ReadFrom failures,
	// and interface combinations.
	seed := func(mask byte, actions ...mwAction) {
		b := []byte{mask}
		for _, a := range actions {
			b = append(b, a.kind)
			switch a.kind {
			case mwAdd:
				ki := 0
				for i, k := range mwKeys {
					if k == a.key {
						ki = i
					}
				}
				b = append(b, byte(ki), 0, byte(len(fmt.Sprint(a.value))))
			case mwWriteHeader:
				si := 0
				for i, s := range mwStatuses {
					if s == a.status {
						si = i
					}
				}
				b = append(b, byte(si))
			case mwPanic:
				b = append(b, 0, byte(len(fmt.Sprint(a.panic))))
			case mwWrite:
				b = append(b, byte(len(a.body)))
				b = append(b, a.body...)
			case mwReadFrom:
				sizeIndex := 0
				for i, size := range mwReadSizes {
					if size == a.readSize {
						sizeIndex = i
						break
					}
				}
				b = append(b, a.readMode, byte(sizeIndex))
			}
		}
		f.Add(b)
	}
	seed(
		0x0,
		mwAction{kind: mwAdd, key: "ua", value: "1"},
		mwAction{kind: mwWriteHeader, status: http.StatusOK},
	)
	seed(
		0x7,
		mwAction{kind: mwAdd, key: "ub", value: int64(7)},
		mwAction{kind: mwWriteHeader, status: http.StatusCreated},
		mwAction{kind: mwWrite, body: "hi"},
	)
	seed(0x7, mwAction{kind: mwPanic, panic: "boom"}) // panic before any write
	seed(
		0x7,
		mwAction{kind: mwWriteHeader, status: http.StatusTeapot},
		mwAction{kind: mwPanic, panic: "late"}, // panic after commit
	)
	seed(
		0x1,
		mwAction{kind: mwFlush},
		mwAction{kind: mwWriteHeader, status: http.StatusServiceUnavailable},
	)
	seed(
		0x0,
		mwAction{kind: mwWriteHeader, status: http.StatusEarlyHints},
		mwAction{kind: mwWriteHeader, status: http.StatusServiceUnavailable},
	)
	seed(
		0x0,
		mwAction{kind: mwWriteHeader, status: http.StatusSwitchingProtocols},
		mwAction{kind: mwWriteHeader, status: http.StatusServiceUnavailable},
	)
	seed(
		0x0,
		mwAction{kind: mwWriteHeader, status: 0},
		mwAction{kind: mwAdd, key: "ua", value: "unreachable"},
	)
	seed(
		0x0,
		mwAction{kind: mwWriteHeader, status: http.StatusOK},
		mwAction{kind: mwWriteHeader, status: 0}, // ignored after a final header
	)
	seed(
		0x0,
		mwAction{kind: mwWriteHeader, status: http.StatusNotFound},
		mwAction{kind: mwWriteHeader, status: http.StatusOK}, // double header
	)
	seed(
		0x2,
		mwAction{kind: mwHijack},
		mwAction{kind: mwWriteHeader, status: http.StatusOK},
	)
	seed(0x0, mwAction{kind: mwWrite, body: "no-header"})
	seed(0x0, mwAction{kind: mwReadFrom, readMode: mwReadOK, readSize: 0})
	seed(0x0, mwAction{kind: mwReadFrom, readMode: mwReadOK, readSize: 512})
	seed(0x0, mwAction{kind: mwReadFrom, readMode: mwReadTailError, readSize: 513})
	seed(0x0, mwAction{kind: mwReadFrom, readMode: mwReadTailPanic, readSize: 511})
	seed(0x0, mwAction{kind: mwReadFrom, readMode: mwReadInvalidNegative})
	seed(0x0, mwAction{kind: mwReadFrom, readMode: mwReadInvalidOversized})
	seed(
		0x7,
		mwAction{kind: mwSetMsg, msg: "custom"},
		mwAction{kind: mwAdd, key: "uc", value: true},
		mwAction{kind: mwWriteHeader, status: http.StatusInternalServerError},
	)

	f.Fuzz(func(t *testing.T, program []byte) {
		mask, actions := decodeScript(program)
		m := &mwModel{userFields: map[string]any{}}
		for _, a := range actions {
			m.apply(a, mask)
			if m.hasPanic {
				break // the panic unwinds; later actions never run
			}
		}

		ts := unolog.NewTestSink()
		rt := unolog.MustCompile(unolog.Config{Sink: ts, SamplingRate: 1})
		mw := Middleware(rt)
		base := maskedWriterFor(mask)

		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			for _, a := range actions {
				switch a.kind {
				case mwAdd:
					unolog.Add(r.Context(), a.key, a.value)
				case mwSetMsg:
					unolog.SetMessage(r.Context(), a.msg)
				case mwSetLevel:
					unolog.SetLevel(r.Context(), a.level)
				case mwWriteHeader:
					w.WriteHeader(a.status)
				case mwWrite:
					_, _ = w.Write([]byte(a.body))
				case mwFlush:
					if fl, ok := w.(http.Flusher); ok {
						fl.Flush()
					}
				case mwHijack:
					if hj, ok := w.(http.Hijacker); ok {
						_, _, _ = hj.Hijack()
					}
				case mwPanic:
					panic(a.panic)
				case mwReadFrom:
					rf, ok := w.(io.ReaderFrom)
					if !ok {
						panic("ReaderFrom missing")
					}
					_, _ = rf.ReadFrom(fuzzReadSource(a))
				}
			}
		}))

		var escaped any
		func() {
			defer func() { escaped = recover() }()
			handler.ServeHTTP(base, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))
		}()

		// The middleware re-raises panics with the original value.
		if m.hasPanic {
			if escaped == nil || !m.panicAny && fmt.Sprint(escaped) != fmt.Sprint(m.panicValue) {
				t.Fatalf("panic escaped as %#v, want %#v (mask %x)", escaped, m.panicValue, mask)
			}
		} else if escaped != nil {
			t.Fatalf("unexpected panic %#v (mask %x)", escaped, mask)
		}

		// Interface delegation: flushes and hijacks reached the writer
		// exactly when the mask promoted them (unwrap the middleware's
		// responseWriter to read the base's flags).
		baseFuzzer, ok := base.(interface{ fuzzBase() *fuzzBaseWriter })
		if !ok {
			t.Fatalf("base writer does not expose fuzzBase: %T", base)
		}
		fb := baseFuzzer.fuzzBase()
		if fb.code != m.committed {
			t.Fatalf("writer status = %d, want %d", fb.code, m.committed)
		}
		if m.flushExecuted != fb.flushed {
			t.Fatalf("flush executed=%v, writer saw %v (mask %x)", m.flushExecuted, fb.flushed, mask)
		}
		if m.hijackExecuted != fb.hijacked {
			t.Fatalf("hijack executed=%v, writer saw %v (mask %x)", m.hijackExecuted, fb.hijacked, mask)
		}
		if fb.body.Len() != m.bodyBytes {
			t.Fatalf("body bytes = %d, want %d (mask %x)", fb.body.Len(), m.bodyBytes, mask)
		}

		// Exactly one event, checked against the model.
		events := ts.Events()
		if len(events) != 1 {
			t.Fatalf("captured %d events, want 1 (mask %x)", len(events), mask)
		}
		ev := events[0]
		outcome, level, status := m.finalize()

		if ev.Message() != m.msg {
			t.Fatalf("message = %q, want %q", ev.Message(), m.msg)
		}
		if ev.Level() != level {
			t.Fatalf("level = %v, want %v (outcome %s)", ev.Level(), level, outcome)
		}
		if v, _ := ev.Lookup("http.status"); v != int64(status) {
			t.Fatalf("http.status = %v, want %d (first-committed)", v, status)
		}
		if v, _ := ev.Lookup("op.outcome"); v != string(outcome) {
			t.Fatalf("op.outcome = %v, want %s", v, outcome)
		}
		if m.hasPanic {
			if p, ok := ev.Lookup("panic"); !ok {
				t.Fatal("missing panic field")
			} else if pm, ok := p.(map[string]any); !ok || !m.panicAny && pm["value"] != fmt.Sprint(m.panicValue) {
				t.Fatalf("panic.value = %v", p)
			}
			if e, ok := ev.Lookup("error"); !ok {
				t.Fatal("missing error field")
			} else if em, ok := e.(map[string]any); !ok || !m.panicAny && em["message"] != "panic: "+fmt.Sprint(m.panicValue) {
				t.Fatalf("error.message = %v", e)
			}
		}
		// user fields, last write wins
		for k, want := range m.userFields {
			if v, ok := ev.Lookup(k); !ok || !mwValueEqual(v, want) {
				t.Fatalf("field %q = %v (%v), want %v", k, v, ok, want)
			}
		}
		// canonical http fields
		if v, _ := ev.Lookup("http.method"); v != http.MethodGet {
			t.Fatalf("http.method = %v", v)
		}
		if v, _ := ev.Lookup("http.path"); v != "/x" {
			t.Fatalf("http.path = %v", v)
		}
	})
}

// mwValueEqual compares Lookup values with the model's (int64 vs int).
func mwValueEqual(got, want any) bool {
	if i, ok := want.(int); ok {
		return got == int64(i)
	}
	return got == want
}
