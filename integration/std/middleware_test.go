package std

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/happytoolin/unolog"
	"go.uber.org/goleak"
)

// Middleware behavior tests: routes, statuses, optional interfaces,
// flush commits, panics, and the nil-runtime passthrough.

func TestMiddlewareDelegatesToCoreAndLogs(t *testing.T) {
	sink := unolog.NewTestSink()
	mw := Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
		Message:      "done",
	}))

	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		unolog.Add(r.Context(), "example", "std-integration")
		w.WriteHeader(http.StatusAccepted)
	}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil))

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Message() != "done" {
		t.Fatalf("expected message done, got %q", events[0].Message())
	}
	if statusField(events[0]) != http.StatusAccepted {
		t.Fatalf("expected status %d, got %v", http.StatusAccepted, statusField(events[0]))
	}
	if fieldValue(events[0], "example") != "std-integration" {
		t.Fatalf("expected example field, got %v", fieldValue(events[0], "example"))
	}
}

func TestMiddlewareAppliesCustomMessageFromHandlerContext(t *testing.T) {
	sink := unolog.NewTestSink()
	mw := Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
		Message:      "done",
	}))

	h := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		unolog.SetMessage(r.Context(), "order shipped")
		w.WriteHeader(http.StatusAccepted)
	}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/orders/123/ship", nil))

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Message() != "order shipped" {
		t.Fatalf("expected message %q, got %q", "order shipped", events[0].Message())
	}
	if statusField(events[0]) != http.StatusAccepted {
		t.Fatalf("expected status %d, got %v", http.StatusAccepted, statusField(events[0]))
	}
}

func TestMiddlewarePanicPropagatesAndLogsError(t *testing.T) {
	sink := unolog.NewTestSink()
	mw := Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
	}))

	h := mw(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("bad")
	}))

	rr := httptest.NewRecorder()
	recovered := false
	func() {
		defer func() {
			if recover() != nil {
				recovered = true
			}
		}()
		h.ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/panic", nil))
	}()
	if !recovered {
		t.Fatal("expected panic to propagate")
	}

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Level() != unolog.LevelError {
		t.Fatalf("expected error level, got %s", events[0].Level())
	}
	if statusField(events[0]) != http.StatusInternalServerError {
		t.Fatalf("expected status 500, got %v", statusField(events[0]))
	}
	if _, ok := fieldValue(events[0], "panic").(map[string]any); !ok {
		t.Fatalf("expected panic field in event")
	}
}

func TestMiddlewareWriteHeaderTwiceLogsFirstCommittedStatus(t *testing.T) {
	backend := unolog.NewTestSink()
	mw := Middleware(unolog.MustCompile(unolog.Config{
		Sink:         backend,
		SamplingRate: 1,
	}))

	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		w.WriteHeader(http.StatusInternalServerError)
	}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/double-header", nil))

	if rr.Code != http.StatusCreated {
		t.Fatalf("expected committed HTTP status %d, got %d", http.StatusCreated, rr.Code)
	}

	events := backend.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if statusField(events[0]) != http.StatusCreated {
		t.Fatalf("expected logged status %d, got %v", http.StatusCreated, statusField(events[0]))
	}
}

func TestMiddlewarePanicAfterCommittedStatusKeepsCommittedStatus(t *testing.T) {
	backend := unolog.NewTestSink()
	mw := Middleware(unolog.MustCompile(unolog.Config{
		Sink:         backend,
		SamplingRate: 1,
	}))

	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		panic("boom")
	}))

	rr := httptest.NewRecorder()
	recovered := false
	func() {
		defer func() {
			if recover() != nil {
				recovered = true
			}
		}()
		h.ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/panic-after-commit", nil))
	}()

	if !recovered {
		t.Fatal("expected panic to propagate")
	}
	if rr.Code != http.StatusCreated {
		t.Fatalf("expected committed HTTP status %d, got %d", http.StatusCreated, rr.Code)
	}

	events := backend.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Level() != unolog.LevelError {
		t.Fatalf("expected error level, got %s", events[0].Level())
	}
	if statusField(events[0]) != http.StatusCreated {
		t.Fatalf("expected logged status %d, got %v", http.StatusCreated, statusField(events[0]))
	}
}

func TestMiddlewareSetsRouteFromRequestPattern(t *testing.T) {
	var sampledOp string
	sink := unolog.NewTestSink()
	mw := Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
		Sampler: func(in unolog.SampleInput) bool {
			sampledOp = in.Operation
			return true
		},
	}))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rr := httptest.NewRecorder()
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/123", nil)
	mw(mux).ServeHTTP(rr, req)

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	route, ok := fieldValue(events[0], "http.route").(string)
	if !ok || route == "" {
		t.Fatalf("expected route template, got %#v", fieldValue(events[0], "http.route"))
	}
	if name, _ := fieldValue(events[0], "op.name").(string); name != route {
		t.Fatalf("wire op.name = %q, want route %q", name, route)
	}
	if sampledOp != route {
		t.Fatalf("SampleInput.Operation = %q, want route %q", sampledOp, route)
	}
}

func TestMiddlewarePreservesOptionalInterfaces(t *testing.T) {
	sink := unolog.NewTestSink()
	mw := Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
	}))

	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			t.Fatalf("expected http.Flusher")
		}
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatalf("expected http.Hijacker")
		}
		pusher, ok := w.(http.Pusher)
		if !ok {
			t.Fatalf("expected http.Pusher")
		}
		readerFrom, ok := w.(io.ReaderFrom)
		if !ok {
			t.Fatalf("expected io.ReaderFrom")
		}
		flusher.Flush()
		if _, err := readerFrom.ReadFrom(strings.NewReader("x")); err != nil {
			t.Fatalf("read from failed: %v", err)
		}
		if err := pusher.Push("/asset.js", nil); err != nil {
			t.Fatalf("push failed: %v", err)
		}
		if _, _, err := hijacker.Hijack(); !errors.Is(err, errHijackNotAvailable) {
			t.Fatalf("unexpected hijack error: %v", err)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	base := &fullOptionalWriter{testOptionalWriter: testOptionalWriter{header: make(http.Header)}}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x", nil)
	h.ServeHTTP(base, req)
	if !base.flushed {
		t.Fatalf("expected flush to be forwarded")
	}
	if !base.pushCalled {
		t.Fatalf("expected push to be forwarded")
	}
	if !base.hijackCalled {
		t.Fatalf("expected hijack to be forwarded")
	}
}

func TestMiddlewareWriteSetsStatusCode(t *testing.T) {
	sink := unolog.NewTestSink()
	mw := Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
	}))

	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if _, err := io.Copy(w, bytes.NewBufferString("ok")); err != nil {
			t.Fatalf("copy failed: %v", err)
		}
	}))

	base := &testOptionalWriter{header: make(http.Header)}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/copy", nil)
	h.ServeHTTP(base, req)

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if statusField(events[0]) != http.StatusOK {
		t.Fatalf("expected status 200, got %v", statusField(events[0]))
	}
}

func TestMiddlewareReadFromSetsStatusCode(t *testing.T) {
	sink := unolog.NewTestSink()
	mw := Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
	}))

	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		readerFrom, ok := w.(io.ReaderFrom)
		if !ok {
			t.Fatalf("expected io.ReaderFrom")
		}
		if _, err := readerFrom.ReadFrom(strings.NewReader("ok")); err != nil {
			t.Fatalf("read from failed: %v", err)
		}
	}))

	base := &fullOptionalWriter{testOptionalWriter: testOptionalWriter{header: make(http.Header)}}
	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/copy-readfrom", nil)
	h.ServeHTTP(base, req)

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if statusField(events[0]) != http.StatusOK {
		t.Fatalf("expected status 200, got %v", statusField(events[0]))
	}
}

func TestMiddlewareNilSinkStillRunsHandler(t *testing.T) {
	mw := Middleware(unolog.MustCompile(unolog.Config{}))
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/no-sink", nil))
	if rr.Code != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusAccepted)
	}
}

func TestMiddlewareSamplingDropForHealthyRequest(t *testing.T) {
	sink := unolog.NewTestSink()
	mw := Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 0,
	}))
	h := mw(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/drop", nil))
	if got := len(sink.Events()); got != 0 {
		t.Fatalf("expected no events, got %d", got)
	}
}

// capturedEvent mirrors the v0 test-facing shape (map fields, int
// numerics) over the v2 TestSink capture, keeping the assertions below
// unchanged from the v0 suite.
// Typed field reads on captured events: fieldValue for any value,
// statusField for the int64 http.status these tests compare against
// int constants.
func fieldValue(ev unolog.CapturedEvent, key string) any {
	v, _ := ev.Lookup(key)
	return v
}

func statusField(ev unolog.CapturedEvent) int64 {
	v, _ := ev.Lookup("http.status")
	n, _ := v.(int64)
	return n
}

type testOptionalWriter struct {
	header http.Header
	code   int
	body   bytes.Buffer
}

func (w *testOptionalWriter) Header() http.Header {
	return w.header
}

func (w *testOptionalWriter) Write(p []byte) (int, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	return w.body.Write(p)
}

func (w *testOptionalWriter) WriteHeader(statusCode int) {
	w.code = statusCode
}

var errHijackNotAvailable = errors.New("hijack unavailable in test writer")

type fullOptionalWriter struct {
	testOptionalWriter
	flushed      bool
	pushCalled   bool
	hijackCalled bool
}

func (w *fullOptionalWriter) Flush() {
	w.flushed = true
}

func (w *fullOptionalWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	w.hijackCalled = true
	return nil, nil, errHijackNotAvailable
}

func (w *fullOptionalWriter) Push(_ string, _ *http.PushOptions) error {
	w.pushCalled = true
	return nil
}

func (w *fullOptionalWriter) ReadFrom(src io.Reader) (int64, error) {
	if w.code == 0 {
		w.code = http.StatusOK
	}
	return io.Copy(&w.body, src)
}

// TestMiddlewareFlushCommitsStatus pins the implicit-commit rule: the
// first Flush sends the header (200 if unset), so the tracker must
// observe it. A panic after the first flush previously resolved to 500
// against a 200 the client already received.
func TestMiddlewareFlushCommitsStatus(t *testing.T) {
	sink := unolog.NewTestSink()
	mw := Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1}))

	t.Run("panic after flush keeps the committed 200", func(t *testing.T) {
		sink.Reset()
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f, ok := w.(http.Flusher)
			if !ok {
				t.Fatal("response writer is not a Flusher")
			}
			f.Flush()
			panic("mid-stream")
		}))
		rec := httptest.NewRecorder()
		func() {
			defer func() { _ = recover() }()
			handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), "GET", "/s", nil))
		}()
		if rec.Code != http.StatusOK {
			t.Fatalf("client saw %d, want 200", rec.Code)
		}
		st, _ := sink.Events()[0].Lookup("http.status")
		o, _ := sink.Events()[0].Lookup("op.outcome")
		if st != int64(http.StatusOK) || o != string(unolog.OutcomePanic) {
			t.Fatalf("log = status:%v outcome:%v, want 200/panic", st, o)
		}
	})

	t.Run("error after flush keeps the committed 200", func(t *testing.T) {
		sink.Reset()
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f, ok := w.(http.Flusher)
			if !ok {
				t.Fatal("response writer is not a Flusher")
			}
			f.Flush()
			unolog.Error(r.Context(), errors.New("post-flush failure"))
		}))
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), "GET", "/s", nil))
		st, _ := sink.Events()[0].Lookup("http.status")
		if st != int64(http.StatusOK) {
			t.Fatalf("status = %v, want committed 200", st)
		}
		// Outcome stays success — outcome is derived from the deferred
		// error pointer, not the recorded error field (v0 semantics) —
		// but the structured error must be present and the event kept.
		if o, _ := sink.Events()[0].Lookup("op.outcome"); o != string(unolog.OutcomeSuccess) {
			t.Fatalf("outcome = %v, want success (error field is metadata)", o)
		}
		if _, ok := sink.Events()[0].Lookup("error"); !ok {
			t.Fatal("error field missing")
		}
	})

	t.Run("flush wrappers on all shapes", func(t *testing.T) {
		// httptest.Recorder implements Flusher only; the other promoted
		// shapes are compile-checked by the wrapper types themselves.
		sink.Reset()
		handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			f, ok := w.(http.Flusher)
			if !ok {
				t.Fatal("flusher not promoted")
			}
			f.Flush()
		}))
		handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), "GET", "/s", nil))
		if st, _ := sink.Events()[0].Lookup("http.status"); st != int64(http.StatusOK) {
			t.Fatalf("status = %v, want 200 after plain flush", st)
		}
	})
}

// Consolidated from integration/std/crash_test.go: nil-runtime
// middleware is a documented passthrough.
func TestCrashNilRuntimePassthrough(t *testing.T) {
	mw := Middleware(nil)
	handler := mw(http.NotFoundHandler())
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), "GET", "/x", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 (passthrough)", rec.Code)
	}
}

// TestMiddlewareConcurrentStatusIntegrity pins the tracker-pool race
// fix: under concurrent requests, every event must carry its own
// request's status (a released-then-reset tracker would log 0→200).
func TestMiddlewareConcurrentStatusIntegrity(t *testing.T) {
	sink := unolog.NewTestSink()
	rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
	mw := Middleware(rt)

	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		code := r.URL.Query().Get("code")
		switch code {
		case "201":
			w.WriteHeader(http.StatusCreated)
		case "404":
			w.WriteHeader(http.StatusNotFound)
		case "500":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusOK)
		}
	}))

	want := map[string]int{
		"201": http.StatusCreated,
		"404": http.StatusNotFound,
		"500": http.StatusInternalServerError,
		"":    http.StatusOK,
	}

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			for i := range 100 {
				code := []string{"201", "404", "500", ""}[(g+i)%4]
				req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/x?code="+code, nil)
				rr := httptest.NewRecorder()
				handler.ServeHTTP(rr, req)
				if got := want[code]; rr.Code != got {
					t.Errorf("code %q: response = %d, want %d", code, rr.Code, got)
					return
				}
			}
		})
	}
	wg.Wait()

	events := sink.Events()
	if len(events) != 800 {
		t.Fatalf("events = %d, want 800", len(events))
	}
	for _, ev := range events {
		status, ok := ev.Lookup("http.status")
		if !ok {
			t.Fatal("missing http.status")
		}
		switch status {
		case int64(http.StatusCreated), int64(http.StatusNotFound), int64(http.StatusInternalServerError), int64(http.StatusOK):
		default:
			t.Fatalf("corrupted status under concurrency: %v", status)
		}
	}
}

// Wire-level reality tests: no test doubles at all — the sink is the
// first-party JSONSink emitting the canonical line, the traffic is
// real HTTP against the real middleware, and the oracle parses the
// actual emitted lines. (The three logger bridges' equivalent runs in
// cmd/examples, where those modules are already dependencies.)

// mustWireLine parses one emitted JSON line and asserts the canonical
// envelope every consumer relies on.
func parseWireLine(t *testing.T, ln string) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(ln), &m); err != nil {
		t.Fatalf("wire line unparseable: %v: %s", err, ln)
	}
	for _, k := range []string{"time", "level", "message", "http.method", "http.path", "http.route", "op.name", "http.status", "op.domain", "duration_ms", "op.outcome"} {
		if _, ok := m[k]; !ok {
			t.Fatalf("canonical key %q missing: %s", k, ln)
		}
	}
	outcome, ocOK := m["op.outcome"].(string)
	statusF, stOK := m["http.status"].(float64)
	if !ocOK || !stOK {
		t.Fatalf("canonical envelope mistyped: %s", ln)
	}
	status := int(statusF)
	switch outcome {
	case "panic":
		if _, ok := m["panic"].(map[string]any); !ok {
			t.Fatalf("panic outcome without structured panic field: %s", ln)
		}
	case "failure":
		if _, ok := m["error"].(map[string]any); !ok {
			t.Fatalf("failure outcome without structured error field: %s", ln)
		}
		if status < 500 {
			t.Fatalf("failure outcome with status %d: %s", status, ln)
		}
	case "success":
		if _, ok := m["error"]; ok {
			t.Fatalf("success outcome carries an error field: %s", ln)
		}
	default:
		t.Fatalf("unexpected outcome %q: %s", outcome, ln)
	}
	return m
}

// TestWireMixedTrafficRealLogger drives concurrent mixed traffic —
// success, error, panic, streaming-flush, kitchen-sink — through the
// real middleware and the first-party JSONSink (one shared sink, the
// production shape; the slog/zap/zerolog equivalents live in
// cmd/examples); every emitted line must parse, carry the full
// canonical envelope, and be internally coherent.
func TestWireMixedTrafficRealLogger(t *testing.T) {
	var buf bytes.Buffer
	// One JSONSink, mutex-serialized by the sink itself: the shared
	// bytes.Buffer is safe exactly as it would be in production.
	rt := unolog.MustCompile(unolog.Config{Sink: unolog.NewJSONSink(&buf), SamplingRate: 1})
	mw := Middleware(rt)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ok/{id}", func(w http.ResponseWriter, r *http.Request) {
		unolog.Add(r.Context(), "id", r.PathValue("id"))
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /err/{id}", func(w http.ResponseWriter, r *http.Request) {
		unolog.Add(r.Context(), "id", r.PathValue("id"))
		unolog.Error(r.Context(), fmt.Errorf("wire failure %s", r.PathValue("id")))
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	mux.HandleFunc("GET /panic/{id}", func(w http.ResponseWriter, r *http.Request) {
		unolog.Add(r.Context(), "id", r.PathValue("id"))
		panic("wire panic " + r.PathValue("id"))
	})
	mux.HandleFunc("GET /stream/{id}", func(w http.ResponseWriter, r *http.Request) {
		unolog.Add(r.Context(), "id", r.PathValue("id"))
		f := w.(http.Flusher) //nolint:forcetypeassert // httptest.ResponseRecorder always implements http.Flusher
		for i := range 3 {
			fmt.Fprintf(w, "chunk %d\n", i)
			f.Flush()
		}
	})
	mux.HandleFunc("GET /kitchen/{id}", func(w http.ResponseWriter, r *http.Request) {
		unolog.Add(r.Context(),
			"id", r.PathValue("id"),
			"utf8", "\xff\xfe garbage",
			"deep", map[string]any{"a": []any{1, "two", nil}},
			"big", strings.Repeat("x", 4096),
		)
		w.WriteHeader(http.StatusTeapot)
	})

	srv := httptest.NewServer(mw(mux))
	defer srv.Close()

	// DisableKeepAlives: a fresh connection per request means the client
	// never retries after the panic route closes a reused connection —
	// one server handling (one event) per request, deterministic without
	// leaving real HTTP behind. srv.Client() is shared: configure once.
	client := srv.Client()
	client.Transport = &http.Transport{DisableKeepAlives: true}

	const workers = 8
	const per = 15
	var wg sync.WaitGroup
	for w := range workers {
		wg.Go(func() {
			for i := range per {
				id := fmt.Sprintf("w%d-%d", w, i)
				for _, p := range []string{"/ok/", "/err/", "/panic/", "/stream/", "/kitchen/"} {
					req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+p+id, nil)
					if err == nil {
						if resp, doErr := client.Do(req); doErr == nil {
							_ = resp.Body.Close()
						}
					}
				}
			}
		})
	}
	wg.Wait()
	// Client returns precede the server's deferred emissions; Close
	// waits for outstanding handlers, so the buffer is quiescent.
	srv.Close()

	lines := nonEmptyLines(buf.String())
	if got, want := len(lines), workers*per*5; got != want {
		t.Fatalf("emitted %d lines, want %d", got, want)
	}
	outcomes := map[string]int{}
	for _, ln := range lines {
		m := parseWireLine(t, ln)
		outcome, ok := m["op.outcome"].(string)
		if !ok {
			t.Fatalf("op.outcome is not a string: %v", m["op.outcome"])
		}
		outcomes[outcome]++
	}
	// Exact mix: every /err is failure, every /panic is panic, the
	// rest success (4xx teapot is success-with-status, not failure).
	if outcomes["failure"] != workers*per || outcomes["panic"] != workers*per || outcomes["success"] != workers*per*3 {
		t.Fatalf("outcome mix = %v", outcomes)
	}
}

func nonEmptyLines(s string) []string {
	var out []string
	for ln := range strings.SplitSeq(s, "\n") {
		if strings.TrimSpace(ln) != "" {
			out = append(out, ln)
		}
	}
	return out
}

// TestWireRouteAndOperationShareTheTemplate pins, on the real wire,
// that the sampler-visible operation name and the emitted op.name are
// the same route template.
func TestWireRouteAndOperationShareTheTemplate(t *testing.T) {
	var sampled string
	var buf bytes.Buffer
	rt := unolog.MustCompile(unolog.Config{
		Sink:         unolog.NewJSONSink(&buf),
		SamplingRate: 1,
		Sampler: func(in unolog.SampleInput) bool {
			sampled = in.Operation
			return true
		},
	})
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(Middleware(rt)(mux))

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+"/orders/o_42", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := srv.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	// Quiesce before reading shared state: client returns precede the
	// server's deferred emission, and this test reads buf and sampled
	// directly (the buffering that makes this safe today is a net/http
	// implementation detail — Close makes it explicit).
	srv.Close()

	lines := nonEmptyLines(buf.String())
	if len(lines) != 1 {
		t.Fatalf("lines = %d", len(lines))
	}
	m := parseWireLine(t, lines[0])
	// req.Pattern carries the method for method-matched patterns; the
	// invariant is that all three views agree on the same template.
	const want = "GET /orders/{id}"
	if sampled != want || m["op.name"] != want || m["http.route"] != want {
		t.Fatalf("sampler=%q op.name=%v route=%v, want all %q", sampled, m["op.name"], m["http.route"], want)
	}
}

// goleak integration: every test in this module runs under
// goleak.VerifyTestMain, failing the suite on any leaked goroutine.
// goleak is test-only: nothing outside _test.go imports it.

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
