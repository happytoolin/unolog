package examples

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	recoverv2 "github.com/gofiber/fiber/v2/middleware/recover"
	fiberv3 "github.com/gofiber/fiber/v3"
	recoverv3 "github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/happytoolin/unolog"
	uslog "github.com/happytoolin/unolog/adapter/slog"
	uzap "github.com/happytoolin/unolog/adapter/zap"
	uzerolog "github.com/happytoolin/unolog/adapter/zerolog"
	uecho "github.com/happytoolin/unolog/integration/echo"
	ufiber "github.com/happytoolin/unolog/integration/fiber"
	ufiberv3 "github.com/happytoolin/unolog/integration/fiberv3"
	ugin "github.com/happytoolin/unolog/integration/gin"
	"github.com/happytoolin/unolog/integration/std"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Cross-bridge wire reality: the std middleware driven by the same
// real traffic through each REAL logger bridge (slog, zap, zerolog)
// plus the first-party JSONSink. No test doubles anywhere: the oracles
// parse each pipeline's native JSON output and assert the bridges
// agree on the canonical fields for identical requests. (The envelope
// member names differ per host — msg/message — but the canonical and
// user fields are shared, which is exactly the parity that matters.)

var wireCases = []struct {
	request   string
	status    int64
	outcome   string
	userField string
}{
	{"/ok/a1", 200, "success", "ok-field"},
	{"/teapot/a2", 418, "success", "teapot-field"},
	{"/fail/a3", 500, "failure", "fail-field"},
}

func wireMux() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ok/{id}", func(w http.ResponseWriter, r *http.Request) {
		unolog.Add(r.Context(), "wire", "ok-field")
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /teapot/{id}", func(w http.ResponseWriter, r *http.Request) {
		unolog.Add(r.Context(), "wire", "teapot-field")
		w.WriteHeader(http.StatusTeapot)
	})
	mux.HandleFunc("GET /fail/{id}", func(w http.ResponseWriter, r *http.Request) {
		unolog.Add(r.Context(), "wire", "fail-field")
		unolog.Error(r.Context(), errors.New("bridge failure"))
		http.Error(w, "boom", http.StatusInternalServerError)
	})
	return mux
}

type parsedWire struct {
	status  int64
	outcome string
	route   string
	user    string
	hasErr  bool
}

func parseWireLine(t *testing.T, ln, pipeline string) parsedWire {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal([]byte(ln), &m); err != nil {
		t.Fatalf("%s line unparseable: %v: %s", pipeline, err, ln)
	}
	st, stOK := m["http.status"].(float64)
	oc, ocOK := m["op.outcome"].(string)
	rt, rtOK := m["op.name"].(string)
	usr, _ := m["wire"].(string)
	_, hasErr := m["error"]
	if !stOK || !ocOK || !rtOK {
		t.Fatalf("%s: canonical fields missing or mistyped: %v", pipeline, m)
	}
	return parsedWire{status: int64(st), outcome: oc, route: rt, user: usr, hasErr: hasErr}
}

// drivePipeline fires the wire cases at a real server built on the
// given real sink and parses the pipeline's emitted lines. The
// explicit Close is load-bearing: client returns precede the server's
// deferred emissions, and Close waits for outstanding handlers.
func drivePipeline(t *testing.T, sink unolog.Sink, buf *bytes.Buffer, pipeline string) []parsedWire {
	t.Helper()
	rt := unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})
	srv := httptest.NewServer(std.Middleware(rt)(wireMux()))
	defer srv.Close() // safety net for early fatals; Close is idempotent

	for _, c := range wireCases {
		req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, srv.URL+c.request, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := srv.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
	srv.Close() // quiesce handlers before reading the buffer

	out := make([]parsedWire, 0, len(wireCases))
	for ln := range strings.SplitSeq(strings.TrimSpace(buf.String()), "\n") {
		if ln == "" {
			continue
		}
		out = append(out, parseWireLine(t, ln, pipeline))
	}
	return out
}

// TestAdaptersWireParity drives identical real traffic through every
// real pipeline — first-party JSONSink, slog, zap, zerolog — and
// asserts BOTH the absolute canonical fields per pipeline AND that the
// bridges agree with the JSONSink reference. Deterministic order
// (jsonsink is the fixed reference), no map-iteration roulette.
func TestAdaptersWireParity(t *testing.T) {
	var jsonBuf bytes.Buffer
	reference := drivePipeline(t, unolog.NewJSONSink(&jsonBuf), &jsonBuf, "jsonsink")

	var slogBuf bytes.Buffer
	slogEvents := drivePipeline(t, uslog.New(slog.New(slog.NewJSONHandler(&slogBuf, nil))), &slogBuf, "slog")

	var zapBuf bytes.Buffer
	zapLogger := zap.New(zapcore.NewCore(zapcore.NewJSONEncoder(zapcore.EncoderConfig{TimeKey: "ts", LevelKey: "level", MessageKey: "msg", EncodeTime: zapcore.EpochTimeEncoder, EncodeLevel: zapcore.LowercaseLevelEncoder}), zapcore.Lock(zapcore.AddSync(&zapBuf)), zapcore.DebugLevel))
	zapEvents := drivePipeline(t, uzap.New(zapLogger), &zapBuf, "zap")

	var zeroBuf bytes.Buffer
	zl := zerolog.New(&zeroBuf)
	zeroEvents := drivePipeline(t, uzerolog.New(&zl), &zeroBuf, "zerolog")

	pipelines := []struct {
		name   string
		events []parsedWire
	}{
		{"jsonsink", reference},
		{"slog", slogEvents},
		{"zap", zapEvents},
		{"zerolog", zeroEvents},
	}
	for _, pl := range pipelines {
		if len(pl.events) != len(wireCases) {
			t.Fatalf("%s: %d events, want %d", pl.name, len(pl.events), len(wireCases))
		}
		for i, ev := range pl.events {
			// Absolute checks for EVERY pipeline — a bridge regressing a
			// canonical field must fail deterministically, not only when
			// map iteration happens to make it the non-reference.
			want := wireCases[i]
			if ev.status != want.status || ev.outcome != want.outcome || ev.user != want.userField {
				t.Fatalf("%s[%d] = %+v, want case %+v", pl.name, i, ev, want)
			}
			if ev.route != reference[i].route || ev.hasErr != reference[i].hasErr {
				t.Fatalf("%s[%d] diverges from jsonsink: %+v vs %+v", pl.name, i, ev, reference[i])
			}
		}
	}
}

type runResult struct {
	event         unolog.CapturedEvent
	panicObserved bool
}

type comparableResult struct {
	level        unolog.Level
	status       int
	message      string
	method       string
	path         string
	hasError     bool
	hasPanic     bool
	errorMessage string
	panicType    string
	panicValue   string
}

func TestIntegrationConsistency(t *testing.T) {
	gin.SetMode(gin.TestMode)

	type runner struct {
		name string
		run  func(t *testing.T, mode string) runResult
	}
	runners := []runner{
		{name: "std", run: runStd},
		{name: "gin", run: runGin},
		{name: "echo", run: runEcho},
		{name: "fiber", run: runFiber},
		{name: "fiberv3", run: runFiberV3},
	}

	modes := []string{"success", "error", "panic"}
	for _, mode := range modes {
		t.Run(mode, func(t *testing.T) {
			results := make(map[string]runResult, len(runners))
			for _, r := range runners {
				results[r.name] = r.run(t, mode)
			}

			var baseline comparableResult
			for i, r := range runners {
				out := results[r.name]
				assertConsistency(t, mode, out)
				got := normalizeResult(t, out)
				if i == 0 {
					baseline = got
					continue
				}
				if got != baseline {
					t.Fatalf("%s result mismatch with baseline: got=%+v want=%+v", r.name, got, baseline)
				}
			}
		})
	}
}

func assertConsistency(t *testing.T, mode string, out runResult) {
	t.Helper()

	status, ok := out.event.Lookup("http.status")
	if !ok {
		t.Fatalf("expected http.status field")
	}
	route := lookupString(out.event, "http.route")
	if route == "" || !strings.Contains(route, "/orders") {
		t.Fatalf("unexpected route field: %v", route)
	}

	switch mode {
	case "success":
		if out.event.Level() != unolog.LevelInfo {
			t.Fatalf("level = %s, want INFO", out.event.Level())
		}
		if statusFromField(t, status) != http.StatusOK {
			t.Fatalf("status = %v, want %d", status, http.StatusOK)
		}
	case "error":
		if out.event.Level() != unolog.LevelError {
			t.Fatalf("level = %s, want ERROR", out.event.Level())
		}
		if statusFromField(t, status) != http.StatusInternalServerError {
			t.Fatalf("status = %v, want %d", status, http.StatusInternalServerError)
		}
		errField, _ := out.event.Lookup("error")
		if _, ok := errField.(map[string]any); !ok {
			t.Fatalf("expected structured error field")
		}
		if _, errorType := errorDetails(errField); errorType == "" {
			t.Fatalf("expected concrete error type")
		}
	case "panic":
		if !out.panicObserved {
			t.Fatal("expected panic propagation/observation")
		}
		if out.event.Level() != unolog.LevelError {
			t.Fatalf("level = %s, want ERROR", out.event.Level())
		}
		if statusFromField(t, status) != http.StatusInternalServerError {
			t.Fatalf("status = %v, want %d", status, http.StatusInternalServerError)
		}
		panicField, _ := out.event.Lookup("panic")
		if _, ok := panicField.(map[string]any); !ok {
			t.Fatalf("expected panic field")
		}
	}
}

func TestIntegrationImplicitErrorStatusConsistency(t *testing.T) {
	gin.SetMode(gin.TestMode)

	type runner struct {
		name string
		run  func(t *testing.T) runResult
	}
	runners := []runner{
		{name: "gin", run: runGinImplicitError},
		{name: "echo", run: runEchoImplicitError},
		{name: "fiber", run: runFiberImplicitError},
		{name: "fiberv3", run: runFiberV3ImplicitError},
	}

	for _, r := range runners {
		t.Run(r.name, func(t *testing.T) {
			out := r.run(t)
			statusVal, _ := out.event.Lookup("http.status")
			status := statusFromField(t, statusVal)
			if status != http.StatusInternalServerError {
				t.Fatalf("status = %d, want %d", status, http.StatusInternalServerError)
			}
			if out.event.Level() != unolog.LevelError {
				t.Fatalf("level = %s, want ERROR", out.event.Level())
			}
			errField, _ := out.event.Lookup("error")
			if _, ok := errField.(map[string]any); !ok {
				t.Fatalf("expected structured error field")
			}
		})
	}
}

func runStd(t *testing.T, mode string) runResult {
	t.Helper()
	sink := unolog.NewTestSink()
	mw := std.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1}))
	mux := http.NewServeMux()
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
		switch mode {
		case "error":
			unolog.Error(r.Context(), errors.New("boom"))
			w.WriteHeader(http.StatusInternalServerError)
			return
		case "panic":
			panic("boom")
		}
		w.WriteHeader(http.StatusOK)
	})
	var panicObserved bool
	func() {
		defer func() {
			if recover() != nil {
				panicObserved = true
			}
		}()
		mw(mux).ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/1", nil))
	}()
	return runResult{event: onlyEvent(t, sink), panicObserved: panicObserved}
}

func runGin(t *testing.T, mode string) runResult {
	t.Helper()
	sink := unolog.NewTestSink()
	r := gin.New()
	r.Use(ugin.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})))
	r.GET("/orders/:id", func(c *gin.Context) {
		switch mode {
		case "error":
			_ = c.Error(errors.New("boom"))
			c.AbortWithStatus(http.StatusInternalServerError)
			return
		case "panic":
			panic("boom")
		}
		c.Status(http.StatusOK)
	})
	var panicObserved bool
	func() {
		defer func() {
			if recover() != nil {
				panicObserved = true
			}
		}()
		r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/1", nil))
	}()
	return runResult{event: onlyEvent(t, sink), panicObserved: panicObserved}
}

func runEcho(t *testing.T, mode string) runResult {
	t.Helper()
	sink := unolog.NewTestSink()
	e := echo.New()
	e.Use(uecho.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})))
	e.GET("/orders/:id", func(c echo.Context) error {
		switch mode {
		case "error":
			return echo.NewHTTPError(http.StatusInternalServerError, "boom")
		case "panic":
			panic("boom")
		}
		return c.NoContent(http.StatusOK)
	})
	var panicObserved bool
	func() {
		defer func() {
			if recover() != nil {
				panicObserved = true
			}
		}()
		e.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/1", nil))
	}()
	return runResult{event: onlyEvent(t, sink), panicObserved: panicObserved}
}

func runFiber(t *testing.T, mode string) runResult {
	t.Helper()
	sink := unolog.NewTestSink()
	app := fiber.New()
	app.Use(recoverv2.New())
	app.Use(ufiber.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})))
	app.Get("/orders/:id", func(c *fiber.Ctx) error {
		switch mode {
		case "error":
			return fiber.NewError(http.StatusInternalServerError, "boom")
		case "panic":
			panic("boom")
		}
		return c.SendStatus(http.StatusOK)
	})
	res, err := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/1", nil))
	if res != nil {
		_ = res.Body.Close()
	}
	event := onlyEvent(t, sink)
	panicField, _ := event.Lookup("panic")
	_, hasPanic := panicField.(map[string]any)
	return runResult{event: event, panicObserved: err != nil || hasPanic}
}

func runFiberV3(t *testing.T, mode string) runResult {
	t.Helper()
	sink := unolog.NewTestSink()
	app := fiberv3.New()
	app.Use(recoverv3.New())
	app.Use(ufiberv3.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})))
	app.Get("/orders/:id", func(c fiberv3.Ctx) error {
		switch mode {
		case "error":
			return fiber.NewError(http.StatusInternalServerError, "boom")
		case "panic":
			panic("boom")
		}
		return c.SendStatus(http.StatusOK)
	})
	res, err := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/1", nil))
	if res != nil {
		_ = res.Body.Close()
	}
	event := onlyEvent(t, sink)
	panicField, _ := event.Lookup("panic")
	_, hasPanic := panicField.(map[string]any)
	return runResult{event: event, panicObserved: err != nil || hasPanic}
}

func runGinImplicitError(t *testing.T) runResult {
	t.Helper()
	sink := unolog.NewTestSink()
	r := gin.New()
	r.Use(ugin.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})))
	r.GET("/orders/:id", func(c *gin.Context) {
		_ = c.Error(errors.New("boom"))
	})
	r.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/1", nil))
	return runResult{event: onlyEvent(t, sink)}
}

func runEchoImplicitError(t *testing.T) runResult {
	t.Helper()
	sink := unolog.NewTestSink()
	e := echo.New()
	e.Use(uecho.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})))
	e.GET("/orders/:id", func(c echo.Context) error {
		return errors.New("boom")
	})
	e.ServeHTTP(httptest.NewRecorder(), httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/1", nil))
	return runResult{event: onlyEvent(t, sink)}
}

func runFiberImplicitError(t *testing.T) runResult {
	t.Helper()
	sink := unolog.NewTestSink()
	app := fiber.New()
	app.Use(ufiber.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})))
	app.Get("/orders/:id", func(c *fiber.Ctx) error {
		return errors.New("boom")
	})
	res, _ := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/1", nil))
	if res != nil {
		_ = res.Body.Close()
	}
	return runResult{event: onlyEvent(t, sink)}
}

func runFiberV3ImplicitError(t *testing.T) runResult {
	t.Helper()
	sink := unolog.NewTestSink()
	app := fiberv3.New()
	app.Use(ufiberv3.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})))
	app.Get("/orders/:id", func(c fiberv3.Ctx) error {
		return errors.New("boom")
	})
	res, _ := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/1", nil))
	if res != nil {
		_ = res.Body.Close()
	}
	return runResult{event: onlyEvent(t, sink)}
}

func onlyEvent(t *testing.T, sink *unolog.TestSink) unolog.CapturedEvent {
	t.Helper()
	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	return events[0]
}

func normalizeResult(t *testing.T, out runResult) comparableResult {
	t.Helper()

	errField, _ := out.event.Lookup("error")
	panicField, _ := out.event.Lookup("panic")
	_, hasError := errField.(map[string]any)
	_, hasPanic := panicField.(map[string]any)
	method := lookupString(out.event, "http.method")
	path := lookupString(out.event, "http.path")
	errorMessage, _ := errorDetails(errField)
	panicType, panicValue := panicDetails(panicField)
	statusVal, _ := out.event.Lookup("http.status")
	return comparableResult{
		level:        out.event.Level(),
		status:       statusFromField(t, statusVal),
		message:      out.event.Message(),
		method:       method,
		path:         path,
		hasError:     hasError,
		hasPanic:     hasPanic,
		errorMessage: errorMessage,
		panicType:    panicType,
		panicValue:   panicValue,
	}
}

func lookupString(ev unolog.CapturedEvent, key string) string {
	v, ok := ev.Lookup(key)
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return s
}

func statusFromField(t *testing.T, value any) int {
	t.Helper()

	switch n := value.(type) {
	case int:
		return n
	case int64:
		return int(n)
	default:
		t.Fatalf("expected int status, got %T (%v)", value, value)
		return 0
	}
}

func errorDetails(value any) (message, typ string) {
	field, ok := value.(map[string]any)
	if !ok {
		return "", ""
	}
	message, _ = field["message"].(string)
	typ, _ = field["type"].(string)
	return message, typ
}

func panicDetails(value any) (typ, panicValue string) {
	field, ok := value.(map[string]any)
	if !ok {
		return "", ""
	}
	typ, _ = field["type"].(string)
	panicValue, _ = field["value"].(string)
	return typ, panicValue
}
