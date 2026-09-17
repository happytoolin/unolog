package fiberv3

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v3"
	recovermw "github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/happytoolin/unolog"
)

func TestMiddlewareCapturesRouteAndFields(t *testing.T) {
	app := fiber.New()
	sink := unolog.NewTestSink()
	app.Use(Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
	})))
	app.Get("/orders/:id", func(c fiber.Ctx) error {
		unolog.Add(c.Context(), "user_id", "u_1")
		return c.SendStatus(http.StatusNoContent)
	})

	req := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/123", nil)
	res, err := app.Test(req)
	if err != nil {
		t.Fatalf("fiber v3 test request failed: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusNoContent {
		t.Fatalf("expected HTTP status %d, got %d", http.StatusNoContent, res.StatusCode)
	}

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if statusField(events[0]) != http.StatusNoContent {
		t.Fatalf("expected status %d, got %v", http.StatusNoContent, statusField(events[0]))
	}
	if fieldValue(events[0], "http.route") != "/orders/:id" {
		t.Fatalf("expected route template, got %v", fieldValue(events[0], "http.route"))
	}
	if fieldValue(events[0], "user_id") != "u_1" {
		t.Fatalf("expected user_id field, got %v", fieldValue(events[0], "user_id"))
	}
}

func TestMiddlewareSinkNilStillRunsHandler(t *testing.T) {
	app := fiber.New()
	app.Use(Middleware(unolog.MustCompile(unolog.Config{})))
	app.Get("/ok", func(c fiber.Ctx) error {
		return c.SendStatus(http.StatusAccepted)
	})

	res, err := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/ok", nil))
	if err != nil {
		t.Fatalf("fiber v3 request failed: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusAccepted {
		t.Fatalf("status = %d, want %d", res.StatusCode, http.StatusAccepted)
	}
}

func TestMiddlewareErrorAndSamplingBehavior(t *testing.T) {
	app := fiber.New()
	sink := unolog.NewTestSink()
	app.Use(Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 0,
	})))
	app.Get("/drop", func(c fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})
	app.Get("/err", func(c fiber.Ctx) error {
		return errors.New("boom")
	})

	res, err := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/drop", nil))
	if err != nil {
		t.Fatalf("fiber v3 request failed: %v", err)
	}
	_ = res.Body.Close()
	if got := len(sink.Events()); got != 0 {
		t.Fatalf("expected sampled request to drop, got %d events", got)
	}

	res, _ = app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/err", nil))
	if res != nil {
		_ = res.Body.Close()
	}
	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Level() != unolog.LevelError {
		t.Fatalf("level = %s, want ERROR", events[0].Level())
	}
	if statusField(events[0]) != http.StatusInternalServerError {
		t.Fatalf("status = %v, want %d", statusField(events[0]), http.StatusInternalServerError)
	}
	if _, ok := fieldValue(events[0], "error").(map[string]any); !ok {
		t.Fatalf("expected structured error field")
	}
}

func TestMiddlewarePanicLogsAndPropagates(t *testing.T) {
	app := fiber.New()
	app.Use(recovermw.New())
	sink := unolog.NewTestSink()
	app.Use(Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
	})))
	app.Get("/panic/:id", func(c fiber.Ctx) error {
		panic("bad")
	})

	res, err := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/panic/1", nil))
	if err != nil {
		t.Fatalf("fiber v3 request failed: %v", err)
	}
	_ = res.Body.Close()
	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if fieldValue(events[0], "http.route") != "/panic/:id" {
		t.Fatalf("route = %v", fieldValue(events[0], "http.route"))
	}
	if statusField(events[0]) != http.StatusInternalServerError {
		t.Fatalf("status = %v, want %d", statusField(events[0]), http.StatusInternalServerError)
	}
	if _, ok := fieldValue(events[0], "panic").(map[string]any); !ok {
		t.Fatalf("expected panic metadata")
	}
}

func TestMiddlewareFiberErrorKeepsHTTPStatus(t *testing.T) {
	app := fiber.New()
	sink := unolog.NewTestSink()
	app.Use(Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
	})))
	app.Get("/too-many", func(c fiber.Ctx) error {
		return fiber.NewError(http.StatusTooManyRequests, "slow down")
	})

	res, err := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/too-many", nil))
	if err != nil {
		t.Fatalf("fiber v3 request failed: %v", err)
	}
	_ = res.Body.Close()
	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if statusField(events[0]) != http.StatusTooManyRequests {
		t.Fatalf("status = %v, want %d", statusField(events[0]), http.StatusTooManyRequests)
	}
	if events[0].Level() != unolog.LevelError {
		t.Fatalf("level = %s, want ERROR", events[0].Level())
	}
}

func TestMiddlewareCustomMessagePropagates(t *testing.T) {
	app := fiber.New()
	sink := unolog.NewTestSink()
	app.Use(Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
		Message:      "done",
	})))
	app.Get("/ok", func(c fiber.Ctx) error {
		return c.SendStatus(http.StatusOK)
	})

	res, err := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/ok", nil))
	if err != nil {
		t.Fatalf("fiber v3 request failed: %v", err)
	}
	_ = res.Body.Close()
	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if events[0].Message() != "done" {
		t.Fatalf("message = %q, want %q", events[0].Message(), "done")
	}
}

func TestMiddlewareLogsStatusFromCustomFiberErrorHandler(t *testing.T) {
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c fiber.Ctx, err error) error {
			return c.Status(http.StatusTeapot).SendString("handled")
		},
	})
	sink := unolog.NewTestSink()
	app.Use(Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
	})))
	app.Get("/custom-err", func(c fiber.Ctx) error {
		return errors.New("boom")
	})

	res, err := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/custom-err", nil))
	if err != nil {
		t.Fatalf("fiber v3 request failed: %v", err)
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusTeapot {
		t.Fatalf("expected HTTP status %d, got %d", http.StatusTeapot, res.StatusCode)
	}

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	if statusField(events[0]) != http.StatusTeapot {
		t.Fatalf("status = %v, want %d", statusField(events[0]), http.StatusTeapot)
	}
	if events[0].Level() != unolog.LevelError {
		t.Fatalf("level = %s, want ERROR", events[0].Level())
	}
}

func TestMiddlewareReturnsCustomFiberErrorHandlerFailure(t *testing.T) {
	handlerErr := errors.New("handler failed")
	app := fiber.New(fiber.Config{
		ErrorHandler: func(c fiber.Ctx, err error) error {
			return handlerErr
		},
	})
	sink := unolog.NewTestSink()
	var upstreamErr error
	app.Use(func(c fiber.Ctx) error {
		upstreamErr = c.Next()
		return upstreamErr
	})
	app.Use(Middleware(unolog.MustCompile(unolog.Config{
		Sink:         sink,
		SamplingRate: 1,
	})))
	app.Get("/custom-err-failure", func(c fiber.Ctx) error {
		return errors.New("boom")
	})

	res, err := app.Test(httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/custom-err-failure", nil))
	if err != nil {
		t.Fatalf("fiber v3 request failed: %v", err)
	}
	_ = res.Body.Close()
	if !errors.Is(upstreamErr, handlerErr) {
		t.Fatalf("upstream error = %v, want %v", upstreamErr, handlerErr)
	}

	events := sink.Events()
	if len(events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(events))
	}
	errField, ok := fieldValue(events[0], "error").(map[string]any)
	if !ok {
		t.Fatal("expected structured error field")
	}
	if errField["message"] != "handler failed" {
		t.Fatalf("error message = %v, want handler failed", errField["message"])
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
