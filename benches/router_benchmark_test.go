package benches_test

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	fiberv3 "github.com/gofiber/fiber/v3"
	"github.com/happytoolin/unolog"
	uecho "github.com/happytoolin/unolog/integration/echo"
	ufiber "github.com/happytoolin/unolog/integration/fiber"
	ufiberv3 "github.com/happytoolin/unolog/integration/fiberv3"
	ugin "github.com/happytoolin/unolog/integration/gin"
	"github.com/happytoolin/unolog/integration/std"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
	"go.uber.org/zap"
)

type noopSlogHandler struct{}

func (noopSlogHandler) Enabled(context.Context, slog.Level) bool  { return true }
func (noopSlogHandler) Handle(context.Context, slog.Record) error { return nil }
func (noopSlogHandler) WithAttrs([]slog.Attr) slog.Handler        { return noopSlogHandler{} }
func (noopSlogHandler) WithGroup(string) slog.Handler             { return noopSlogHandler{} }

// benchResponseWriter is a reusable, allocation-free ResponseWriter so
// the middleware gates measure the middleware, not the test recorder.
type benchResponseWriter struct{ status int }

func (w *benchResponseWriter) Header() http.Header         { return benchHeader }
func (w *benchResponseWriter) Write(p []byte) (int, error) { return len(p), nil }
func (w *benchResponseWriter) WriteHeader(code int)        { w.status = code }

var benchHeader = http.Header{}

func BenchmarkRouterStd(b *testing.B) {
	req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
	handlerHappycontextAPI := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		unolog.Add(r.Context(), "user_id", "u_1")
		w.WriteHeader(http.StatusNoContent)
	})

	b.Run("middleware_on_sink_noop", func(b *testing.B) {
		mw := std.Middleware(unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 1}))
		wrapped := mw(handlerHappycontextAPI)
		bww := &benchResponseWriter{}
		b.ReportAllocs()
		for b.Loop() {
			bww.status = 0
			wrapped.ServeHTTP(bww, req)
		}
	})

	b.Run("normal_logging_slog_noop_handler_no_middleware", func(b *testing.B) {
		logger := slog.New(noopSlogHandler{})
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.InfoContext(
				r.Context(), "request_completed",
				slog.String("http.method", r.Method),
				slog.String("http.path", r.URL.Path),
				slog.Int("http.status", http.StatusNoContent),
				slog.String("user_id", "u_1"),
			)
			w.WriteHeader(http.StatusNoContent)
		})
		b.ReportAllocs()
		bww := &benchResponseWriter{}
		for b.Loop() {
			bww.status = 0
			h.ServeHTTP(bww, req)
		}
	})

	b.Run("normal_logging_slog_json_no_middleware", func(b *testing.B) {
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.InfoContext(
				r.Context(), "request_completed",
				slog.String("http.method", r.Method),
				slog.String("http.path", r.URL.Path),
				slog.Int("http.status", http.StatusNoContent),
				slog.String("user_id", "u_1"),
			)
			w.WriteHeader(http.StatusNoContent)
		})
		b.ReportAllocs()
		bww := &benchResponseWriter{}
		for b.Loop() {
			bww.status = 0
			h.ServeHTTP(bww, req)
		}
	})

	b.Run("normal_logging_zap_nop_no_middleware", func(b *testing.B) {
		logger := zap.NewNop()
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.Info(
				"request_completed",
				zap.String("http.method", r.Method),
				zap.String("http.path", r.URL.Path),
				zap.Int("http.status", http.StatusNoContent),
				zap.String("user_id", "u_1"),
			)
			w.WriteHeader(http.StatusNoContent)
		})
		b.ReportAllocs()
		bww := &benchResponseWriter{}
		for b.Loop() {
			bww.status = 0
			h.ServeHTTP(bww, req)
		}
	})

	b.Run("normal_logging_zerolog_nop_no_middleware", func(b *testing.B) {
		logger := zerolog.Nop()
		h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.Info().
				Str("http.method", r.Method).
				Str("http.path", r.URL.Path).
				Int("http.status", http.StatusNoContent).
				Str("user_id", "u_1").
				Msg("request_completed")
			w.WriteHeader(http.StatusNoContent)
		})
		b.ReportAllocs()
		bww := &benchResponseWriter{}
		for b.Loop() {
			bww.status = 0
			h.ServeHTTP(bww, req)
		}
	})
}

func BenchmarkRouterGin(b *testing.B) {
	gin.SetMode(gin.TestMode)

	b.Run("middleware_on_sink_noop", func(b *testing.B) {
		r := gin.New()
		r.Use(ugin.Middleware(unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 1})))
		r.GET("/orders/:id", func(c *gin.Context) {
			unolog.Add(c.Request.Context(), "user_id", "u_1")
			c.Status(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)
		}
	})

	b.Run("normal_logging_slog_noop_handler_no_middleware", func(b *testing.B) {
		logger := slog.New(noopSlogHandler{})
		r := gin.New()
		r.GET("/orders/:id", func(c *gin.Context) {
			logger.InfoContext(
				c.Request.Context(), "request_completed",
				slog.String("http.method", c.Request.Method),
				slog.String("http.path", c.Request.URL.Path),
				slog.Int("http.status", http.StatusNoContent),
				slog.String("user_id", "u_1"),
			)
			c.Status(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)
		}
	})

	b.Run("normal_logging_slog_json_no_middleware", func(b *testing.B) {
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		r := gin.New()
		r.GET("/orders/:id", func(c *gin.Context) {
			logger.InfoContext(
				c.Request.Context(), "request_completed",
				slog.String("http.method", c.Request.Method),
				slog.String("http.path", c.Request.URL.Path),
				slog.Int("http.status", http.StatusNoContent),
				slog.String("user_id", "u_1"),
			)
			c.Status(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)
		}
	})

	b.Run("normal_logging_zap_nop_no_middleware", func(b *testing.B) {
		logger := zap.NewNop()
		r := gin.New()
		r.GET("/orders/:id", func(c *gin.Context) {
			logger.Info(
				"request_completed",
				zap.String("http.method", c.Request.Method),
				zap.String("http.path", c.Request.URL.Path),
				zap.Int("http.status", http.StatusNoContent),
				zap.String("user_id", "u_1"),
			)
			c.Status(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)
		}
	})

	b.Run("normal_logging_zerolog_nop_no_middleware", func(b *testing.B) {
		logger := zerolog.Nop()
		r := gin.New()
		r.GET("/orders/:id", func(c *gin.Context) {
			logger.Info().
				Str("http.method", c.Request.Method).
				Str("http.path", c.Request.URL.Path).
				Int("http.status", http.StatusNoContent).
				Str("user_id", "u_1").
				Msg("request_completed")
			c.Status(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			rr := httptest.NewRecorder()
			r.ServeHTTP(rr, req)
		}
	})
}

func BenchmarkRouterEcho(b *testing.B) {
	b.Run("middleware_on_sink_noop", func(b *testing.B) {
		e := echo.New()
		e.Use(uecho.Middleware(unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 1})))
		e.GET("/orders/:id", func(c echo.Context) error {
			unolog.Add(c.Request().Context(), "user_id", "u_1")
			return c.NoContent(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			rr := httptest.NewRecorder()
			e.ServeHTTP(rr, req)
		}
	})

	b.Run("normal_logging_slog_noop_handler_no_middleware", func(b *testing.B) {
		logger := slog.New(noopSlogHandler{})
		e := echo.New()
		e.GET("/orders/:id", func(c echo.Context) error {
			logger.InfoContext(
				c.Request().Context(), "request_completed",
				slog.String("http.method", c.Request().Method),
				slog.String("http.path", c.Request().URL.Path),
				slog.Int("http.status", http.StatusNoContent),
				slog.String("user_id", "u_1"),
			)
			return c.NoContent(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			rr := httptest.NewRecorder()
			e.ServeHTTP(rr, req)
		}
	})

	b.Run("normal_logging_slog_json_no_middleware", func(b *testing.B) {
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		e := echo.New()
		e.GET("/orders/:id", func(c echo.Context) error {
			logger.InfoContext(
				c.Request().Context(), "request_completed",
				slog.String("http.method", c.Request().Method),
				slog.String("http.path", c.Request().URL.Path),
				slog.Int("http.status", http.StatusNoContent),
				slog.String("user_id", "u_1"),
			)
			return c.NoContent(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			rr := httptest.NewRecorder()
			e.ServeHTTP(rr, req)
		}
	})

	b.Run("normal_logging_zap_nop_no_middleware", func(b *testing.B) {
		logger := zap.NewNop()
		e := echo.New()
		e.GET("/orders/:id", func(c echo.Context) error {
			logger.Info(
				"request_completed",
				zap.String("http.method", c.Request().Method),
				zap.String("http.path", c.Request().URL.Path),
				zap.Int("http.status", http.StatusNoContent),
				zap.String("user_id", "u_1"),
			)
			return c.NoContent(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			rr := httptest.NewRecorder()
			e.ServeHTTP(rr, req)
		}
	})

	b.Run("normal_logging_zerolog_nop_no_middleware", func(b *testing.B) {
		logger := zerolog.Nop()
		e := echo.New()
		e.GET("/orders/:id", func(c echo.Context) error {
			logger.Info().
				Str("http.method", c.Request().Method).
				Str("http.path", c.Request().URL.Path).
				Int("http.status", http.StatusNoContent).
				Str("user_id", "u_1").
				Msg("request_completed")
			return c.NoContent(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			rr := httptest.NewRecorder()
			e.ServeHTTP(rr, req)
		}
	})
}

func BenchmarkRouterFiber(b *testing.B) {
	b.Run("middleware_on_sink_noop", func(b *testing.B) {
		app := fiber.New()
		app.Use(ufiber.Middleware(unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 1})))
		app.Get("/orders/:id", func(c *fiber.Ctx) error {
			unolog.Add(c.UserContext(), "user_id", "u_1")
			return c.SendStatus(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			res, _ := app.Test(req, -1)
			if res != nil {
				_ = res.Body.Close()
			}
		}
	})

	b.Run("normal_logging_slog_noop_handler_no_middleware", func(b *testing.B) {
		logger := slog.New(noopSlogHandler{})
		app := fiber.New()
		app.Get("/orders/:id", func(c *fiber.Ctx) error {
			logger.InfoContext(
				c.UserContext(), "request_completed",
				slog.String("http.method", c.Method()),
				slog.String("http.path", c.Path()),
				slog.Int("http.status", http.StatusNoContent),
				slog.String("user_id", "u_1"),
			)
			return c.SendStatus(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			res, _ := app.Test(req, -1)
			if res != nil {
				_ = res.Body.Close()
			}
		}
	})

	b.Run("normal_logging_slog_json_no_middleware", func(b *testing.B) {
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		app := fiber.New()
		app.Get("/orders/:id", func(c *fiber.Ctx) error {
			logger.InfoContext(
				c.UserContext(), "request_completed",
				slog.String("http.method", c.Method()),
				slog.String("http.path", c.Path()),
				slog.Int("http.status", http.StatusNoContent),
				slog.String("user_id", "u_1"),
			)
			return c.SendStatus(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			res, _ := app.Test(req, -1)
			if res != nil {
				_ = res.Body.Close()
			}
		}
	})

	b.Run("normal_logging_zap_nop_no_middleware", func(b *testing.B) {
		logger := zap.NewNop()
		app := fiber.New()
		app.Get("/orders/:id", func(c *fiber.Ctx) error {
			logger.Info(
				"request_completed",
				zap.String("http.method", c.Method()),
				zap.String("http.path", c.Path()),
				zap.Int("http.status", http.StatusNoContent),
				zap.String("user_id", "u_1"),
			)
			return c.SendStatus(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			res, _ := app.Test(req, -1)
			if res != nil {
				_ = res.Body.Close()
			}
		}
	})

	b.Run("normal_logging_zerolog_nop_no_middleware", func(b *testing.B) {
		logger := zerolog.Nop()
		app := fiber.New()
		app.Get("/orders/:id", func(c *fiber.Ctx) error {
			logger.Info().
				Str("http.method", c.Method()).
				Str("http.path", c.Path()).
				Int("http.status", http.StatusNoContent).
				Str("user_id", "u_1").
				Msg("request_completed")
			return c.SendStatus(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			res, _ := app.Test(req, -1)
			if res != nil {
				_ = res.Body.Close()
			}
		}
	})
}

func BenchmarkRouterFiberV3(b *testing.B) {
	b.Run("middleware_on_sink_noop", func(b *testing.B) {
		app := fiberv3.New()
		app.Use(ufiberv3.Middleware(unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 1})))
		app.Get("/orders/:id", func(c fiberv3.Ctx) error {
			unolog.Add(c.Context(), "user_id", "u_1")
			return c.SendStatus(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			res, _ := app.Test(req, fiberv3.TestConfig{Timeout: -1})
			if res != nil {
				_ = res.Body.Close()
			}
		}
	})

	b.Run("normal_logging_slog_noop_handler_no_middleware", func(b *testing.B) {
		logger := slog.New(noopSlogHandler{})
		app := fiberv3.New()
		app.Get("/orders/:id", func(c fiberv3.Ctx) error {
			logger.InfoContext(
				c.Context(), "request_completed",
				slog.String("http.method", c.Method()),
				slog.String("http.path", c.Path()),
				slog.Int("http.status", http.StatusNoContent),
				slog.String("user_id", "u_1"),
			)
			return c.SendStatus(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			res, _ := app.Test(req, fiberv3.TestConfig{Timeout: -1})
			if res != nil {
				_ = res.Body.Close()
			}
		}
	})

	b.Run("normal_logging_slog_json_no_middleware", func(b *testing.B) {
		logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
		app := fiberv3.New()
		app.Get("/orders/:id", func(c fiberv3.Ctx) error {
			logger.InfoContext(
				c.Context(), "request_completed",
				slog.String("http.method", c.Method()),
				slog.String("http.path", c.Path()),
				slog.Int("http.status", http.StatusNoContent),
				slog.String("user_id", "u_1"),
			)
			return c.SendStatus(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			res, _ := app.Test(req, fiberv3.TestConfig{Timeout: -1})
			if res != nil {
				_ = res.Body.Close()
			}
		}
	})

	b.Run("normal_logging_zap_nop_no_middleware", func(b *testing.B) {
		logger := zap.NewNop()
		app := fiberv3.New()
		app.Get("/orders/:id", func(c fiberv3.Ctx) error {
			logger.Info(
				"request_completed",
				zap.String("http.method", c.Method()),
				zap.String("http.path", c.Path()),
				zap.Int("http.status", http.StatusNoContent),
				zap.String("user_id", "u_1"),
			)
			return c.SendStatus(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			res, _ := app.Test(req, fiberv3.TestConfig{Timeout: -1})
			if res != nil {
				_ = res.Body.Close()
			}
		}
	})

	b.Run("normal_logging_zerolog_nop_no_middleware", func(b *testing.B) {
		logger := zerolog.Nop()
		app := fiberv3.New()
		app.Get("/orders/:id", func(c fiberv3.Ctx) error {
			logger.Info().
				Str("http.method", c.Method()).
				Str("http.path", c.Path()).
				Int("http.status", http.StatusNoContent).
				Str("user_id", "u_1").
				Msg("request_completed")
			return c.SendStatus(http.StatusNoContent)
		})
		req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		b.ReportAllocs()
		for b.Loop() {
			res, _ := app.Test(req, fiberv3.TestConfig{Timeout: -1})
			if res != nil {
				_ = res.Body.Close()
			}
		}
	})
}
