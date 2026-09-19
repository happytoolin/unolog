package benches_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gofiber/fiber/v2"
	fiberv3 "github.com/gofiber/fiber/v3"
	"github.com/happytoolin/unolog"
	uslog "github.com/happytoolin/unolog/adapter/slog"
	uzap "github.com/happytoolin/unolog/adapter/zap"
	uzerolog "github.com/happytoolin/unolog/adapter/zerolog"
	uecho "github.com/happytoolin/unolog/integration/echo"
	ufiber "github.com/happytoolin/unolog/integration/fiber"
	ufiberv3 "github.com/happytoolin/unolog/integration/fiberv3"
	ugin "github.com/happytoolin/unolog/integration/gin"
	ustd "github.com/happytoolin/unolog/integration/std"
	"github.com/labstack/echo/v4"
	"github.com/rs/zerolog"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	benchRequestID   = "req_01K59MF6Y4T7W8Y3N2M1K0J9Z8"
	benchUserID      = "usr_77451"
	benchTenant      = "enterprise"
	benchWarehouse   = "bom-2"
	benchPaymentID   = "pay_94820"
	benchProvider    = "stripe"
	benchSuccessJSON = `{"id":"123","status":"paid"}`
	benchFailureJSON = `{"error":"payment unavailable"}`
)

var errBenchPayment = errors.New("payment provider timed out")

// routeLogger makes every router run the same six-step request workload.
// Manual loggers emit at each step; unolog records the same fields and emits
// once when its router middleware finalizes the request.
type routeLogger struct {
	requestReceived   func(context.Context, string, string)
	userAuthorized    func(context.Context)
	orderLoaded       func(context.Context, string)
	inventoryReserved func(context.Context)
	paymentAuthorized func(context.Context)
	requestCompleted  func(context.Context, int)
	requestFailed     func(context.Context, error, int)
}

func runOrderRoute(ctx context.Context, logger routeLogger, method, path, orderID string, failure error) int {
	logger.requestReceived(ctx, method, path)
	logger.userAuthorized(ctx)
	logger.orderLoaded(ctx, orderID)
	logger.inventoryReserved(ctx)
	if failure != nil {
		logger.requestFailed(ctx, failure, http.StatusServiceUnavailable)
		return http.StatusServiceUnavailable
	}
	logger.paymentAuthorized(ctx)
	logger.requestCompleted(ctx, http.StatusOK)
	return http.StatusOK
}

func unologRouteLogger() routeLogger {
	return routeLogger{
		requestReceived: func(ctx context.Context, _, _ string) {
			unolog.Add(ctx, "request_id", benchRequestID)
		},
		userAuthorized: func(ctx context.Context) {
			unolog.Add(ctx, "user_id", benchUserID, "tenant", benchTenant)
		},
		orderLoaded: func(ctx context.Context, orderID string) {
			unolog.Add(ctx, "order_id", orderID, "item_count", 3, "total_cents", 12450)
		},
		inventoryReserved: func(ctx context.Context) {
			unolog.Add(ctx, "warehouse", benchWarehouse, "reserved_units", 3)
		},
		paymentAuthorized: func(ctx context.Context) {
			unolog.Add(ctx, "payment_provider", benchProvider, "payment_id", benchPaymentID)
		},
		requestCompleted: func(ctx context.Context, _ int) {
			unolog.SetMessage(ctx, "checkout_completed")
		},
		requestFailed: func(ctx context.Context, err error, _ int) {
			unolog.Add(ctx, "payment_provider", benchProvider, "retryable", true)
			unolog.Error(ctx, err)
			unolog.SetMessage(ctx, "checkout_failed")
		},
	}
}

func slogRouteLogger(logger *slog.Logger) routeLogger {
	return routeLogger{
		requestReceived: func(ctx context.Context, method, path string) {
			logger.InfoContext(ctx, "request_received",
				slog.String("request_id", benchRequestID),
				slog.String("http.method", method),
				slog.String("http.path", path))
		},
		userAuthorized: func(ctx context.Context) {
			logger.InfoContext(ctx, "user_authorized",
				slog.String("request_id", benchRequestID),
				slog.String("user_id", benchUserID),
				slog.String("tenant", benchTenant))
		},
		orderLoaded: func(ctx context.Context, orderID string) {
			logger.InfoContext(ctx, "order_loaded",
				slog.String("request_id", benchRequestID),
				slog.String("order_id", orderID),
				slog.Int("item_count", 3),
				slog.Int("total_cents", 12450))
		},
		inventoryReserved: func(ctx context.Context) {
			logger.InfoContext(ctx, "inventory_reserved",
				slog.String("request_id", benchRequestID),
				slog.String("warehouse", benchWarehouse),
				slog.Int("reserved_units", 3))
		},
		paymentAuthorized: func(ctx context.Context) {
			logger.InfoContext(ctx, "payment_authorized",
				slog.String("request_id", benchRequestID),
				slog.String("payment_provider", benchProvider),
				slog.String("payment_id", benchPaymentID))
		},
		requestCompleted: func(ctx context.Context, status int) {
			logger.InfoContext(ctx, "request_completed",
				slog.String("request_id", benchRequestID),
				slog.Int("http.status", status))
		},
		requestFailed: func(ctx context.Context, err error, status int) {
			logger.ErrorContext(ctx, "checkout_failed",
				slog.String("request_id", benchRequestID),
				slog.String("payment_provider", benchProvider),
				slog.Bool("retryable", true),
				slog.Int("http.status", status),
				slog.Any("error", err))
		},
	}
}

func zapRouteLogger(logger *zap.Logger) routeLogger {
	return routeLogger{
		requestReceived: func(_ context.Context, method, path string) {
			logger.Info("request_received",
				zap.String("request_id", benchRequestID),
				zap.String("http.method", method),
				zap.String("http.path", path))
		},
		userAuthorized: func(context.Context) {
			logger.Info("user_authorized",
				zap.String("request_id", benchRequestID),
				zap.String("user_id", benchUserID),
				zap.String("tenant", benchTenant))
		},
		orderLoaded: func(_ context.Context, orderID string) {
			logger.Info("order_loaded",
				zap.String("request_id", benchRequestID),
				zap.String("order_id", orderID),
				zap.Int("item_count", 3),
				zap.Int("total_cents", 12450))
		},
		inventoryReserved: func(context.Context) {
			logger.Info("inventory_reserved",
				zap.String("request_id", benchRequestID),
				zap.String("warehouse", benchWarehouse),
				zap.Int("reserved_units", 3))
		},
		paymentAuthorized: func(context.Context) {
			logger.Info("payment_authorized",
				zap.String("request_id", benchRequestID),
				zap.String("payment_provider", benchProvider),
				zap.String("payment_id", benchPaymentID))
		},
		requestCompleted: func(_ context.Context, status int) {
			logger.Info("request_completed",
				zap.String("request_id", benchRequestID),
				zap.Int("http.status", status))
		},
		requestFailed: func(_ context.Context, err error, status int) {
			logger.Error("checkout_failed",
				zap.String("request_id", benchRequestID),
				zap.String("payment_provider", benchProvider),
				zap.Bool("retryable", true),
				zap.Int("http.status", status),
				zap.Error(err))
		},
	}
}

func zerologRouteLogger(logger *zerolog.Logger) routeLogger {
	return routeLogger{
		requestReceived: func(_ context.Context, method, path string) {
			logger.Info().
				Str("request_id", benchRequestID).
				Str("http.method", method).
				Str("http.path", path).
				Msg("request_received")
		},
		userAuthorized: func(context.Context) {
			logger.Info().
				Str("request_id", benchRequestID).
				Str("user_id", benchUserID).
				Str("tenant", benchTenant).
				Msg("user_authorized")
		},
		orderLoaded: func(_ context.Context, orderID string) {
			logger.Info().
				Str("request_id", benchRequestID).
				Str("order_id", orderID).
				Int("item_count", 3).
				Int("total_cents", 12450).
				Msg("order_loaded")
		},
		inventoryReserved: func(context.Context) {
			logger.Info().
				Str("request_id", benchRequestID).
				Str("warehouse", benchWarehouse).
				Int("reserved_units", 3).
				Msg("inventory_reserved")
		},
		paymentAuthorized: func(context.Context) {
			logger.Info().
				Str("request_id", benchRequestID).
				Str("payment_provider", benchProvider).
				Str("payment_id", benchPaymentID).
				Msg("payment_authorized")
		},
		requestCompleted: func(_ context.Context, status int) {
			logger.Info().
				Str("request_id", benchRequestID).
				Int("http.status", status).
				Msg("request_completed")
		},
		requestFailed: func(_ context.Context, err error, status int) {
			logger.Error().
				Str("request_id", benchRequestID).
				Str("payment_provider", benchProvider).
				Bool("retryable", true).
				Int("http.status", status).
				Err(err).
				Msg("checkout_failed")
		},
	}
}

type loggerBackend struct {
	name       string
	manual     func(io.Writer) routeLogger
	unologSink func(io.Writer) unolog.Sink
}

func loggerBackends() []loggerBackend {
	return []loggerBackend{
		{
			name: "slog",
			manual: func(output io.Writer) routeLogger {
				return slogRouteLogger(slog.New(slog.NewJSONHandler(output, nil)))
			},
			unologSink: func(output io.Writer) unolog.Sink {
				return uslog.New(slog.New(slog.NewJSONHandler(output, nil)))
			},
		},
		{
			name: "zap",
			manual: func(output io.Writer) routeLogger {
				return zapRouteLogger(newBenchZapLogger(output))
			},
			unologSink: func(output io.Writer) unolog.Sink {
				return uzap.New(newBenchZapLogger(output))
			},
		},
		{
			name: "zerolog",
			manual: func(output io.Writer) routeLogger {
				logger := zerolog.New(output).With().Timestamp().Logger()
				return zerologRouteLogger(&logger)
			},
			unologSink: func(output io.Writer) unolog.Sink {
				logger := zerolog.New(output).With().Timestamp().Logger()
				return uzerolog.NewWithLoggerTimestamp(&logger)
			},
		},
	}
}

func newBenchZapLogger(output io.Writer) *zap.Logger {
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		zapcore.AddSync(output),
		zapcore.DebugLevel,
	)
	return zap.New(core)
}

type benchmarkLogWriter struct {
	file   *os.File
	bytes  uint64
	writes uint64
}

func newBenchmarkLogWriter(b *testing.B) *benchmarkLogWriter {
	b.Helper()
	file, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := file.Close(); err != nil {
			b.Error(err)
		}
	})
	return &benchmarkLogWriter{file: file}
}

func (w *benchmarkLogWriter) Write(p []byte) (int, error) {
	n, err := w.file.Write(p)
	w.bytes += uint64(n)
	w.writes++
	return n, err
}

func (w *benchmarkLogWriter) reset() {
	w.bytes = 0
	w.writes = 0
}

type routeRunner func(*http.Request) int

type routerBackend struct {
	name  string
	build func(routeLogger, *unolog.Runtime) routeRunner
}

func BenchmarkRealisticRoutes(b *testing.B) {
	gin.SetMode(gin.TestMode)
	for _, router := range routerBackends() {
		b.Run("router="+router.name, func(b *testing.B) {
			for _, backend := range loggerBackends() {
				b.Run("logger="+backend.name, func(b *testing.B) {
					b.Run("style=manual", func(b *testing.B) {
						output := newBenchmarkLogWriter(b)
						benchmarkRouteCases(b, router.build(backend.manual(output), nil), output)
					})
					b.Run("style=unolog", func(b *testing.B) {
						output := newBenchmarkLogWriter(b)
						rt := unolog.MustCompile(unolog.Config{Sink: backend.unologSink(output), SamplingRate: 1})
						benchmarkRouteCases(b, router.build(unologRouteLogger(), rt), output)
					})
				})
			}
		})
	}
}

func benchmarkRouteCases(b *testing.B, run routeRunner, output *benchmarkLogWriter) {
	b.Helper()
	cases := []struct {
		name, method, path string
		status             int
	}{
		{name: "success", method: http.MethodGet, path: "/orders/123", status: http.StatusOK},
		{name: "failure", method: http.MethodPost, path: "/orders/123/checkout", status: http.StatusServiceUnavailable},
	}
	for _, tc := range cases {
		b.Run("route="+tc.name, func(b *testing.B) {
			req := httptest.NewRequestWithContext(b.Context(), tc.method, tc.path, nil)
			if status := run(req); status != tc.status {
				b.Fatalf("status = %d, want %d", status, tc.status)
			}
			output.reset()
			b.ReportAllocs()
			for b.Loop() {
				_ = run(req)
			}
			reportLogMetrics(b, output)
		})
	}

	b.Run("route=mixed_95_5", func(b *testing.B) {
		success := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
		failure := httptest.NewRequestWithContext(b.Context(), http.MethodPost, "/orders/123/checkout", nil)
		if status := run(success); status != http.StatusOK {
			b.Fatalf("success status = %d, want %d", status, http.StatusOK)
		}
		if status := run(failure); status != http.StatusServiceUnavailable {
			b.Fatalf("failure status = %d, want %d", status, http.StatusServiceUnavailable)
		}
		output.reset()
		iteration := 0
		b.ReportAllocs()
		for b.Loop() {
			req := success
			if iteration%20 == 19 {
				req = failure
			}
			_ = run(req)
			iteration++
		}
		reportLogMetrics(b, output)
	})
}

func reportLogMetrics(b *testing.B, output *benchmarkLogWriter) {
	b.Helper()
	b.ReportMetric(float64(output.bytes)/float64(b.N), "log-bytes/op")
	b.ReportMetric(float64(output.writes)/float64(b.N), "log-writes/op")
}

func routerBackends() []routerBackend {
	return []routerBackend{
		{name: "std", build: buildStdRouter},
		{name: "gin", build: buildGinRouter},
		{name: "echo", build: buildEchoRouter},
		{name: "fiber_v2", build: buildFiberV2Router},
		{name: "fiber_v3", build: buildFiberV3Router},
	}
}

func buildStdRouter(logger routeLogger, rt *unolog.Runtime) routeRunner {
	mux := http.NewServeMux()
	handler := func(failure error) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			status := runOrderRoute(r.Context(), logger, r.Method, r.URL.Path, r.PathValue("id"), failure)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(status)
			if status == http.StatusOK {
				_, _ = io.WriteString(w, benchSuccessJSON)
				return
			}
			_, _ = io.WriteString(w, benchFailureJSON)
		}
	}
	mux.Handle("GET /orders/{id}", handler(nil))
	mux.Handle("POST /orders/{id}/checkout", handler(errBenchPayment))
	var h http.Handler = mux
	if rt != nil {
		h = ustd.Middleware(rt)(h)
	}
	return func(req *http.Request) int {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec.Code
	}
}

func buildGinRouter(logger routeLogger, rt *unolog.Runtime) routeRunner {
	router := gin.New()
	if rt != nil {
		router.Use(ugin.Middleware(rt))
	}
	handler := func(failure error) gin.HandlerFunc {
		return func(c *gin.Context) {
			status := runOrderRoute(c.Request.Context(), logger, c.Request.Method, c.Request.URL.Path, c.Param("id"), failure)
			if status == http.StatusOK {
				c.Data(status, "application/json", []byte(benchSuccessJSON))
				return
			}
			c.Data(status, "application/json", []byte(benchFailureJSON))
		}
	}
	router.GET("/orders/:id", handler(nil))
	router.POST("/orders/:id/checkout", handler(errBenchPayment))
	return func(req *http.Request) int {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code
	}
}

func buildEchoRouter(logger routeLogger, rt *unolog.Runtime) routeRunner {
	router := echo.New()
	if rt != nil {
		router.Use(uecho.Middleware(rt))
	}
	handler := func(failure error) echo.HandlerFunc {
		return func(c echo.Context) error {
			request := c.Request()
			status := runOrderRoute(request.Context(), logger, request.Method, request.URL.Path, c.Param("id"), failure)
			if status == http.StatusOK {
				return c.Blob(status, "application/json", []byte(benchSuccessJSON))
			}
			return c.Blob(status, "application/json", []byte(benchFailureJSON))
		}
	}
	router.GET("/orders/:id", handler(nil))
	router.POST("/orders/:id/checkout", handler(errBenchPayment))
	return func(req *http.Request) int {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		return rec.Code
	}
}

func buildFiberV2Router(logger routeLogger, rt *unolog.Runtime) routeRunner {
	router := fiber.New(fiber.Config{DisableStartupMessage: true})
	if rt != nil {
		router.Use(ufiber.Middleware(rt))
	}
	handler := func(failure error) fiber.Handler {
		return func(c *fiber.Ctx) error {
			status := runOrderRoute(c.UserContext(), logger, c.Method(), c.Path(), c.Params("id"), failure)
			c.Status(status).Type("json")
			if status == http.StatusOK {
				return c.SendString(benchSuccessJSON)
			}
			return c.SendString(benchFailureJSON)
		}
	}
	router.Get("/orders/:id", handler(nil))
	router.Post("/orders/:id/checkout", handler(errBenchPayment))
	return func(req *http.Request) int {
		res, err := router.Test(req, -1)
		if err != nil {
			return 0
		}
		status := res.StatusCode
		_ = res.Body.Close()
		return status
	}
}

func buildFiberV3Router(logger routeLogger, rt *unolog.Runtime) routeRunner {
	router := fiberv3.New()
	if rt != nil {
		router.Use(ufiberv3.Middleware(rt))
	}
	handler := func(failure error) fiberv3.Handler {
		return func(c fiberv3.Ctx) error {
			status := runOrderRoute(c.Context(), logger, c.Method(), c.Path(), c.Params("id"), failure)
			c.Status(status).Type("json")
			if status == http.StatusOK {
				return c.SendString(benchSuccessJSON)
			}
			return c.SendString(benchFailureJSON)
		}
	}
	router.Get("/orders/:id", handler(nil))
	router.Post("/orders/:id/checkout", handler(errBenchPayment))
	return func(req *http.Request) int {
		res, err := router.Test(req, fiberv3.TestConfig{Timeout: -1})
		if err != nil {
			return 0
		}
		status := res.StatusCode
		_ = res.Body.Close()
		return status
	}
}
