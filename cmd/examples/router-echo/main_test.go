package main

import (
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/happytoolin/unolog"
	uslog "github.com/happytoolin/unolog/adapter/slog"
	uecho "github.com/happytoolin/unolog/integration/echo"
	"github.com/labstack/echo/v4"
)

func TestRouterEchoMiddleware(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	sink := uslog.New(logger)

	e := echo.New()
	e.Use(uecho.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})))
	e.GET("/users/:id", func(c echo.Context) error {
		ctx := c.Request().Context()
		id := c.Param("id")

		unolog.Add(ctx, "router", "echo")
		unolog.Add(
			ctx,
			"user", map[string]any{
				"id":   id,
				"plan": "pro",
			},
			"request", map[string]any{
				"feature": "profile",
				"tags":    []string{"examples", "router-echo"},
			},
		)
		unolog.SetRoute(ctx, "/users/:id")

		if c.QueryParam("debug") == "1" {
			unolog.SetLevel(ctx, unolog.LevelDebug)
			unolog.Add(ctx, "requested_level", unolog.LevelDebug)
		}
		if c.QueryParam("fail") == "1" {
			unolog.Error(ctx, errors.New("demo failure"))
			return c.NoContent(http.StatusInternalServerError)
		}

		return c.NoContent(http.StatusOK)
	})

	t.Run("successful request", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/users/123", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}
	})

	t.Run("request with debug", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/users/123?debug=1", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			t.Errorf("expected status 200, got %d", rec.Code)
		}
	})

	t.Run("request with failure", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/users/123?fail=1", nil)
		rec := httptest.NewRecorder()
		e.ServeHTTP(rec, req)

		if rec.Code != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", rec.Code)
		}
	})
}
