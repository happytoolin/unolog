package main

import (
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/gofiber/fiber/v3"
	"github.com/happytoolin/unolog"
	uslog "github.com/happytoolin/unolog/adapter/slog"
	ufiberv3 "github.com/happytoolin/unolog/integration/fiberv3"
)

func TestRouterFiberv3Middleware(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	sink := uslog.New(logger)

	app := fiber.New()
	app.Use(ufiberv3.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1})))
	app.Get("/users/:id", func(c fiber.Ctx) error {
		ctx := c.Context()
		id := c.Params("id")

		unolog.Add(ctx, "router", "fiber-v3")
		unolog.Add(
			ctx,
			"user", map[string]any{
				"id":   id,
				"plan": "pro",
			},
			"request", map[string]any{
				"feature": "profile",
				"tags":    []string{"examples", "router-fiberv3"},
			},
		)
		unolog.SetRoute(ctx, "/users/:id")

		if c.Query("debug") == "1" {
			unolog.SetLevel(ctx, unolog.LevelDebug)
			unolog.Add(ctx, "requested_level", unolog.LevelDebug)
		}
		if c.Query("fail") == "1" {
			unolog.Error(ctx, errors.New("demo failure"))
			return c.SendStatus(500)
		}

		return c.SendStatus(200)
	})

	t.Run("successful request", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/users/123", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", resp.StatusCode)
		}
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read body: %v", err)
		}
		if err := resp.Body.Close(); err != nil {
			t.Fatalf("close body: %v", err)
		}
		if string(body) != "OK" {
			t.Errorf("expected body 'OK', got %q", string(body))
		}
	})

	t.Run("request with debug", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/users/123?debug=1", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("expected status 200, got %d", resp.StatusCode)
		}
	})

	t.Run("request with failure", func(t *testing.T) {
		req := httptest.NewRequestWithContext(t.Context(), "GET", "/users/123?fail=1", nil)
		resp, err := app.Test(req)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != http.StatusInternalServerError {
			t.Errorf("expected status 500, got %d", resp.StatusCode)
		}
	})
}
