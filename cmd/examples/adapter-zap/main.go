package main

import (
	"errors"
	"net/http"
	"time"

	"github.com/happytoolin/unolog"
	uzap "github.com/happytoolin/unolog/adapter/zap"
	"github.com/happytoolin/unolog/integration/std"
	"go.uber.org/zap"
)

func main() {
	logger := zap.NewExample()
	sink := uzap.New(logger)
	mw := std.Middleware(unolog.MustCompile(unolog.Config{Sink: sink, SamplingRate: 1}))

	mux := http.NewServeMux()
	mux.HandleFunc("/users/{id}", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := r.PathValue("id")

		unolog.Add(ctx, "example", "adapter-zap")
		unolog.Add(
			ctx,
			"user", map[string]any{
				"id":   id,
				"plan": "pro",
			},
			"request", map[string]any{
				"feature": "checkout",
				"tags":    []string{"examples", "zap"},
			},
		)
		unolog.SetRoute(ctx, "/users/{id}")

		if r.URL.Query().Get("debug") == "1" {
			unolog.SetLevel(ctx, unolog.LevelDebug)
			unolog.Add(ctx, "requested_level", unolog.LevelDebug)
		}
		if r.URL.Query().Get("fail") == "1" {
			unolog.Error(ctx, errors.New("demo failure"))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              ":8102",
		Handler:           mw(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	_ = srv.ListenAndServe()
}
