package main

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/happytoolin/unolog"
	uslog "github.com/happytoolin/unolog/adapter/slog"
	"github.com/happytoolin/unolog/integration/std"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	sink := uslog.New(logger)

	mw := std.Middleware(unolog.MustCompile(unolog.Config{
		Sink: sink,
		Sampler: func(in unolog.SampleInput) bool {
			if in.HasError || in.StatusCode >= 500 {
				return true
			}
			if in.Duration >= 500*time.Millisecond {
				return true
			}
			v, ok := in.Lookup("user_tier")
			tier, _ := v.(string)
			return ok && tier == "enterprise"
		},
	}))

	mux := http.NewServeMux()
	mux.HandleFunc("/users/{id}", func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		id := r.PathValue("id")
		tier := r.URL.Query().Get("tier")
		if tier == "" {
			tier = "free"
		}

		unolog.Add(ctx, "router", "sampling-custom")
		unolog.Add(ctx, "user_id", id)
		unolog.Add(ctx, "user_tier", tier)
		unolog.SetRoute(ctx, r.Pattern)

		if r.URL.Query().Get("slow") == "1" {
			time.Sleep(650 * time.Millisecond)
		}
		if r.URL.Query().Get("fail") == "1" {
			unolog.Error(ctx, errors.New("demo failure"))
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
	})

	srv := &http.Server{
		Addr:              ":8110",
		Handler:           mw(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	_ = srv.ListenAndServe()
}
