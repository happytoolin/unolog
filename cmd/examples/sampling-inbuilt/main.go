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
		Sampler: unolog.ChainSampler(
			unolog.RateSampler(0.05),
			unolog.KeepErrors(),
			unolog.KeepPathPrefix("/users/vip"),
			unolog.KeepSlowerThan(250*time.Millisecond),
		),
	}))

	mux := http.NewServeMux()
	mux.HandleFunc("/users/{id}", func(w http.ResponseWriter, r *http.Request) {
		handleUser(w, r, "standard")
	})
	mux.HandleFunc("/users/vip/{id}", func(w http.ResponseWriter, r *http.Request) {
		handleUser(w, r, "vip")
	})

	srv := &http.Server{
		Addr:              ":8109",
		Handler:           mw(mux),
		ReadHeaderTimeout: 5 * time.Second,
	}
	_ = srv.ListenAndServe()
}

func handleUser(w http.ResponseWriter, r *http.Request, tier string) {
	ctx := r.Context()
	id := r.PathValue("id")

	unolog.Add(ctx, "router", "sampling-inbuilt")
	unolog.Add(ctx, "user_id", id)
	unolog.Add(ctx, "user_tier", tier)
	unolog.SetRoute(ctx, r.Pattern)

	if r.URL.Query().Get("slow") == "1" {
		time.Sleep(350 * time.Millisecond)
	}
	if r.URL.Query().Get("fail") == "1" {
		unolog.Error(ctx, errors.New("demo failure"))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
}
