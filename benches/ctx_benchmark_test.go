package benches_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/happytoolin/unolog"
	"github.com/happytoolin/unolog/integration/std"
)

// The ctx form — unolog.Add through op.Context() — is the form an HTTP
// handler uses, because the handler never holds the operation handle.
// The older corpus measured the handle form almost exclusively, so the
// ctx form's own cost was unpinned. These rows pin it.

// BenchmarkScratchHandle1 and BenchmarkScratchHandle12 are the handle
// form of the one-field and twelve-field request.
func BenchmarkScratchHandle1(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
		op.Add("user_id", "u_8472")
		op.End(nil)
	}
}

func BenchmarkScratchHandle12(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
		benchmarkFieldsOp(&op, 12)
		op.End(nil)
	}
}

func BenchmarkScratchCtx1(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
		ctx := op.Context()
		unolog.Add(ctx, "user_id", "u_8472")
		op.End(nil)
	}
}

// BenchmarkScratchCtx12 is twelve separate unolog.Add calls.
func BenchmarkScratchCtx12(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
		ctx := op.Context()
		benchmarkFields(ctx, 12)
		op.End(nil)
	}
}

// BenchmarkScratchCtx12Batch writes the same twelve fields through ONE
// variadic call: one context lookup and one mutex round trip instead of
// twelve. Batching is the ctx form's own speed knob.
func BenchmarkScratchCtx12Batch(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
		ctx := op.Context()
		unolog.Add(ctx,
			"http.method", "GET",
			"http.path", "/api/v1/orders/12345",
			"http.route", "/api/v1/orders/:id",
			"http.status", 200,
			"op.domain", "http",
			"op.name", "GET /api/v1/orders/:id",
			"op.outcome", "success",
			"op.code", 200,
			"duration_ms", 12,
			"request_id", "req_01HZX4T7W8Y3N2M1K0J9Z8X7V6",
			"user_id", "usr_77451",
			"cache.hit", true)
		op.End(nil)
	}
}

// ctxVarString is a package variable: a real handler passes a value it
// read at runtime, so the compiler cannot fold the interface conversion
// into a static box.
var ctxVarString = "u_8472"

// BenchmarkScratchCtxBoxed1 and BenchmarkScratchCtxTyped1 are the same
// one-field write with a non-constant value. The boxed row pays one
// 16-byte allocation for the interface; the typed row does not.
func BenchmarkScratchCtxBoxed1(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
		ctx := op.Context()
		unolog.Add(ctx, "user_id", ctxVarString)
		op.End(nil)
	}
}

func BenchmarkScratchCtxTyped1(b *testing.B) {
	rt := runtimeFor()
	b.ReportAllocs()
	for b.Loop() {
		op := unolog.Start(context.Background(), rt, unolog.OperationStart{Domain: unolog.DomainHTTP, Name: "GET /api/v1/orders/:id"})
		ctx := op.Context()
		unolog.AddString(ctx, "user_id", ctxVarString)
		op.End(nil)
	}
}

// routeUserID is a package variable, so the handler's write boxes.
var routeUserID = "u_1"

// BenchmarkRouterStdRoute is the real-server shape the older router row
// missed: the router resolved a route template, and the handler passes a
// runtime value. FinalizeRequest writes the canonical http.route,
// op.name, and http.status fields.
func BenchmarkRouterStdRoute(b *testing.B) {
	req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
	req.Pattern = "/orders/:id"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		unolog.Add(r.Context(), "user_id", routeUserID)
		w.WriteHeader(http.StatusNoContent)
	})
	mw := std.Middleware(unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 1}))
	wrapped := mw(handler)
	bww := &benchResponseWriter{}
	b.ReportAllocs()
	for b.Loop() {
		bww.status = 0
		wrapped.ServeHTTP(bww, req)
	}
}

// BenchmarkRouterStdRouteTyped is the same request with the handler's
// own write on the typed ctx form — the shape the library recommends.
func BenchmarkRouterStdRouteTyped(b *testing.B) {
	req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/orders/123", nil)
	req.Pattern = "/orders/:id"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		unolog.AddString(r.Context(), "user_id", routeUserID)
		w.WriteHeader(http.StatusNoContent)
	})
	mw := std.Middleware(unolog.MustCompile(unolog.Config{Sink: discardSink{}, SamplingRate: 1}))
	wrapped := mw(handler)
	bww := &benchResponseWriter{}
	b.ReportAllocs()
	for b.Loop() {
		bww.status = 0
		wrapped.ServeHTTP(bww, req)
	}
}
