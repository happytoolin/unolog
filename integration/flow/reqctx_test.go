package flow

import (
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
)

type ctxKey struct{}

func TestAttachRequestContext(t *testing.T) {
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/orders/1", nil)
	base := r.Context()
	ctx := context.WithValue(base, ctxKey{}, "v")

	if got := AttachRequestContext(r, ctx); got != r {
		t.Fatalf("AttachRequestContext returned a copy, want the same request")
	}
	if r.Context().Value(ctxKey{}) != "v" {
		t.Fatalf("context not attached in place")
	}

	// Move memory so a missing GC write barrier would surface as a
	// collected context.
	runtime.GC()
	junk := make([]any, 0, 1<<16)
	for range 1 << 16 {
		junk = append(junk, make([]byte, 64))
	}
	runtime.GC()
	runtime.KeepAlive(junk)

	if r.Context().Value(ctxKey{}) != "v" {
		t.Fatalf("context value lost across GC")
	}
	if err := r.Context().Err(); err != nil {
		t.Fatalf("Err() = %v, want nil", err)
	}
}

func TestAttachRequestContextFallback(t *testing.T) {
	saved := reqCtxOffset
	reqCtxOffset = 0
	defer func() { reqCtxOffset = saved }()

	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/", nil)
	ctx := context.WithValue(r.Context(), ctxKey{}, "v")
	got := AttachRequestContext(r, ctx)
	if got == r {
		t.Fatalf("fallback must return WithContext's copy")
	}
	if got.Context().Value(ctxKey{}) != "v" {
		t.Fatalf("fallback lost the context")
	}
}
