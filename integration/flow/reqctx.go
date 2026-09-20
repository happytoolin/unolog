package flow

import (
	"context"
	"net/http"
	"reflect"
	"unsafe"
)

// reqCtxOffset is the byte offset of net/http.Request's unexported ctx
// field, resolved once. Zero means the field was not found (a future Go
// renames it), in which case AttachRequestContext falls back to the
// allocating WithContext.
var reqCtxOffset = func() uintptr {
	f, ok := reflect.TypeFor[http.Request]().FieldByName("ctx")
	if !ok {
		return 0
	}
	return f.Offset
}()

// AttachRequestContext returns r carrying ctx. It sets the ctx field in
// place, avoiding the shallow Request copy that WithContext allocates
// (~1 alloc and ~200 B per request). The middleware owns r at this
// point — no other goroutine holds it — so an in-place write is race
// free; the store goes through a normal pointer assignment, so the GC
// write barrier still applies.
func AttachRequestContext(r *http.Request, ctx context.Context) *http.Request {
	if reqCtxOffset == 0 {
		return r.WithContext(ctx)
	}
	// The store replaces the request's ctx in place; nothing nests.
	p := unsafe.Add(unsafe.Pointer(r), reqCtxOffset)
	*(*context.Context)(p) = ctx //nolint:fatcontext // in-place replacement, not a nested chain
	return r
}
