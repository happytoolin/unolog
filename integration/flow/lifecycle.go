// Package common carries the framework-agnostic pieces of the HTTP
// integrations: request start/finalize on the v2 lifecycle plus status
// resolution. Framework middlewares stay thin adapters over these.
package flow

import (
	"context"
	"fmt"
	"net/http"

	"github.com/happytoolin/unolog"
)

// StartRequest opens the request lifecycle: it attaches the WAL to the
// request context and records the base HTTP fields. It returns the
// operation handle by value, so the middleware keeps it on its own stack,
// plus the context node that carries the WAL to unolog.Add. Only the node
// escapes into the request; the handle never does.
func StartRequest(baseCtx context.Context, rt *unolog.Runtime, method, path string) (unolog.Operation, context.Context) {
	op := unolog.Start(baseCtx, rt, unolog.OperationStart{
		Domain: unolog.DomainHTTP,
		Name:   "request",
	})
	ctx := op.Context()
	// Handle writes (not unolog.Add(ctx, …)): no context lookup and no
	// variadic []any allocation per call. Nothing else holds ctx yet, so
	// the unguarded single-goroutine handle path is safe here.
	op.AddString(unolog.KeyHTTPMethod, method)
	op.AddString(unolog.KeyHTTPPath, path)
	return op, ctx
}

// FinalizeRequest records the resolved route and status, then commits
// the single event via op.End.
//
// recovered is the panic the middleware's defer captured (nil in the
// normal case). Because the middleware — not End — performed the
// recover, the panic reaches the event through the public write API:
// the canonical panic field, an error field carrying "panic: …", and
// an explicit op.outcome=panic (End is called with a nil error so the
// explicit outcome wins). The resulting wire fields are identical to
// End's own panic handling. The non-panic error path needs no explicit
// unolog.Error: End(&err) records it.
func FinalizeRequest(op *unolog.Operation, ctx context.Context, route string, statusCode int, err error, recovered any) {
	if op == nil {
		return
	}
	if route != "" {
		// Handle writes, not unolog.Add(ctx, …): each ctx write pays a
		// guarded acquisition (event mutex) that the middleware does not
		// need. The middleware owns op and holds it on its own stack for
		// the whole request, and finalize runs on the request goroutine
		// after the handler returned, so no other writer can be live
		// (mixing the handle and ctx forms from separate goroutines is
		// unsupported). Three guarded writes cost ~46 ns; the handle
		// form costs none. (v0 parity: op.name is the resolved route
		// template, last-write-wins over StartRequest's "request".)
		op.AddString(unolog.KeyHTTPRoute, route)
		op.AddString(unolog.KeyOpName, route)
		op.AddInt(unolog.KeyHTTPStatus, int64(statusCode))
	} else {
		op.AddInt(unolog.KeyHTTPStatus, int64(statusCode))
	}

	if recovered != nil {
		unolog.Add(ctx, unolog.KeyPanic, PanicField(recovered))
		// The real error (if any) wins over the synthetic panic one.
		if err != nil {
			unolog.Error(ctx, err)
		} else {
			unolog.Error(ctx, fmt.Errorf("panic: %v", recovered))
		}
		unolog.Add(ctx, unolog.KeyOpOutcome, string(unolog.OutcomePanic))
		_ = op.End(nil)
		return
	}
	// No explicit unolog.Error on the error path: End(&err) writes the
	// canonical error field itself (annotateOperationFailures).
	_ = op.End(&err)
}

// PanicField builds the canonical panic field value (type + value),
// matching the core's structured panic shape.
func PanicField(recovered any) map[string]any {
	return map[string]any{
		"type":  fmt.Sprintf("%T", recovered),
		"value": fmt.Sprint(recovered),
	}
}

// StatusInput carries what a middleware knows when it finalizes: the
// status the framework writer currently holds, the terminal error and
// recovered panic, whether the response already started, and the HTTP
// status the error implies (0 if none).
type StatusInput struct {
	Committed       int
	Err             error
	Recovered       any
	ResponseStarted bool
	ErrorStatus     int
}

// ResolveStatus determines the final HTTP status to log from in:
// panics and handler errors before the response started resolve to a
// 5xx (the error's own status when it carries one), an unwritten
// writer resolves to 200, and a committed status stands.
func ResolveStatus(in StatusInput) int {
	if in.Recovered != nil && !in.ResponseStarted {
		return http.StatusInternalServerError
	}

	if in.Err != nil && !in.ResponseStarted {
		if in.ErrorStatus >= 400 {
			return in.ErrorStatus
		}
		return http.StatusInternalServerError
	}

	if in.Committed == 0 {
		return http.StatusOK
	}

	return in.Committed
}
