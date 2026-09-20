// Package fiberv3 provides the Fiber v3 unolog
// middleware: one canonical event per request, with errors, panics,
// status, and route resolved from the Fiber context.
package fiberv3

import (
	"errors"
	"net/http"

	"github.com/gofiber/fiber/v3"
	"github.com/happytoolin/unolog"
	"github.com/happytoolin/unolog/integration/flow"
)

// Middleware returns a Fiber v3 middleware that captures one event per request.
func Middleware(rt *unolog.Runtime) fiber.Handler {
	if rt == nil {
		return func(c fiber.Ctx) error {
			return c.Next()
		}
	}

	return func(c fiber.Ctx) (err error) {
		op, ctx := flow.StartRequest(c.Context(), rt, c.Method(), c.Path())
		c.SetContext(ctx)
		var finalizeErr error

		defer func() {
			recovered := recover()
			routePath := ""
			if route := c.Route(); route != nil && route.Path != "" {
				routePath = route.Path
			}
			status := c.Response().StatusCode()
			// Fiber's response struct defaults its status to 200 and does
			// not track whether the handler committed anything, so "the
			// response started" must be inferred: any status other than
			// the 200 default, or a non-empty body under 200, means bytes
			// or a status actually went out. A bare 200 with no body is
			// treated as not started so a pre-response panic still
			// resolves to 500. Do not "simplify" this — it is the
			// panic-vs-committed-status contract.
			responseStarted := status != 0 && (status != http.StatusOK || len(c.Response().Body()) > 0)
			status = flow.ResolveStatus(flow.StatusInput{
				Committed:       status,
				Err:             finalizeErr,
				Recovered:       recovered,
				ResponseStarted: responseStarted,
				ErrorStatus:     statusFromFiberError(finalizeErr),
			})
			flow.FinalizeRequest(&op, ctx, routePath, status, finalizeErr, recovered)

			if recovered != nil {
				panic(recovered)
			}
		}()

		err = c.Next()
		finalizeErr = err
		if err != nil {
			if errorHandler := c.App().Config().ErrorHandler; errorHandler != nil {
				if handlerErr := errorHandler(c, err); handlerErr != nil {
					// Error handler failed; capture this as the final error
					finalizeErr = handlerErr
					err = handlerErr
					return err
				}
			}
			err = nil
		}
		return err
	}
}

// statusFromFiberError extracts the HTTP status code from a Fiber error.
// Duplicated from the fiber v2 middleware: the Error type is compatible,
// but the context types are not.
func statusFromFiberError(err error) int {
	if err == nil {
		return 0
	}
	var fiberErr *fiber.Error
	if errors.As(err, &fiberErr) {
		return fiberErr.Code
	}
	return http.StatusInternalServerError
}
