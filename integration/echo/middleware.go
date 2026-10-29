// Package echo provides the Echo unolog middleware:
// one canonical event per request, with errors, panics, status, and
// route resolved from the Echo context.
package echo

import (
	"errors"
	"net/http"

	"github.com/happytoolin/unolog"
	"github.com/happytoolin/unolog/integration/flow"
	goecho "github.com/labstack/echo/v4"
)

// Middleware returns an Echo middleware that captures one event per
// request. rt comes from unolog.Compile/MustCompile; nil is a passthrough.
func Middleware(rt *unolog.Runtime) goecho.MiddlewareFunc {
	if rt == nil {
		return func(next goecho.HandlerFunc) goecho.HandlerFunc {
			return func(c goecho.Context) error {
				return next(c)
			}
		}
	}

	return func(next goecho.HandlerFunc) goecho.HandlerFunc {
		return func(c goecho.Context) (err error) {
			op := flow.StartRequest(c.Request().Context(), rt, c.Request().Method, c.Request().URL.Path)
			c.SetRequest(c.Request().WithContext(op.Context()))
			var finalizeErr error

			defer func() {
				recovered := recover()
				route := c.Path()
				status := flow.ResolveStatus(flow.StatusInput{
					Committed:       c.Response().Status,
					Err:             finalizeErr,
					Recovered:       recovered,
					ResponseStarted: c.Response().Committed,
					ErrorStatus:     statusFromEchoError(finalizeErr),
				})
				flow.FinalizeRequest(op, route, status, finalizeErr, recovered)

				if recovered != nil {
					panic(recovered)
				}
			}()

			err = next(c)
			finalizeErr = err
			if err != nil && !c.Response().Committed {
				c.Error(err)
				err = nil
			}
			return err
		}
	}
}

func statusFromEchoError(err error) int {
	if err == nil {
		return 0
	}
	if httpErr, ok := errors.AsType[*goecho.HTTPError](err); ok {
		return httpErr.Code
	}
	return http.StatusInternalServerError
}
