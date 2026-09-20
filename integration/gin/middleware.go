// Package gin provides the Gin unolog middleware: one
// canonical event per request, with errors, panics, status, and route
// resolved from the Gin context.
package gin

import (
	gogin "github.com/gin-gonic/gin"
	"github.com/happytoolin/unolog"
	"github.com/happytoolin/unolog/integration/flow"
)

// Middleware returns a Gin middleware that captures one event per
// request. rt comes from unolog.Compile/MustCompile; nil is a passthrough.
func Middleware(rt *unolog.Runtime) gogin.HandlerFunc {
	if rt == nil {
		return func(c *gogin.Context) {
			c.Next()
		}
	}

	return func(c *gogin.Context) {
		op, ctx := flow.StartRequest(c.Request.Context(), rt, c.Request.Method, c.Request.URL.Path)
		flow.AttachRequestContext(c.Request, ctx)

		defer func() {
			recovered := recover()
			var err error
			if len(c.Errors) > 0 {
				if last := c.Errors.Last(); last != nil {
					err = last.Err
				}
			}
			status := flow.ResolveStatus(flow.StatusInput{
				Committed:       c.Writer.Status(),
				Err:             err,
				Recovered:       recovered,
				ResponseStarted: c.Writer.Written(),
			})
			flow.FinalizeRequest(&op, ctx, c.FullPath(), status, err, recovered)

			if recovered != nil {
				panic(recovered)
			}
		}()

		c.Next()
	}
}
