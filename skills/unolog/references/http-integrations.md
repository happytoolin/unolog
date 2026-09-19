# HTTP integrations

Use the package that matches the existing router. Each middleware creates one operation per request and records method, path, resolved route, status, duration, outcome, errors, and panics. Add only business fields in handlers.

Install the root module, the selected logger adapter or JSON sink, and exactly one framework integration.

## net/http

```bash
go get github.com/happytoolin/unolog/integration/std
```

```go
mw := std.Middleware(rt)

mux := http.NewServeMux()
mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, r *http.Request) {
	unolog.Add(r.Context(), "order_id", r.PathValue("id"))
	w.WriteHeader(http.StatusOK)
})

server.Handler = mw(mux)
```

Use `r.Context()` after the middleware. The integration preserves the optional `http.ResponseWriter` interfaces and uses the resolved `Request.Pattern` when available.

## Gin

```bash
go get github.com/happytoolin/unolog/integration/gin
```

```go
r := gin.New()
r.Use(ugin.Middleware(rt))
r.GET("/orders/:id", func(c *gin.Context) {
	unolog.Add(c.Request.Context(), "order_id", c.Param("id"))
	c.Status(http.StatusOK)
})
```

Use `c.Request.Context()`. Gin errors added to `c.Errors` are included. If a handler consumes an error without adding it to Gin, call `unolog.Error`.

## Echo v4

```bash
go get github.com/happytoolin/unolog/integration/echo
```

```go
e := echo.New()
e.Use(uecho.Middleware(rt))
e.GET("/orders/:id", func(c echo.Context) error {
	unolog.Add(c.Request().Context(), "order_id", c.Param("id"))
	return c.NoContent(http.StatusOK)
})
```

Use `c.Request().Context()`. Return handler errors normally. The integration records the error and resolves its HTTP status.

## Fiber v2

```bash
go get github.com/happytoolin/unolog/integration/fiber
```

```go
app := fiber.New()
app.Use(ufiber.Middleware(rt))
app.Get("/orders/:id", func(c *fiber.Ctx) error {
	unolog.Add(c.UserContext(), "order_id", c.Params("id"))
	return c.SendStatus(fiber.StatusOK)
})
```

Use `c.UserContext()`. Do not use a new `context.Background()` inside the handler.

## Fiber v3

```bash
go get github.com/happytoolin/unolog/integration/fiberv3
```

```go
app := fiber.New()
app.Use(ufiberv3.Middleware(rt))
app.Get("/orders/:id", func(c fiber.Ctx) error {
	unolog.Add(c.Context(), "order_id", c.Params("id"))
	return c.SendStatus(fiber.StatusOK)
})
```

Use `c.Context()`. Do not use the Fiber v2 integration with Fiber v3.

## Error and middleware behavior

Register the middleware before the routes it must observe. Keep the application's existing recovery and error-handler setup. The unolog middleware records a panic and re-panics so the normal recovery path still controls the response.

Framework-returned errors and server status codes are captured by the integration. Call `unolog.Error(ctx, err)` only for handled errors that no longer reach it. Do not log the same terminal error again through the host logger.

Use `unolog.SetRoute` only when a custom router does not expose its normalized route. Use `unolog.SetMessage` when a specific final message is more useful than `request_completed`.
