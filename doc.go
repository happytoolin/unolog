// Package unolog emits one structured, canonical event per request: Start
// attaches a write-ahead log to the context, the Add helpers annotate
// it anywhere the enriched context flows, and the deferred End commits
// exactly one record through a Sink (slog/zap/zerolog bridges, the
// first-party JSONSink and AsyncJSONSink, or a custom writer).
//
// Configuration is compiled once into an immutable *Runtime with
// Compile (bad configuration is a construction-time error) and shared
// by all requests. Sampling drops healthy traffic by rate or custom
// Sampler; error and panic events always bypass it.
//
// Config.MaxFields bounds the distinct keys of an event and
// Config.MaxValueBytes bounds its string values; both have defaults and
// can be removed. A panicking Sink or Sampler is contained and reported
// to Config.ErrorHandler instead of crashing the request.
package unolog
