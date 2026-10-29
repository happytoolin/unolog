// Package unolog emits one structured, canonical event per request: Start
// attaches a write-ahead log to the context, the Add helpers annotate
// it anywhere the enriched context flows, and the deferred End commits
// exactly one record through a Sink (slog/zap/zerolog bridges, the
// first-party JSONSink, or a custom writer).
//
// Configuration is compiled once into an immutable *Runtime with
// Compile (bad configuration is a construction-time error) and shared
// by all requests. Sampling drops healthy traffic by rate or custom
// Sampler; error and panic events always bypass it.
//
// Every event is bounded (Config.MaxFields, Config.MaxValueBytes, with
// defaults), and a panicking Sink or Sampler is contained and reported
// to Config.ErrorHandler instead of crashing the request.
package unolog
