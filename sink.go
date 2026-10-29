package unolog

import "context"

// Sink receives finalized request events as read-only records, following
// the slog.Handler.Handle shape. Implementations must be safe for
// concurrent use and must not retain the record or its bytes past the
// call (copy anything you keep). The ctx is the request's context at
// End time: valid for cancellation observation, but background work
// started with it inherits the request's cancellation.
type Sink interface {
	Write(ctx context.Context, rec *Record)
}

// FallibleSink is a Sink that can report a failed write. When
// Config.Sink implements it, End calls WriteErr instead of Write. A
// non-nil error goes to Config.ErrorHandler (errors.Is matches
// ErrSinkWrite and the returned error), and End returns false.
// JSONSink implements it.
type FallibleSink interface {
	Sink
	WriteErr(ctx context.Context, rec *Record) error
}
