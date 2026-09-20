// Package worker provides the background-job unolog
// lifecycle: Start opens a job operation from JobMeta and returns the
// deferred-End handle.
package worker

import (
	"context"
	"time"

	"github.com/happytoolin/unolog"
)

// JobMeta describes background job execution metadata.
type JobMeta struct {
	Name        string
	ID          string
	Queue       string
	Attempt     int
	MaxAttempts int
	ScheduledAt time.Time
}

// Start initializes a worker operation handle. rt comes from
// unolog.Compile/MustCompile; a nil *unolog.Runtime runs the operation with no
// emission. End the operation with the deferred-error idiom — and
// switch to the operation context, or every unolog.Add below is a silent
// no-op (the original ctx carries no WAL):
//
//	func run(ctx context.Context, rt *unolog.Runtime) (err error) {
//		op := worker.Start(ctx, rt, meta)
//		ctx = op.Context()
//		defer op.End(&err)
//		unolog.Add(ctx, "rows", 42)
//		...
//	}
func Start(ctx context.Context, rt *unolog.Runtime, meta JobMeta) unolog.Operation {
	op := unolog.Start(ctx, rt, unolog.OperationStart{
		Domain:      unolog.DomainJob,
		Name:        meta.Name,
		ID:          meta.ID,
		Source:      meta.Queue,
		Attempt:     meta.Attempt,
		MaxAttempts: meta.MaxAttempts,
	})
	addJobFields(op.Context(), meta)
	return op
}

// addJobFields records only what op.* does not already carry: the
// mirrors (name/id/queue/attempt/max_attempts) were dropped with the
// canonical-field pass; scheduled_at has no op.* equivalent.
func addJobFields(ctx context.Context, meta JobMeta) {
	if !meta.ScheduledAt.IsZero() {
		unolog.Add(ctx, unolog.KeyJobScheduledAt, meta.ScheduledAt.UTC())
	}
}
