# Background and custom operations

Use the worker integration for jobs. Use the core lifecycle for message consumers, CLI commands, and other operations.

## Worker jobs

```bash
go get github.com/happytoolin/unolog/integration/worker
```

```go
func runJob(ctx context.Context, rt *unolog.Runtime, meta worker.JobMeta) (err error) {
	op := worker.Start(ctx, rt, meta)
	defer op.End(&err)

	ctx = op.Context()
	unolog.Add(ctx, "tenant_id", tenantID, "rows", rowCount)
	return process(ctx)
}
```

Fill the metadata that the queue provides:

```go
meta := worker.JobMeta{
	Name:        "billing.reconcile",
	ID:          job.ID,
	Queue:       job.Queue,
	Attempt:     job.Attempt,
	MaxAttempts: job.MaxAttempts,
	ScheduledAt: job.ScheduledAt,
}
```

Use the operation context for all unolog calls and downstream work. The original context does not carry the event.

The defer must be the direct form `defer op.End(&err)`. Do not wrap it in a closure. The direct form lets `End` capture a panic, write the event, and re-panic.

## Message consumers and CLI commands

Use `unolog.Start` when no integration matches:

```go
func consume(ctx context.Context, rt *unolog.Runtime, msg Message) (err error) {
	op := unolog.Start(ctx, rt, unolog.OperationStart{
		Domain:  unolog.DomainMessage,
		Name:    "invoice.received",
		ID:      msg.ID,
		Source:  msg.Topic,
		Attempt: msg.Attempt,
	})
	defer op.End(&err)

	ctx = op.Context()
	unolog.Add(ctx, "account_id", msg.AccountID)
	return handle(ctx, msg)
}
```

Use `unolog.DomainCLI` for commands and `unolog.DomainJob` for jobs without the worker metadata helper. The zero domain is a generic operation.

## Completion and failures

Return errors when the caller already expects them. `End(&err)` records returned errors, cancellation, timeouts, and panics. Use `unolog.Error` only when code handles an error and returns nil but the final event must still be a failure.

`End` is one-shot. After it runs, later writes through the operation context are dropped. Do not end an operation in a custom sink.

Set a specific message when it improves search:

```go
unolog.SetMessage(ctx, "invoice_reconciled")
```

Use stable operation names and normalized identifiers. Put high-cardinality IDs in fields, not in the operation name.
