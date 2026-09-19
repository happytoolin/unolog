package unolog

import (
	"cmp"
	"context"
	"fmt"
	"runtime"
	"slices"
	"sync/atomic"
	"time"
)

// OperationStart describes operation metadata initialized at start.
type OperationStart struct {
	// Domain is the operation category, one of the Domain constants;
	// the zero value defaults to the "operation" domain.
	Domain               Domain
	Name, ID, Source     string
	Attempt, MaxAttempts int
}

// Operation is the request lifecycle handle. Exactly one goroutine — the
// request's — drives it; End is one-shot.
type Operation struct {
	ctx   context.Context
	rt    *Runtime
	start OperationStart
	ref   walRef // embedded-by-value: the ctx points here, no extra alloc
	ev    *event

	// record is the pooled Record view for this operation's single
	// commit: embedding it replaces the per-commit &Record{...}
	// allocation with one that dies with the operation.
	record Record

	// endState is the one-shot claim word: 0 open, 1 claimed by the
	// winning End caller, 2 published (emitted is valid). Exactly one
	// caller CASes 0→1 and commits; the others wait for publication
	// (characterized by TestConcurrentEndCharacterization). emitted is
	// read only after observing 2, so the read is race-free.
	endState atomic.Uint32
	emitted  bool
}

// Start attaches a new request WAL to ctx and returns the operation
// handle. A nil *Runtime is valid: requests run, nothing emits.
func Start(ctx context.Context, rt *Runtime, start OperationStart) *Operation {
	if ctx == nil {
		ctx = context.Background()
	}
	ev := newEvent()
	op := &Operation{
		rt:    rt,
		start: normalizedOperationStart(start),
		ref:   walRef{ev: ev, gen: ev.state.Load() >> walStateBits},
		ev:    ev,
	}
	op.ctx = context.WithValue(ctx, contextKey{}, &op.ref)
	// Start metadata is appended lazily at End (annotatePostSeal), so
	// the live WAL carries no start fields during the request.
	return op
}

// Context returns the operation context — the ctx to pass down so the
// unolog.Add helpers reach this operation's WAL.
func (op *Operation) Context() context.Context {
	if op == nil {
		return nil
	}
	return op.ctx
}

// End finalizes the operation from the deferred error pointer and panic
// state, committing exactly one event, and returns whether an event was
// emitted. One-shot: a second call is a no-op returning the first
// result (false, by definition, if the first call crashed mid-commit —
// a panic in the sampler or a sink never publishes an emission). If the
// surrounding function is panicking, End records the panic, commits,
// and re-panics.
//
// End MUST be deferred directly (defer op.End(&err)) — the closure form
// silently disables panic capture. Reentrant use is not supported: a
// second End from inside a sink's Write deadlocks on the one-shot
// claim. Concurrent first calls are safe: exactly one wins the claim
// and commits; the others wait and return the published result. After
// End every write through the operation context is dropped.
// Error, Is, Unwrap, String, marshaling, sampler, and sink callbacks run
// synchronously and must return; Go cannot forcibly stop a blocked method.
func (op *Operation) End(errp *error) (emitted bool) {
	// A nil *Operation and the zero Operation are both no-ops: the zero
	// value carries no event, so there is nothing to commit. This keeps
	// the zero value from dereferencing a nil event in release, matching
	// the nil-guards on Context.
	if op == nil || op.ev == nil {
		return false
	}
	// Claim the one-shot commit with one inline CAS. Only a concurrent
	// second End pays for the wait, so the request hot path avoids a
	// non-inlinable call.
	if !op.endState.CompareAndSwap(0, 1) {
		return op.awaitPublication()
	}
	// Publish on every exit path, including panics, so waiting callers
	// can never spin on a winner that died mid-commit. LIFO order: the
	// event is released (sealed + pooled) first, then the claim word
	// publishes — waiters only read Operation fields, never the pooled
	// event, so recycling before publication is safe.
	defer func() { op.endState.Store(2) }()
	defer func() { op.ev.release() }()

	var err error
	if errp != nil {
		err = *errp
	}
	recovered := recover()

	ev := op.ev
	start := op.start

	// The owner's final writes, then SEAL before any WAL read: from
	// here on the event is immutable, so stragglers cannot race the
	// scan, the record handed to sinks, or the encode.
	now := time.Now() // one clock read: completion stamp + duration base
	duration := now.Sub(ev.startedAt)
	annotateOperationFailures(ev, &op.ref, err, recovered)
	ev.seal()

	scan := scanWAL(ev)
	code := scan.code
	isHTTP := start.Domain == DomainHTTP
	// The canonical code drives the 5xx outcome rule: http.status for
	// HTTP, op.code for everything else (a non-HTTP op's http.status is
	// user data, not canonical) — so a job surfacing failure via
	// op.code >= 500 resolves failure (and bypasses sampling) exactly
	// like its HTTP twin, instead of logging a self-contradictory
	// op.code=503 + op.outcome=success line.
	outcomeCode := 0
	if isHTTP {
		outcomeCode = code
	} else if scan.hasOpCode {
		outcomeCode = scan.opCode
	}
	outcome := resolveOutcome(err, recovered, outcomeCode, scan.outcome)

	in := &commitInput{
		outcome:  outcome,
		code:     code,
		duration: duration,
		now:      now,
		err:      err,
		panicked: recovered != nil,
		scan:     scan,
	}
	emitted = op.commit(in)
	op.emitted = emitted

	if recovered != nil {
		panic(recovered)
	}
	return emitted
}

// awaitPublication waits for the winning End caller to publish the
// one-shot result (endState 1 → 2, on every exit path including
// panics) and returns it. Concurrent End is a rare, documented-safe
// path; the wait yields with runtime.Gosched instead of blocking on a
// per-operation primitive, so the common single-caller path pays
// nothing.
func (op *Operation) awaitPublication() bool {
	for op.endState.Load() != 2 {
		runtime.Gosched()
	}
	return op.emitted
}

// walScan is the single backward walk over the sealed WAL collecting
// everything End needs: explicit outcome, HTTP status, the sampler
// scalars (first — i.e. last-written — values win), and whether the
// request wrote any start-metadata key itself (lazy start fields must
// not clobber a user override — suppressing the canonical append
// reproduces the old LWW fold exactly).
//
// Each scalar takes the last write of its expected kind: a later write
// of another kind (for example a string http.status) is not a usable
// canonical value and does not erase an earlier usable one. The
// encode-time dedupe still resolves the wire member last-write-wins;
// only the sampler's typed view is kind-sensitive.
type walScan struct {
	outcome Outcome
	code    int // resolved http.status (outcome + sampling input)
	opCode  int // explicit op.code field (non-HTTP operations)
	method  string
	path    string
	name    string // last-write op.name (string), if any

	hasOutcome     bool
	hasCode        bool
	hasOpCode      bool
	hasMethod      bool
	hasPath        bool
	hasDomain      bool // user wrote op.domain/op.name/... (any kind)
	hasName        bool
	hasStringName  bool
	hasID          bool
	hasSource      bool
	hasAttempt     bool
	hasMaxAttempts bool
}

func scanWAL(ev *event) walScan {
	var s walScan
	for _, f := range slices.Backward(ev.fields) {
		switch f.key {
		case KeyOpOutcome:
			if !s.hasOutcome && f.kind == KindString {
				if o := Outcome(f.str); IsValidOutcome(o) {
					s.outcome = o
					s.hasOutcome = true
				}
			}
		case KeyHTTPStatus:
			if !s.hasCode && f.kind == KindInt {
				s.code = int(f.num)
				s.hasCode = true
			}
		case KeyOpCode:
			if !s.hasOpCode && f.kind == KindInt {
				s.opCode = int(f.num)
				s.hasOpCode = true
			}
		case KeyHTTPMethod:
			if !s.hasMethod && f.kind == KindString {
				s.method = f.str
				s.hasMethod = true
			}
		case KeyHTTPPath:
			if !s.hasPath && f.kind == KindString {
				s.path = f.str
				s.hasPath = true
			}
		case KeyOpDomain:
			s.hasDomain = true
		case KeyOpName:
			s.hasName = true
			if !s.hasStringName && f.kind == KindString {
				s.name = f.str
				s.hasStringName = true
			}
		case KeyOpID:
			s.hasID = true
		case KeyOpSource:
			s.hasSource = true
		case KeyOpAttempt:
			s.hasAttempt = true
		case KeyOpMaxAttempts:
			s.hasMaxAttempts = true
		}
	}
	return s
}

// commitInput bundles what End resolved about the completed operation
// for the commit stage — everything that is not already reachable from
// the Operation itself (ev, rt, start, ctx, record). One struct instead
// of a ten-parameter call, and the natural home for these semantics:
// all of it describes the sealed event between seal and commit. End
// passes it by pointer: the struct embeds walScan,
// and by-value copies showed up in the lifecycle benchmarks.
type commitInput struct {
	outcome  Outcome // resolved outcome (panic > error > explicit > 5xx > success)
	code     int     // resolved http.status (the canonical code for HTTP operations)
	duration time.Duration
	now      time.Time // completion stamp (single clock read from End)
	err      error
	panicked bool
	scan     walScan
}

// commit resolves level, message, and sampling, then writes the record.
func (op *Operation) commit(in *commitInput) bool {
	rt := op.rt
	if rt.noop() {
		return false
	}

	ev := op.ev
	start := op.start
	policy := rt.policyFor(start.Domain)
	level := levelFloor(levelFromPolicy(policy, in.outcome), ev.requestedLevel, ev.hasRequestedLvl)

	// Custom samplers can look up completion fields. Built-in sampling
	// only needs scalars, so dropped events need no final annotations.
	if rt.sampler != nil {
		op.annotatePostSeal(in)
	}

	// The keep-everything fast path (rate == 1.0, no sampler, no level
	// rates, no policies): healthy events can never be dropped, so the
	// gate is skipped entirely. Error/panic events bypass the gate
	// structurally, so the flag only short-circuits the healthy branch.
	if !rt.alwaysKeep {
		sampleIn := buildSampleInput(ev, start, in, level)
		if !sampleIn.HasError {
			if rt.sampler != nil {
				if !rt.sampler(sampleIn) {
					return false
				}
			} else if !shouldWriteHealthy(rt, policy, sampleIn) {
				return false
			}
		}
	}
	if rt.sampler == nil {
		op.annotatePostSeal(in)
	}

	rec := &op.record
	rec.level = level
	rec.msg = resolveEventMessage(rt.message, start.Domain, ev.msg)
	rec.fields = ev.fields
	rec.completedAt = in.now
	rt.emit(op.ctx, rec)
	return true
}

// shouldWriteHealthy applies the compiled rate configuration for
// non-error events.
func shouldWriteHealthy(rt *Runtime, policy OperationPolicy, in SampleInput) bool {
	rate := rt.rate
	if policy.SamplingRate != nil {
		rate = *policy.SamplingRate
	} else if levelRate, ok := rt.levelRates[in.Level]; ok {
		rate = levelRate
	}
	return shouldSample(rate)
}

// applyOperationStartFields appends the normalized start metadata,
// lazily: End writes it post-seal so the record keeps the canonical
// start-metadata → completion order without the request paying for it
// while it runs. A key the request already wrote is skipped — appending
// later would flip the LWW fold and clobber the user's override (the
// route-name contract in the HTTP integrations depends on this).
func appendStartFields(start OperationStart, scan walScan, add func(Field)) {
	if !scan.hasDomain {
		add(fieldStr(KeyOpDomain, string(start.Domain)))
	}
	if !scan.hasName {
		add(fieldStr(KeyOpName, start.Name))
	}
	if !scan.hasID && start.ID != "" {
		add(fieldStr(KeyOpID, start.ID))
	}
	if !scan.hasSource && start.Source != "" {
		add(fieldStr(KeyOpSource, start.Source))
	}
	if !scan.hasAttempt && start.Attempt > 0 {
		add(fieldInt64(KeyOpAttempt, int64(start.Attempt)))
	}
	if !scan.hasMaxAttempts && start.MaxAttempts > 0 {
		add(fieldInt64(KeyOpMaxAttempts, int64(start.MaxAttempts)))
	}
}

func annotateOperationFailures(ev *event, ref *walRef, err error, recovered any) {
	if recovered != nil {
		ev.appendAny(ref.gen, KeyPanic, structuredPanicField(recovered))
	}
	if err != nil {
		ev.setError(ref, err)
	} else if recovered != nil {
		ev.setError(ref, fmt.Errorf("panic: %v", recovered))
	}
}

// annotatePostSeal appends the completion fields after sealing; the
// owner's post-seal writes cannot race stragglers. Start metadata
// joins first, keeping the wire's start-metadata → completion order
// with only the WAL position of the start fields moved.
//
// Canonical fields: HTTP operations carry http.status; non-HTTP
// operations surface their explicit op.code here (ledger: canonical
// fields).
func (op *Operation) annotatePostSeal(in *commitInput) {
	ev := op.ev
	// The owner's post-seal writes take the same mutex the seal and the
	// watchdog snapshots use, so the record handed to sinks is complete
	// and race-free. The owner is the only writer past the seal, so one
	// lock/unlock bracket covers the whole block.
	ev.mu.Lock()
	defer ev.mu.Unlock()

	fields := ev.fields
	add := func(f Field) { fields = append(fields, f) }
	appendStartFields(op.start, in.scan, add)
	add(fieldInt64(KeyDurationMS, in.duration.Milliseconds()))
	if op.start.Domain != DomainHTTP && in.scan.hasOpCode {
		add(fieldInt64(KeyOpCode, int64(in.scan.opCode)))
	}
	add(fieldStr(KeyOpOutcome, string(in.outcome)))
	ev.fields = fields
}

// resolveOutcome applies the precedence: panic > error > explicit
// (a valid op.outcome the caller wrote) > 5xx > success.
func resolveOutcome(err error, recovered any, code int, explicit Outcome) Outcome {
	if recovered != nil {
		return OutcomePanic
	}
	if err != nil {
		switch {
		case safeErrorIs(err, context.Canceled):
			return OutcomeCanceled
		case safeErrorIs(err, context.DeadlineExceeded):
			return OutcomeTimeout
		default:
			return OutcomeFailure
		}
	}
	if explicit != "" {
		return explicit
	}
	if code >= 500 {
		return OutcomeFailure
	}
	return OutcomeSuccess
}

func buildSampleInput(ev *event, start OperationStart, in *commitInput, level Level) SampleInput {
	// A retry is an explicit non-success outcome, not an error: it does
	// not bypass sampling (SampleInput.HasError is documented as
	// error-or-panic).
	hasError := in.err != nil || in.panicked || ev.hasErr ||
		(in.outcome != OutcomeSuccess && in.outcome != OutcomeRetry)
	opName := start.Name
	if in.scan.hasStringName {
		opName = in.scan.name
	}
	// HTTP samplers see http.status; non-HTTP samplers see their
	// canonical op.code (the README's non-HTTP contract for Code).
	// StatusCode stays the HTTP-compat view of http.status in both.
	samplerCode := in.code
	if start.Domain != DomainHTTP {
		samplerCode = in.scan.opCode
	}
	return SampleInput{
		Domain:     start.Domain,
		Operation:  opName,
		Outcome:    in.outcome,
		Code:       samplerCode,
		StatusCode: in.code,
		Method:     in.scan.method,
		Path:       in.scan.path,
		Duration:   in.duration,
		Level:      level,
		HasError:   hasError,
		ev:         ev,
	}
}

func resolveEventMessage(configured string, domain Domain, eventMessage string) string {
	if domain == DomainHTTP {
		return cmp.Or(eventMessage, configured, DefaultMessage)
	}
	return cmp.Or(eventMessage, configured, DefaultOperationMessage)
}

const defaultDomainValue Domain = "operation"

const defaultOpName = "operation"

func normalizedOperationStart(start OperationStart) OperationStart {
	start.Domain = normalizeDomain(start.Domain)
	if start.Name == "" {
		start.Name = defaultOpName
	}
	return start
}
