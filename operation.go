package unolog

import (
	"context"
	"fmt"
	"runtime"
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
	rt  *Runtime
	ref walRef // the WAL handle: ev + the generation this request owns

	// parent is the context the request started with. It is kept on the
	// handle, not inside a node, so Context() can mint a node from it
	// without the handle itself escaping. Context() mints a FRESH node
	// per call: it is documented safe to call from straggler goroutines
	// (TestSynctestGuardedWritersAllExit calls it from six at once), so a
	// cached node field would be a data race on the handle. The cache
	// bought nothing anyway — a request calls Context() once.
	parent context.Context

	// endState is the one-shot claim word: 0 open, 1 claimed by the
	// winning End caller, 2 published (emitted is valid). Exactly one
	// caller CASes 0→1 and commits; the others wait for publication
	// (characterized by TestConcurrentEndCharacterization). emitted is
	// read only after observing 2, so the read is race-free.
	//
	// A plain word, not an atomic.Uint32: the handle is returned and
	// copied by value (Start → StartRequest → middleware), and the noCopy
	// marker on atomic.Uint32 made vet reject every such copy. The atomic
	// helpers give the same semantics and the field sits at the same
	// offset, so the handle keeps its size.
	endState uint32
	emitted  bool
}

// Start attaches a new request WAL to ctx and returns the operation
// handle. A nil *Runtime is valid: requests run, nothing emits.
//
// The handle is returned by value so it can live on the caller's
// stack: a request that never calls Context() (never hands the node
// out) allocates nothing at all for the operation.
func Start(ctx context.Context, rt *Runtime, start OperationStart) Operation {
	if ctx == nil {
		ctx = context.Background()
	}
	var startWall int64
	var startMono int64
	if clock := rt.clockOf(); clock != nil {
		startWall = clock().UnixNano()
	} else {
		// Default clock: one monotonic read, and the wall stamp from the
		// cached anchor advanced by it. No walltime syscall per request.
		startMono = runtimeNano()
		startWall = wallNanoAt(startMono)
	}
	ev := newEventAt(startWall)
	ev.parent = ctx
	ev.startMono = startMono
	// Word-by-word, not ev.start = normalizedOperationStart(start): the
	// struct copy lowers to runtime.wbMove (a bulk write-barrier memmove
	// with a type-pointer walk). Assigning the eight string words
	// directly emits one inline barrier each and skips the walk — the
	// profile put wbMove at 6.9% of a parallel lifecycle, a quarter of
	// it under Start.
	ev.start.Domain = normalizeDomain(start.Domain)
	ev.start.Name = start.Name
	if ev.start.Name == "" {
		ev.start.Name = defaultOpName
	}
	ev.start.ID = start.ID
	ev.start.Source = start.Source
	ev.start.Attempt = start.Attempt
	ev.start.MaxAttempts = start.MaxAttempts
	// One allocation per request: the handle must stay valid for as long
	// as any caller holds it (the sink may retain the Record, a straggler
	// may hold the context), so it can never be recycled. A bump slab was
	// measured: it removes the allocation, but the slab spans are so much
	// larger than a 144-byte object that the mcentral traffic costs more
	// under GOMAXPROCS>1 than the malloc it saves.
	// Returned as a composite literal rather than a copy of a named
	// local: the handle embeds a noCopy claim word, and vet flags any
	// copy — including inside Start itself. A fresh literal is the one
	// copy of an Operation that is safe.
	//
	// Start metadata is appended lazily at End (annotatePostSeal), so
	// the live WAL carries no start fields during the request.
	return Operation{
		rt:     rt,
		ref:    walRef{ev: ev, gen: ev.state.Load() >> walStateBits},
		parent: ctx,
	}
}

// Context returns the operation context — the ctx to pass down so the
// unolog.Add helpers reach this operation's WAL. It mints a fresh 32-byte
// node per call and hands out only that node. The operation handle stays
// with its caller (usually the request's stack), and the request context
// carries half as many bytes as the handle itself. Minting per call is
// what makes it safe for straggler goroutines. Hoist the call: one
// Context() per operation, reused, costs one allocation; calling it once
// per field costs one allocation per field.
func (op *Operation) Context() context.Context {
	// A never-Started zero Operation has no parent; report the nil
	// context that guard has always documented.
	if op == nil || op.parent == nil {
		return nil
	}
	n := walNode(op.ref)
	return &n
}

// Operation implements context.Context as the escape hatch for callers
// that hand the handle itself out: it delegates Deadline/Done/Err/Value to
// the request's parent context and answers the package's private WAL key
// with its own handle. Ordinary callers never need this — Context() hands
// out a walNode instead, which is half the size. The node is immutable
// after Start, so straggler goroutines may share it.
func (op *Operation) Deadline() (time.Time, bool) {
	if op == nil || op.parent == nil {
		return time.Time{}, false
	}
	return op.parent.Deadline()
}

func (op *Operation) Done() <-chan struct{} {
	if op == nil || op.parent == nil {
		return nil
	}
	return op.parent.Done()
}

func (op *Operation) Err() error {
	if op == nil || op.parent == nil {
		return nil
	}
	return op.parent.Err()
}

func (op *Operation) Value(key any) any {
	if _, ok := key.(contextKey); ok {
		return &op.ref
	}
	if op == nil || op.parent == nil {
		return nil
	}
	return op.parent.Value(key)
}

// Add records fields directly on the operation handle, without going
// through Context(). It is the same write path as unolog.Add(ctx, …),
// minus the context lookup, and — because the handle never has to be
// handed out — it lets a caller keep the whole operation on the stack:
//
//	op := unolog.Start(ctx, rt, start)
//	defer op.End(&err)
//	op.Add("worker", "payments")
//
// costs zero allocations per request, and it takes no lock: the handle
// is documented single-goroutine, and End runs on that same goroutine,
// so nothing can interleave with these appends and the post-End drop is
// still exact. Use one form at a time — a concurrent
// unolog.Add(op.Context(), …) from another goroutine is guarded, but it
// would race these unguarded appends.
//
// Add takes exactly one pair. Multi-pair writes go through AddMany, or
// through unolog.Add(ctx, …) when the operation has been handed out.
// The split is not cosmetic: dropping the variadic slice from the hot
// signature removes three argument words and the closure-free empty
// slice the compiler used to build at every call site, which MEASURED
// ~1% of a twelve-field kept lifecycle on its own.
func (op *Operation) Add(key string, value any) {
	if op == nil || op.ref.ev == nil {
		return
	}
	ref := &op.ref
	if key == "" {
		return
	}
	e := ref.ev
	// One atomic load decides the fast drop. The state word packs the
	// generation above the one-bit state, so a single compare against
	// gen<<1 rejects both a stale generation (the event was recycled)
	// and a sealed event (End ran). No mutex: the handle is
	// single-goroutine, so this load is the whole guard.
	if e.state.Load() != ref.gen<<walStateBits {
		return
	}
	// The dispatch and the slot fill live here, not behind a helper:
	// Add is over the inline budget anyway, so a helper only buys an
	// extra call frame per field, which measured 13-19% of a
	// twelve-field lifecycle. Rare kinds fall back to fillField.
	f := e.reserveSlot()
	switch v := value.(type) {
	case string:
		f.key, f.kind, f.num, f.pay = key, KindString, 0, packString(v)
	case int:
		// The payload word is nil'd, not left alone: the slot may
		// still hold the previous generation's string pointer, and
		// resetAt no longer clears the backing array. A nil store
		// emits no write barrier, so the release is free.
		f.key, f.kind, f.num = key, KindInt, int64(v)
		f.pay.data = nil
	case bool:
		f.key, f.kind, f.num = key, KindBool, b2i(v)
		f.pay.data = nil
	case int64:
		f.key, f.kind, f.num = key, KindInt, v
		f.pay.data = nil
	case uint64:
		f.key, f.kind, f.num = key, KindUint, int64(v)
		f.pay.data = nil
	case time.Duration:
		f.key, f.kind, f.num = key, KindDuration, int64(v)
		f.pay.data = nil
	default:
		fillField(f, key, value)
	}
}

// AddMany is Add plus trailing pairs: AddMany("a", 1, "b", 2). It is
// the handle form of unolog.Add(ctx, "a", 1, "b", 2) and has the same
// key rules: a pair is written only when its key is a non-empty string,
// and an odd trailing value is dropped. Every pair takes the same
// generation guard as Add, so a post-End call is a silent no-op.
func (op *Operation) AddMany(key string, value any, kv ...any) {
	if op == nil || op.ref.ev == nil {
		return
	}
	ref := &op.ref
	ref.ev.addKVOwner(ref, key, value, kv...)
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
	if op == nil || op.ref.ev == nil {
		return false
	}
	// Claim the one-shot commit with one inline CAS. Only a concurrent
	// second End pays for the wait, so the request hot path avoids a
	// non-inlinable call.
	if !atomic.CompareAndSwapUint32(&op.endState, 0, 1) {
		return op.awaitPublication()
	}
	// Publish on every exit path, including panics, so waiting callers
	// can never spin on a winner that died mid-commit. The event is
	// released (sealed + pooled) first, then the claim word publishes —
	// waiters only read Operation fields, never the pooled event, so
	// recycling before publication is safe. One deferred method value,
	// not a pair of closures: the two closure frames were MEASURED at
	// ~4% of a kept twelve-field lifecycle by themselves.
	defer op.publish()

	var err error
	if errp != nil {
		err = *errp
	}
	recovered := recover()

	ev := op.ref.ev
	start := &ev.start

	// The owner's final writes, then SEAL before any WAL read: from
	// here on the event is immutable, so stragglers cannot race the
	// scan, the record handed to sinks, or the encode.
	// One clock read: the completion stamp and the duration base. With
	// the default clock the read is monotonic only and the stamp is the
	// start stamp's wall nanosecond advanced by the measured duration —
	// no time.Time arithmetic (Add is ~5 ns) and no walltime syscall.
	var completedAt int64
	var duration time.Duration
	if clock := op.rt.clockOf(); clock != nil {
		now := clock()
		completedAt = now.UnixNano()
		duration = time.Duration(completedAt - ev.startWall)
	} else {
		duration = time.Duration(runtimeNano() - ev.startMono)
		completedAt = ev.startWall + int64(duration)
	}
	if err != nil || recovered != nil {
		annotateOperationFailures(ev, &op.ref, err, recovered)
	}
	// errors.Is reaches user code (Is/Unwrap), so the error's outcome
	// class is resolved here, before the event mutex is taken below.
	var errOutcome Outcome
	if err != nil {
		errOutcome = outcomeForError(err)
	}
	isHTTP := start.Domain == DomainHTTP
	// The canonical code drives the 5xx outcome rule: http.status for
	// HTTP, op.code for everything else (a non-HTTP op's http.status is
	// user data, not canonical) — so a job surfacing failure via
	// op.code >= 500 resolves failure (and bypasses sampling) exactly
	// like its HTTP twin, instead of logging a self-contradictory
	// op.code=503 + op.outcome=success line.
	//
	// Seal, scan, resolve, and the owner's post-seal annotations all
	// run under ONE acquisition of the append mutex: seal's own
	// lock/unlock pair and the annotation's were MEASURED as two
	// round trips per operation, and nothing between them needs the
	// lock released. The scan writes straight into in.scan — returning
	// the 96-byte walScan by value cost a second zeroed local and a
	// copy at the call boundary (~2% of a kept lifecycle).
	var in commitInput
	in.duration = duration
	in.completedAt = completedAt
	in.err = err
	in.panicked = recovered != nil
	ev.mu.Lock()
	ev.state.Or(uint64(walSealed))
	// capture is true only when a custom sampler exists: the three
	// captured strings are read solely by buildSampleInput, and the
	// built-in rate configuration decides on scalars alone (see commit).
	scanWAL(&in.scan, ev, op.rt != nil && op.rt.sampler != nil)
	outcomeCode := 0
	if isHTTP {
		outcomeCode = in.scan.code
	} else if in.scan.hasOpCode {
		outcomeCode = in.scan.opCode
	}
	in.outcome = resolveOutcome(recovered != nil, errOutcome, outcomeCode, in.scan.outcome)
	op.annotatePostSeal(&in)
	ev.mu.Unlock()

	emitted = op.commit(&in)
	op.emitted = emitted

	if recovered != nil {
		panic(recovered)
	}
	return emitted
}

// publish is End's single deferred step: release the event (seal +
// pool it) and then publish the one-shot claim. Order matters — a
// waiter reads Operation fields only, never the pooled event, so
// recycling before publication is safe, and the claim word must be the
// last write so awaitPublication can never observe 2 with emitted
// unset.
func (op *Operation) publish() {
	op.ref.ev.release()
	atomic.StoreUint32(&op.endState, 2)
}

// awaitPublication waits for the winning End caller to publish the
// one-shot result (endState 1 → 2, on every exit path including
// panics) and returns it. Concurrent End is a rare, documented-safe
// path; the wait yields with runtime.Gosched instead of blocking on a
// per-operation primitive, so the common single-caller path pays
// nothing.
func (op *Operation) awaitPublication() bool {
	for atomic.LoadUint32(&op.endState) != 2 {
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
// scanWAL writes into the caller's walScan rather than returning one:
// the 96-byte value cost a zeroed local plus a result slot plus the
// copy between them at every call (MEASURED ~2% of a kept twelve-field
// lifecycle). The caller must pass a zeroed struct — the walk only
// sets what it finds.
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

func scanWAL(s *walScan, ev *event, capture bool) {
	// Indexed walk, not slices.Backward: the iterator's yield closure
	// showed up as its own profile node (~1.2% flat) and the pointer
	// keeps the 48-byte Field copy out of the loop.
	fields := ev.fields
	for i := len(fields) - 1; i >= 0; i-- {
		f := &fields[i]
		switch f.key {
		case KeyOpOutcome:
			if !s.hasOutcome && f.kind == KindString {
				if o := Outcome(f.pay.stringValue()); IsValidOutcome(o) {
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
			if capture && !s.hasMethod && f.kind == KindString {
				s.method = f.pay.stringValue()
				s.hasMethod = true
			}
		case KeyHTTPPath:
			if capture && !s.hasPath && f.kind == KindString {
				s.path = f.pay.stringValue()
				s.hasPath = true
			}
		case KeyOpDomain:
			s.hasDomain = true
		case KeyOpName:
			s.hasName = true
			if capture && !s.hasStringName && f.kind == KindString {
				s.name = f.pay.stringValue()
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
}

// commitInput bundles what End resolved about the completed operation
// for the commit stage — everything that is not already reachable from
// the Operation itself (ev, rt, start, ctx, record). One struct instead
// of a ten-parameter call, and the natural home for these semantics:
// all of it describes the sealed event between seal and commit. End
// passes it by pointer: the struct embeds walScan,
// and by-value copies showed up in the lifecycle benchmarks.
type commitInput struct {
	outcome     Outcome // resolved outcome (panic > error > explicit > 5xx > success)
	duration    time.Duration
	completedAt int64 // completion stamp, unix nanoseconds (one clock read from End)
	err         error
	panicked    bool
	scan        walScan
}

// commit resolves level, message, and sampling, then writes the record.
func (op *Operation) commit(in *commitInput) bool {
	rt := op.rt
	if rt.noop() {
		return false
	}

	ev := op.ref.ev
	start := &ev.start
	policy := rt.policyFor(start.Domain)
	level := levelFloor(levelFromPolicy(policy, in.outcome), ev.requestedLevel, ev.hasRequestedLvl)

	// Custom samplers can look up completion fields. Built-in sampling
	// only needs scalars, so dropped events need no final annotations.
	// (The completion annotations are already in the WAL: End appends
	// them under the same mutex acquisition as the seal and the scan, so
	// a custom sampler's Lookup sees them here.)
	// The keep-everything fast path (rate == 1.0, no sampler, no level
	// rates, no policies): healthy events can never be dropped, so the
	// gate is skipped entirely. Error/panic events bypass the gate
	// structurally, so the flag only short-circuits the healthy branch.
	if !rt.alwaysKeep {
		// The built-in rate configuration reads only scalars, so a custom
		// sampler is the ONLY consumer of the SampleInput view (its
		// Lookup/Fields and the captured method/path/name). Without one,
		// skip the view entirely and decide on level + the error bits.
		if rt.sampler != nil {
			sampleIn := buildSampleInput(ev, start, in, level)
			if !sampleIn.HasError && !rt.sampler(sampleIn) {
				return false
			}
		} else if !sampleHasError(ev, in) && !shouldWriteHealthy(rt, policy, level) {
			return false
		}
	}
	rec := &ev.rec
	rec.level = level
	rec.msg = resolveEventMessage(rt.message, start.Domain, ev.msg)
	rec.fields = ev.fields
	rec.completedAt = in.completedAt
	rt.emit(op.parent, rec)
	return true
}

// sampleHasError is buildSampleInput's HasError rule, split out so the
// sampler-free path can reach it without materialising the view.
func sampleHasError(ev *event, in *commitInput) bool {
	// A retry is an explicit non-success outcome, not an error: it does
	// not bypass sampling (SampleInput.HasError is documented as
	// error-or-panic).
	return in.err != nil || in.panicked || ev.hasErr ||
		(in.outcome != OutcomeSuccess && in.outcome != OutcomeRetry)
}

// shouldWriteHealthy applies the compiled rate configuration for
// non-error events.
func shouldWriteHealthy(rt *Runtime, policy OperationPolicy, level Level) bool {
	rate := rt.rate
	if policy.SamplingRate != nil {
		rate = *policy.SamplingRate
	} else if levelRate, ok := rt.levelRates[level]; ok {
		rate = levelRate
	}
	return shouldSample(rate)
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
//
// The caller (End) holds e.mu: the same mutex the seal and the
// watchdog snapshots use, so the record handed to sinks is complete
// and race-free. End holds it across the seal, the scan, and this
// annotation — one acquisition where the three steps used to take two.
func (op *Operation) annotatePostSeal(in *commitInput) {
	ev := op.ref.ev
	// Appends are inlined here instead of routed through a closure: the
	// func value captured the slice header and showed up as a hot,
	// partially-inlined call in the sparse-lifecycle profile.
	//
	// A key the request already wrote is skipped — appending later would
	// flip the LWW fold and clobber the user's override (the route-name
	// contract in the HTTP integrations depends on this).
	// The in-place push helpers fill each slot directly: appending a
	// fieldStr/fieldInt64 result would return a 48-byte Field through
	// stack slots and copy it in with a typedmemmove plus a bulk
	// write-barrier walk, once per canonical field (round-20 finding).
	if !in.scan.hasDomain {
		ev.pushStr(KeyOpDomain, string(ev.start.Domain))
	}
	if !in.scan.hasName {
		ev.pushStr(KeyOpName, ev.start.Name)
	}
	if !in.scan.hasID && ev.start.ID != "" {
		ev.pushStr(KeyOpID, ev.start.ID)
	}
	if !in.scan.hasSource && ev.start.Source != "" {
		ev.pushStr(KeyOpSource, ev.start.Source)
	}
	if !in.scan.hasAttempt && ev.start.Attempt > 0 {
		ev.pushInt(KeyOpAttempt, int64(ev.start.Attempt))
	}
	if !in.scan.hasMaxAttempts && ev.start.MaxAttempts > 0 {
		ev.pushInt(KeyOpMaxAttempts, int64(ev.start.MaxAttempts))
	}
	ev.pushInt(KeyDurationMS, in.duration.Milliseconds())
	if ev.start.Domain != DomainHTTP && in.scan.hasOpCode {
		ev.pushInt(KeyOpCode, int64(in.scan.opCode))
	}
	ev.pushStr(KeyOpOutcome, string(in.outcome))
}

// outcomeForError classifies a non-nil error's outcome. It is split out
// of resolveOutcome so that its errors.Is calls — which reach user code,
// Is/Unwrap — run before End takes the event mutex for the seal.
func outcomeForError(err error) Outcome {
	switch {
	case safeErrorIs(err, context.Canceled):
		return OutcomeCanceled
	case safeErrorIs(err, context.DeadlineExceeded):
		return OutcomeTimeout
	default:
		return OutcomeFailure
	}
}

// resolveOutcome applies the precedence: panic > error > explicit
// (a valid op.outcome the caller wrote) > 5xx > success. errOutcome is
// outcomeForError's classification of the non-nil error (zero when
// err == nil), so this function runs no user code.
func resolveOutcome(panicked bool, errOutcome Outcome, code int, explicit Outcome) Outcome {
	if panicked {
		return OutcomePanic
	}
	if errOutcome != "" {
		return errOutcome
	}
	if explicit != "" {
		return explicit
	}
	if code >= 500 {
		return OutcomeFailure
	}
	return OutcomeSuccess
}

func buildSampleInput(ev *event, start *OperationStart, in *commitInput, level Level) SampleInput {
	hasError := sampleHasError(ev, in)
	opName := start.Name
	if in.scan.hasStringName {
		opName = in.scan.name
	}
	// HTTP samplers see http.status; non-HTTP samplers see their
	// canonical op.code (the README's non-HTTP contract for Code).
	// StatusCode stays the HTTP-compat view of http.status in both.
	samplerCode := in.scan.code
	if start.Domain != DomainHTTP {
		samplerCode = in.scan.opCode
	}
	return SampleInput{
		Domain:     start.Domain,
		Operation:  opName,
		Outcome:    in.outcome,
		Code:       samplerCode,
		StatusCode: in.scan.code,
		Method:     in.scan.method,
		Path:       in.scan.path,
		Duration:   in.duration,
		Level:      level,
		HasError:   hasError,
		ev:         ev,
	}
}

func resolveEventMessage(configured string, domain Domain, eventMessage string) string {
	// Hand-rolled, not cmp.Or: the generic call per arm measured a small but
	// repeatable win on the thin paths (no_fields -2.8%, 12 fields -1.4% at six
	// interleaved reps). One string compare per arm, no generic instantiation.
	if eventMessage != "" {
		return eventMessage
	}
	if configured != "" {
		return configured
	}
	if domain == DomainHTTP {
		return DefaultMessage
	}
	return DefaultOperationMessage
}

const defaultDomainValue Domain = "operation"

const defaultOpName = "operation"
