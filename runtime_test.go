package unolog

// Runtime/Compile tests: construction-time validation, policy
// resolution, and the config fuzz target.

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

func TestCompileValid(t *testing.T) {
	rate := 0.25
	rt, err := Compile(Config{
		Sink:               NewTestSink(),
		SamplingRate:       1,
		LevelSamplingRates: map[Level]float64{LevelDebug: 0.1},
		OperationPolicies: map[Domain]OperationPolicy{
			DomainJob: {SuccessLevel: LevelDebug, SamplingRate: &rate},
		},
		Message: "done",
	})
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	if rt.noop() {
		t.Fatal("valid sink compiled to noop")
	}
	if rt.message != "done" {
		t.Errorf("message = %q", rt.message)
	}
}

// TestPolicyAliasPrecedence pins the deterministic winner: an explicit
// "operation" key beats the "" alias when both are present.
func TestPolicyAliasPrecedence(t *testing.T) {
	rt := MustCompile(Config{
		SamplingRate: 1,
		OperationPolicies: map[Domain]OperationPolicy{
			"":          {SuccessLevel: LevelWarn},
			"operation": {SuccessLevel: LevelDebug},
		},
	})
	if p := rt.policyFor(Domain("")); p.SuccessLevel != LevelDebug {
		t.Fatalf("alias won over explicit key: %+v", p)
	}
}

// TestCompileRuntimeImmutable pins the deep copy: mutating the caller's
// Config (and its nested maps) after Compile cannot affect the runtime.
func TestCompileRuntimeImmutable(t *testing.T) {
	rate := 0.5
	cfg := Config{
		Sink:         NewTestSink(),
		SamplingRate: 1,
		OperationPolicies: map[Domain]OperationPolicy{
			DomainJob: {
				SuccessLevel:  LevelInfo,
				OutcomeLevels: map[Outcome]Level{OutcomeRetry: LevelWarn},
				SamplingRate:  &rate,
			},
		},
		LevelSamplingRates: map[Level]float64{LevelWarn: 1},
	}
	rt, err := Compile(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mutated := cfg.OperationPolicies[DomainJob]
	mutated.SuccessLevel = LevelError
	if mutated.SuccessLevel != LevelError {
		t.Fatal("test setup: mutation did not apply")
	}
	mutated.OutcomeLevels[OutcomeRetry] = LevelDebug
	*mutated.SamplingRate = 0
	cfg.LevelSamplingRates[LevelWarn] = 0
	cfg.Message = "mutated"

	if p := rt.policyFor(DomainJob); p.SuccessLevel != LevelInfo {
		t.Fatalf("policy level mutated: %v", p.SuccessLevel)
	} else if p.OutcomeLevels[OutcomeRetry] != LevelWarn {
		t.Fatalf("outcome level mutated: %v", p.OutcomeLevels[OutcomeRetry])
	} else if *p.SamplingRate != 0.5 {
		t.Fatalf("sampling rate mutated: %v", *p.SamplingRate)
	}
	if rt.levelRates[LevelWarn] != 1 || rt.message != "" {
		t.Fatal("level rates or message mutated")
	}
}

func TestCompileNilSinkIsValidNoop(t *testing.T) {
	rt, err := Compile(Config{})
	if err != nil {
		t.Fatalf("nil sink must be valid: %v", err)
	}
	if !rt.noop() {
		t.Fatal("nil sink must be a no-op runtime")
	}
}

func TestCompileSentinels(t *testing.T) {
	cases := []struct {
		name  string
		cfg   Config
		sent  error
		inMsg string
	}{
		{
			name:  "rate above one",
			cfg:   Config{SamplingRate: 1.5},
			sent:  ErrInvalidRate,
			inMsg: "unolog: sampling rate 1.5",
		},
		{
			name:  "rate negative",
			cfg:   Config{SamplingRate: -0.1},
			sent:  ErrInvalidRate,
			inMsg: "unolog: sampling rate -0.1",
		},
		{
			name: "rate NaN",
			cfg:  Config{SamplingRate: math.NaN()},
			sent: ErrInvalidRate,
		},
		{
			name: "level rate bad level key",
			cfg:  Config{LevelSamplingRates: map[Level]float64{Level(3): 0.5}},
			sent: ErrInvalidLevel,
		},
		{
			name: "level rate bad rate",
			cfg:  Config{LevelSamplingRates: map[Level]float64{LevelWarn: 2}},
			sent: ErrInvalidRate,
		},
		{
			name: "policy bad success level",
			cfg:  Config{OperationPolicies: map[Domain]OperationPolicy{DomainJob: {SuccessLevel: Level(7)}}},
			sent: ErrInvalidLevel,
		},
		{
			name: "policy bad outcome key",
			cfg:  Config{OperationPolicies: map[Domain]OperationPolicy{DomainJob: {OutcomeLevels: map[Outcome]Level{Outcome("nope"): LevelWarn}}}},
			sent: ErrInvalidOutcome,
		},
		{
			name: "policy bad rate",
			cfg:  Config{OperationPolicies: map[Domain]OperationPolicy{DomainJob: {SamplingRate: floatPtr(1.2)}}},
			sent: ErrInvalidRate,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := Compile(c.cfg)
			if err == nil {
				t.Fatal("expected error")
			}
			if !errors.Is(err, c.sent) {
				t.Fatalf("error %v does not wrap sentinel %v", err, c.sent)
			}
			if c.inMsg != "" && err.Error()[:len(c.inMsg)] != c.inMsg {
				t.Fatalf("error %q lacks %q prefix", err, c.inMsg)
			}
		})
	}
}

func TestMustCompilePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("MustCompile should panic on bad config")
		}
	}()
	MustCompile(Config{SamplingRate: 2})
}

func TestMustCompileLiteralOK(t *testing.T) {
	rt := MustCompile(Config{Sink: NewTestSink(), SamplingRate: 1})
	if rt == nil {
		t.Fatal("nil runtime")
	}
}

func TestLevelString(t *testing.T) {
	cases := map[Level]string{
		LevelDebug: "DEBUG",
		LevelInfo:  "INFO",
		LevelWarn:  "WARN",
		LevelError: "ERROR",
		Level(42):  "INFO", // unknown renders as info, wire unchanged
	}
	for level, want := range cases {
		if got := level.String(); got != want {
			t.Errorf("Level(%d).String() = %q, want %q", int(level), got, want)
		}
	}
}

// TestLevelConstantCompatibility pins the slog ranks so v0 source that
// references the constants keeps compiling and behaving identically.
func TestLevelConstantCompatibility(t *testing.T) {
	if LevelDebug != -4 || LevelInfo != 0 || LevelWarn != 4 || LevelError != 8 {
		t.Fatal("level ranks drifted from the slog-compatible values")
	}
	if !IsValidLevel(LevelWarn) || IsValidLevel(Level(3)) {
		t.Fatal("IsValidLevel misbehaves")
	}
}

func floatPtr(f float64) *float64 { return &f }

func TestPolicyForDomain(t *testing.T) {
	rt := MustCompile(Config{
		OperationPolicies: map[Domain]OperationPolicy{
			DomainJob: {SuccessLevel: LevelDebug},
		},
	})
	if p := rt.policyFor(DomainJob); p.SuccessLevel != LevelDebug {
		t.Errorf("job policy = %+v", p)
	}
	if p := rt.policyFor(DomainHTTP); p.SuccessLevel != LevelInfo {
		t.Errorf("unconfigured policy not zero: %+v", p)
	}
	// empty-string domain normalizes to the default domain
	rt2 := MustCompile(Config{
		OperationPolicies: map[Domain]OperationPolicy{"": {SuccessLevel: LevelWarn}},
	})
	if p := rt2.policyFor(Domain("")); p.SuccessLevel != LevelWarn {
		t.Errorf("alias policy = %+v", p)
	}
}

// TestPolicyOutcomeLevelsCanSelectInfo pins the documented workaround:
// the three level fields cannot express an explicit INFO because the
// zero Level is the "use the default" sentinel, but OutcomeLevels can.
func TestPolicyOutcomeLevelsCanSelectInfo(t *testing.T) {
	policy := OperationPolicy{OutcomeLevels: map[Outcome]Level{OutcomeFailure: LevelInfo}}
	if got := levelFromPolicy(policy, OutcomeFailure); got != LevelInfo {
		t.Fatalf("levelFromPolicy = %v, want INFO via OutcomeLevels", got)
	}
	if got := levelFromPolicy(OperationPolicy{FailureLevel: LevelInfo}, OutcomeFailure); got != LevelError {
		t.Fatalf("zero FailureLevel = %v, want the ERROR default", got)
	}
}

type wrappedTestError struct{ err error }

func (w wrappedTestError) Error() string { return "wrapped: " + w.err.Error() }
func (w wrappedTestError) Unwrap() error { return w.err }

type frameworkStyleTestError struct {
	Code    int
	Message string
}

func (f *frameworkStyleTestError) Error() string {
	return fmt.Sprintf("framework %d: %s", f.Code, f.Message)
}

func TestStructuredErrorField(t *testing.T) {
	field := structuredErrorField(errors.New("boom"))
	if field["message"] != "boom" {
		t.Errorf("message = %v", field["message"])
	}
	if field["type"] != "*errors.errorString" {
		t.Errorf("type = %v", field["type"])
	}
	if _, hasCause := field["cause.message"]; hasCause {
		t.Error("simple error should have no cause")
	}

	wrapped := structuredErrorField(wrappedTestError{err: errors.New("inner")})
	if wrapped["message"] != "wrapped: inner" {
		t.Errorf("message = %v", wrapped["message"])
	}
	if wrapped["cause.message"] != "inner" {
		t.Errorf("cause.message = %v", wrapped["cause.message"])
	}

	fw := structuredErrorField(&frameworkStyleTestError{Code: 500, Message: "kaput"})
	if fw["message"] != "kaput" {
		t.Errorf("framework message = %v", fw["message"])
	}
}

func TestStructuredPanicField(t *testing.T) {
	field := structuredPanicField("boom")
	if field["value"] != "boom" || field["type"] != "string" {
		t.Fatalf("panic field = %v", field)
	}
}

func TestCyclicErrorUnwrap(t *testing.T) {
	a := &cyclicError{}
	b := &cyclicError{next: a}
	a.next = b
	field := structuredErrorField(a)
	if field == nil {
		t.Fatal("cyclic unwrap must terminate")
	}
}

type cyclicError struct{ next error }

func (c *cyclicError) Error() string { return "cyclic" }
func (c *cyclicError) Unwrap() error { return c.next }

func sampleIn(path string, hasError bool) SampleInput {
	return SampleInput{
		Domain:     DomainHTTP,
		Operation:  "GET /x",
		Outcome:    OutcomeSuccess,
		StatusCode: 200,
		Path:       path,
		Duration:   10 * time.Millisecond,
		Level:      LevelInfo,
		HasError:   hasError,
	}
}

func TestSamplerHelpers(t *testing.T) {
	base := func(SampleInput) bool { return false }

	if !KeepErrors()(base)(sampleIn("/x", true)) {
		t.Fatal("KeepErrors dropped an error")
	}
	if KeepErrors()(base)(sampleIn("/x", false)) {
		t.Fatal("KeepErrors kept a healthy event past base")
	}
	if !KeepSlowerThan(5 * time.Millisecond)(base)(sampleIn("/x", false)) {
		t.Fatal("KeepSlowerThan dropped a slow event")
	}
	if KeepPathPrefix("/health")(base)(sampleIn("/api", false)) {
		t.Fatal("KeepPathPrefix kept a non-matching path")
	}
	if !KeepPathPrefix("/api")(base)(sampleIn("/api/v1", false)) {
		t.Fatal("KeepPathPrefix dropped a matching prefix")
	}
	if KeepPathPrefix()(base)(sampleIn("/api", false)) {
		t.Fatal("empty KeepPathPrefix must pass through to base (drop)")
	}
}

func TestChainSamplerOrder(t *testing.T) {
	calls := []string{}
	mk := func(name string) SamplerMiddleware {
		return func(next Sampler) Sampler {
			return func(in SampleInput) bool {
				calls = append(calls, name)
				return next(in)
			}
		}
	}
	chained := ChainSampler(AlwaysSampler(), mk("a"), mk("b"))
	if !chained(sampleIn("/x", false)) {
		t.Fatal("chained sampler dropped")
	}
	if len(calls) != 2 || calls[0] != "a" || calls[1] != "b" {
		t.Fatalf("middleware order = %v, want [a b] (a wraps b)", calls)
	}
}

func TestRateSamplerBoundaries(t *testing.T) {
	if RateSampler(-1)(sampleIn("/x", false)) {
		t.Fatal("negative rate kept")
	}
	if RateSampler(0)(sampleIn("/x", false)) {
		t.Fatal("zero rate kept")
	}
	if RateSampler(2)(sampleIn("/x", false)) == false {
		t.Fatal("rate >= 1 dropped")
	}
	// NaN never keeps
	if RateSampler(math.NaN())(sampleIn("/x", false)) {
		t.Fatal("NaN rate kept")
	}
}

func TestSampleInputSyntheticLookup(t *testing.T) {
	in := SampleInput{}
	if _, ok := in.Lookup("k"); ok {
		t.Fatal("synthetic input found a key")
	}
	if in.Fields() != nil {
		t.Fatal("synthetic input returned fields")
	}
}

// TestSampleInputView pins the sampler's view of the in-flight WAL
// (amendment 8): Lookup resolves the last value under a key, Fields()
// iterates the insertion-ordered fields with typed accessors, and the
// sampler's decision governs emission. (Merged from the former
// TestSampleInputLookupAndFields and TestSampleInputFieldsAssertion.)
func TestSampleInputView(t *testing.T) {
	var seenTier any
	var fieldCount, sawCounter int
	rt, ts := testRT(t, func(c *Config) {
		c.Sampler = func(in SampleInput) bool {
			if v, ok := in.Lookup("user_tier"); ok {
				seenTier = v
			}
			fieldCount = len(in.Fields())
			for _, f := range in.Fields() {
				if f.Key() == "counter" {
					if v, ok := f.Int(); ok {
						sawCounter = int(v)
					}
				}
			}
			return in.Outcome == OutcomeSuccess || in.HasError
		}
	})
	op := Start(context.Background(), rt, OperationStart{})
	Add(op.Context(), "user_tier", "enterprise", "counter", 41, "other", "x")
	op.End(nil)

	if seenTier != "enterprise" {
		t.Fatalf("Lookup(user_tier) = %v", seenTier)
	}
	if sawCounter != 41 {
		t.Fatalf("typed Int() view of counter = %d", sawCounter)
	}
	if fieldCount < 3 { // user fields + the canonical op.* pair
		t.Fatalf("Fields() view has %d fields, want >= 3", fieldCount)
	}
	if len(ts.Events()) != 1 {
		t.Fatal("success event dropped by its own sampler")
	}
}

// FanoutSink writes every record to each member sink in order. If a
// member panics, the remaining members still receive the record and
// the panic propagates to the caller (who decides what to do with it).
type FanoutSink struct {
	sinks []Sink
}

// NewFanoutSink creates a fan-out over the given sinks.
func NewFanoutSink(sinks ...Sink) *FanoutSink {
	return &FanoutSink{sinks: sinks}
}

// Write delivers the record to every member sink, continuing past
// panicking members (their panic is re-raised after the fan-out so the
// caller sees the first failure).
func (f *FanoutSink) Write(ctx context.Context, rec *Record) {
	if f == nil {
		return
	}
	var firstPanic any
	for _, s := range f.sinks {
		func() {
			defer func() {
				if r := recover(); r != nil && firstPanic == nil {
					firstPanic = r
				}
			}()
			s.Write(ctx, rec)
		}()
	}
	if firstPanic != nil {
		panic(firstPanic)
	}
}

var _ Sink = (*FanoutSink)(nil)

// captureSink records every record it receives (deep copy via the
// shared TestSink machinery).
// panicSink panics on Write.
type panicSink struct{}

func (panicSink) Write(context.Context, *Record) { panic("sink exploded") }

// TestFanoutSinkPanicIsolation: one panicking member must not stop the
// others from receiving the record.
func TestFanoutSinkPanicIsolation(t *testing.T) {
	for _, n := range []int{2, 4, 8} {
		for _, panicIdx := range []int{0, n / 2, n - 1} {
			t.Run(fmt.Sprintf("n=%d panic=%d", n, panicIdx), func(t *testing.T) {
				captures := make([]*TestSink, n)
				sinks := make([]Sink, n)
				for i := range sinks {
					captures[i] = NewTestSink()
					sinks[i] = captures[i]
				}
				sinks[panicIdx] = panicSink{}
				ts := NewFanoutSink(sinks...)
				rt := MustCompile(Config{Sink: ts, SamplingRate: 1})
				op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "fan"})
				Add(op.Context(), "k", "v")

				var escaped any
				func() {
					defer func() { escaped = recover() }()
					op.End(nil)
				}()
				if escaped == nil {
					t.Fatal("the panicking sink's panic did not escape")
				}
				// every non-panicking member received the record
				for i, c := range captures {
					if i == panicIdx {
						continue // this slot holds the panicking sink
					}
					events := c.Events()
					if len(events) != 1 {
						t.Fatalf("sink %d captured %d events", i, len(events))
					}
					if v, _ := events[0].Lookup("k"); v != "v" {
						t.Fatalf("sink %d event corrupted", i)
					}
				}
				// pool clean after the panic
				ok := NewTestSink()
				rt2 := MustCompile(Config{Sink: ok, SamplingRate: 1})
				op2 := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "after"})
				Add(op2.Context(), "clean", true)
				op2.End(nil)
				if len(ok.Events()) != 1 {
					t.Fatal("pool corrupted after the panicking fan-out")
				}
			})
		}
	}
}

// TestFanoutAllPanicking: when every member panics the caller sees the
// first panic, and the request still does not corrupt the pool.
func TestFanoutAllPanicking(t *testing.T) {
	for _, n := range []int{2, 4} {
		sinks := make([]Sink, n)
		for i := range sinks {
			sinks[i] = panicSink{}
		}
		rt := MustCompile(Config{Sink: NewFanoutSink(sinks...), SamplingRate: 1})
		op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "all-panic"})
		Add(op.Context(), "k", 1)
		var escaped any
		func() {
			defer func() { escaped = recover() }()
			op.End(nil)
		}()
		if escaped == nil || escaped != "sink exploded" {
			t.Fatalf("escaped %v", escaped)
		}
		// pool clean: the next request works
		ok := NewTestSink()
		rt2 := MustCompile(Config{Sink: ok, SamplingRate: 1})
		op2 := Start(context.Background(), rt2, OperationStart{Domain: DomainJob, Name: "after"})
		op2.End(nil)
		if len(ok.Events()) != 1 {
			t.Fatal("pool corrupted after all-panic fan-out")
		}
	}
}

// TestFanoutRetainingSink: a member that retains the Record past Write
// must copy it (amendment 9) — a buggy retaining member would observe
// recycled memory, which the pool-safety tests pin elsewhere; here we
// pin that the fan-out passes the SAME record view to every member.
func TestFanoutSinkOrder(t *testing.T) {
	a, b := NewTestSink(), NewTestSink()
	rt := MustCompile(Config{Sink: NewFanoutSink(a, b), SamplingRate: 1})
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: "order"})
	Add(op.Context(), "k", "v")
	op.End(nil)
	ea, eb := a.Events(), b.Events()
	if len(ea) != 1 || len(eb) != 1 {
		t.Fatalf("members captured %d/%d events", len(ea), len(eb))
	}
	la, lb := ea[0], eb[0]
	if la.Message() != lb.Message() || la.Level() != lb.Level() {
		t.Fatalf("members disagree: %+v vs %+v", la, lb)
	}
	if v, _ := la.Lookup("k"); v != "v" {
		t.Fatalf("first member lost the field: %v", v)
	}
	if _, ok := lb.Lookup("k"); !ok {
		t.Fatal("second member lost the field")
	}
}

// FuzzCompileConfig fuzzes unolog.Compile with extreme configurations; the
// oracle checks the documented contract:
//
//  1. Compile never panics.
//  2. err != nil ⇒ the error wraps one of the three sentinels
//     (errors.Is works) with the "unolog: " prefix.
//  3. err == nil ⇒ the runtime is usable end-to-end (a full request
//     runs against a TestSink) and immutable: mutating every field of
//     the caller's Config after Compile changes nothing the runtime
//     observes.
//
// Accepted rates are exactly [0,1]: NaN and friends must be rejected —
// the negated-range idiom in Compile (`!(rate >= 0 && rate <= 1)`)
// makes NaN fall into the error branch, which this target pins.

// compileRate draws a hostile rate from a byte: NaN, ±Inf, denormals,
// out-of-range, and the valid edges.
func compileRate(b byte) float64 {
	switch b % 8 {
	case 0:
		return math.NaN()
	case 1:
		return math.Inf(1)
	case 2:
		return math.Inf(-1)
	case 3:
		return -1
	case 4:
		return 1.0000001
	case 5:
		return 0
	case 6:
		return 1
	default:
		return float64(b) / 255 // interior of [0,1]
	}
}

var compileLevels = []Level{LevelDebug, LevelInfo, LevelWarn, LevelError, Level(99), Level(-100), Level(0)}

var compileDomains = []Domain{DomainHTTP, DomainJob, "", "operation", "weird domain", "http"}

var compileOutcomes = []Outcome{
	OutcomeSuccess, OutcomeFailure, OutcomePanic, OutcomeCanceled, OutcomeTimeout, OutcomeRetry, "nonsense", "",
}

// configFromBytes decodes a hostile Config from fuzz bytes.
func configFromBytes(b []byte, sink Sink) Config {
	next := func() byte {
		if len(b) == 0 {
			return 0
		}
		v := b[0]
		b = b[1:]
		return v
	}
	cfg := Config{Sink: sink, SamplingRate: compileRate(next())}

	if next()&1 != 0 {
		cfg.LevelSamplingRates = map[Level]float64{}
		for i := next() % 5; i > 0; i-- {
			cfg.LevelSamplingRates[compileLevels[int(next())%len(compileLevels)]] = compileRate(next())
		}
	}
	if next()&1 != 0 {
		cfg.Sampler = NeverSampler()
	}
	if next()&1 != 0 {
		cfg.OperationPolicies = map[Domain]OperationPolicy{}
		for i := next() % 5; i > 0; i-- {
			domain := compileDomains[int(next())%len(compileDomains)]
			policy := OperationPolicy{
				SuccessLevel: compileLevels[int(next())%len(compileLevels)],
				FailureLevel: compileLevels[int(next())%len(compileLevels)],
				PanicLevel:   compileLevels[int(next())%len(compileLevels)],
			}
			if next()&1 != 0 {
				policy.OutcomeLevels = map[Outcome]Level{}
				for j := next() % 4; j > 0; j-- {
					policy.OutcomeLevels[compileOutcomes[int(next())%len(compileOutcomes)]] = compileLevels[int(next())%len(compileLevels)]
				}
			}
			if next()&1 != 0 {
				r := compileRate(next())
				policy.SamplingRate = &r
			}
			cfg.OperationPolicies[domain] = policy
		}
	}
	if next()&1 != 0 {
		msg := b
		cfg.Message = string(msg)
	}
	return cfg
}

// configIsValid mirrors Compile's acceptance predicate independently:
// the rate ranges and the level/outcome keys. (Domain keys never fail
// validation — any string domain compiles.)
func configIsValid(cfg Config) bool {
	if !(cfg.SamplingRate >= 0 && cfg.SamplingRate <= 1) {
		return false
	}
	for level, rate := range cfg.LevelSamplingRates {
		if !IsValidLevel(level) || !(rate >= 0 && rate <= 1) {
			return false
		}
	}
	for _, policy := range cfg.OperationPolicies {
		if !IsValidLevel(policy.SuccessLevel) || !IsValidLevel(policy.FailureLevel) || !IsValidLevel(policy.PanicLevel) {
			return false
		}
		for outcome, level := range policy.OutcomeLevels {
			if !IsValidOutcome(outcome) || !IsValidLevel(level) {
				return false
			}
		}
		if policy.SamplingRate != nil {
			if r := *policy.SamplingRate; !(r >= 0 && r <= 1) {
				return false
			}
		}
	}
	return true
}

// driveOneRequest runs one full request through rt, asserting nothing
// panics and that emission is consistent with the sink. withErr makes
// the event an error (amendment-4 bypass): its emission is then
// deterministic — immune to every rate and sampler in the config.
// Returns the captured level, message, and emission for the probes.
func driveOneRequest(t *testing.T, rt *Runtime, name string, withErr bool) (level Level, msg string, emitted bool) {
	t.Helper()
	ts, _ := rt.sink.(*TestSink)
	if ts != nil {
		ts.Reset()
	}
	op := Start(context.Background(), rt, OperationStart{Domain: DomainJob, Name: name})
	Add(op.Context(), "k", "v")
	if withErr {
		err := errors.New("probe-error")
		emitted = op.End(&err)
	} else {
		emitted = op.End(nil)
	}
	if ts != nil {
		events := ts.Events()
		if emitted != (len(events) == 1) {
			t.Fatalf("emitted=%v but captured %d events", emitted, len(events))
		}
		if emitted {
			return events[0].Level(), events[0].Message(), true
		}
	} else if emitted {
		t.Fatal("runtime with a nil sink reported emission")
	}
	return 0, "", emitted
}

func FuzzCompileConfig(f *testing.F) {
	f.Add([]byte{0})                      // all defaults
	f.Add([]byte{0, 1, 5})                // NaN global rate
	f.Add([]byte{3, 255})                 // negative rate
	f.Add([]byte{4, 1, 2, 3, 4, 5, 6, 7}) // levelRates with invalid level
	f.Add([]byte{6, 0})                   // valid interior rate
	f.Fuzz(func(t *testing.T, b []byte) {
		cfg := configFromBytes(b, NewTestSink())
		rt, err := Compile(cfg)
		if err != nil {
			if configIsValid(cfg) {
				t.Fatalf("Compile rejected a valid config: %v", err)
			}
			// Sentinel contract: errors.Is works against the wrapped
			// sentinel, with the "unolog: " prefix.
			sentinel := false
			for _, s := range []error{ErrInvalidRate, ErrInvalidLevel, ErrInvalidOutcome} {
				if errors.Is(err, s) {
					sentinel = true
				}
			}
			if !sentinel {
				t.Fatalf("error %q wraps no sentinel", err)
			}
			if !strings.HasPrefix(err.Error(), "unolog: ") {
				t.Fatalf("error %q lacks the unolog: prefix", err)
			}
			return
		}

		if !configIsValid(cfg) {
			t.Fatalf("Compile accepted an invalid config: rate=%v levels=%v policies=%v",
				cfg.SamplingRate, cfg.LevelSamplingRates, cfg.OperationPolicies)
		}

		// Probe 1 — the compiled runtime is usable end to end. An error
		// event emits deterministically (the amendment-4 bypass makes
		// emission immune to every rate and sampler in the config).
		if _, _, emitted := driveOneRequest(t, rt, "error-drive", true); !emitted {
			t.Fatal("valid runtime dropped an error event")
		}

		// Probe 2 — immutability under deterministic rates. Healthy
		// emission must be decided by rate 1 (keep), never by a coin
		// flip: the pre/post comparison below then catches ANY aliasing
		// of the caller's config — a leaked map or *float64 flips
		// emission, level, or message between the two drives (review
		// findings GLM-2 / DS-9). The hostile cfg itself was validated
		// by the Compile above; the probe clone only makes the rates
		// deterministic.
		probeCfg := cfg
		probeCfg.SamplingRate = 1
		for level := range probeCfg.LevelSamplingRates {
			probeCfg.LevelSamplingRates[level] = 1
		}
		for domain, policy := range probeCfg.OperationPolicies {
			if policy.SamplingRate != nil {
				one := 1.0
				policy.SamplingRate = &one
				probeCfg.OperationPolicies[domain] = policy
			}
		}
		rt2, err := Compile(probeCfg)
		if err != nil {
			t.Fatalf("deterministic probe config rejected: %v", err)
		}
		preLevel, preMsg, preEmitted := driveOneRequest(t, rt2, "pre-mutation", false)

		// Mutate every mutable field of the caller's config.
		probeCfg.SamplingRate = 0 // was 1: a leaked rate drops the healthy event
		probeCfg.Message = "mutated-message"
		for level := range probeCfg.LevelSamplingRates {
			probeCfg.LevelSamplingRates[level] = 0 // leaked map drops the healthy event
		}
		for domain, policy := range probeCfg.OperationPolicies {
			policy.SuccessLevel = LevelError // leaked policy raises the level
			policy.FailureLevel = LevelDebug
			if policy.OutcomeLevels != nil {
				policy.OutcomeLevels[OutcomeRetry] = LevelWarn
			}
			if policy.SamplingRate != nil {
				zero := 0.0
				policy.SamplingRate = &zero // leaked rate pointer drops the event
			}
			probeCfg.OperationPolicies[domain] = policy
		}
		postLevel, postMsg, postEmitted := driveOneRequest(t, rt2, "post-mutation", false)

		// The compiled runtime must behave identically: emission first
		// (unconditional — a leaked rate would flip it), then level and
		// message when the healthy event was emitted at all.
		if postEmitted != preEmitted {
			t.Fatalf("config mutation flipped emission: pre=%v post=%v — "+
				"the runtime aliases the caller's config", preEmitted, postEmitted)
		}
		if preEmitted {
			if postMsg != preMsg {
				t.Fatalf("runtime message changed after config mutation: %q -> %q", preMsg, postMsg)
			}
			if postLevel != preLevel {
				t.Fatalf("runtime level changed after config mutation: %v -> %v", preLevel, postLevel)
			}
		}
	})
}

// TestCompileConfigPropertyRandom drives the same oracle over
// PCG-generated configs — seed-only coverage without the fuzzer.
func TestCompileConfigPropertyRandom(t *testing.T) {
	rng := rand.New(rand.NewPCG(0xC0FFE5eed, 0x00F1E5))
	for i := range 2000 {
		buf := make([]byte, 1+rng.IntN(60))
		for j := range buf {
			buf[j] = byte(rng.Uint64())
		}
		cfg := configFromBytes(buf, NewTestSink())
		rt, err := Compile(cfg)
		if err != nil {
			if configIsValid(cfg) {
				t.Fatalf("iter %d: Compile rejected valid config %+v: %v", i, cfg, err)
			}
			sentinel := errors.Is(err, ErrInvalidRate) || errors.Is(err, ErrInvalidLevel) || errors.Is(err, ErrInvalidOutcome)
			if !sentinel {
				t.Fatalf("iter %d: error %q wraps no sentinel", i, err)
			}
			continue
		}
		if !configIsValid(cfg) {
			t.Fatalf("iter %d: Compile accepted invalid config", i)
		}
		driveOneRequest(t, rt, fmt.Sprintf("iter-%d", i), true) // error drive: deterministic
	}
}
