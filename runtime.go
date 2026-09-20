package unolog

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"
)

// Compile-time error contract: Compile returns errors wrapping these
// sentinels with an "unolog: " prefix; errors.Is works.
var (
	// ErrInvalidRate wraps out-of-range sampling rates.
	ErrInvalidRate = errors.New("invalid rate")
	// ErrInvalidLevel wraps unknown level keys in configuration maps.
	ErrInvalidLevel = errors.New("invalid level")
	// ErrInvalidOutcome wraps unknown outcome keys in configuration maps.
	ErrInvalidOutcome = errors.New("invalid outcome")
)

// Config describes how a Runtime finalizes events. Bad configuration is
// a construction-time error (Compile), never a per-request clamp.
type Config struct {
	// Sink receives finalized records. Nil is valid: the runtime emits
	// nothing (the v0 zero-config behavior, kept deliberately).
	Sink Sink

	// SamplingRate controls random sampling for healthy traffic in [0,1].
	SamplingRate float64

	// LevelSamplingRates optionally overrides SamplingRate by final
	// level. Keys must be valid levels; values in [0,1].
	LevelSamplingRates map[Level]float64

	// Sampler overrides built-in rate sampling when set. Errors and
	// panics bypass it structurally.
	Sampler Sampler

	// OperationPolicies optionally customizes lifecycle behavior by
	// domain; a domain SamplingRate overrides generic rates (and, when
	// both are set, overrides LevelSamplingRates for that domain — v0
	// precedence, documented in the README). The "" key is an alias
	// for the default domain ("operation"): it applies only there,
	// and an explicit "operation" key beats it.
	OperationPolicies map[Domain]OperationPolicy

	// Message is the default final message, overriding the per-domain
	// defaults (request_completed / operation_completed).
	Message string

	// Clock optionally supplies every timestamp the hot path reads: the
	// operation's start, its completion stamp, and the duration between
	// them. Nil uses the built-in clock: a direct arm64 counter read (or
	// the runtime's monotonic clock elsewhere) plus a wall anchor
	// refreshed at most once per 100 ms, so the hot path makes no
	// wall-clock syscall. A coarse clock (a value refreshed off the hot
	// path, e.g. by a shared ticker the caller owns) turns each read into
	// an atomic load. The trade-off is
	// resolution, not correctness: duration_ms and Record.Time() quantize
	// to the clock's tick, while the wire stamp stays RFC 3339
	// (second precision) — so keep the tick well under a second unless
	// sub-second duration_ms matters to the sampler.
	//
	// A supplied clock must return non-decreasing times for the runtime's
	// lifetime. It is called on the request goroutine; do not block.
	Clock func() time.Time
}

// Runtime is the immutable, compiled form of Config, shared by all
// requests. Integrations take *Runtime, never Config.
type Runtime struct {
	sink       Sink
	sampler    Sampler
	rate       float64
	levelRates map[Level]float64
	policies   map[Domain]OperationPolicy
	message    string
	clock      func() time.Time

	// alwaysKeep is the compiled keep-everything fast path: rate
	// exactly 1.0 with no sampler, per-level rates, or policies means
	// healthy events can never be sampled out, so commit skips the
	// whole gate. Error events bypass sampling structurally anyway
	// (amendment 4), so the flag is unaffected by them.
	alwaysKeep bool
}

// Compile validates cfg and returns an immutable *Runtime. Errors wrap
// the sentinel values (ErrInvalidRate, ErrInvalidLevel, ErrInvalidOutcome)
// with %w and an "unolog: " prefix; errors.Is works.
func Compile(cfg Config) (*Runtime, error) {
	rt := &Runtime{
		sink:    cfg.Sink,
		sampler: cfg.Sampler,
		message: cfg.Message,
		rate:    cfg.SamplingRate,
		clock:   cfg.Clock,
	}

	if !(rt.rate >= 0 && rt.rate <= 1) {
		return nil, fmt.Errorf("unolog: sampling rate %g: %w", rt.rate, ErrInvalidRate)
	}

	if len(cfg.LevelSamplingRates) > 0 {
		rt.levelRates = make(map[Level]float64, len(cfg.LevelSamplingRates))
		// Sorted iteration (like the policies loop): with multiple
		// invalid entries, the first error reported is deterministic.
		for _, level := range slices.Sorted(maps.Keys(cfg.LevelSamplingRates)) {
			rate := cfg.LevelSamplingRates[level]
			if !IsValidLevel(level) {
				return nil, fmt.Errorf("unolog: level sampling rate for level %d: %w", int(level), ErrInvalidLevel)
			}
			if !(rate >= 0 && rate <= 1) {
				return nil, fmt.Errorf("unolog: sampling rate %g for %s: %w", rate, level, ErrInvalidRate)
			}
			rt.levelRates[level] = rate
		}
	}

	if len(cfg.OperationPolicies) > 0 {
		rt.policies = make(map[Domain]OperationPolicy, len(cfg.OperationPolicies))
		// Sorted iteration: the "" alias (which sorts first) lands before
		// any explicit key, so an explicit "operation" overwrites it
		// deterministically — and with multiple invalid policies, the
		// first error reported is deterministic too.
		for _, domain := range slices.Sorted(maps.Keys(cfg.OperationPolicies)) {
			policy := cfg.OperationPolicies[domain]
			explicit := domain != ""
			if !explicit {
				domain = defaultDomainValue
			}
			if _, clash := rt.policies[domain]; clash && !explicit {
				continue // unreachable under sorted iteration; kept as a guard
			}
			if err := validatePolicy(policy); err != nil {
				return nil, fmt.Errorf("unolog: policy for domain %q: %w", domain, err)
			}
			rt.policies[domain] = copyPolicy(policy)
		}
	}

	// Compiled keep-everything fast path (see the field comment).
	rt.alwaysKeep = rt.rate == 1.0 && rt.sampler == nil && len(rt.policies) == 0 && len(rt.levelRates) == 0

	return rt, nil
}

// MustCompile is Compile for literal configurations in main; a bad
// config panics at startup, the regexp idiom.
func MustCompile(cfg Config) *Runtime {
	rt, err := Compile(cfg)
	if err != nil {
		panic(err)
	}
	return rt
}

func validatePolicy(policy OperationPolicy) error {
	if !IsValidLevel(policy.SuccessLevel) {
		return fmt.Errorf("success level %d: %w", int(policy.SuccessLevel), ErrInvalidLevel)
	}
	if !IsValidLevel(policy.FailureLevel) {
		return fmt.Errorf("failure level %d: %w", int(policy.FailureLevel), ErrInvalidLevel)
	}
	if !IsValidLevel(policy.PanicLevel) {
		return fmt.Errorf("panic level %d: %w", int(policy.PanicLevel), ErrInvalidLevel)
	}
	for outcome, level := range policy.OutcomeLevels {
		if !IsValidOutcome(outcome) {
			return fmt.Errorf("outcome %q: %w", string(outcome), ErrInvalidOutcome)
		}
		if !IsValidLevel(level) {
			return fmt.Errorf("level %d for outcome %q: %w", int(level), string(outcome), ErrInvalidLevel)
		}
	}
	if policy.SamplingRate != nil {
		if r := *policy.SamplingRate; !(r >= 0 && r <= 1) {
			return fmt.Errorf("sampling rate %g: %w", r, ErrInvalidRate)
		}
	}
	return nil
}

// copyPolicy deep-copies a policy so the compiled runtime shares no
// mutable state with the caller's Config.
func copyPolicy(policy OperationPolicy) OperationPolicy {
	if policy.OutcomeLevels != nil {
		policy.OutcomeLevels = maps.Clone(policy.OutcomeLevels)
	}
	if policy.SamplingRate != nil {
		rate := *policy.SamplingRate
		policy.SamplingRate = &rate
	}
	return policy
}

// levelFromPolicy resolves the final severity for an outcome under a
// domain policy. Zero fields mean the defaults; every other value was
// validated by Compile, so no re-validation is needed here.
func levelFromPolicy(policy OperationPolicy, outcome Outcome) Level {
	// The zero policy (default config) has a nil OutcomeLevels, so the
	// common path pays one pointer compare instead of a map probe.
	if policy.OutcomeLevels != nil {
		if outcomeLevel, ok := policy.OutcomeLevels[outcome]; ok {
			return outcomeLevel
		}
	}

	// Zero policy fields mean the defaults; the zero Level is Info,
	// which is the success default anyway.
	successLevel, failureLevel, panicLevel := LevelInfo, LevelError, LevelError
	if policy.SuccessLevel != 0 {
		successLevel = policy.SuccessLevel
	}
	if policy.FailureLevel != 0 {
		failureLevel = policy.FailureLevel
	}
	if policy.PanicLevel != 0 {
		panicLevel = policy.PanicLevel
	}

	switch outcome {
	case OutcomeSuccess:
		return successLevel
	case OutcomePanic:
		return panicLevel
	default:
		return failureLevel
	}
}

// noop reports whether the runtime can never emit (nil sink).
func (rt *Runtime) noop() bool { return rt == nil || rt.sink == nil }

// clockOf returns the configured clock, or nil for the wall clock. It
// stays call-free, so it inlines into wherever the clock is read and
// the default path keeps a direct time.Now: a wrapper holding both
// calls never fits the inline budget, and that extra frame on every
// operation costs more than the clock it saves.
func (rt *Runtime) clockOf() func() time.Time {
	if rt == nil {
		return nil
	}
	return rt.clock
}

// normalizeDomain maps the zero Domain to the default "operation"
// domain; every other value passes through unchanged.
func normalizeDomain(domain Domain) Domain {
	if domain == "" {
		return defaultDomainValue
	}
	return domain
}

// policyFor returns the domain policy (zero value when unconfigured).
func (rt *Runtime) policyFor(domain Domain) OperationPolicy {
	if rt.policies == nil {
		return OperationPolicy{}
	}
	domain = normalizeDomain(domain)
	if policy, ok := rt.policies[domain]; ok {
		return policy
	}
	return OperationPolicy{}
}

// Emit writes rec to the configured sink. The nil-runtime and nil-sink
// cases are no-ops by contract.
func (rt *Runtime) emit(ctx context.Context, rec *Record) {
	if rt.noop() {
		return
	}
	rt.sink.Write(ctx, rec)
}
