package unolog

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
)

type interfaceValueError struct{ value any }

func (interfaceValueError) Error() string { return "interface value" }

type promotedErrorFields struct {
	Code    int
	Message string
}

type nilPromotedError struct{ *promotedErrorFields }

func (nilPromotedError) Error() string { return "nil embedded field" }

type panicUnwrapError struct{}

func (panicUnwrapError) Error() string { return "panic unwrap" }
func (panicUnwrapError) Unwrap() error { panic("unwrap") }

type panicIsError struct{}

func (panicIsError) Error() string { return "panic is" }
func (panicIsError) Is(error) bool { panic("is") }

type matchingContextError struct{ target error }

func (matchingContextError) Error() string { return "custom context match" }
func (e matchingContextError) Is(target error) bool {
	return target == e.target
}

// A counter makes an unbounded regression fail instead of hanging a
// test goroutine. The slice form also exercises an incomparable cycle.
type countedErrorCycle []int

func (countedErrorCycle) Error() string { return "cycle" }
func (e countedErrorCycle) Unwrap() error {
	e[0]++
	if e[0] > 1000 {
		panic("unbounded error traversal")
	}
	return e
}

type comparableErrorCycle struct{ countedErrorCycle }

func (e *comparableErrorCycle) Unwrap() error {
	_ = e.countedErrorCycle.Unwrap()
	return e
}

type referenceSliceError []error

func (referenceSliceError) Error() string     { return "reference slice" }
func (e referenceSliceError) Unwrap() []error { return []error(e) }

type nilSliceError []int

func (nilSliceError) Error() string { panic("nil slice Error") }
func (nilSliceError) Unwrap() error {
	nilSliceUnwrapCalls.Add(1)
	return nilSliceError(nil)
}

var nilSliceUnwrapCalls atomic.Int32

type fuzzGraphError struct {
	mode     byte
	children []error
}

const (
	graphOrdinary byte = iota
	graphCanceled
	graphDeadline
	graphPanicIs
	graphPanicUnwrap
	graphPanicError
)

func (e *fuzzGraphError) Error() string {
	if e.mode == graphPanicError {
		panic("graph Error")
	}
	return "graph error"
}

func (e *fuzzGraphError) Is(target error) bool {
	switch e.mode {
	case graphCanceled:
		return target == context.Canceled
	case graphDeadline:
		return target == context.DeadlineExceeded
	case graphPanicIs:
		panic("graph Is")
	default:
		return false
	}
}

func (e *fuzzGraphError) Unwrap() []error {
	if e.mode == graphPanicUnwrap {
		panic("graph Unwrap")
	}
	return e.children
}

var errFuzzPlain = errors.New("plain graph error")

func TestErrorFinalizationDefensiveTraversal(t *testing.T) {
	cycle := &comparableErrorCycle{countedErrorCycle: countedErrorCycle{0}}
	countedCycle := countedErrorCycle{0}
	var typedNil *os.PathError
	cases := []struct {
		name string
		err  error
		want Outcome
	}{
		{name: "interface containing slice", err: interfaceValueError{value: []int{1}}, want: OutcomeFailure},
		{name: "nil promoted fields", err: nilPromotedError{}, want: OutcomeFailure},
		{name: "typed nil unwrap", err: typedNil, want: OutcomeFailure},
		{name: "panic unwrap", err: panicUnwrapError{}, want: OutcomeFailure},
		{name: "panic is", err: panicIsError{}, want: OutcomeFailure},
		{name: "incomparable cycle", err: countedCycle, want: OutcomeFailure},
		{name: "comparable cycle", err: cycle, want: OutcomeFailure},
		{name: "cycle then cancellation", err: errors.Join(cycle, context.Canceled), want: OutcomeCanceled},
		{name: "panic then cancellation", err: errors.Join(panicIsError{}, context.Canceled), want: OutcomeCanceled},
		{name: "wrapped cancellation", err: fmt.Errorf("wrapped: %w", context.Canceled), want: OutcomeCanceled},
		{name: "wrapped deadline", err: fmt.Errorf("wrapped: %w", context.DeadlineExceeded), want: OutcomeTimeout},
		{name: "custom cancellation", err: matchingContextError{target: context.Canceled}, want: OutcomeCanceled},
		{name: "custom deadline", err: matchingContextError{target: context.DeadlineExceeded}, want: OutcomeTimeout},
		{
			name: "cancellation precedes deadline",
			err:  errors.Join(context.DeadlineExceeded, fmt.Errorf("wrapped: %w", context.Canceled)),
			want: OutcomeCanceled,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sink := NewTestSink()
			rt := MustCompile(Config{Sink: sink, SamplingRate: 0})
			op := Start(t.Context(), rt, OperationStart{Domain: DomainJob})
			err := tc.err
			if !op.End(&err) {
				t.Fatal("error event was not emitted")
			}
			events := sink.Events()
			if len(events) != 1 {
				t.Fatalf("event count = %d, want 1", len(events))
			}
			if got, _ := events[0].Lookup(KeyOpOutcome); got != string(tc.want) {
				t.Fatalf("outcome = %v, want %s", got, tc.want)
			}
			field, _ := events[0].Lookup(KeyError)
			detail, ok := field.(map[string]any)
			message, _ := detail["message"].(string)
			if !ok || message == "" {
				t.Fatalf("missing error message: %#v", field)
			}
		})
	}
	if countedCycle[0] > 3 {
		t.Fatalf("error traversal made %d unwrap calls", countedCycle[0])
	}
	if cycle.countedErrorCycle[0] > 6 {
		t.Fatalf("cyclic graph made %d unwrap calls", cycle.countedErrorCycle[0])
	}
}

func TestSafeErrorIsMatchesDeepAcyclicChain(t *testing.T) {
	err := context.Canceled
	for i := range 512 {
		err = fmt.Errorf("layer %d: %w", i, err)
	}
	if !safeErrorIs(err, context.Canceled) {
		t.Fatal("cancellation hidden by a valid 512-node chain")
	}
}

func TestSafeErrorIsDistinguishesSharedSlicePrefixes(t *testing.T) {
	backing := referenceSliceError{errors.New("other"), context.Canceled}
	err := errors.Join(backing[:1], backing[:2])
	if !safeErrorIs(err, context.Canceled) {
		t.Fatal("cancellation hidden by a longer slice sharing the same backing array")
	}
}

func TestTypedNilSliceErrorDoesNotCallError(t *testing.T) {
	nilSliceUnwrapCalls.Store(0)
	var err nilSliceError
	if got := structuredErrorField(err)["message"]; got != "<nil>" {
		t.Fatalf("message = %v, want <nil>", got)
	}
	if safeErrorIs(err, context.Canceled) {
		t.Fatal("typed-nil error matched cancellation")
	}
	if calls := nilSliceUnwrapCalls.Load(); calls != 0 {
		t.Fatalf("Unwrap calls = %d, want 0", calls)
	}
}

func FuzzErrorGraph(f *testing.F) {
	f.Add([]byte{graphOrdinary}, []byte{1})
	f.Add([]byte{graphOrdinary, graphPanicIs}, []byte{1, 2, 2})
	f.Add([]byte{graphOrdinary, graphCanceled}, []byte{1, 0})
	f.Add([]byte{graphPanicUnwrap}, []byte{1})
	f.Add([]byte{graphPanicError}, []byte{2})

	f.Fuzz(func(t *testing.T, modes, edges []byte) {
		root := buildFuzzErrorGraph(modes, edges)
		for _, target := range []error{context.Canceled, context.DeadlineExceeded} {
			got := safeErrorIs(root, target)
			want := referenceGraphIs(root, target, make(map[*fuzzGraphError]struct{}))
			if got != want {
				t.Fatalf("safeErrorIs(%v) = %t, want %t", target, got, want)
			}
		}
		field := structuredErrorField(root)
		if field["message"] == "" || field["type"] == "" {
			t.Fatalf("incomplete structured error: %#v", field)
		}
	})
}

func buildFuzzErrorGraph(modes, edges []byte) *fuzzGraphError {
	n := min(max(1, len(modes)), 32)
	nodes := make([]*fuzzGraphError, n)
	for i := range nodes {
		mode := byte(0)
		if i < len(modes) {
			mode = modes[i] % 6
		}
		nodes[i] = &fuzzGraphError{mode: mode}
	}
	for i, edge := range edges[:min(len(edges), 96)] {
		parent := nodes[i%n]
		switch child := int(edge) % (n + 3); child {
		case n:
			parent.children = append(parent.children, context.Canceled)
		case n + 1:
			parent.children = append(parent.children, context.DeadlineExceeded)
		case n + 2:
			parent.children = append(parent.children, errFuzzPlain)
		default:
			parent.children = append(parent.children, nodes[child])
		}
	}
	return nodes[0]
}

func referenceGraphIs(err, target error, seen map[*fuzzGraphError]struct{}) bool {
	if err == target { //nolint:errorlint // the reference oracle checks exact sentinel identity
		return true
	}
	node, ok := err.(*fuzzGraphError) //nolint:errorlint // the oracle owns this concrete graph type
	if !ok {
		return false
	}
	if _, exists := seen[node]; exists {
		return false
	}
	seen[node] = struct{}{}
	switch node.mode {
	case graphCanceled:
		if target == context.Canceled { //nolint:errorlint // exact oracle target
			return true
		}
	case graphDeadline:
		if target == context.DeadlineExceeded { //nolint:errorlint // exact oracle target
			return true
		}
	case graphPanicIs, graphPanicUnwrap:
		return false
	}
	for _, child := range node.children {
		if referenceGraphIs(child, target, seen) {
			return true
		}
	}
	return false
}
