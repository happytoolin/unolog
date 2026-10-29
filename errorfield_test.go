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

type matchesAnything struct{}

func (matchesAnything) Error() string { return "matches anything" }
func (matchesAnything) Is(error) bool { return true }

// safeErrorIs and sameError keep errors.Is semantics for nil operands:
// a nil target matches only a nil error, and Is is not called for it.
func TestSafeErrorIsNilOperands(t *testing.T) {
	boom := errors.New("boom")
	if structuredErrorField(nil) != nil {
		t.Fatal("structuredErrorField(nil) is not nil")
	}
	for _, c := range []struct {
		err, target error
	}{{nil, nil}, {nil, boom}, {boom, nil}, {boom, boom}, {boom, errors.New("boom")}, {matchesAnything{}, nil}} {
		want := errors.Is(c.err, c.target)
		if got := safeErrorIs(c.err, c.target); got != want {
			t.Fatalf("safeErrorIs(%v, %v) = %t, want %t", c.err, c.target, got, want)
		}
		if got := sameError(c.err, c.target); got != want {
			t.Fatalf("sameError(%v, %v) = %t, want %t", c.err, c.target, got, want)
		}
	}
}

type stringerFunc func() string

func (f stringerFunc) String() string { return f() }

type frameworkAnyError struct {
	Code    int
	Message any
}

func (e frameworkAnyError) Error() string {
	return fmt.Sprintf("code=%d, message=%v", e.Code, e.Message)
}

// A framework error logs its Message when the Message has text, and its
// own Error() text when the Message is unset or empty.
func TestFrameworkErrorMessageValues(t *testing.T) {
	for _, c := range []struct {
		message any
		want    string
	}{
		{"not found", "not found"},
		{stringerFunc(func() string { return "from stringer" }), "from stringer"},
		{42, "42"},
		{nil, "code=404, message=<nil>"},
		{"", "code=404, message="},
		{stringerFunc(func() string { return "" }), "code=404, message="},
		// A panicking String() gets the fmt rendering, as Error() does.
		{stringerFunc(func() string { panic("boom") }), "%!v(PANIC=String method: boom)"},
	} {
		err := frameworkAnyError{Code: 404, Message: c.message}
		if got := structuredErrorMessage(err); got != c.want {
			t.Fatalf("Message %#v: message = %q, want %q", c.message, got, c.want)
		}
	}
}

type emptyText string

type frameworkTextCodeError struct {
	Code    string
	Message string
}

func (frameworkTextCodeError) Error() string { return "text code" }

// A framework error needs an integer Code, and a Message whose text is
// not empty. Otherwise its own Error() text is the message.
func TestFrameworkErrorMessageNeedsIntCodeAndText(t *testing.T) {
	if got := structuredErrorMessage(frameworkTextCodeError{Code: "E1", Message: "m"}); got != "text code" {
		t.Fatalf("string Code: message = %q, want the Error() text", got)
	}
	if got := structuredErrorMessage(frameworkAnyError{Code: 404, Message: emptyText("")}); got != "code=404, message=" {
		t.Fatalf("empty named Message: message = %q, want the Error() text", got)
	}
}

type nilTextError struct{}

func (*nilTextError) Error() string { return "nil receiver text" }

// A typed-nil error encodes as <nil>: its Error method is not called,
// also when it would not panic. A nil error has no message.
func TestTypedNilErrorFieldEncodesNil(t *testing.T) {
	if got := string(appendFieldJSON([]byte("{"), Any("e", (*nilTextError)(nil)), 0)); got != `{"e":"<nil>"` {
		t.Fatalf("typed-nil error field = %s", got)
	}
	if safeErrorMessage(nil) != "" || structuredErrorMessage(nil) != "" {
		t.Fatal("nil error has a message")
	}
}

// layerError is a cheap comparable wrapper for deep chains.
type layerError struct{ next error }

func (*layerError) Error() string   { return "layer" }
func (e *layerError) Unwrap() error { return e.next }

func layers(n int, root error) error {
	err := root
	for range n {
		err = &layerError{next: err}
	}
	return err
}

// A chain of inlineVisits errors fits the inline visit set, so neither
// walk allocates.
func TestShortErrorWalksDoNotAllocate(t *testing.T) {
	err := layers(inlineVisits-1, errors.New("root"))
	target := errors.New("absent")
	allocs := testing.AllocsPerRun(100, func() {
		safeErrorIs(err, target)
		_ = deepestUnwrappedError(err)
	})
	if allocs != 0 {
		t.Fatalf("short error walks allocated %.1f times per call, want 0", allocs)
	}
}

type arrayAnyError [1]any

func (arrayAnyError) Error() string { return "array" }

// isComparableError reports whether == on err cannot panic: a struct or
// an array depends on its dynamic contents, every other kind on its type.
func TestIsComparableError(t *testing.T) {
	for i, c := range []struct {
		err  error
		want bool
	}{
		{nil, true},
		{errors.New("pointer"), true},
		{frameworkAnyError{Code: 1, Message: "text"}, true},
		{frameworkAnyError{Code: 1, Message: []int{1}}, false},
		{untrackedError{}, false},
		{arrayAnyError{"text"}, true},
		{arrayAnyError{[]int{1}}, false},
		{referenceSliceError{}, false},
	} {
		if got := isComparableError(c.err); got != c.want {
			t.Errorf("case %d: isComparableError(%T) = %t, want %t", i, c.err, got, c.want)
		}
	}
}

// The walks visit at most maxUnwrapDepth errors: a root at the last
// node within the budget is found, one node deeper is not.
func TestErrorWalksStopAtTheBudget(t *testing.T) {
	if !safeErrorIs(layers(maxUnwrapDepth-1, context.Canceled), context.Canceled) {
		t.Fatal("safeErrorIs missed a target inside the budget")
	}
	if safeErrorIs(layers(maxUnwrapDepth, context.Canceled), context.Canceled) {
		t.Fatal("safeErrorIs found a target outside the budget")
	}
	// errors.Join shares the budget: the second child is found while
	// steps remain after the first child.
	first := layers(maxUnwrapDepth-3, errors.New("x")) // maxUnwrapDepth-2 errors
	if !safeErrorIs(errors.Join(first, context.Canceled), context.Canceled) {
		t.Fatal("safeErrorIs missed the second child inside the budget")
	}
	root := errors.New("root")
	if got := structuredErrorField(layers(maxUnwrapDepth, root))["cause.message"]; got != "root" {
		t.Fatalf("cause inside the budget = %v, want root", got)
	}
	if got := structuredErrorField(layers(maxUnwrapDepth+1, root))["cause.message"]; got != "layer" {
		t.Fatalf("cause outside the budget = %v, want the last layer in the budget", got)
	}
}

var ringUnwraps atomic.Int64

// ringError is comparable; sliceRingError is incomparable but has a
// reference. Graphs longer than inlineVisits move the visited set to
// its maps.
type ringError struct {
	name string
	next *ringError
}

func (e *ringError) Error() string { return e.name }
func (e *ringError) Unwrap() error {
	ringUnwraps.Add(1)
	return e.next
}

type sliceRingError []error

func (sliceRingError) Error() string { return "slice ring" }
func (e sliceRingError) Unwrap() error {
	ringUnwraps.Add(1)
	return e[0]
}

// A cycle longer than the inline visited slots stops at the first error
// the walk sees again. The loop goes back to an error from the inline
// slots, or to one that the walk saw after it moved to the map.
func TestErrorWalksStopLongCycles(t *testing.T) {
	const n = 3 * inlineVisits
	for _, back := range []int{0, 1, inlineVisits + 2} {
		nodes := make([]*ringError, n)
		for i := range n {
			nodes[i] = &ringError{name: fmt.Sprintf("e%d", i)}
		}
		for i := range n - 1 {
			nodes[i].next = nodes[i+1]
		}
		nodes[n-1].next = nodes[back]
		ringUnwraps.Store(0)
		if got, want := structuredErrorField(nodes[0])["cause.message"], fmt.Sprintf("e%d", back); back > 0 && got != want {
			t.Errorf("loop to e%d: cause = %v, want %s", back, got, want)
		}
		if safeErrorIs(nodes[0], context.Canceled) {
			t.Errorf("loop to e%d matched cancellation", back)
		}
		if calls := ringUnwraps.Load(); calls > 4*n {
			t.Errorf("loop to e%d: %d Unwrap calls for %d errors", back, calls, n)
		}
	}

	refs := make([]sliceRingError, n)
	for i := range n {
		refs[i] = make(sliceRingError, 1)
	}
	for i := range n {
		refs[i][0] = refs[(i+1)%n]
	}
	ringUnwraps.Store(0)
	_ = structuredErrorField(refs[0])
	if safeErrorIs(refs[0], context.Canceled) {
		t.Error("reference ring matched cancellation")
	}
	if calls := ringUnwraps.Load(); calls > 4*n {
		t.Errorf("reference ring: %d Unwrap calls for %d errors", calls, n)
	}
}

type frameworkPointerError struct {
	Code    int
	Message *string
}

func (frameworkPointerError) Error() string { return "pointer message" }

// A pointer Message is read through the pointer.
func TestFrameworkErrorPointerMessage(t *testing.T) {
	text := "from pointer"
	if got := structuredErrorMessage(frameworkPointerError{Code: 400, Message: &text}); got != text {
		t.Fatalf("message = %q, want %q", got, text)
	}
	if got := structuredErrorMessage(frameworkPointerError{Code: 400}); got != "pointer message" {
		t.Fatalf("nil Message: message = %q, want the Error() text", got)
	}
}

// untrackedError is neither comparable nor reference-backed: the walk
// cannot track it, so two of them in a chain are not a cycle.
type untrackedError struct {
	_    []string // makes the type incomparable
	next error
}

func (untrackedError) Error() string   { return "untracked" }
func (e untrackedError) Unwrap() error { return e.next }

func TestSafeErrorIsWalksUntrackedChains(t *testing.T) {
	err := untrackedError{next: untrackedError{next: context.Canceled}}
	if !safeErrorIs(err, context.Canceled) {
		t.Fatal("two untracked errors hid the cancellation")
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
