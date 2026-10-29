package unolog

import (
	"errors"
	"fmt"
	"reflect"
)

func structuredErrorField(err error) map[string]any {
	if err == nil {
		return nil
	}
	// Typed-nil errors must not reach Error()/Unwrap(): methods on any
	// nil-capable defined type can panic or loop during finalization.
	if isTypedNilError(err) {
		return map[string]any{
			"message": "<nil>",
			"type":    typeName(err),
		}
	}
	cause := deepestUnwrappedError(err)
	if cause == nil || sameError(cause, err) {
		return map[string]any{
			"message": structuredErrorMessage(err),
			"type":    typeName(err),
		}
	}
	return map[string]any{
		"message":       structuredErrorMessage(err),
		"type":          typeName(err),
		"cause.message": structuredErrorMessage(cause),
		"cause.type":    typeName(cause),
	}
}

// typeName is fmt's %T rendering for a non-nil value without the fmt
// machinery: reflect's type string lives in the binary, no allocation.
func typeName(v any) string {
	return reflect.TypeOf(v).String()
}

// isTypedNilError reports whether err is a non-nil interface holding a
// nil value. Error implementations can use any nil-capable defined type.
func isTypedNilError(err error) bool {
	if err == nil {
		return false
	}
	v := reflect.ValueOf(err)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	default:
		return false
	}
}

func structuredPanicField(recovered any) map[string]any {
	return map[string]any{
		"type":  typeName(recovered),
		"value": fmt.Sprint(recovered),
	}
}

func structuredErrorMessage(err error) string {
	if err == nil {
		return ""
	}

	if message, ok := frameworkStyleErrorMessage(err); ok {
		return message
	}

	return safeErrorMessage(err)
}

// safeErrorMessage extracts err's message without ever panicking:
// typed-nil receivers and wrapped typed-nils (whose Error() nil-derefs)
// and arbitrary panicking Error() implementations are all contained —
// the same fence the encoder puts around user MarshalJSON. fmt.Sprint
// recovers Error()-panics itself, so it is the fallback rendering.
func safeErrorMessage(err error) (msg string) {
	if err == nil {
		return ""
	}
	if isTypedNilError(err) {
		return "<nil>"
	}
	defer func() {
		if recover() != nil {
			msg = fmt.Sprint(err)
		}
	}()
	return err.Error()
}

// maxUnwrapDepth bounds genuinely acyclic, user-generated error graphs.
// Comparable errors and reference-backed incomparable errors stop on cycles.
const maxUnwrapDepth = 4096

func deepestUnwrappedError(err error) error {
	current := err
	var visited errorVisits
	for depth := 0; current != nil; depth++ {
		if depth >= maxUnwrapDepth {
			return current
		}
		if visited.revisit(current) {
			return current
		}
		next := safeUnwrap(current)
		if next == nil {
			return current
		}
		current = next
	}
	return nil
}

// inlineVisits is how many visited errors a walk tracks without
// allocating. Ordinary wrap chains are shorter; deeper or cyclic graphs
// move to maps.
const inlineVisits = 8

// errorVisits is the visited set of one error-graph walk. Comparable
// errors match by ==, reference-backed incomparable errors by their
// reference, exactly like a map keyed the same way; the first
// inlineVisits entries live in an array, so short chains never
// allocate.
type errorVisits struct {
	n        int
	inline   [inlineVisits]errorVisit
	seen     map[error]struct{}
	seenRefs map[errorReference]struct{}
}

type errorVisit struct {
	err error // set for comparable errors
	ref errorReference
}

// revisit records err and reports whether the walk saw it before.
// Errors that are neither comparable nor reference-backed cannot be
// tracked; the step budget bounds them.
func (v *errorVisits) revisit(err error) bool {
	var key errorVisit
	if isComparableError(err) {
		key.err = err
	} else if ref, ok := errorReferenceOf(err); ok {
		key.ref = ref
	} else {
		return false
	}
	if v.seen == nil {
		for i := range v.n {
			// Only comparable errors are stored in err, and interfaces
			// with different dynamic types compare unequal: no panic.
			if v.inline[i] == key {
				return true
			}
		}
		if v.n < inlineVisits {
			v.inline[v.n] = key
			v.n++
			return false
		}
		v.seen = make(map[error]struct{}, 2*inlineVisits)
		v.seenRefs = make(map[errorReference]struct{})
		for _, prior := range v.inline {
			v.insert(prior)
		}
	}
	if key.err != nil {
		if _, ok := v.seen[key.err]; ok {
			return true
		}
	} else if _, ok := v.seenRefs[key.ref]; ok {
		return true
	}
	v.insert(key)
	return false
}

func (v *errorVisits) insert(key errorVisit) {
	if key.err != nil {
		v.seen[key.err] = struct{}{}
	} else {
		v.seenRefs[key.ref] = struct{}{}
	}
}

func sameError(a, b error) bool {
	if a == nil || b == nil {
		return a == b //nolint:errorlint // exact identity is the point: cycle detection
	}
	if reflect.TypeOf(a) != reflect.TypeOf(b) {
		return false
	}
	if !isComparableError(a) {
		return false
	}
	return a == b //nolint:errorlint // exact identity is the point: cycle detection
}

func isComparableError(err error) bool {
	if err == nil {
		return true
	}
	// Type.Comparable is static and free. Only a struct or an array can
	// hold an interface whose dynamic value makes == panic, so only those
	// need Value.Comparable, which allocates on every call.
	t := reflect.TypeOf(err)
	switch t.Kind() {
	case reflect.Struct, reflect.Array:
		return t.Comparable() && reflect.ValueOf(err).Comparable()
	default:
		return t.Comparable()
	}
}

type errorReference struct {
	typ reflect.Type
	ptr uintptr
	len int
	cap int
}

func errorReferenceOf(err error) (errorReference, bool) {
	v := reflect.ValueOf(err)
	switch v.Kind() {
	case reflect.Slice:
		if v.Len() == 0 {
			return errorReference{}, false
		}
		return errorReference{typ: v.Type(), ptr: v.Pointer(), len: v.Len(), cap: v.Cap()}, true
	case reflect.Chan, reflect.Map, reflect.Pointer:
		return errorReference{typ: v.Type(), ptr: v.Pointer()}, true
	default:
		return errorReference{}, false
	}
}

// safeErrorIs preserves errors.Is matching for ordinary error trees,
// but bounds hostile graphs and contains panics in Is and Unwrap.
func safeErrorIs(err, target error) bool {
	if err == nil || target == nil {
		return err == target //nolint:errorlint // match errors.Is nil semantics without traversal
	}
	if isTypedNilError(err) {
		return false
	}
	remaining := maxUnwrapDepth
	var visited errorVisits
	return matchError(err, target, &visited, &remaining)
}

func matchError(err, target error, visited *errorVisits, remaining *int) (matched bool) {
	defer func() {
		if recover() != nil {
			matched = false
		}
	}()
	for *remaining > 0 {
		*remaining--
		if err == nil {
			return false
		}
		if sameError(err, target) {
			return true
		}
		if visited.revisit(err) {
			return false
		}
		if matcher, ok := err.(interface{ Is(error) bool }); ok && matcher.Is(target) {
			return true
		}
		switch wrapper := err.(type) { //nolint:errorlint // walk one node; errors.Is itself is unbounded
		case interface{ Unwrap() error }:
			err = wrapper.Unwrap()
		case interface{ Unwrap() []error }:
			for _, child := range wrapper.Unwrap() {
				if *remaining == 0 {
					return false
				}
				if matchError(child, target, visited, remaining) {
					return true
				}
			}
			return false
		default:
			return false
		}
	}
	return false
}

// frameworkStyleErrorMessage recognizes framework-shaped errors — any
// error whose struct (possibly behind a pointer) has exported Code
// (integer) and Message (string or fmt.Stringer) fields, the
// echo.HTTPError and fiber.Error shape — and surfaces Message as the
// canonical error.message instead of the wrapper's own Error() text.
// This is deliberate wire behavior (v0 parity): framework errors log
// the human message, not the wrapper text. Reflection is guarded
// (invalid, nil, and unexported values are skipped); String() calls
// are panic-fenced.
func frameworkStyleErrorMessage(err error) (text string, ok bool) {
	// FieldByName can panic when a promoted field crosses a nil embedded
	// pointer. Such an error still has its ordinary Error() fallback.
	defer func() {
		if recover() != nil {
			text, ok = "", false
		}
	}()
	value := reflect.ValueOf(err)
	if !value.IsValid() {
		return "", false
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return "", false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return "", false
	}

	codeField := value.FieldByName("Code")
	messageField := value.FieldByName("Message")
	if !codeField.IsValid() || !messageField.IsValid() {
		return "", false
	}
	if !isIntKind(codeField.Kind()) {
		return "", false
	}

	message, ok := messageValue(messageField)
	if !ok {
		return "", false
	}
	text = fmt.Sprint(message)
	if text == "" {
		return "", false
	}
	return text, true
}

func messageValue(field reflect.Value) (any, bool) {
	if !field.IsValid() {
		return nil, false
	}
	if field.Kind() == reflect.Pointer {
		if field.IsNil() {
			return nil, false
		}
		field = field.Elem()
	}
	if !field.CanInterface() {
		return nil, false
	}

	value := field.Interface()
	switch v := value.(type) {
	case string:
		if v == "" {
			return nil, false
		}
		return v, true
	case fmt.Stringer:
		text := safeString(v)
		if text == "" {
			return nil, false
		}
		return text, true
	default:
		// Interface returns the dynamic value of an interface field, so
		// nil here is an unset Message: Error() is the better text.
		return value, value != nil
	}
}

func isIntKind(kind reflect.Kind) bool {
	switch kind {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return true
	default:
		return false
	}
}

// safeString calls s.String with the same panic fence safeErrorMessage
// applies to Error.
func safeString(s fmt.Stringer) (text string) {
	defer func() {
		if recover() != nil {
			text = fmt.Sprint(s)
		}
	}()
	return s.String()
}

// safeUnwrap fences errors.Unwrap: wrappers with state-reading Unwrap
// methods panic on typed-nil receivers.
func safeUnwrap(err error) (next error) {
	defer func() {
		if recover() != nil {
			next = nil
		}
	}()
	return errors.Unwrap(err)
}
