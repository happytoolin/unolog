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
	// Typed-nil errors (non-nil interface, nil pointer) must not reach
	// Error()/Unwrap(): they panic on nil dereference and would crash
	// finalization. fmt renders them safely as "<nil>".
	if isTypedNilError(err) {
		return map[string]any{
			"message": fmt.Sprint(err),
			"type":    fmt.Sprintf("%T", err),
		}
	}
	field := map[string]any{
		"message": structuredErrorMessage(err),
		"type":    fmt.Sprintf("%T", err),
	}

	if cause := deepestUnwrappedError(err); cause != nil && !sameError(cause, err) {
		field["cause.message"] = structuredErrorMessage(cause)
		field["cause.type"] = fmt.Sprintf("%T", cause)
	}

	return field
}

// isTypedNilError reports whether err is a non-nil interface holding
// a nil pointer — the value whose Error() call would panic.
func isTypedNilError(err error) bool {
	if err == nil {
		return false
	}
	v := reflect.ValueOf(err)
	return v.Kind() == reflect.Pointer && v.IsNil()
}

func structuredPanicField(recovered any) map[string]any {
	return map[string]any{
		"type":  fmt.Sprintf("%T", recovered),
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
		return fmt.Sprint(err) // renders "<nil>"
	}
	defer func() {
		if recover() != nil {
			msg = fmt.Sprint(err)
		}
	}()
	return err.Error()
}

// maxUnwrapDepth bounds the Unwrap walk in deepestUnwrappedError.
// Real wrapped chains are shallow; the bound also ends any chain that
// manages to cycle without repeating a comparable error value.
const maxUnwrapDepth = 100

func deepestUnwrappedError(err error) error {
	current := err
	seen := make(map[error]struct{})
	for depth := 0; current != nil; depth++ {
		if depth >= maxUnwrapDepth {
			return current
		}
		if isComparableError(current) {
			if _, ok := seen[current]; ok {
				return current
			}
			seen[current] = struct{}{}
		}
		next := safeUnwrap(current)
		if next == nil {
			return current
		}
		current = next
	}
	return nil
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
	return reflect.TypeOf(err).Comparable()
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
func frameworkStyleErrorMessage(err error) (string, bool) {
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
	text := fmt.Sprint(message)
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
		if field.Kind() == reflect.Interface && !field.IsNil() {
			inner := field.Elem()
			if inner.IsValid() && inner.CanInterface() {
				return inner.Interface(), true
			}
		}
		return value, true
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
