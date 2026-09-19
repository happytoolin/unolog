// Package bridge provides the helpers shared by the first-party sink
// bridges (the slog, zap, and zerolog adapters) and the core encoder, so
// every output path resolves duplicate fields identically and renders
// error fields with the same panic fencing. The bridges live in separate
// modules that require this one, so the shared code is exported here
// rather than under internal/.
package wire

import (
	"fmt"
	"reflect"
	"slices"
)

// NarrowLimit is the crossover between the allocation-free
// last-occurrence scan and the seen-set path for wide field lists. It is
// a cross-module contract: the canonical encoder and every bridge must
// resolve duplicate keys identically, so they share this one constant
// (pinned by the golden parity tests).
const NarrowLimit = 24

// LastIndices returns the indices of each key's last occurrence, in
// forward order — last-write-wins duplicate resolution. Narrow lists
// use a bounded scan; wide ones collect seen keys backward in
// a map. The key function selects each item's comparison key (bridges
// that alias envelope-colliding keys pass the aliased view).
func LastIndices[T any](items []T, key func(T) string) []int {
	return AppendLastIndices(nil, items, key)
}

// AppendLastIndices appends each key's last occurrence to dst. Callers with
// narrow hot paths can provide stack storage and avoid allocating the result.
func AppendLastIndices[T any](dst []int, items []T, key func(T) string) []int {
	if len(items) <= NarrowLimit {
		var keys [NarrowLimit]string
		for i := range items {
			keys[i] = key(items[i])
		}
		for i := range items {
			last := true
			for j := i + 1; j < len(items); j++ {
				if keys[j] == keys[i] {
					last = false
					break
				}
			}
			if last {
				dst = append(dst, i)
			}
		}
		return dst
	}
	seen := make(map[string]struct{}, len(items)*2)
	start := len(dst)
	for i, item := range slices.Backward(items) {
		k := key(item)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		dst = append(dst, i)
	}
	slices.Reverse(dst[start:])
	return dst
}

// ErrorMessage renders an error field's message, tolerating the two
// ways a user error can explode: typed-nil errors and Error()
// implementations that panic (the panic is
// contained and the value rendered via fmt, the same fence the core
// encoder applies).
func ErrorMessage(err error) (msg string) {
	if err == nil {
		return ""
	}
	v := reflect.ValueOf(err)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		if v.IsNil() {
			return "<nil>"
		}
	default:
	}
	defer func() {
		if recover() != nil {
			msg = fmt.Sprint(err)
		}
	}()
	return err.Error()
}
