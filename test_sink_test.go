package unolog

import (
	"reflect"
	"testing"
)

func TestTestSinkPreservesSharedSliceLengths(t *testing.T) {
	shared := []any{"first", "second"}
	cyclic := make([]any, 2)
	cyclic[0], cyclic[1] = cyclic[:1], "tail"
	for _, tt := range []struct {
		name  string
		value any
	}{
		{name: "any short first", value: []any{shared[:1], shared[:2]}},
		{name: "any long first", value: []any{shared[:2], shared[:1]}},
		{name: "reflect short first", value: [][]any{shared[:1], shared[:2]}},
		{name: "reflect long first", value: [][]any{shared[:2], shared[:1]}},
		{name: "any cycle", value: cyclic},
		{name: "reflect cycle", value: [][]any{cyclic}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sink := NewTestSink()
			sink.Write(t.Context(), recOf(LevelInfo, "copy", fieldOf("value", tt.value)))
			got, _ := sink.Events()[0].Lookup("value")
			if !reflect.DeepEqual(got, tt.value) {
				t.Fatal("capture changed the slice lengths or values")
			}
		})
	}
}

func TestTestSinkPreservesNilContainers(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value any
	}{
		{name: "any map", value: map[string]any(nil)},
		{name: "typed map", value: map[string]int(nil)},
		{name: "any slice", value: []any(nil)},
		{name: "typed slice", value: []int(nil)},
		{name: "empty map", value: map[string]any{}},
		{name: "empty slice", value: []any{}},
		{name: "reflect nil element", value: [][]any{{nil, "x"}}},
		{name: "reflect nil pointer", value: []*int{nil}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			sink := NewTestSink()
			sink.Write(t.Context(), recOf(LevelInfo, "copy", fieldOf("value", tt.value)))
			got, _ := sink.Events()[0].Lookup("value")
			if !reflect.DeepEqual(got, tt.value) {
				t.Fatalf("captured %#v, want %#v", got, tt.value)
			}
		})
	}
}

// A capture is a deep copy: a later change to a nested map, slice, or
// pointer does not change it. Shared and cyclic references stay shared in
// the copy, and the copy terminates. Struct fields are copied as values:
// reflect cannot set unexported fields.
func TestTestSinkDeepCopiesNestedContainers(t *testing.T) {
	inner := map[string]int{"a": 1}
	ring := make([]any, 2)
	ptr := &ring
	ring[0], ring[1] = ptr, 1
	maps := []map[string]int{inner, inner}
	ptrs := []*[]any{ptr, ptr}
	byKey := map[string][]int{"k": {1}}
	sink := NewTestSink()
	sink.Write(t.Context(), recOf(LevelInfo, "copy", fieldOf("maps", maps), fieldOf("ptrs", ptrs), fieldOf("by_key", byKey)))
	inner["a"] = 2
	ring[1] = 2
	byKey["k"][0] = 2

	ev := sink.Events()[0]
	got, _ := ev.Lookup("maps")
	gotMaps, _ := got.([]map[string]int)
	if len(gotMaps) != 2 || gotMaps[0]["a"] != 1 {
		t.Fatalf("maps = %v, want the value at capture", got)
	}
	if reflect.ValueOf(gotMaps[0]).Pointer() != reflect.ValueOf(gotMaps[1]).Pointer() {
		t.Fatal("copy split a shared map")
	}
	got, _ = ev.Lookup("ptrs")
	gotPtrs, _ := got.([]*[]any)
	if len(gotPtrs) != 2 || gotPtrs[0] == ptr || len(*gotPtrs[0]) != 2 || (*gotPtrs[0])[1] != 1 {
		t.Fatalf("ptrs = %v, want a copy with the value at capture", got)
	}
	if gotPtrs[0] != gotPtrs[1] || (*gotPtrs[0])[0] != gotPtrs[0] {
		t.Fatal("copy split a shared pointer or broke the cycle")
	}
	got, _ = ev.Lookup("by_key")
	if gotByKey, _ := got.(map[string][]int); len(gotByKey["k"]) != 1 || gotByKey["k"][0] != 1 {
		t.Fatalf("by_key = %v, want the value at capture", got)
	}
}

// Write on a nil TestSink, or with a nil record, is a no-op.
func TestTestSinkNilWrite(t *testing.T) {
	var nilSink *TestSink
	nilSink.Write(t.Context(), recOf(LevelInfo, "m"))
	sink := NewTestSink()
	sink.Write(t.Context(), nil)
	if n := len(sink.Events()); n != 0 {
		t.Fatalf("nil record captured %d events", n)
	}
}
