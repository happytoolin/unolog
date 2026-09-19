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
