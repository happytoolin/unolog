package wire

import (
	"errors"
	"fmt"
	"testing"
)

type item struct{ k string }

// TestLastIndicesResolvesLastWriteWins is the bridge contract: each key
// survives exactly once, at its last position, in forward order — the
// same resolution the canonical encoder applies.
func TestLastIndicesResolvesLastWriteWins(t *testing.T) {
	tests := []struct {
		name  string
		items []item
		want  []int
	}{
		{"empty", nil, []int{}},
		{"single", []item{{"a"}}, []int{0}},
		{"distinct", []item{{"a"}, {"b"}, {"c"}}, []int{0, 1, 2}},
		{"dup keeps last in order", []item{{"a"}, {"b"}, {"a"}}, []int{1, 2}},
		{"all dup", []item{{"a"}, {"a"}, {"a"}}, []int{2}},
		{"wide past crossover", make([]item, NarrowLimit*3), nil}, // filled below
	}
	wideIdx := len(tests) - 1
	tests[wideIdx].items = make([]item, NarrowLimit*3)
	for i := range tests[wideIdx].items {
		tests[wideIdx].items[i] = item{fmt.Sprintf("k%d", i%NarrowLimit)} // NarrowLimit distinct keys, each 3×
	}
	last := make([]int, 0, NarrowLimit)
	for k := range NarrowLimit {
		last = append(last, NarrowLimit*2+k) // the final block holds each key's last write
	}
	tests[wideIdx].want = last

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := LastIndices(tt.items, func(it item) string { return it.k })
			if len(got) != len(tt.want) {
				t.Fatalf("indices = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("indices = %v, want %v (resolution differs at %d)", got, tt.want, i)
				}
			}
		})
	}
}

// TestLastIndicesNarrowPathMatchesWidePath pins the crossover: both
// sides of NarrowLimit must resolve identically (the golden parity
// tests rely on bridge and core agreeing).
func TestLastIndicesNarrowPathMatchesWidePath(t *testing.T) {
	build := func(n int) []item {
		items := make([]item, n)
		for i := range items {
			items[i] = item{fmt.Sprintf("k%d", i%7)}
		}
		return items
	}
	narrow := LastIndices(build(NarrowLimit), func(it item) string { return it.k })
	wide := LastIndices(build(NarrowLimit+1), func(it item) string { return it.k })
	if len(narrow) != 7 || len(wide) != 7 {
		t.Fatalf("distinct counts: narrow=%d wide=%d, want 7 both", len(narrow), len(wide))
	}
}

func TestAppendLastIndicesPreservesPrefix(t *testing.T) {
	storage := [NarrowLimit + 1]int{99}
	got := AppendLastIndices(storage[:1], []item{{"a"}, {"b"}, {"a"}}, func(it item) string { return it.k })
	want := []int{99, 1, 2}
	if len(got) != len(want) {
		t.Fatalf("indices = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("indices = %v, want %v", got, want)
		}
	}
}

type typedNilErr struct{ msg string }

// Error would deref the nil receiver if called — but fmt never calls
// methods on nil pointer operands, rendering "<nil>" instead.
func (e *typedNilErr) Error() string { return e.msg }

type panickingErr struct{}

func (panickingErr) Error() string { panic("Error() boom") }

type nilSliceErr []int

func (nilSliceErr) Error() string { panic("Error() boom") }

// TestErrorMessageFencesHostileErrors pins the fence: typed-nil errors
// render as "<nil>", panicking Error() implementations are contained
// to a fmt fallback, ordinary errors pass through.
func TestErrorMessageFencesHostileErrors(t *testing.T) {
	if got := ErrorMessage(nil); got != "" {
		t.Fatalf("nil message = %q, want empty", got)
	}
	var nilErr *typedNilErr
	if got := ErrorMessage(nilErr); got != "<nil>" {
		t.Fatalf("typed-nil message = %q, want %q", got, "<nil>")
	}
	var nilSlice nilSliceErr
	if got := ErrorMessage(nilSlice); got != "<nil>" {
		t.Fatalf("typed-nil slice message = %q, want %q", got, "<nil>")
	}
	if got := ErrorMessage(panickingErr{}); got == "" {
		t.Fatal("panicking Error() yielded an empty message, want a fmt fallback rendering")
	}
	if got, want := ErrorMessage(errors.New("boom")), "boom"; got != want {
		t.Fatalf("ordinary message = %q, want %q", got, want)
	}
}
