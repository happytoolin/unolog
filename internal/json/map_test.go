package json

import (
	"bytes"
	"testing"
)

func TestAppendStringMapMatchesEncodingJSON(t *testing.T) {
	cases := []map[string]any{
		{},
		{"message": "db down", "type": "*errors.errorString"},
		{"message": "a", "type": "b", "cause.message": "c", "cause.type": "d"},
		{"z": "\x00\x1f\b\f\n\r\t\"\\", "a": "<>&\x7f  é😀", "m": "bad\xff\xfeutf8"},
		{"k\"ey\\": "v", "\x01": ""},
	}
	for _, m := range cases {
		assertMapMatches(t, m)
	}
}

func TestAppendStringMapFallsBack(t *testing.T) {
	cases := map[string]map[string]any{
		"nil map":        nil,
		"non-string":     {"a": 1},
		"named string":   {"a": namedString("x")},
		"nil value":      {"a": nil},
		"non-ASCII key":  {"é": "x"},
		"too many items": {"1": "", "2": "", "3": "", "4": "", "5": "", "6": "", "7": "", "8": "", "9": ""},
	}
	for name, m := range cases {
		if _, ok := appendStringMap(nil, m); ok {
			t.Fatalf("%s: fast path accepted %#v", name, m)
		}
		// The public entry point must still match encoding/json.
		want, err := jsonMarshal(m)
		if err != nil {
			t.Fatal(err)
		}
		if got := (Encoder{}).AppendInterface(nil, m); !bytes.Equal(got, want) {
			t.Fatalf("%s: AppendInterface = %s, want %s", name, got, want)
		}
	}
}

type namedString string

func TestAppendStringMapDoesNotAllocate(t *testing.T) {
	m := map[string]any{"message": "a", "type": "b", "cause.message": "c", "cause.type": "d"}
	buf := make([]byte, 0, 256)
	if allocs := testing.AllocsPerRun(100, func() { _, _ = appendStringMap(buf[:0], m) }); allocs != 0 {
		t.Fatalf("appendStringMap allocated %.1f times", allocs)
	}
}

func assertMapMatches(t *testing.T, m map[string]any) {
	t.Helper()
	got, ok := appendStringMap([]byte("prefix"), m)
	if !ok {
		return
	}
	want, err := jsonMarshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, append([]byte("prefix"), want...)) {
		t.Fatalf("appendStringMap(%q)\n got %s\nwant prefix%s", m, got, want)
	}
}

// FuzzAppendStringMap checks the reflection-free map path against
// encoding/json byte for byte, for arbitrary keys and values.
func FuzzAppendStringMap(f *testing.F) {
	f.Add("message", "db down", "type", "*errors.errorString", "cause.message", " \x7f\xff")
	f.Add("", "", "\x00", "\"\\", "a\tb", "<&>")
	f.Fuzz(func(t *testing.T, k1, v1, k2, v2, k3, v3 string) {
		assertMapMatches(t, map[string]any{k1: v1, k2: v2, k3: v3})
		assertMapMatches(t, map[string]any{k1: v1})
		// The string escaper alone, through a one-entry map.
		assertMapMatches(t, map[string]any{"k": v1 + k1 + v2})
	})
}
