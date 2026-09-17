package json

import (
	"strings"
	"testing"
)

// Payload shapes mirror the §4 gate's prototype table so the gate
// ("escape, 96-char clean ASCII ≤ 26 ns") is measured on the same inputs.
var (
	benchKey7   = "user_id"
	benchPath22 = "/api/v1/users/12345/order"
	benchURL96  = "/api/v1/users/12345/orders?include=items&fields=all&cursor=abcdefghijklmnopqrstuvwxyz0123456789A"
	benchEscape = strings.Repeat("x", 15) + `"quoted` + strings.Repeat("y", 15)
	benchUni    = strings.Repeat("héllo☃日本語🍜", 4)
)

// TestGatePayloadsTakeFastPath pins the benchmark payloads to the paths
// they are supposed to measure: the 96-char gate URL must be SWAR-clean,
// the escape-heavy payload must not be (it benchmarks the fallback).
func TestGatePayloadsTakeFastPath(t *testing.T) {
	if !isCleanASCII(benchURL96) {
		t.Fatal("96-char gate URL no longer takes the SWAR fast path — " +
			"the escape gate benchmark would silently measure the table path")
	}
	if isCleanASCII(benchEscape) {
		t.Fatal("escape-heavy payload unexpectedly clean")
	}
	if !isCleanASCII(benchPath22) || !isCleanASCII(strings.Repeat("k", 40)) {
		t.Fatal("ordinary clean payloads rejected")
	}
}

func BenchmarkAppendString7CharKey(b *testing.B) {
	benchmarkAppendString(b, benchKey7)
}

func BenchmarkAppendString22CharPath(b *testing.B) {
	benchmarkAppendString(b, benchPath22)
}

func BenchmarkAppendString96CharURL(b *testing.B) {
	benchmarkAppendString(b, benchURL96)
}

func BenchmarkAppendStringEscapeHeavy(b *testing.B) {
	benchmarkAppendString(b, benchEscape)
}

func BenchmarkAppendStringUnicode(b *testing.B) {
	benchmarkAppendString(b, benchUni)
}

// BenchmarkAppendStringTable96CharURL is the vendored zerolog reference on
// the gate payload, for the fork-vs-origin comparison.
func BenchmarkAppendStringTable96CharURL(b *testing.B) {
	b.ReportAllocs()
	dst := make([]byte, 0, 128)
	for b.Loop() {
		dst = appendStringTable(dst[:0], benchURL96)
	}
}

func benchmarkAppendString(b *testing.B, s string) {
	b.Helper()
	b.ReportAllocs()
	dst := make([]byte, 0, 128)
	for b.Loop() {
		dst = Encoder{}.AppendString(dst[:0], s)
	}
}
