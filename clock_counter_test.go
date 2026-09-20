//go:build darwin && arm64

package unolog

import (
	"testing"
	"time"
)

// TestCounterClockAgreesWithWall covers both arms of runtimeNano: the
// register clock when it qualifies, and the runtime clock when it does
// not. A wrong calibration or a broken register read shows up here as a
// duration that does not track wall time.
func TestCounterClockAgreesWithWall(t *testing.T) {
	if f := counterFrequency(); f != counterNanosPerSecond {
		t.Fatalf("counterFrequency() = %d, want %d", f, counterNanosPerSecond)
	}
	if !counterNanos {
		t.Fatal("counter did not qualify on this host; the register path is dead code")
	}
	for _, tc := range []struct {
		name  string
		clock func() int64
	}{
		{"register", func() int64 { return int64(readCounter()) }},
		{"runtimeNano", runtimeNano},
		{"fallback", runtimeNanoSlow},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := time.Now()
			a := tc.clock()
			time.Sleep(30 * time.Millisecond)
			b := tc.clock()
			wall := time.Since(start)
			if d := time.Duration(b-a) - wall; d < -2*time.Millisecond || d > 2*time.Millisecond {
				t.Fatalf("clock disagrees with wall time by %v", d)
			}
		})
	}
}

// TestCounterClockFallsBack checks the untrusted path directly: with the
// register disqualified, runtimeNano must still be a working clock.
func TestCounterClockFallsBack(t *testing.T) {
	prev := counterNanos
	counterNanos = false
	t.Cleanup(func() { counterNanos = prev })

	a := runtimeNano()
	time.Sleep(5 * time.Millisecond)
	b := runtimeNano()
	if d := time.Duration(b - a); d < 4*time.Millisecond || d > 20*time.Millisecond {
		t.Fatalf("fallback clock advanced %v over a 5ms sleep", d)
	}
}
