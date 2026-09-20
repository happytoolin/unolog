//go:build darwin && arm64

package unolog

// readCounter reads the arm64 virtual counter (CNTVCT_EL0) directly: one
// system-register move, no libc call. Apple fixes the generic timer at
// 1 GHz on every M-series part, so the raw register is already a
// nanosecond clock -- about six times cheaper than the runtime's
// mach_absolute_time path, which is a libc call plus a timebase convert.
//
//go:noescape
func readCounter() uint64

// counterFrequency reads CNTFRQ_EL0, the architectural timer frequency.
// The SoC fixes it, so this is the exact rate -- nothing is estimated.
//
//go:noescape
func counterFrequency() uint64

const (
	// counterNanosPerSecond is the only rate this file trusts. Any other
	// rate needs a scale multiply, whose rounding would reintroduce the
	// sub-nanosecond error the register read exists to avoid.
	counterNanosPerSecond = 1_000_000_000
	// calibrationSpan is the counter span init cross-checks against the
	// runtime clock before trusting the register.
	calibrationSpan = 100_000
	// calibrationDeadline bounds that cross-check so a frozen counter
	// cannot hang init.
	calibrationDeadline = 5_000_000
)

// counterNanos is written once by init, before any goroutine exists, and
// only read afterwards.
var counterNanos bool

func init() {
	if counterFrequency() != counterNanosPerSecond {
		return
	}
	// CNTFRQ_EL0 says 1 GHz, but that is a claim about the host. Confirm
	// the register really tracks the runtime clock, so a hypervisor that
	// reports a rate it does not deliver falls back instead of skewing
	// every duration.
	deadline := runtimeNanoSlow() + calibrationDeadline
	prevR := runtimeNanoSlow()
	prevC := readCounter()
	var sumR, sumC int64
	for i := 0; i < 4096 && sumC < calibrationSpan; i++ {
		// Bracket the runtime read with counter reads and compare
		// accumulations: the per-pair runtime granularity cancels in the
		// sums, where a single pair would read several percent off.
		var c uint64
		for j := 0; ; j++ {
			c = readCounter()
			if c-prevC > 200 {
				break
			}
			if j&63 == 63 && runtimeNanoSlow() >= deadline {
				return
			}
		}
		r := runtimeNanoSlow()
		sumR += r - prevR
		sumC += int64(c - prevC)
		prevR, prevC = r, c
	}
	if sumR <= 0 {
		return
	}
	// 1% of the observed span, with a floor for a short span. This covers
	// the runtime clock's own granularity and drift; a counter on a
	// different rate misses by far more than 1%.
	slack := max(sumC/100, 256)
	if d := sumR - sumC; d < slack && d > -slack {
		counterNanos = true
	}
}

// runtimeNano is the monotonic nanosecond reading used across the
// lifecycle: one register read on a qualified darwin/arm64 host, the
// runtime clock everywhere else.
//
// ponytail: CNTVCT_EL0 is not suspend-aware, and the init check only
// validates it at startup, so a host that suspends mid-flight could
// skew a duration that spans the sleep. Requests do not span sleeps, and
// the alternative (mach_absolute_time) is a libc call per read; revisit
// if sub-millisecond accuracy across suspend ever matters.
func runtimeNano() int64 {
	if counterNanos {
		return int64(readCounter())
	}
	return runtimeNanoSlow()
}
