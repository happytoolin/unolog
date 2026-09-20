package unolog

import (
	"sync/atomic"
	"time"
	_ "unsafe" // //go:linkname
)

// runtimeNanoSlow is the runtime's monotonic nanosecond clock. On
// darwin a bare time.Now() is two libc calls (gettimeofday plus
// mach_absolute_time); the monotonic read alone is about half that, and
// the wall anchor below only needs refreshing occasionally.
//
//go:linkname runtimeNanoSlow runtime.nanotime
func runtimeNanoSlow() int64

// wallRefreshNanos bounds how stale the cached wall anchor may get. The
// anchor is advanced by the monotonic delta, so resolution is not lost;
// what this bounds is drift from an NTP step, not the tick.
const wallRefreshNanos = 100 * time.Millisecond

type wallPoint struct {
	sec  int64 // unix seconds at the anchor
	nsec int32 // nanosecond part, [0, 1e9)
	mono int64 // runtimeNano() at the anchor
}

var wallAnchor atomic.Pointer[wallPoint]

// wallNanoAt returns the wall time at the given monotonic reading as
// unix nanoseconds, refreshing the anchor when it has gone stale: one
// time.Now per refresh window instead of one per hot-path read. A store
// may race a concurrent refresh; the reader only ever sees a whole
// point.
//
// The stamp stays an int64 rather than a time.Time: the request hot path
// only ever needs the nanoseconds, and materializing a time.Time costs
// an out-of-line time.Unix call per request plus a Location pointer for
// the GC to walk on the pooled event.
func wallNanoAt(mono int64) int64 {
	if p := wallAnchor.Load(); p != nil {
		if d := mono - p.mono; uint64(d) < uint64(wallRefreshNanos) {
			return p.sec*1e9 + int64(p.nsec) + d
		}
	}
	t := time.Now()
	n := t.UnixNano()
	wallAnchor.Store(&wallPoint{sec: n / 1e9, nsec: int32(n % 1e9), mono: runtimeNano()})
	return n
}
