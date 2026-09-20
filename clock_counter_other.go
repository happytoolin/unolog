//go:build !(darwin && arm64)

package unolog

// runtimeNano is the monotonic nanosecond reading used across the
// lifecycle. Platforms without a calibrated register clock read the
// runtime's own monotonic clock.
func runtimeNano() int64 {
	return runtimeNanoSlow()
}
