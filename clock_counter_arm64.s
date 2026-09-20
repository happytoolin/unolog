#include "textflag.h"

// func readCounter() uint64
//
// CNTVCT_EL0 is the arm64 virtual counter. It is readable from EL0 on
// every Apple silicon part; the calibration in clock_counter_darwin_arm64.go
// confirms the rate before the register is trusted.
TEXT ·readCounter(SB), NOSPLIT, $0-8
	MRS	CNTVCT_EL0, R0
	MOVD	R0, ret+0(FP)
	RET

// func counterFrequency() uint64
//
// CNTFRQ_EL0 is the architectural timer frequency register. It is
// readable from EL0 and fixed by the SoC.
TEXT ·counterFrequency(SB), NOSPLIT, $0-8
	MRS	CNTFRQ_EL0, R0
	MOVD	R0, ret+0(FP)
	RET
