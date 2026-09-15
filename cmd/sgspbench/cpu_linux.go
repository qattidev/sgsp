//go:build linux

package main

import "syscall"

// processCPUSeconds reports this process's user-plus-system CPU time. The
// benchmark's Linux reference host makes this a dependency-free interval
// measurement; the non-Linux fallback explicitly reports it as unavailable.
func processCPUSeconds() float64 {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1_000_000
}
