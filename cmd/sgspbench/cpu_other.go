//go:build !linux

package main

// processCPUSeconds is unavailable without a portable standard-library API.
// Zero is serialized as an explicit unavailable value on non-Linux hosts.
func processCPUSeconds() float64 { return 0 }
