package sgsp

import "testing"

func TestLimitsNormalization(t *testing.T) {
	defaults, err := normalizeLimits(Limits{})
	if err != nil || defaults != DefaultLimits() {
		t.Fatalf("defaults = %#v, %v", defaults, err)
	}
	valid := DefaultLimits()
	if _, err := normalizeLimits(valid); err != nil {
		t.Fatalf("default limits rejected: %v", err)
	}
	valid.QueueMessages = 1
	if _, err := normalizeLimits(valid); err == nil {
		t.Fatal("partial/invalid queue limit accepted")
	}
	valid = DefaultLimits()
	valid.ConnectionReceiveWindow = valid.StreamReceiveWindow
	if _, err := normalizeLimits(valid); err == nil {
		t.Fatal("impossible transport flow-control budget accepted")
	}
}
