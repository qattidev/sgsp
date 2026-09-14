package main

import (
	"bytes"
	"context"
	"testing"
	"time"
)

func TestRunSGSPTrial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	measurement, err := runSGSPTrial(ctx, config{Implementation: "sgsp", Clients: 1, Hz: 60, InputBytes: 64, UpdateBytes: 512, Warmup: 20 * time.Millisecond, Duration: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if measurement.Inputs.Offered == 0 || measurement.Inputs.Accepted == 0 || measurement.Inputs.Delivered == 0 {
		t.Fatalf("input measurement = %#v", measurement.Inputs)
	}
	if measurement.Updates.Offered == 0 || measurement.Updates.Accepted == 0 || measurement.Updates.Delivered == 0 {
		t.Fatalf("update measurement = %#v", measurement.Updates)
	}
	if measurement.Relay.Overloaded {
		t.Fatal("short trial overloaded relay")
	}
}

func TestRunQUICTrial(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	measurement, err := runQUICTrial(ctx, config{Implementation: "quic", Clients: 1, Hz: 60, InputBytes: 64, UpdateBytes: 512, Warmup: 20 * time.Millisecond, Duration: 150 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	if measurement.Inputs.Offered == 0 || measurement.Inputs.Accepted == 0 || measurement.Inputs.Delivered == 0 {
		t.Fatalf("input measurement = %#v", measurement.Inputs)
	}
	if measurement.Updates.Offered == 0 || measurement.Updates.Accepted == 0 || measurement.Updates.Delivered == 0 {
		t.Fatalf("update measurement = %#v", measurement.Updates)
	}
	if measurement.Relay.Overloaded {
		t.Fatal("short trial overloaded relay")
	}
}

func TestCopyBenchmarkBulkCountsIncrementally(t *testing.T) {
	counters := &trialCounters{}
	copyBenchmarkBulk(bytes.NewBufferString("bulk"), counters)
	if got := counters.bulkDelivered.Load(); got != 4 {
		t.Fatalf("delivered bulk bytes = %d", got)
	}
}
