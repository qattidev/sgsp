package main

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

func TestCountWorkloadFailureExcludesMeasurementCutoff(t *testing.T) {
	if !countWorkloadFailure(context.Background(), errors.New("write failed")) {
		t.Fatal("live workload failure was not counted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if countWorkloadFailure(ctx, context.Canceled) {
		t.Fatal("cutoff cancellation was counted as a workload failure")
	}
}

func TestDurationHistogramUsesBoundedUpperBounds(t *testing.T) {
	histogram := &durationHistogram{}
	histogram.Record(time.Nanosecond)
	histogram.Record(2 * time.Nanosecond)
	histogram.Record(2 * time.Millisecond)
	summary := histogram.Snapshot()
	if summary.Samples != 3 || summary.P50UpperBound < 2*time.Nanosecond || summary.P99UpperBound < 2*time.Millisecond || summary.MaxUpperBound < 2*time.Millisecond {
		t.Fatalf("histogram summary = %#v", summary)
	}
	histogram.Reset()
	if summary := histogram.Snapshot(); summary.Samples != 0 {
		t.Fatalf("histogram retained samples after reset: %#v", summary)
	}
}

func TestBenchmarkPayloadPreservesClockStamp(t *testing.T) {
	payload := benchmarkPayload(24, 7, 9, 11)
	tick, sequence, stamp, ok := benchmarkPayloadFields(payload)
	if !ok || tick != 7 || sequence != 9 || stamp != 11 {
		t.Fatalf("benchmark payload fields = %d/%d/%d/%t", tick, sequence, stamp, ok)
	}
	if _, _, _, ok := benchmarkPayloadFields(payload[:23]); ok {
		t.Fatal("truncated benchmark payload exposed a timestamp")
	}
}

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
	if measurement.Timing.InputSendOverhead.Samples == 0 || measurement.Timing.UpdateAge.Samples == 0 {
		t.Fatalf("timing measurement = %#v", measurement.Timing)
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
	if measurement.Timing.InputSendOverhead.Samples == 0 || measurement.Timing.UpdateAge.Samples == 0 {
		t.Fatalf("timing measurement = %#v", measurement.Timing)
	}
}

func TestTrialRequestsAndBulk(t *testing.T) {
	for _, implementation := range []string{"sgsp", "quic"} {
		t.Run(implementation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			measurement, err := runTrial(ctx, config{Implementation: implementation, Clients: 1, Hz: 60, InputBytes: 64, UpdateBytes: 512, RPCPerSecond: 2, BulkBytes: 4096, Warmup: 25 * time.Millisecond, Duration: 650 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			if measurement.Requests.Offered == 0 || measurement.Requests.Accepted == 0 || measurement.Requests.Delivered == 0 {
				t.Fatalf("request measurement = %#v", measurement.Requests)
			}
			if measurement.Bulk.Offered == 0 || measurement.Bulk.Accepted == 0 || measurement.Bulk.Delivered == 0 {
				t.Fatalf("bulk measurement = %#v", measurement.Bulk)
			}
			if measurement.Timing.RequestRoundTrip.Samples == 0 {
				t.Fatalf("request timing measurement = %#v", measurement.Timing)
			}
		})
	}
}

func TestCopyBenchmarkBulkCountsIncrementally(t *testing.T) {
	counters := &trialCounters{}
	copyBenchmarkBulk(bytes.NewBufferString("bulk"), counters)
	if got := counters.bulkDelivered.Load(); got != 4 {
		t.Fatalf("delivered bulk bytes = %d", got)
	}
}

func TestBulkPacerPreservesConfiguredRate(t *testing.T) {
	for _, rate := range []int{1, 4_096, 64 << 10} {
		pacer := bulkPacer{perSecond: rate}
		total := 0
		for range 100 {
			total += pacer.Next()
		}
		if total != rate {
			t.Fatalf("%d bytes/s paced as %d bytes/s", rate, total)
		}
	}
}

func TestRuntimeDeltaUsesIntervalAllocationCounters(t *testing.T) {
	start := runtimeMeasurement{CPUSeconds: 1.25, TotalAlloc: 100, Mallocs: 40}
	end := runtimeMeasurement{HeapAlloc: 50, RSSBytes: 70, CPUSeconds: 2, TotalAlloc: 125, Mallocs: 49}
	got := runtimeDelta(start, end)
	if got.HeapAlloc != 50 || got.RSSBytes != 70 || got.CPUSeconds != 0.75 || got.TotalAlloc != 25 || got.Mallocs != 9 {
		t.Fatalf("runtime delta = %#v", got)
	}
}
