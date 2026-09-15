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
