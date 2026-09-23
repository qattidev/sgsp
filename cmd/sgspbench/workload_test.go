package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"qattidev/sgsp"
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

func TestRequestOutcomeUnknown(t *testing.T) {
	if !requestOutcomeUnknown(&sgsp.Error{Code: sgsp.OutcomeUnknown, OutcomeUnknown: true}) {
		t.Fatal("SGSP unknown-outcome error was not classified")
	}
	if requestOutcomeUnknown(errors.New("ordinary request failure")) {
		t.Fatal("ordinary error was classified as an unknown outcome")
	}
}

func TestBareRequestOutcomeUnknown(t *testing.T) {
	for _, test := range []struct {
		name             string
		requestWritten   int
		completeResponse bool
		want             bool
	}{
		{name: "pre-send failure", requestWritten: 0, want: false},
		{name: "partial write", requestWritten: 1, want: true},
		{name: "lost response", requestWritten: benchmarkRPCBytes, want: true},
		{name: "complete response", requestWritten: benchmarkRPCBytes, completeResponse: true, want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := bareRequestOutcomeUnknown(test.requestWritten, test.completeResponse); got != test.want {
				t.Fatalf("bareRequestOutcomeUnknown(%d, %t) = %t, want %t", test.requestWritten, test.completeResponse, got, test.want)
			}
		})
	}
}

func TestScheduledTickerCountsCoalescedTicks(t *testing.T) {
	started := time.Unix(0, 0)
	ticker := scheduledTicker{interval: 10 * time.Millisecond}
	if missed := ticker.Observe(started); missed != 0 {
		t.Fatalf("first scheduled tick missed %d intervals", missed)
	}
	if missed := ticker.Observe(started.Add(10 * time.Millisecond)); missed != 0 {
		t.Fatalf("adjacent scheduled tick missed %d intervals", missed)
	}
	if missed := ticker.Observe(started.Add(40 * time.Millisecond)); missed != 2 {
		t.Fatalf("coalesced scheduled ticks missed %d intervals, want 2", missed)
	}
}

func TestWorkloadStartPhaseSpreadsClientsAcrossActivePeriod(t *testing.T) {
	withBulk := config{Clients: 32, Hz: 128, BulkBytes: 64 << 10}
	period := 10 * time.Millisecond
	if inputPeriod := time.Second / time.Duration(withBulk.Hz); inputPeriod < period {
		period = inputPeriod
	}
	if got := workloadStartPhase(withBulk, 0); got != 0 {
		t.Fatalf("client zero phase = %s, want zero", got)
	}
	if got := workloadStartPhase(withBulk, 1); got <= 0 || got >= period {
		t.Fatalf("client one phase = %s, want within (0, %s)", got, period)
	}
	if got, want := workloadStartPhase(withBulk, 7), workloadStartPhase(withBulk, 0); got != want {
		t.Fatalf("phase should repeat after available millisecond slots: client 7 = %s, client 0 = %s", got, want)
	}

	withoutBulk := config{Clients: 8, Hz: 60}
	if got := workloadStartPhase(withoutBulk, 1); got <= 0 || got >= time.Second/time.Duration(withoutBulk.Hz) {
		t.Fatalf("input-only phase = %s, want within input period", got)
	}
	if got := workloadStartPhase(config{Clients: 1, Hz: 60, BulkBytes: 64 << 10}, 0); got != 0 {
		t.Fatalf("single-client phase = %s, want zero", got)
	}
}

func TestBenchmarkWorkloadPhaseMarkers(t *testing.T) {
	measured := benchmarkPayloadForClientPhase(32, 7, 9, 11, 3, true)
	if !benchmarkPayloadIsMeasuredFromPayload(measured) {
		t.Fatal("measurement payload did not retain its phase marker")
	}
	warmup := benchmarkPayloadForClientPhase(32, 7, 9, 11, 3, false)
	if benchmarkPayloadIsMeasuredFromPayload(warmup) {
		t.Fatal("warmup payload was marked as measured")
	}
	if !benchmarkRequestPayloadIsMeasured(benchmarkRequestPayload(true)) || benchmarkRequestPayloadIsMeasured(benchmarkRequestPayload(false)) {
		t.Fatal("request phase marker was not preserved")
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

func TestDurationHistogramSeparatesOverflow(t *testing.T) {
	histogram := &durationHistogram{}
	histogram.Record(time.Microsecond)
	histogram.Record(time.Microsecond + 1)
	histogram.Record(time.Second)
	histogram.Record(time.Second + time.Nanosecond)
	summary := histogram.Snapshot()
	if summary.Samples != 4 || summary.Overflow != 1 {
		t.Fatalf("histogram samples/overflow = %d/%d", summary.Samples, summary.Overflow)
	}
	if !summary.P99Overflow || !summary.MaxOverflow || summary.MaxUpperBound != time.Second {
		t.Fatalf("histogram overflow metadata = %#v", summary)
	}
	encoded, err := json.Marshal(summary)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(encoded, []byte(`"overflow":1`)) || !bytes.Contains(encoded, []byte(`"max_overflow":true`)) {
		t.Fatalf("histogram JSON does not identify overflow: %s", encoded)
	}
}

func TestLocalOverheadCorrelatorPairsMatchingSequences(t *testing.T) {
	combined := &durationHistogram{}
	correlator := newLocalOverheadCorrelator(2, combined)
	correlator.RecordReceive(7, 64*time.Microsecond) // stale warmup update
	correlator.RecordSend(7, 8*time.Microsecond)
	correlator.RecordReceive(7, 16*time.Microsecond)
	correlator.RecordSend(10, time.Microsecond) // outside the fixed window

	summary := correlator.Snapshot()
	if summary.Sent != 1 || summary.Received != 1 || summary.Matched != 1 || summary.UnmatchedSent != 0 || summary.UnmatchedReceived != 0 {
		t.Fatalf("correlation summary = %#v", summary)
	}
	if !summary.BufferOverflow {
		t.Fatal("sequence outside the fixed correlation window did not invalidate the sample buffer")
	}
	if got := combined.Snapshot(); got.Samples != 1 || got.P99UpperBound < 24*time.Microsecond {
		t.Fatalf("combined overhead histogram = %#v", got)
	}
}

func TestNetworkAccumulatorSeparatesLocalDropCauses(t *testing.T) {
	accumulator := &networkAccumulator{}
	accumulator.addSGSP(sgsp.Stats{TransportStatsAvailable: true, RTT: 20 * time.Millisecond, LocalDatagramsDropped: 5, CoalescedDatagramsDropped: 2, StaleUpdatesDropped: 1})
	accumulator.addSGSP(sgsp.Stats{TransportStatsAvailable: true, RTT: 30 * time.Millisecond, LocalDatagramsDropped: 3, CoalescedDatagramsDropped: 1, StaleUpdatesDropped: 2})
	measurement := accumulator.Snapshot()
	if !measurement.LocalDropMetricsAvailable || measurement.RTT.Samples != 2 || measurement.RTT.P99UpperBound < 30*time.Millisecond || measurement.LocalDatagramsDropped != 8 || measurement.CoalescedDatagramsDropped != 3 || measurement.StaleUpdatesDropped != 3 {
		t.Fatalf("network measurement = %#v", measurement)
	}
}

func TestBenchmarkObserverRecordsBoundedQueueMetrics(t *testing.T) {
	observer := &benchmarkObserver{}
	observer.Observe(sgsp.Observation{Name: "queue_items", Value: 3})
	observer.Observe(sgsp.Observation{Name: "queue_items", Value: 2})
	observer.Observe(sgsp.Observation{Name: "queue_bytes", Value: 128})
	observer.Observe(sgsp.Observation{Name: "queue_age", Kind: "in_event_le_16us", Value: 4})
	observer.Observe(sgsp.Observation{Name: "messages", Kind: "ignored", Value: 9})
	measurement := observer.Snapshot()
	if !measurement.Available || measurement.MaximumItems != 3 || measurement.MaximumBytes != 128 || measurement.AgeBuckets["in_event_le_16us"] != 4 || len(measurement.AgeBuckets) != 1 {
		t.Fatalf("queue measurement = %#v", measurement)
	}
	observer.Reset()
	if measurement := observer.Snapshot(); !measurement.Available || measurement.MaximumItems != 0 || measurement.MaximumBytes != 0 || len(measurement.AgeBuckets) != 0 {
		t.Fatalf("queue measurement after reset = %#v", measurement)
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
	withClient := benchmarkPayloadForClient(32, 7, 9, 11, 3)
	if clientIndex, ok := benchmarkPayloadClientIndex(withClient); !ok || clientIndex != 3 {
		t.Fatalf("benchmark payload client index = %d/%t", clientIndex, ok)
	}
	if _, ok := benchmarkPayloadClientIndex(withClient[:31]); ok {
		t.Fatal("truncated benchmark payload exposed a client index")
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
	if measurement.Timing.InputSendOverhead.Samples == 0 || measurement.Timing.InputReceiveOverhead.Samples == 0 || measurement.Timing.InputSendPlusReceive.Matched == 0 || measurement.Timing.UpdateSendOverhead.Samples == 0 || measurement.Timing.UpdateReceiveOverhead.Samples == 0 || measurement.Timing.UpdateSendPlusReceive.Matched == 0 || measurement.Timing.UpdateAge.Samples == 0 {
		t.Fatalf("timing measurement = %#v", measurement.Timing)
	}
	if measurement.Validity.InputSampleBufferOverflow || measurement.Validity.UpdateSampleBufferOverflow {
		t.Fatalf("short trial overflowed correlation buffer: %#v", measurement.Validity)
	}
	assertMeasurementCountsBounded(t, measurement)
	if !measurement.Queues.Available || !measurement.Network.LocalDropMetricsAvailable {
		t.Fatalf("SGSP-only metrics were unavailable: queues=%#v network=%#v", measurement.Queues, measurement.Network)
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
	if measurement.Timing.InputSendOverhead.Samples == 0 || measurement.Timing.InputReceiveOverhead.Samples == 0 || measurement.Timing.InputSendPlusReceive.Matched == 0 || measurement.Timing.UpdateSendOverhead.Samples == 0 || measurement.Timing.UpdateReceiveOverhead.Samples == 0 || measurement.Timing.UpdateSendPlusReceive.Matched == 0 || measurement.Timing.UpdateAge.Samples == 0 {
		t.Fatalf("timing measurement = %#v", measurement.Timing)
	}
	if measurement.Validity.InputSampleBufferOverflow || measurement.Validity.UpdateSampleBufferOverflow {
		t.Fatalf("short trial overflowed correlation buffer: %#v", measurement.Validity)
	}
	assertMeasurementCountsBounded(t, measurement)
	if measurement.Queues.Available || measurement.Network.LocalDropMetricsAvailable {
		t.Fatalf("raw baseline fabricated SGSP-only metrics: queues=%#v network=%#v", measurement.Queues, measurement.Network)
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
	counters.startMeasurement()
	copyBenchmarkBulk(bytes.NewBufferString("bulk"), counters, true)
	if got := counters.bulkDelivered.Load(); got != 4 {
		t.Fatalf("delivered bulk bytes = %d", got)
	}
}

func assertMeasurementCountsBounded(t *testing.T, measurement measurement) {
	t.Helper()
	for name, counts := range map[string]operationCounts{
		"inputs":   measurement.Inputs,
		"updates":  measurement.Updates,
		"requests": measurement.Requests,
		"bulk":     measurement.Bulk,
	} {
		if counts.Accepted > counts.Offered || counts.Delivered > counts.Offered {
			t.Fatalf("%s crossed the measurement boundary: %#v", name, counts)
		}
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
	got.normalizeByOfferedOperations(4)
	if got.OfferedOperations != 4 || got.AllocatedBytesPerOperation != 6.25 || got.AllocationsPerOperation != 2.25 {
		t.Fatalf("normalized runtime delta = %#v", got)
	}
	got.normalizeByOfferedOperations(0)
	if got.OfferedOperations != 0 || got.AllocatedBytesPerOperation != 0 || got.AllocationsPerOperation != 0 {
		t.Fatalf("zero-operation normalization changed rates = %#v", got)
	}
}
