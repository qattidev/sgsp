package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadTrialsAndRenderReport(t *testing.T) {
	directory := t.TempDir()
	data := `{"status":"completed","config":{"implementation":"sgsp","clients":32,"hz":128,"rtt":20000000,"jitter":0,"loss":0,"reorder":0,"seed":1},"measurement":{"inputs":{"offered":10,"accepted":9,"delivered":8},"updates":{"offered":8,"accepted":8,"delivered":7},"requests":{"offered":2,"accepted":2,"delivered":2,"unknown":1},"bulk":{"offered":64,"accepted":64,"delivered":64},"timing":{"input_send_overhead":{"samples":8,"p99_upper_bound":1023},"input_receive_overhead":{"samples":8,"p99_upper_bound":511},"input_send_plus_receive":{"duration":{"samples":8,"p99_upper_bound":2047},"sent":8,"received":8,"matched":8},"update_send_overhead":{"samples":7,"p99_upper_bound":1023},"update_receive_overhead":{"samples":7,"p99_upper_bound":511},"update_send_plus_receive":{"duration":{"samples":7,"p99_upper_bound":2047},"sent":8,"received":7,"matched":7,"unmatched_sent":1,"unmatched_fraction":0.125},"request_round_trip":{"samples":2,"p99_upper_bound":2047},"update_age":{"samples":7,"p99_upper_bound":4095}},"validity":{},"network":{"local_drop_metrics_available":true,"rtt":{"samples":1,"p99_upper_bound":20000000},"local_datagrams_dropped":3,"coalesced_datagrams_dropped":1,"stale_updates_dropped":2},"queues":{"available":true,"maximum_items":3,"maximum_bytes":128,"age_buckets":{"in_event_le_16us":4}},"relay":{"forwarded":42,"dropped":1,"pending_packets":2,"pending_bytes":64},"runtime":{"cpu_seconds":0.25,"heap_alloc":512,"rss_bytes":1024,"allocated_bytes":2048,"allocations":12,"goroutines":9}},"timestamp":"2026-09-15T00:00:00Z"}`
	path := filepath.Join(directory, "trial.json")
	if err := os.WriteFile(path, []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
	trials, err := loadTrials(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(trials) != 1 || trials[0].Config.RTT != 20*time.Millisecond {
		t.Fatalf("trials = %#v", trials)
	}
	report := renderReport(trials)
	for _, want := range []string{"SGSP benchmark trial index", "| sgsp | 32 | 128 | 20ms |", "10 / 9 / 8", "2 / 2 / 2 / 0 / 1", "≤ 1.023µs", "≤ 511ns", "12.50%", "valid", "rtt p99=≤ 20ms; local=3; coalesced=1; stale=2", "max=3 items/128B; age samples=4", "forwarded=42", "peak=0/0B", "cpu=0.250s", "trial.json"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q:\n%s", want, report)
		}
	}
}

func TestLoadTrialsRejectsEmptyDirectory(t *testing.T) {
	if _, err := loadTrials(t.TempDir()); err == nil {
		t.Fatal("empty result directory accepted")
	}
}

func TestRenderReportMarksMissingPairedOverheadUnmeasured(t *testing.T) {
	report := renderReport([]trialResult{{
		Status:      "completed",
		Config:      trialConfig{Implementation: "sgsp", Clients: 1, Hz: 60},
		Measurement: measurement{Inputs: operationCounts{Offered: 1}},
	}})
	if !strings.Contains(report, "unmeasured paired input overhead") {
		t.Fatalf("report did not identify missing paired overhead:\n%s", report)
	}
}

func TestRenderReportMarksBaselineOnlyMetricsUnavailable(t *testing.T) {
	report := renderReport([]trialResult{{
		Status:      "completed",
		Config:      trialConfig{Implementation: "quic", Clients: 1, Hz: 60},
		Measurement: measurement{Network: networkMeasurement{RTT: durationSummary{Samples: 1, P99UpperBound: time.Millisecond}}, Queues: queueMeasurement{AgeBuckets: map[string]uint64{}}},
	}})
	for _, want := range []string{"SGSP local drops=n/a", "n/a (no SGSP dispatch queue)"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q:\n%s", want, report)
		}
	}
}

func TestFormatRuntimeIncludesOfferedOperationRates(t *testing.T) {
	got := formatRuntime(runtimeMeasurement{CPUSeconds: 1.25, HeapAlloc: 50, RSSBytes: 70, TotalAlloc: 125, Mallocs: 9, OfferedOperations: 4, AllocatedBytesPerOperation: 31.25, AllocationsPerOperation: 2.25, Goroutines: 3})
	for _, want := range []string{"offered-ops=4", "alloc/op=31.25B/2.2500"} {
		if !strings.Contains(got, want) {
			t.Fatalf("runtime summary missing %q: %s", want, got)
		}
	}
}

func TestRenderReportMarksMissedGeneratorTicksInvalid(t *testing.T) {
	report := renderReport([]trialResult{{
		Status: "completed",
		Config: trialConfig{Implementation: "sgsp", Clients: 1, Hz: 60},
		Measurement: measurement{Generator: generatorMeasurement{
			InputTicksScheduled: 60,
			InputTicksMissed:    1,
			Unable:              true,
		}},
	}})
	for _, want := range []string{"invalid: workload generator missed ticks", "scheduled i/r/b=60/0/0; missed=1/0/0"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q:\n%s", want, report)
		}
	}
}
