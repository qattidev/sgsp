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
	data := `{"status":"completed","config":{"implementation":"sgsp","clients":32,"hz":128,"rtt":20000000,"jitter":0,"loss":0,"reorder":0,"seed":1},"measurement":{"inputs":{"offered":10,"accepted":9,"delivered":8},"updates":{"offered":8,"accepted":8,"delivered":7},"requests":{"offered":2,"accepted":2,"delivered":2},"bulk":{"offered":64,"accepted":64,"delivered":64},"timing":{"input_send_overhead":{"samples":8,"p99_upper_bound":1023},"request_round_trip":{"samples":2,"p99_upper_bound":2047},"update_age":{"samples":7,"p99_upper_bound":4095}},"relay":{"forwarded":42,"dropped":1,"pending_packets":2,"pending_bytes":64},"runtime":{"cpu_seconds":0.25,"heap_alloc":512,"rss_bytes":1024,"allocated_bytes":2048,"allocations":12,"goroutines":9}},"timestamp":"2026-09-15T00:00:00Z"}`
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
	for _, want := range []string{"SGSP benchmark trial index", "| sgsp | 32 | 128 | 20ms |", "10 / 9 / 8", "≤ 1.023µs", "forwarded=42", "cpu=0.250s", "trial.json"} {
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
