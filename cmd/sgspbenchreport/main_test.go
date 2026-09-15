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
	data := `{"status":"completed","config":{"implementation":"sgsp","clients":32,"hz":128,"rtt":20000000,"jitter":0,"loss":0,"reorder":0,"seed":1},"measurement":{"inputs":{"offered":10,"accepted":9,"delivered":8},"updates":{"offered":8,"accepted":8,"delivered":7},"requests":{"offered":2,"accepted":2,"delivered":2},"bulk":{"offered":64,"accepted":64,"delivered":64}},"timestamp":"2026-09-15T00:00:00Z"}`
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
	for _, want := range []string{"SGSP benchmark trial index", "| sgsp | 32 | 128 | 20ms |", "10 / 9 / 8", "trial.json"} {
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
