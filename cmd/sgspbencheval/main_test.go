package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEvaluateCampaignRetainsDiagnosticComparison(t *testing.T) {
	root := writeCampaign(t, `{"status":"completed","config":{"implementation":"sgsp","clients":1,"hz":60,"rtt":20000000,"jitter":0,"loss":0,"reorder":0,"seed":1,"duration":100000000},"measurement":{"inputs":{"offered":1},"updates":{"offered":1,"delivered":1},"timing":{"input_send_plus_receive":{"duration":{"samples":1,"p99_upper_bound":1000}},"update_send_plus_receive":{"duration":{"samples":1,"p99_upper_bound":1000}}},"network":{"local_drop_metrics_available":true},"relay":{},"runtime":{"cpu_seconds":0.1,"heap_alloc":123,"allocated_bytes_per_offered_operation":4.5}}}`)
	result, err := evaluateCampaign(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Trials) != 1 || result.Trials[0].Problem != "" {
		t.Fatalf("trials = %#v", result.Trials)
	}
	report := renderEvaluation(result)
	for _, want := range []string{"Matrix verdict: INCOMPLETE", "Planned trials: 1; complete and valid: 1", "reference-host validation is absent", "| healthy | sgsp | 1 | 60 |", "| n/a | n/a | n/a | n/a | n/a |"} {
		if !strings.Contains(report, want) {
			t.Fatalf("report missing %q:\n%s", want, report)
		}
	}
}

func TestEvaluateCampaignRejectsMismatchedDuration(t *testing.T) {
	root := writeCampaign(t, `{"status":"completed","config":{"implementation":"sgsp","clients":1,"hz":60,"rtt":20000000,"jitter":0,"loss":0,"reorder":0,"seed":1,"duration":200000000},"measurement":{}}`)
	result, err := evaluateCampaign(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := result.Trials[0].Problem; got != "raw result configuration does not match expected trial plan" {
		t.Fatalf("problem = %q", got)
	}
}

func writeCampaign(t *testing.T, raw string) string {
	t.Helper()
	root := t.TempDir()
	rawDir := filepath.Join(root, "raw")
	if err := os.Mkdir(rawDir, 0755); err != nil {
		t.Fatal(err)
	}
	trialPath := filepath.Join(rawDir, "trial.json")
	if err := os.WriteFile(trialPath, []byte(raw), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "campaign.json"), []byte(`{"reference_host":"0"}`), 0644); err != nil {
		t.Fatal(err)
	}
	plan := fmt.Sprintf("sgsp\t1\t60\t20ms\t20000000\t0\t0ms\t0\t0\t1\thealthy\t100000000\t%s\n", trialPath)
	if err := os.WriteFile(filepath.Join(root, "expected-trials.tsv"), []byte(plan), 0644); err != nil {
		t.Fatal(err)
	}
	return root
}
