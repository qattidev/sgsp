// Command sgspbenchreport renders a concise Markdown index for machine-readable
// sgspbench trial results. It summarizes recorded trials; it never infers an
// unmeasured capacity or release-gate result.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type trialConfig struct {
	Implementation string        `json:"implementation"`
	Clients        int           `json:"clients"`
	Hz             int           `json:"hz"`
	RTT            time.Duration `json:"rtt"`
	Jitter         time.Duration `json:"jitter"`
	Loss           float64       `json:"loss"`
	Reorder        float64       `json:"reorder"`
	Seed           int64         `json:"seed"`
}

type operationCounts struct {
	Offered   uint64 `json:"offered,omitempty"`
	Accepted  uint64 `json:"accepted,omitempty"`
	Delivered uint64 `json:"delivered,omitempty"`
	Failed    uint64 `json:"failed,omitempty"`
}

type measurement struct {
	Duration time.Duration      `json:"duration"`
	Inputs   operationCounts    `json:"inputs"`
	Updates  operationCounts    `json:"updates"`
	Requests operationCounts    `json:"requests"`
	Bulk     operationCounts    `json:"bulk"`
	Timing   timingMeasurement  `json:"timing"`
	Relay    relayMeasurement   `json:"relay"`
	Runtime  runtimeMeasurement `json:"runtime"`
}

type durationSummary struct {
	Samples       uint64        `json:"samples"`
	P50UpperBound time.Duration `json:"p50_upper_bound"`
	P95UpperBound time.Duration `json:"p95_upper_bound"`
	P99UpperBound time.Duration `json:"p99_upper_bound"`
	MaxUpperBound time.Duration `json:"max_upper_bound"`
}

type timingMeasurement struct {
	InputSendOverhead durationSummary `json:"input_send_overhead"`
	RequestRoundTrip  durationSummary `json:"request_round_trip"`
	UpdateAge         durationSummary `json:"update_age"`
}

type relayMeasurement struct {
	Forwarded      uint64 `json:"forwarded"`
	Dropped        uint64 `json:"dropped"`
	PendingPackets uint64 `json:"pending_packets"`
	PendingBytes   int64  `json:"pending_bytes"`
	Overloaded     bool   `json:"overloaded"`
}

type runtimeMeasurement struct {
	Goroutines int     `json:"goroutines"`
	HeapAlloc  uint64  `json:"heap_alloc"`
	RSSBytes   uint64  `json:"rss_bytes"`
	CPUSeconds float64 `json:"cpu_seconds"`
	TotalAlloc uint64  `json:"allocated_bytes"`
	Mallocs    uint64  `json:"allocations"`
}

type trialResult struct {
	Status      string      `json:"status"`
	Error       string      `json:"error,omitempty"`
	Config      trialConfig `json:"config"`
	Measurement measurement `json:"measurement"`
	GoVersion   string      `json:"go_version"`
	Timestamp   time.Time   `json:"timestamp"`
	Path        string      `json:"-"`
}

func main() {
	var input, output string
	flag.StringVar(&input, "input", "", "directory containing sgspbench JSON trial results")
	flag.StringVar(&output, "output", "", "Markdown report path")
	flag.Parse()
	if input == "" || output == "" {
		fatal(errors.New("--input and --output are required"))
	}
	trials, err := loadTrials(input)
	if err != nil {
		fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		fatal(err)
	}
	if err := os.WriteFile(output, []byte(renderReport(trials)), 0644); err != nil {
		fatal(err)
	}
}

func fatal(err error) { fmt.Fprintln(os.Stderr, "sgspbenchreport:", err); os.Exit(2) }

func loadTrials(root string) ([]trialResult, error) {
	var trials []trialResult
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".json" {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var trial trialResult
		if err := json.Unmarshal(data, &trial); err != nil {
			return fmt.Errorf("decode %s: %w", path, err)
		}
		if trial.Status == "" || (trial.Config.Implementation != "sgsp" && trial.Config.Implementation != "quic") {
			return fmt.Errorf("invalid trial result %s", path)
		}
		trial.Path = path
		trials = append(trials, trial)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if len(trials) == 0 {
		return nil, errors.New("no JSON trial results found")
	}
	sort.Slice(trials, func(i, j int) bool {
		left, right := trials[i], trials[j]
		if left.Config.Implementation != right.Config.Implementation {
			return left.Config.Implementation < right.Config.Implementation
		}
		if left.Config.Clients != right.Config.Clients {
			return left.Config.Clients < right.Config.Clients
		}
		if left.Config.Hz != right.Config.Hz {
			return left.Config.Hz < right.Config.Hz
		}
		if left.Config.RTT != right.Config.RTT {
			return left.Config.RTT < right.Config.RTT
		}
		if left.Config.Loss != right.Config.Loss {
			return left.Config.Loss < right.Config.Loss
		}
		return left.Config.Seed < right.Config.Seed
	})
	return trials, nil
}

func renderReport(trials []trialResult) string {
	var completed, failed int
	for _, trial := range trials {
		if trial.Status == "completed" {
			completed++
		} else {
			failed++
		}
	}
	var report strings.Builder
	report.WriteString("# SGSP benchmark trial index\n\n")
	fmt.Fprintf(&report, "Generated from %d recorded trial(s): %d completed, %d non-completed. This is an index of measurements, not a release-gate verdict.\n\n", len(trials), completed, failed)
	report.WriteString("| implementation | clients | Hz | RTT | loss | jitter / reorder | seed | status | inputs offered / accepted / delivered | updates offered / accepted / delivered | requests offered / accepted / delivered / failed | bulk offered / accepted / delivered | input-send p99 | request RTT p99 | update-age p99 | relay | runtime interval | artifact |\n")
	report.WriteString("| --- | ---: | ---: | --- | ---: | --- | ---: | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, trial := range trials {
		fmt.Fprintf(&report, "| %s | %d | %d | %s | %.2f%% | %s / %.2f%% | %d | %s | %s | %s | %s | %s | %s | %s | %s | %s | %s | `%s` |\n",
			trial.Config.Implementation,
			trial.Config.Clients,
			trial.Config.Hz,
			trial.Config.RTT,
			100*trial.Config.Loss,
			trial.Config.Jitter,
			100*trial.Config.Reorder,
			trial.Config.Seed,
			trial.Status,
			formatCounts(trial.Measurement.Inputs),
			formatCounts(trial.Measurement.Updates),
			formatCountsWithFailure(trial.Measurement.Requests),
			formatCounts(trial.Measurement.Bulk),
			formatP99(trial.Measurement.Timing.InputSendOverhead),
			formatP99(trial.Measurement.Timing.RequestRoundTrip),
			formatP99(trial.Measurement.Timing.UpdateAge),
			formatRelay(trial.Measurement.Relay),
			formatRuntime(trial.Measurement.Runtime),
			filepath.ToSlash(trial.Path),
		)
		if trial.Error != "" {
			fmt.Fprintf(&report, "\n  Trial error: %s\n", sanitizeCell(trial.Error))
		}
	}
	return report.String()
}

func formatCounts(counts operationCounts) string {
	return fmt.Sprintf("%d / %d / %d", counts.Offered, counts.Accepted, counts.Delivered)
}

func formatCountsWithFailure(counts operationCounts) string {
	return fmt.Sprintf("%d / %d / %d / %d", counts.Offered, counts.Accepted, counts.Delivered, counts.Failed)
}

func formatP99(summary durationSummary) string {
	if summary.Samples == 0 {
		return "-"
	}
	return "≤ " + summary.P99UpperBound.String()
}

func formatRelay(relay relayMeasurement) string {
	if relay.Forwarded == 0 && relay.Dropped == 0 && relay.PendingPackets == 0 && relay.PendingBytes == 0 && !relay.Overloaded {
		return "-"
	}
	return fmt.Sprintf("forwarded=%d; dropped=%d; pending=%d/%dB; overloaded=%t", relay.Forwarded, relay.Dropped, relay.PendingPackets, relay.PendingBytes, relay.Overloaded)
}

func formatRuntime(runtime runtimeMeasurement) string {
	if runtime.Goroutines == 0 && runtime.HeapAlloc == 0 && runtime.RSSBytes == 0 && runtime.CPUSeconds == 0 && runtime.TotalAlloc == 0 && runtime.Mallocs == 0 {
		return "-"
	}
	return fmt.Sprintf("cpu=%.3fs; heap=%dB; rss=%dB; alloc=%dB/%d; goroutines=%d", runtime.CPUSeconds, runtime.HeapAlloc, runtime.RSSBytes, runtime.TotalAlloc, runtime.Mallocs, runtime.Goroutines)
}

func sanitizeCell(value string) string {
	return strings.NewReplacer("|", "\\|", "\n", " ", "\r", " ").Replace(value)
}
