// Command sgspbench runs and records one bounded SGSP or bare-QUIC workload
// trial. It intentionally records a trial, not a fabricated capacity claim:
// M8's full repeated matrix remains a separate release-evidence task.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

type config struct {
	Implementation string        `json:"implementation"`
	Clients        int           `json:"clients"`
	Hz             int           `json:"hz"`
	InputBytes     int           `json:"input_bytes"`
	UpdateBytes    int           `json:"update_bytes"`
	RPCPerSecond   int           `json:"rpc_per_second"`
	BulkBytes      int           `json:"bulk_bytes_per_second"`
	Warmup         time.Duration `json:"warmup"`
	Duration       time.Duration `json:"duration"`
	RTT            time.Duration `json:"rtt"`
	Jitter         time.Duration `json:"jitter"`
	Loss           float64       `json:"loss"`
	Reorder        float64       `json:"reorder"`
	Seed           int64         `json:"seed"`
}
type result struct {
	Status      string      `json:"status"`
	Error       string      `json:"error,omitempty"`
	Config      config      `json:"config"`
	Measurement measurement `json:"measurement,omitempty"`
	GoVersion   string      `json:"go_version"`
	Timestamp   time.Time   `json:"timestamp"`
}

func main() {
	var output string
	cfg := config{}
	flag.StringVar(&cfg.Implementation, "implementation", "", "sgsp or quic")
	flag.IntVar(&cfg.Clients, "clients", 0, "client count")
	flag.IntVar(&cfg.Hz, "hz", 0, "60 or 128")
	flag.IntVar(&cfg.InputBytes, "input-bytes", 64, "input payload bytes (at least 32 for benchmark metadata)")
	flag.IntVar(&cfg.UpdateBytes, "update-bytes", 512, "update payload bytes (at least 32 for benchmark metadata)")
	flag.IntVar(&cfg.RPCPerSecond, "rpc-per-second", 2, "request/reply rate per client")
	flag.IntVar(&cfg.BulkBytes, "bulk-bytes-per-second", 64<<10, "reliable bulk bytes per client per second")
	flag.DurationVar(&cfg.Warmup, "warmup", 10*time.Second, "warmup duration")
	flag.DurationVar(&cfg.Duration, "duration", 60*time.Second, "measurement duration")
	flag.DurationVar(&cfg.RTT, "rtt", 20*time.Millisecond, "simulated round-trip time")
	flag.DurationVar(&cfg.Jitter, "jitter", 0, "one-way jitter amplitude")
	flag.Float64Var(&cfg.Loss, "loss", 0, "packet-loss fraction")
	flag.Float64Var(&cfg.Reorder, "reorder", 0, "packet-reorder fraction")
	flag.Int64Var(&cfg.Seed, "seed", 1, "deterministic relay seed")
	flag.StringVar(&output, "output", "", "machine-readable result path")
	flag.Parse()
	if err := validate(cfg, output); err != nil {
		fatalWithoutResult(err)
	}
	value := result{Config: cfg, GoVersion: runtime.Version(), Timestamp: time.Now().UTC()}
	measurement, err := runTrial(context.Background(), cfg)
	value.Measurement = measurement
	var exitCode int
	value.Status, value.Error, exitCode = trialOutcome(measurement, err)
	writeResult(output, value)
	if exitCode != 0 {
		fmt.Fprintln(os.Stderr, "sgspbench:", value.Error)
		os.Exit(exitCode)
	}
}

// trialOutcome keeps the JSON status and command exit contract aligned. An
// invalid sample is evidence to retain, but it is not a successful benchmark
// invocation: callers such as the matrix runner must be able to fail a gate
// while continuing to collect the remaining trial records.
func trialOutcome(measurement measurement, err error) (status, message string, exitCode int) {
	if err != nil {
		return trialStatus(err), err.Error(), 1
	}
	if measurement.Validity.InputSampleBufferOverflow || measurement.Validity.UpdateSampleBufferOverflow {
		return "invalid", "correlated directional local-overhead sample buffer overflow", 1
	}
	if measurement.Generator.Unable {
		return "invalid", "workload generator missed scheduled ticks", 1
	}
	return "completed", "", 0
}
func validate(cfg config, output string) error {
	if cfg.Implementation != "sgsp" && cfg.Implementation != "quic" {
		return errors.New("--implementation must be sgsp or quic")
	}
	if cfg.Clients < 1 || (cfg.Hz != 60 && cfg.Hz != 128) ||
		cfg.InputBytes < 32 ||
		cfg.InputBytes > 1000 ||
		cfg.UpdateBytes < 32 ||
		cfg.UpdateBytes > 1000 ||
		cfg.RPCPerSecond < 0 ||
		cfg.BulkBytes < 0 ||
		cfg.Warmup < 0 ||
		cfg.Duration <= 0 ||
		cfg.RTT < 0 ||
		cfg.Jitter < 0 ||
		cfg.Loss < 0 ||
		cfg.Loss > 1 ||
		cfg.Reorder < 0 ||
		cfg.Reorder > 1 ||
		output == "" {
		return errors.New("invalid benchmark configuration")
	}
	return nil
}
func writeResult(path string, value result) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		fatalWithoutResult(err)
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		fatalWithoutResult(err)
	}
	if err := os.WriteFile(path, append(data, '\n'), 0644); err != nil {
		fatalWithoutResult(err)
	}
}
func fatalWithoutResult(err error) { fmt.Fprintln(os.Stderr, "sgspbench:", err); os.Exit(2) }
