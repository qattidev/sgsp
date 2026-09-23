// Command sgspbencheval turns one retained benchmark campaign into an
// auditable matrix assessment. It does not turn missing evidence into a pass:
// incomplete or failed gates remain explicit in evaluation.md.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const localOverheadGate = time.Millisecond

type campaign struct {
	ReferenceHost string `json:"reference_host"`
}

type expectedTrial struct {
	Implementation string
	Clients        int
	Hz             int
	RTT            time.Duration
	Loss           float64
	Jitter         time.Duration
	Reorder        float64
	Seed           int64
	Profile        string
	Duration       time.Duration
	Path           string
}

type config struct {
	Implementation string        `json:"implementation"`
	Clients        int           `json:"clients"`
	Hz             int           `json:"hz"`
	RTT            time.Duration `json:"rtt"`
	Jitter         time.Duration `json:"jitter"`
	Loss           float64       `json:"loss"`
	Reorder        float64       `json:"reorder"`
	Seed           int64         `json:"seed"`
	Duration       time.Duration `json:"duration"`
}

type durationSummary struct {
	Samples       uint64        `json:"samples"`
	P99UpperBound time.Duration `json:"p99_upper_bound"`
	P99Overflow   bool          `json:"p99_overflow"`
}

type correlatedOverhead struct {
	Duration durationSummary `json:"duration"`
}

type generatorMeasurement struct {
	Unable bool `json:"unable_to_produce_scheduled_work"`
}

type validityMeasurement struct {
	InputSampleBufferOverflow  bool `json:"input_sample_buffer_overflow"`
	UpdateSampleBufferOverflow bool `json:"update_sample_buffer_overflow"`
}

type operationCounts struct {
	Offered   uint64 `json:"offered"`
	Delivered uint64 `json:"delivered"`
}

type timingMeasurement struct {
	InputSendPlusReceive  correlatedOverhead `json:"input_send_plus_receive"`
	UpdateSendPlusReceive correlatedOverhead `json:"update_send_plus_receive"`
}

type networkMeasurement struct {
	LocalDropMetricsAvailable bool   `json:"local_drop_metrics_available"`
	LocalDatagramsDropped     uint64 `json:"local_datagrams_dropped"`
	CoalescedDatagramsDropped uint64 `json:"coalesced_datagrams_dropped"`
}

type relayMeasurement struct {
	Overloaded bool `json:"overloaded"`
}

type runtimeMeasurement struct {
	CPUSeconds                 float64 `json:"cpu_seconds"`
	HeapAlloc                  uint64  `json:"heap_alloc"`
	AllocatedBytesPerOperation float64 `json:"allocated_bytes_per_offered_operation"`
}

type measurement struct {
	Inputs    operationCounts      `json:"inputs"`
	Updates   operationCounts      `json:"updates"`
	Generator generatorMeasurement `json:"generator"`
	Validity  validityMeasurement  `json:"validity"`
	Timing    timingMeasurement    `json:"timing"`
	Network   networkMeasurement   `json:"network"`
	Relay     relayMeasurement     `json:"relay"`
	Runtime   runtimeMeasurement   `json:"runtime"`
}

type trial struct {
	Status      string      `json:"status"`
	Error       string      `json:"error"`
	Config      config      `json:"config"`
	Measurement measurement `json:"measurement"`
}

type evaluatedTrial struct {
	Expected expectedTrial
	Trial    trial
	Problem  string
}

type pointKey struct {
	Profile string
	Clients int
	Hz      int
	RTT     time.Duration
	Loss    float64
	Jitter  time.Duration
	Reorder float64
}

type pointSummary struct {
	Key                              pointKey
	Implementation                   string
	Planned, Completed, Valid        int
	InputP99, UpdateP99              time.Duration
	InputOverflow, UpdateOverflow    bool
	UpdatesOffered, UpdatesDelivered uint64
	LocalDrops, CoalescedDrops       uint64
	LocalMetricsMissing              int
	RelayOverloads                   int
	CPUSeconds                       float64
	HeapAlloc                        uint64
	AllocatedBytesPerOperation       float64
}

type evaluation struct {
	Root                   string
	Campaign               campaign
	ReferenceHostValidated bool
	Trials                 []evaluatedTrial
	Points                 []pointSummary
	Issues                 []string
	HealthyCapacity        int
	Healthy32At128         bool
	ImpairmentComplete     bool
	ComparisonComplete     bool
	HealthyGatePassed      bool
}

func main() {
	var input string
	flag.StringVar(&input, "input", "", "repository-local campaign artifact directory")
	flag.Parse()
	if input == "" {
		fatal(errors.New("--input is required"))
	}
	root, err := filepath.Abs(input)
	if err != nil {
		fatal(err)
	}
	result, err := evaluateCampaign(root)
	if err != nil {
		fatal(err)
	}
	output := filepath.Join(root, "evaluation.md")
	if err := os.WriteFile(output, []byte(renderEvaluation(result)), 0644); err != nil {
		fatal(err)
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "sgspbencheval:", err)
	os.Exit(2)
}

func evaluateCampaign(root string) (evaluation, error) {
	manifest, err := loadCampaign(filepath.Join(root, "campaign.json"))
	if err != nil {
		return evaluation{}, err
	}
	expected, err := loadExpected(root, filepath.Join(root, "expected-trials.tsv"))
	if err != nil {
		return evaluation{}, err
	}
	result := evaluation{Root: root, Campaign: manifest, ReferenceHostValidated: referenceHostValidated(root)}
	for _, item := range expected {
		value, problem := loadAndMatch(item)
		result.Trials = append(result.Trials, evaluatedTrial{Expected: item, Trial: value, Problem: problem})
	}
	result.Points = summarizePoints(result.Trials)
	result.assess()
	return result, nil
}

func loadCampaign(path string) (campaign, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return campaign{}, fmt.Errorf("read campaign manifest: %w", err)
	}
	var value campaign
	if err := json.Unmarshal(data, &value); err != nil {
		return campaign{}, fmt.Errorf("decode campaign manifest: %w", err)
	}
	if value.ReferenceHost != "0" && value.ReferenceHost != "1" {
		return campaign{}, errors.New("campaign manifest has no valid reference_host value")
	}
	return value, nil
}

func loadExpected(root, path string) ([]expectedTrial, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read expected trial plan: %w", err)
	}
	defer file.Close()
	var expected []expectedTrial
	scanner := bufio.NewScanner(file)
	line := 0
	for scanner.Scan() {
		line++
		fields := strings.Split(scanner.Text(), "\t")
		if len(fields) != 13 {
			return nil, fmt.Errorf("expected trial plan line %d has %d fields, want 13", line, len(fields))
		}
		clients, err := strconv.Atoi(fields[1])
		if err != nil {
			return nil, fmt.Errorf("expected trial plan line %d clients: %w", line, err)
		}
		hz, err := strconv.Atoi(fields[2])
		if err != nil {
			return nil, fmt.Errorf("expected trial plan line %d hz: %w", line, err)
		}
		rtt, err := parseDurationNanos(fields[4])
		if err != nil {
			return nil, fmt.Errorf("expected trial plan line %d rtt: %w", line, err)
		}
		loss, err := strconv.ParseFloat(fields[5], 64)
		if err != nil {
			return nil, fmt.Errorf("expected trial plan line %d loss: %w", line, err)
		}
		jitter, err := parseDurationNanos(fields[7])
		if err != nil {
			return nil, fmt.Errorf("expected trial plan line %d jitter: %w", line, err)
		}
		reorder, err := strconv.ParseFloat(fields[8], 64)
		if err != nil {
			return nil, fmt.Errorf("expected trial plan line %d reorder: %w", line, err)
		}
		seed, err := strconv.ParseInt(fields[9], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("expected trial plan line %d seed: %w", line, err)
		}
		duration, err := parseDurationNanos(fields[11])
		if err != nil {
			return nil, fmt.Errorf("expected trial plan line %d duration: %w", line, err)
		}
		artifactPath, err := filepath.Abs(fields[12])
		if err != nil {
			return nil, fmt.Errorf("expected trial plan line %d path: %w", line, err)
		}
		relativePath, err := filepath.Rel(root, artifactPath)
		if err != nil || relativePath == ".." || strings.HasPrefix(relativePath, ".."+string(filepath.Separator)) {
			return nil, fmt.Errorf("expected trial plan line %d path escapes campaign: %s", line, fields[12])
		}
		expected = append(expected, expectedTrial{Implementation: fields[0], Clients: clients, Hz: hz, RTT: rtt, Loss: loss, Jitter: jitter, Reorder: reorder, Seed: seed, Profile: fields[10], Duration: duration, Path: artifactPath})
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read expected trial plan: %w", err)
	}
	if len(expected) == 0 {
		return nil, errors.New("expected trial plan is empty")
	}
	return expected, nil
}

func parseDurationNanos(value string) (time.Duration, error) {
	nanoseconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, err
	}
	if nanoseconds < 0 {
		return 0, errors.New("must not be negative")
	}
	return time.Duration(nanoseconds), nil
}

func referenceHostValidated(root string) bool {
	manifests, err := filepath.Glob(filepath.Join(root, "environment*.txt"))
	if err != nil {
		return false
	}
	for _, path := range manifests {
		data, err := os.ReadFile(path)
		if err == nil && strings.Contains(string(data), "reference_host_validation=passed") {
			return true
		}
	}
	return false
}

func loadAndMatch(expected expectedTrial) (trial, string) {
	data, err := os.ReadFile(expected.Path)
	if err != nil {
		return trial{}, "missing or unreadable raw result"
	}
	var value trial
	if err := json.Unmarshal(data, &value); err != nil {
		return trial{}, "raw result is not valid JSON"
	}
	if value.Config.Implementation != expected.Implementation || value.Config.Clients != expected.Clients || value.Config.Hz != expected.Hz || value.Config.RTT != expected.RTT || value.Config.Loss != expected.Loss || value.Config.Jitter != expected.Jitter || value.Config.Reorder != expected.Reorder || value.Config.Seed != expected.Seed || value.Config.Duration != expected.Duration {
		return value, "raw result configuration does not match expected trial plan"
	}
	if value.Status != "completed" {
		return value, "raw result is not completed"
	}
	if !validTrial(value) {
		return value, "completed raw result has an invalid measurement"
	}
	return value, ""
}

func validTrial(value trial) bool {
	if value.Measurement.Generator.Unable || value.Measurement.Validity.InputSampleBufferOverflow || value.Measurement.Validity.UpdateSampleBufferOverflow {
		return false
	}
	if value.Measurement.Inputs.Offered > 0 && value.Measurement.Timing.InputSendPlusReceive.Duration.Samples == 0 {
		return false
	}
	if value.Measurement.Updates.Offered > 0 && value.Measurement.Timing.UpdateSendPlusReceive.Duration.Samples == 0 {
		return false
	}
	return true
}

func summarizePoints(values []evaluatedTrial) []pointSummary {
	points := make(map[string]*pointSummary)
	for _, value := range values {
		key := pointKey{Profile: value.Expected.Profile, Clients: value.Expected.Clients, Hz: value.Expected.Hz, RTT: value.Expected.RTT, Loss: value.Expected.Loss, Jitter: value.Expected.Jitter, Reorder: value.Expected.Reorder}
		mapKey := fmt.Sprintf("%s/%s/%d/%d/%d/%g/%d/%g", key.Profile, value.Expected.Implementation, key.Clients, key.Hz, key.RTT, key.Loss, key.Jitter, key.Reorder)
		point := points[mapKey]
		if point == nil {
			point = &pointSummary{Key: key, Implementation: value.Expected.Implementation}
			points[mapKey] = point
		}
		point.Planned++
		if value.Trial.Status == "completed" {
			point.Completed++
		}
		if value.Problem != "" {
			continue
		}
		point.Valid++
		input := value.Trial.Measurement.Timing.InputSendPlusReceive.Duration
		update := value.Trial.Measurement.Timing.UpdateSendPlusReceive.Duration
		if input.P99UpperBound > point.InputP99 {
			point.InputP99 = input.P99UpperBound
		}
		if update.P99UpperBound > point.UpdateP99 {
			point.UpdateP99 = update.P99UpperBound
		}
		point.InputOverflow = point.InputOverflow || input.P99Overflow
		point.UpdateOverflow = point.UpdateOverflow || update.P99Overflow
		point.UpdatesOffered += value.Trial.Measurement.Updates.Offered
		point.UpdatesDelivered += value.Trial.Measurement.Updates.Delivered
		point.LocalDrops += value.Trial.Measurement.Network.LocalDatagramsDropped
		point.CoalescedDrops += value.Trial.Measurement.Network.CoalescedDatagramsDropped
		if !value.Trial.Measurement.Network.LocalDropMetricsAvailable && point.Implementation == "sgsp" {
			point.LocalMetricsMissing++
		}
		if value.Trial.Measurement.Relay.Overloaded {
			point.RelayOverloads++
		}
		point.CPUSeconds += value.Trial.Measurement.Runtime.CPUSeconds
		point.HeapAlloc += value.Trial.Measurement.Runtime.HeapAlloc
		point.AllocatedBytesPerOperation += value.Trial.Measurement.Runtime.AllocatedBytesPerOperation
	}
	result := make([]pointSummary, 0, len(points))
	for _, point := range points {
		result = append(result, *point)
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i], result[j]
		if left.Key.Profile != right.Key.Profile {
			return left.Key.Profile < right.Key.Profile
		}
		if left.Key.Clients != right.Key.Clients {
			return left.Key.Clients < right.Key.Clients
		}
		if left.Key.Hz != right.Key.Hz {
			return left.Key.Hz < right.Key.Hz
		}
		if left.Key.RTT != right.Key.RTT {
			return left.Key.RTT < right.Key.RTT
		}
		if left.Key.Loss != right.Key.Loss {
			return left.Key.Loss < right.Key.Loss
		}
		if left.Key.Jitter != right.Key.Jitter {
			return left.Key.Jitter < right.Key.Jitter
		}
		return left.Implementation < right.Implementation
	})
	return result
}

func (result *evaluation) assess() {
	if result.Campaign.ReferenceHost != "1" || !result.ReferenceHostValidated {
		result.Issues = append(result.Issues, "reference-host validation is absent; this campaign is diagnostic only")
	}
	for _, value := range result.Trials {
		if value.Problem != "" {
			result.Issues = append(result.Issues, fmt.Sprintf("%s: %s", filepath.Base(value.Expected.Path), value.Problem))
		}
	}
	result.Healthy32At128 = result.pointHasFiveValidTrials("healthy", "sgsp", 32, 128, 20*time.Millisecond, 0, 0, 0)
	if !result.Healthy32At128 {
		result.Issues = append(result.Issues, "the required 32-client, 128 Hz SGSP healthy point lacks five valid trials")
	}
	result.HealthyCapacity = result.highestHealthyCapacity()
	if result.HealthyCapacity == 0 {
		result.Issues = append(result.Issues, "no healthy SGSP client count has ten valid trials across 60 and 128 Hz that meet local gates")
	}
	result.HealthyGatePassed = result.HealthyCapacity > 0
	if !result.HealthyGatePassed {
		result.Issues = append(result.Issues, "a healthy local-overhead/drop/relay gate failed or lacked measurements")
	}
	result.ImpairmentComplete = result.hasRequiredImpairment()
	if !result.ImpairmentComplete {
		result.Issues = append(result.Issues, "the complete half-capacity impairment matrix is absent or has invalid trials")
	}
	result.ComparisonComplete = result.hasCompleteComparison()
	if !result.ComparisonComplete {
		result.Issues = append(result.Issues, "one or more planned points lack an SGSP/bare-QUIC comparison")
	}
}

func (result evaluation) pointHasFiveValidTrials(profile, implementation string, clients, hz int, rtt time.Duration, loss float64, jitter time.Duration, reorder float64) bool {
	count := 0
	for _, value := range result.Trials {
		expected := value.Expected
		if expected.Profile == profile && expected.Implementation == implementation && expected.Clients == clients && expected.Hz == hz && expected.RTT == rtt && expected.Loss == loss && expected.Jitter == jitter && expected.Reorder == reorder && value.Problem == "" {
			count++
		}
	}
	return count == 5
}

func (result evaluation) highestHealthyCapacity() int {
	clients := map[int]struct{}{}
	for _, value := range result.Trials {
		if value.Expected.Profile == "healthy" && value.Expected.Implementation == "sgsp" {
			clients[value.Expected.Clients] = struct{}{}
		}
	}
	var candidates []int
	for clientCount := range clients {
		if result.healthyCapacityPasses(clientCount) {
			candidates = append(candidates, clientCount)
		}
	}
	sort.Ints(candidates)
	if len(candidates) == 0 {
		return 0
	}
	return candidates[len(candidates)-1]
}

func (result evaluation) healthyCapacityPasses(clientCount int) bool {
	count := 0
	for _, value := range result.Trials {
		expected, trial := value.Expected, value.Trial
		if expected.Profile != "healthy" || expected.Implementation != "sgsp" || expected.Clients != clientCount || (expected.Hz != 60 && expected.Hz != 128) {
			continue
		}
		count++
		input := trial.Measurement.Timing.InputSendPlusReceive.Duration
		update := trial.Measurement.Timing.UpdateSendPlusReceive.Duration
		if value.Problem != "" || input.Samples == 0 || update.Samples == 0 || input.P99Overflow || update.P99Overflow || input.P99UpperBound >= localOverheadGate || update.P99UpperBound >= localOverheadGate || !trial.Measurement.Network.LocalDropMetricsAvailable || trial.Measurement.Network.LocalDatagramsDropped != 0 || trial.Measurement.Relay.Overloaded || coalescedFraction(trial) >= 0.001 {
			return false
		}
	}
	return count == 10
}

func coalescedFraction(value trial) float64 {
	offered := value.Measurement.Updates.Offered
	if offered == 0 {
		return 0
	}
	return float64(value.Measurement.Network.CoalescedDatagramsDropped) / float64(offered)
}

func (result evaluation) hasRequiredImpairment() bool {
	if result.HealthyCapacity == 0 || result.HealthyCapacity%2 != 0 {
		return false
	}
	clients := result.HealthyCapacity / 2
	for _, hz := range []int{60, 128} {
		for _, rtt := range []time.Duration{20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond} {
			for _, loss := range []float64{0, 0.01, 0.05} {
				for _, implementation := range []string{"sgsp", "quic"} {
					if !result.pointHasFiveValidTrials("impairment", implementation, clients, hz, rtt, loss, 0, 0) {
						return false
					}
				}
			}
		}
		for _, loss := range []float64{0, 0.01, 0.05} {
			for _, implementation := range []string{"sgsp", "quic"} {
				if !result.pointHasFiveValidTrials("impairment-jitter", implementation, clients, hz, 50*time.Millisecond, loss, 10*time.Millisecond, 0.02) {
					return false
				}
			}
		}
	}
	return true
}

func (result evaluation) hasCompleteComparison() bool {
	type comparedPoint struct{ sgsp, quic int }
	points := map[pointKey]comparedPoint{}
	for _, value := range result.Trials {
		key := pointKey{Profile: value.Expected.Profile, Clients: value.Expected.Clients, Hz: value.Expected.Hz, RTT: value.Expected.RTT, Loss: value.Expected.Loss, Jitter: value.Expected.Jitter, Reorder: value.Expected.Reorder}
		point := points[key]
		if value.Expected.Implementation == "sgsp" && value.Problem == "" {
			point.sgsp++
		}
		if value.Expected.Implementation == "quic" && value.Problem == "" {
			point.quic++
		}
		points[key] = point
	}
	for _, point := range points {
		if point.sgsp != 5 || point.quic != 5 {
			return false
		}
	}
	return len(points) > 0
}

func renderEvaluation(result evaluation) string {
	var report strings.Builder
	report.WriteString("# SGSP benchmark matrix evaluation\n\n")
	verdict := "PASS"
	if len(result.Issues) > 0 {
		verdict = "INCOMPLETE"
		if result.Campaign.ReferenceHost == "1" && result.ReferenceHostValidated && result.Healthy32At128 && result.HealthyCapacity == 0 {
			verdict = "FAILED"
		}
	}
	fmt.Fprintf(&report, "**Matrix verdict: %s.** This is a verdict for the retained benchmark matrix only, not an M8 release verdict; fuzz, SQL, and resource evidence remain separately required.\n\n", verdict)
	fmt.Fprintf(&report, "- Campaign: `campaign.json`; expected plan: `expected-trials.tsv`; raw trials: `raw/`\n- Reference host requested: `%s`; environment validation passed: `%t`\n- Planned trials: %d; complete and valid: %d\n- Highest healthy SGSP point passing the local matrix gates: %s\n- Required 32-client / 128 Hz healthy point: %s\n- Full half-capacity impairment matrix: %s\n- SGSP versus bare-QUIC comparison coverage: %s\n\n", result.Campaign.ReferenceHost, result.ReferenceHostValidated, len(result.Trials), validTrialCount(result.Trials), capacityText(result.HealthyCapacity), passText(result.Healthy32At128), passText(result.ImpairmentComplete), passText(result.ComparisonComplete))

	report.WriteString("## Point summaries\n\n")
	report.WriteString("| profile | implementation | clients | Hz | RTT / loss / jitter / reorder | valid / planned | max paired input p99 | max paired update p99 | update delivered / offered | SGSP local / coalesced drops | relay overloads | mean CPU s | mean heap B | mean alloc B/op |\n")
	report.WriteString("| --- | --- | ---: | ---: | --- | --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: |\n")
	for _, point := range result.Points {
		fmt.Fprintf(&report, "| %s | %s | %d | %d | %s / %.2f%% / %s / %.2f%% | %d / %d | %s | %s | %d / %d | %s | %d | %.3f | %.0f | %.2f |\n", point.Key.Profile, point.Implementation, point.Key.Clients, point.Key.Hz, point.Key.RTT, 100*point.Key.Loss, point.Key.Jitter, 100*point.Key.Reorder, point.Valid, point.Planned, formatP99(point.InputP99, point.InputOverflow), formatP99(point.UpdateP99, point.UpdateOverflow), point.UpdatesDelivered, point.UpdatesOffered, formatDrops(point), point.RelayOverloads, average(point.CPUSeconds, point.Valid), averageUint(point.HeapAlloc, point.Valid), average(point.AllocatedBytesPerOperation, point.Valid))
	}

	report.WriteString("\n## SGSP versus bare-QUIC comparison\n\n")
	report.WriteString("Values are per-point means across valid seeds; p99 values in the point table remain worst seed upper bounds. `n/a` means the paired point is incomplete.\n\n")
	report.WriteString("| profile | clients | Hz | RTT / loss / jitter / reorder | SGSP CPU / QUIC CPU s | SGSP heap / QUIC heap B | SGSP alloc / QUIC alloc B/op | SGSP input / QUIC input p99 | SGSP update / QUIC update p99 |\n")
	report.WriteString("| --- | ---: | ---: | --- | --- | --- | --- | --- | --- |\n")
	for _, pair := range comparisonRows(result.Points) {
		fmt.Fprintf(&report, "| %s | %d | %d | %s / %.2f%% / %s / %.2f%% | %s | %s | %s | %s | %s |\n", pair.Key.Profile, pair.Key.Clients, pair.Key.Hz, pair.Key.RTT, 100*pair.Key.Loss, pair.Key.Jitter, 100*pair.Key.Reorder, pairMetric(pair.SGSP, pair.QUIC, func(point pointSummary) string {
			return fmt.Sprintf("%.3f", average(point.CPUSeconds, point.Valid))
		}), pairMetric(pair.SGSP, pair.QUIC, func(point pointSummary) string {
			return fmt.Sprintf("%.0f", averageUint(point.HeapAlloc, point.Valid))
		}), pairMetric(pair.SGSP, pair.QUIC, func(point pointSummary) string {
			return fmt.Sprintf("%.2f", average(point.AllocatedBytesPerOperation, point.Valid))
		}), pairMetric(pair.SGSP, pair.QUIC, func(point pointSummary) string { return formatP99(point.InputP99, point.InputOverflow) }), pairMetric(pair.SGSP, pair.QUIC, func(point pointSummary) string { return formatP99(point.UpdateP99, point.UpdateOverflow) }))
	}

	report.WriteString("\n## Evidence-supported limitations and bottlenecks\n\n")
	if len(result.Issues) == 0 {
		report.WriteString("- No missing, invalid, overloaded, or local-gate-failing planned point was found. Review the point rows for workload-specific tradeoffs.\n")
	} else {
		for _, issue := range result.Issues {
			fmt.Fprintf(&report, "- %s\n", issue)
		}
	}
	report.WriteString("\nThe evaluator never substitutes this campaign for the ten-minute resource plateau or functional loss/epoch/reconnect evidence. Raw JSON remains the source of record for every row.\n")
	return report.String()
}

type comparisonRow struct {
	Key        pointKey
	SGSP, QUIC *pointSummary
}

func comparisonRows(points []pointSummary) []comparisonRow {
	rows := map[pointKey]comparisonRow{}
	for index := range points {
		point := &points[index]
		row := rows[point.Key]
		row.Key = point.Key
		if point.Implementation == "sgsp" {
			row.SGSP = point
		} else if point.Implementation == "quic" {
			row.QUIC = point
		}
		rows[point.Key] = row
	}
	result := make([]comparisonRow, 0, len(rows))
	for _, row := range rows {
		result = append(result, row)
	}
	sort.Slice(result, func(i, j int) bool {
		left, right := result[i].Key, result[j].Key
		if left.Profile != right.Profile {
			return left.Profile < right.Profile
		}
		if left.Clients != right.Clients {
			return left.Clients < right.Clients
		}
		if left.Hz != right.Hz {
			return left.Hz < right.Hz
		}
		if left.RTT != right.RTT {
			return left.RTT < right.RTT
		}
		if left.Loss != right.Loss {
			return left.Loss < right.Loss
		}
		return left.Jitter < right.Jitter
	})
	return result
}

func pairMetric(sgsp, quic *pointSummary, format func(pointSummary) string) string {
	if sgsp == nil || quic == nil || sgsp.Valid != sgsp.Planned || quic.Valid != quic.Planned {
		return "n/a"
	}
	return format(*sgsp) + " / " + format(*quic)
}

func validTrialCount(values []evaluatedTrial) int {
	count := 0
	for _, value := range values {
		if value.Problem == "" {
			count++
		}
	}
	return count
}

func capacityText(value int) string {
	if value == 0 {
		return "none"
	}
	return strconv.Itoa(value) + " clients"
}

func passText(value bool) string {
	if value {
		return "complete"
	}
	return "incomplete"
}

func formatP99(value time.Duration, overflow bool) string {
	if overflow {
		return "> 1s (overflow)"
	}
	if value == 0 {
		return "n/a"
	}
	return "≤ " + value.String()
}

func formatDrops(point pointSummary) string {
	if point.Implementation != "sgsp" {
		return "n/a"
	}
	if point.LocalMetricsMissing > 0 {
		return "metrics unavailable"
	}
	if point.UpdatesOffered == 0 {
		return fmt.Sprintf("%d / %d", point.LocalDrops, point.CoalescedDrops)
	}
	return fmt.Sprintf("%d / %d (%.3f%%)", point.LocalDrops, point.CoalescedDrops, 100*float64(point.CoalescedDrops)/float64(point.UpdatesOffered))
}

func average(total float64, count int) float64 {
	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func averageUint(total uint64, count int) float64 {
	if count == 0 {
		return 0
	}
	return float64(total) / float64(count)
}
