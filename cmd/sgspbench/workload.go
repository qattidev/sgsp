package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"qattidev/sgsp"
	"qattidev/sgsp/internal/quictransport"
	"qattidev/sgsp/internal/transport"
	"qattidev/sgsp/internal/udprelay"
	"qattidev/sgsp/internal/wire"
)

const (
	benchmarkInputType          sgsp.MessageType = 1
	benchmarkUpdateType         sgsp.MessageType = 2
	benchmarkRequestType        sgsp.MessageType = 3
	benchmarkStreamType         sgsp.MessageType = 4
	benchmarkMeasuredStreamType sgsp.MessageType = 5
	benchmarkRPCBytes                            = 128
	benchmarkMeasuredTick       uint64           = 1 << 63
)

type measurement struct {
	Duration  time.Duration        `json:"duration"`
	Inputs    operationCounts      `json:"inputs"`
	Updates   operationCounts      `json:"updates"`
	Requests  operationCounts      `json:"requests"`
	Bulk      operationCounts      `json:"bulk"`
	Generator generatorMeasurement `json:"generator"`
	Timing    timingMeasurement    `json:"timing"`
	Validity  validityMeasurement  `json:"validity"`
	Network   networkMeasurement   `json:"network"`
	Queues    queueMeasurement     `json:"queues"`
	Relay     relayMeasurement     `json:"relay"`
	Runtime   runtimeMeasurement   `json:"runtime"`
}

type operationCounts struct {
	Offered   uint64 `json:"offered"`
	Accepted  uint64 `json:"accepted"`
	Delivered uint64 `json:"delivered"`
	Failed    uint64 `json:"failed"`
	Unknown   uint64 `json:"unknown"`
}

// generatorMeasurement exposes gaps in the ticker-driven workload schedule.
// A ticker can coalesce sends while its consumer is busy, so offered operation
// counts alone cannot establish that the configured workload was generated.
// Missed ticks make a performance trial invalid rather than silently lowering
// its offered rate.
type generatorMeasurement struct {
	InputTicksScheduled   uint64 `json:"input_ticks_scheduled"`
	InputTicksMissed      uint64 `json:"input_ticks_missed"`
	RequestTicksScheduled uint64 `json:"request_ticks_scheduled"`
	RequestTicksMissed    uint64 `json:"request_ticks_missed"`
	BulkTicksScheduled    uint64 `json:"bulk_ticks_scheduled"`
	BulkTicksMissed       uint64 `json:"bulk_ticks_missed"`
	Unable                bool   `json:"unable_to_produce_scheduled_work"`
}

type relayMeasurement struct {
	Forwarded          uint64 `json:"forwarded"`
	Dropped            uint64 `json:"dropped"`
	PendingPackets     uint64 `json:"pending_packets"`
	PendingBytes       int64  `json:"pending_bytes"`
	PeakPendingPackets uint64 `json:"peak_pending_packets"`
	PeakPendingBytes   int64  `json:"peak_pending_bytes"`
	Overloaded         bool   `json:"overloaded"`
}

// networkMeasurement keeps SGSP's local datagram-drop causes separate from
// the relay's observed UDP drops. A transport packet-loss counter alone cannot
// prove application delivery, so the relay remains a distinct measurement.
type networkMeasurement struct {
	LocalDropMetricsAvailable bool            `json:"local_drop_metrics_available"`
	RTT                       durationSummary `json:"rtt"`
	LocalDatagramsDropped     uint64          `json:"local_datagrams_dropped"`
	CoalescedDatagramsDropped uint64          `json:"coalesced_datagrams_dropped"`
	StaleUpdatesDropped       uint64          `json:"stale_updates_dropped"`
}

// queueMeasurement records bounded observer labels from the library's queue
// samplers. The age map is keyed only by SGSP's fixed histogram kinds, never
// by a session or application-controlled value.
type queueMeasurement struct {
	Available    bool              `json:"available"`
	MaximumItems uint64            `json:"maximum_items"`
	MaximumBytes uint64            `json:"maximum_bytes"`
	AgeBuckets   map[string]uint64 `json:"age_buckets"`
}

// benchmarkObserver collects the library's bounded aggregate observations.
// It is deliberately not used for the benchmark-only timestamp measurements;
// those stay on their dedicated fixed-size instrumentation path.
type benchmarkObserver struct {
	mu       sync.Mutex
	maxItems uint64
	maxBytes uint64
	age      map[string]uint64
}

func (o *benchmarkObserver) Observe(observation sgsp.Observation) {
	if o == nil || observation.Value <= 0 {
		return
	}
	value := uint64(observation.Value)
	o.mu.Lock()
	defer o.mu.Unlock()
	switch observation.Name {
	case "queue_items":
		if value > o.maxItems {
			o.maxItems = value
		}
	case "queue_bytes":
		if value > o.maxBytes {
			o.maxBytes = value
		}
	case "queue_age":
		if o.age == nil {
			o.age = make(map[string]uint64)
		}
		o.age[observation.Kind] += value
	}
}

func (o *benchmarkObserver) Reset() {
	if o == nil {
		return
	}
	o.mu.Lock()
	o.maxItems, o.maxBytes = 0, 0
	o.age = make(map[string]uint64)
	o.mu.Unlock()
}

func (o *benchmarkObserver) Snapshot() queueMeasurement {
	if o == nil {
		return queueMeasurement{AgeBuckets: map[string]uint64{}}
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	age := make(map[string]uint64, len(o.age))
	for kind, count := range o.age {
		age[kind] = count
	}
	return queueMeasurement{Available: true, MaximumItems: o.maxItems, MaximumBytes: o.maxBytes, AgeBuckets: age}
}

type runtimeMeasurement struct {
	GoMaxProcs                 int     `json:"gomaxprocs"`
	Goroutines                 int     `json:"goroutines"`
	HeapAlloc                  uint64  `json:"heap_alloc"`
	RSSBytes                   uint64  `json:"rss_bytes"`
	CPUSeconds                 float64 `json:"cpu_seconds"`
	TotalAlloc                 uint64  `json:"allocated_bytes"`
	Mallocs                    uint64  `json:"allocations"`
	OfferedOperations          uint64  `json:"offered_operations"`
	AllocatedBytesPerOperation float64 `json:"allocated_bytes_per_offered_operation"`
	AllocationsPerOperation    float64 `json:"allocations_per_offered_operation"`
	NumCPU                     int     `json:"num_cpu"`
	OperatingSystem            string  `json:"operating_system"`
	Architecture               string  `json:"architecture"`
}

// durationSummary reports upper bounds from a fixed binary histogram. Keeping
// the samples in fixed buckets makes long trials bounded independently of the
// offered workload while retaining enough resolution to evaluate a 1 ms gate.
type durationSummary struct {
	Samples       uint64        `json:"samples"`
	Overflow      uint64        `json:"overflow"`
	P50UpperBound time.Duration `json:"p50_upper_bound"`
	P50Overflow   bool          `json:"p50_overflow"`
	P95UpperBound time.Duration `json:"p95_upper_bound"`
	P95Overflow   bool          `json:"p95_overflow"`
	P99UpperBound time.Duration `json:"p99_upper_bound"`
	P99Overflow   bool          `json:"p99_overflow"`
	MaxUpperBound time.Duration `json:"max_upper_bound"`
	MaxOverflow   bool          `json:"max_overflow"`
}

type timingMeasurement struct {
	InputSendOverhead     durationSummary           `json:"input_send_overhead"`
	InputReceiveOverhead  durationSummary           `json:"input_receive_overhead"`
	InputSendPlusReceive  correlatedOverheadSummary `json:"input_send_plus_receive"`
	UpdateSendOverhead    durationSummary           `json:"update_send_overhead"`
	UpdateReceiveOverhead durationSummary           `json:"update_receive_overhead"`
	UpdateSendPlusReceive correlatedOverheadSummary `json:"update_send_plus_receive"`
	RequestRoundTrip      durationSummary           `json:"request_round_trip"`
	UpdateAge             durationSummary           `json:"update_age"`
}

// correlatedOverheadSummary pairs one direction's measured local
// API-to-adapter handoff with the matching decoded-frame-to-handler/poll-return
// receive time. The p99 is calculated over those paired sums, not by adding
// independent percentiles. Unmatched sends are reported instead of silently
// dropping them.
type correlatedOverheadSummary struct {
	Duration          durationSummary `json:"duration"`
	Sent              uint64          `json:"sent"`
	Received          uint64          `json:"received"`
	Matched           uint64          `json:"matched"`
	UnmatchedSent     uint64          `json:"unmatched_sent"`
	UnmatchedReceived uint64          `json:"unmatched_received"`
	UnmatchedFraction float64         `json:"unmatched_fraction"`
	BufferOverflow    bool            `json:"buffer_overflow"`
}

// validityMeasurement contains conditions which change whether a recorded
// trial can be used for a release gate. An overflowing correlation buffer is
// explicitly invalid: dropping timestamps would fabricate a percentile.
type validityMeasurement struct {
	InputSampleBufferOverflow  bool `json:"input_sample_buffer_overflow"`
	UpdateSampleBufferOverflow bool `json:"update_sample_buffer_overflow"`
}

// The last regular bucket is one second. Values beyond that are recorded in
// the explicit overflow bucket instead of being folded into the one-second
// bucket. The bounds are inclusive.
var durationBucketBounds = [...]time.Duration{
	time.Microsecond,
	2 * time.Microsecond,
	4 * time.Microsecond,
	8 * time.Microsecond,
	16 * time.Microsecond,
	32 * time.Microsecond,
	64 * time.Microsecond,
	128 * time.Microsecond,
	256 * time.Microsecond,
	512 * time.Microsecond,
	1024 * time.Microsecond,
	2048 * time.Microsecond,
	4096 * time.Microsecond,
	8192 * time.Microsecond,
	16384 * time.Microsecond,
	32768 * time.Microsecond,
	65536 * time.Microsecond,
	131072 * time.Microsecond,
	262144 * time.Microsecond,
	524288 * time.Microsecond,
	time.Second,
}

const durationHistogramBuckets = len(durationBucketBounds) + 1

type durationHistogram struct {
	buckets [durationHistogramBuckets]atomic.Uint64
}

func (h *durationHistogram) Record(value time.Duration) {
	if h == nil || value < 0 {
		return
	}
	bucket := len(durationBucketBounds) // explicit overflow bucket
	for index, bound := range durationBucketBounds {
		if value <= bound {
			bucket = index
			break
		}
	}
	h.buckets[bucket].Add(1)
}

func (h *durationHistogram) Reset() {
	if h == nil {
		return
	}
	for index := range h.buckets {
		h.buckets[index].Store(0)
	}
}

func (h *durationHistogram) Snapshot() durationSummary {
	if h == nil {
		return durationSummary{}
	}
	var counts [durationHistogramBuckets]uint64
	var total uint64
	for index := range h.buckets {
		counts[index] = h.buckets[index].Load()
		total += counts[index]
	}
	if total == 0 {
		return durationSummary{}
	}
	quantile := func(numerator, denominator uint64) (time.Duration, bool) {
		target := (total*numerator + denominator - 1) / denominator
		var seen uint64
		for index, count := range counts {
			seen += count
			if seen >= target {
				return durationBucketUpperBound(index), index == len(durationBucketBounds)
			}
		}
		return durationBucketUpperBound(len(h.buckets) - 1), true
	}
	max := time.Duration(0)
	maxOverflow := false
	for index := len(h.buckets) - 1; index >= 0; index-- {
		if counts[index] > 0 {
			max = durationBucketUpperBound(index)
			maxOverflow = index == len(durationBucketBounds)
			break
		}
	}
	p50, p50Overflow := quantile(50, 100)
	p95, p95Overflow := quantile(95, 100)
	p99, p99Overflow := quantile(99, 100)
	return durationSummary{Samples: total, Overflow: counts[len(durationBucketBounds)], P50UpperBound: p50, P50Overflow: p50Overflow, P95UpperBound: p95, P95Overflow: p95Overflow, P99UpperBound: p99, P99Overflow: p99Overflow, MaxUpperBound: max, MaxOverflow: maxOverflow}
}

func durationBucketUpperBound(bucket int) time.Duration {
	if bucket < 0 {
		return 0
	}
	if bucket >= len(durationBucketBounds) {
		return durationBucketBounds[len(durationBucketBounds)-1]
	}
	return durationBucketBounds[bucket]
}

type trialCounters struct {
	inputOffered, inputAccepted, inputDelivered, inputFailed                         atomic.Uint64
	updateOffered, updateAccepted, updateDelivered, updateFailed                     atomic.Uint64
	requestOffered, requestAccepted, requestDelivered, requestFailed, requestUnknown atomic.Uint64
	bulkOffered, bulkAccepted, bulkDelivered, bulkFailed                             atomic.Uint64
	inputTicksScheduled, inputTicksMissed                                            atomic.Uint64
	requestTicksScheduled, requestTicksMissed                                        atomic.Uint64
	bulkTicksScheduled, bulkTicksMissed                                              atomic.Uint64
	inputSendOverhead, inputReceiveOverhead, inputSendPlusReceive                    durationHistogram
	updateSendOverhead, updateReceiveOverhead, updateSendPlusReceive                 durationHistogram
	requestRoundTrip, updateAge                                                      durationHistogram
	measuring                                                                        atomic.Bool
}

// scheduledTicker detects coalesced time.Ticker events. Its observed values
// are the tick's scheduled times, so a gap of N intervals means N-1 scheduled
// workload opportunities were not generated.
type scheduledTicker struct {
	interval time.Duration
	previous time.Time
}

func (t *scheduledTicker) Observe(at time.Time) uint64 {
	if t == nil || t.interval <= 0 || at.IsZero() {
		return 0
	}
	missed := uint64(0)
	if !t.previous.IsZero() && at.After(t.previous) {
		intervals := at.Sub(t.previous) / t.interval
		if intervals > 1 {
			missed = uint64(intervals - 1)
		}
	}
	t.previous = at
	return missed
}

// workloadStartPhase spreads clients over the shortest active periodic
// workload interval. A trial releases its client generators from one boundary;
// creating every ticker immediately would make their input and 10 ms bulk
// work fire in lockstep. That artificial burst can coalesce ticker events
// before the transport has a chance to run. A phase is at least roughly one
// scheduler quantum, while every client retains the same ticker interval and
// therefore its configured steady-state rate.
func workloadStartPhase(cfg config, clientIndex int) time.Duration {
	if cfg.Clients <= 1 || clientIndex <= 0 || cfg.Hz <= 0 {
		return 0
	}
	period := time.Second / time.Duration(cfg.Hz)
	if cfg.BulkBytes > 0 && 10*time.Millisecond < period {
		period = 10 * time.Millisecond
	}
	slots := int(period / time.Millisecond)
	if slots < 2 {
		return 0
	}
	if cfg.Clients < slots {
		slots = cfg.Clients
	}
	return time.Duration(clientIndex%slots) * period / time.Duration(slots)
}

const maxCorrelatedSamplesPerClient = 8_192

type correlatedOverheadSample struct {
	send, receive       time.Duration
	hasSend, hasReceive bool
}

// localOverheadCorrelator holds a fixed, per-client sequence window. It is
// deliberately preallocated so an overly long or unexpectedly fast workload
// invalidates the trial rather than growing benchmark memory or losing timing
// samples without notice.
type localOverheadCorrelator struct {
	mu       sync.Mutex
	samples  []correlatedOverheadSample
	base     uint64
	started  bool
	overflow bool
	sent     uint64
	received uint64
	matched  uint64
	combined *durationHistogram
}

func newLocalOverheadCorrelator(capacity int, combined *durationHistogram) *localOverheadCorrelator {
	if capacity < 1 {
		capacity = 1
	}
	if capacity > maxCorrelatedSamplesPerClient {
		capacity = maxCorrelatedSamplesPerClient
	}
	return &localOverheadCorrelator{samples: make([]correlatedOverheadSample, capacity), combined: combined}
}

func correlatedSampleCapacity(cfg config) int {
	// A one-second slack covers ticker coalescing and final in-flight updates;
	// the fixed maximum makes longer workloads visibly invalid on overflow.
	return int((cfg.Duration*time.Duration(cfg.Hz)+time.Second-1)/time.Second) + cfg.Hz + 32
}

func (c *localOverheadCorrelator) Reset() {
	if c == nil {
		return
	}
	c.mu.Lock()
	clear(c.samples)
	c.base, c.sent, c.received, c.matched = 0, 0, 0, 0
	c.started, c.overflow = false, false
	c.mu.Unlock()
}

func (c *localOverheadCorrelator) RecordSend(sequence uint64, duration time.Duration) {
	if c == nil || sequence == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.started {
		c.base, c.started = sequence, true
	}
	entry, ok := c.entryLocked(sequence)
	if !ok {
		return
	}
	if !entry.hasSend {
		entry.hasSend, entry.send = true, duration
		c.sent++
	}
	c.matchLocked(entry)
}

func (c *localOverheadCorrelator) RecordReceive(sequence uint64, duration time.Duration) {
	if c == nil || sequence == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// A returned update already in flight from warmup has no measured send. Do
	// not charge it to the measurement window before its first measured input.
	if !c.started {
		return
	}
	entry, ok := c.entryLocked(sequence)
	if !ok {
		return
	}
	if !entry.hasReceive {
		entry.hasReceive, entry.receive = true, duration
		c.received++
	}
	c.matchLocked(entry)
}

func (c *localOverheadCorrelator) entryLocked(sequence uint64) (*correlatedOverheadSample, bool) {
	if !c.started || sequence < c.base {
		return nil, false
	}
	index := sequence - c.base
	if index >= uint64(len(c.samples)) {
		c.overflow = true
		return nil, false
	}
	return &c.samples[index], true
}

func (c *localOverheadCorrelator) matchLocked(entry *correlatedOverheadSample) {
	if entry == nil || !entry.hasSend || !entry.hasReceive {
		return
	}
	// Mark this pair consumed before recording to ensure a duplicate datagram
	// cannot contribute another synthetic local-overhead sample.
	entry.hasSend, entry.hasReceive = false, false
	c.matched++
	if c.combined != nil {
		c.combined.Record(entry.send + entry.receive)
	}
}

func (c *localOverheadCorrelator) Snapshot() correlatedOverheadSummary {
	if c == nil {
		return correlatedOverheadSummary{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	result := correlatedOverheadSummary{Sent: c.sent, Received: c.received, Matched: c.matched, BufferOverflow: c.overflow}
	if result.Sent >= result.Matched {
		result.UnmatchedSent = result.Sent - result.Matched
	}
	if result.Received >= result.Matched {
		result.UnmatchedReceived = result.Received - result.Matched
	}
	if result.Sent > 0 {
		result.UnmatchedFraction = float64(result.UnmatchedSent) / float64(result.Sent)
	}
	return result
}

// countWorkloadFailure reports whether an operation failed while the measured
// workload was still active. An error caused by the measurement cutoff leaves
// an offered operation unmatched; it is not a transport/application failure.
func countWorkloadFailure(ctx context.Context, err error) bool {
	return err != nil && ctx.Err() == nil
}

func requestOutcomeUnknown(err error) bool {
	var protocol *sgsp.Error
	return errors.As(err, &protocol) && protocol.OutcomeUnknown
}

func bareRequestOutcomeUnknown(requestBytesWritten int, completeResponse bool) bool {
	return requestBytesWritten > 0 && !completeResponse
}

// bulkPacer distributes a byte-per-second target across 100 ten-millisecond
// writes without rounding the configured workload down.
type bulkPacer struct {
	perSecond int
	remainder int
}

func (p *bulkPacer) Next() int {
	if p == nil || p.perSecond <= 0 {
		return 0
	}
	p.remainder += p.perSecond
	bytes := p.remainder / 100
	p.remainder %= 100
	return bytes
}

func (c *trialCounters) reset() {
	if c == nil {
		return
	}
	c.measuring.Store(false)
	for _, counter := range []*atomic.Uint64{
		&c.inputOffered, &c.inputAccepted, &c.inputDelivered, &c.inputFailed,
		&c.updateOffered, &c.updateAccepted, &c.updateDelivered, &c.updateFailed,
		&c.requestOffered, &c.requestAccepted, &c.requestDelivered, &c.requestFailed, &c.requestUnknown,
		&c.bulkOffered, &c.bulkAccepted, &c.bulkDelivered, &c.bulkFailed,
		&c.inputTicksScheduled, &c.inputTicksMissed,
		&c.requestTicksScheduled, &c.requestTicksMissed,
		&c.bulkTicksScheduled, &c.bulkTicksMissed,
	} {
		counter.Store(0)
	}
	c.inputSendOverhead.Reset()
	c.inputReceiveOverhead.Reset()
	c.inputSendPlusReceive.Reset()
	c.updateSendOverhead.Reset()
	c.updateReceiveOverhead.Reset()
	c.updateSendPlusReceive.Reset()
	c.requestRoundTrip.Reset()
	c.updateAge.Reset()
}

func (c *trialCounters) startMeasurement() {
	if c != nil {
		c.measuring.Store(true)
	}
}

func (c *trialCounters) stopMeasurement() {
	if c != nil {
		c.measuring.Store(false)
	}
}

func (c *trialCounters) recording() bool {
	return c != nil && c.measuring.Load()
}

func (c *trialCounters) recordInputTick(missed uint64) {
	if c == nil || !c.recording() {
		return
	}
	c.inputTicksScheduled.Add(1 + missed)
	c.inputTicksMissed.Add(missed)
}

func (c *trialCounters) recordRequestTick(missed uint64) {
	if c == nil || !c.recording() {
		return
	}
	c.requestTicksScheduled.Add(1 + missed)
	c.requestTicksMissed.Add(missed)
}

func (c *trialCounters) recordBulkTick(missed uint64) {
	if c == nil || !c.recording() {
		return
	}
	c.bulkTicksScheduled.Add(1 + missed)
	c.bulkTicksMissed.Add(missed)
}

func (c *trialCounters) snapshot() measurement {
	if c == nil {
		return measurement{}
	}
	counts := func(offered, accepted, delivered, failed, unknown *atomic.Uint64) operationCounts {
		result := operationCounts{Offered: offered.Load(), Accepted: accepted.Load(), Delivered: delivered.Load(), Failed: failed.Load()}
		if unknown != nil {
			result.Unknown = unknown.Load()
		}
		return result
	}
	generator := generatorMeasurement{
		InputTicksScheduled:   c.inputTicksScheduled.Load(),
		InputTicksMissed:      c.inputTicksMissed.Load(),
		RequestTicksScheduled: c.requestTicksScheduled.Load(),
		RequestTicksMissed:    c.requestTicksMissed.Load(),
		BulkTicksScheduled:    c.bulkTicksScheduled.Load(),
		BulkTicksMissed:       c.bulkTicksMissed.Load(),
	}
	generator.Unable = generator.InputTicksMissed > 0 || generator.RequestTicksMissed > 0 || generator.BulkTicksMissed > 0
	return measurement{
		Inputs:    counts(&c.inputOffered, &c.inputAccepted, &c.inputDelivered, &c.inputFailed, nil),
		Updates:   counts(&c.updateOffered, &c.updateAccepted, &c.updateDelivered, &c.updateFailed, nil),
		Requests:  counts(&c.requestOffered, &c.requestAccepted, &c.requestDelivered, &c.requestFailed, &c.requestUnknown),
		Bulk:      counts(&c.bulkOffered, &c.bulkAccepted, &c.bulkDelivered, &c.bulkFailed, nil),
		Generator: generator,
		Timing: timingMeasurement{
			InputSendOverhead:     c.inputSendOverhead.Snapshot(),
			InputReceiveOverhead:  c.inputReceiveOverhead.Snapshot(),
			UpdateSendOverhead:    c.updateSendOverhead.Snapshot(),
			UpdateReceiveOverhead: c.updateReceiveOverhead.Snapshot(),
			RequestRoundTrip:      c.requestRoundTrip.Snapshot(),
			UpdateAge:             c.updateAge.Snapshot(),
		},
	}
}

func summarizeCorrelatedOverhead(histogram *durationHistogram, correlators []*localOverheadCorrelator) correlatedOverheadSummary {
	combined := correlatedOverheadSummary{}
	if histogram != nil {
		combined.Duration = histogram.Snapshot()
	}
	for _, correlator := range correlators {
		summary := correlator.Snapshot()
		combined.Sent += summary.Sent
		combined.Received += summary.Received
		combined.Matched += summary.Matched
		combined.UnmatchedSent += summary.UnmatchedSent
		combined.UnmatchedReceived += summary.UnmatchedReceived
		combined.BufferOverflow = combined.BufferOverflow || summary.BufferOverflow
	}
	if combined.Sent > 0 {
		combined.UnmatchedFraction = float64(combined.UnmatchedSent) / float64(combined.Sent)
	}
	return combined
}

func applyCorrelatedOverhead(result *measurement, counters *trialCounters, inputCorrelators, updateCorrelators []*localOverheadCorrelator) {
	if result == nil || counters == nil {
		return
	}
	input := summarizeCorrelatedOverhead(&counters.inputSendPlusReceive, inputCorrelators)
	update := summarizeCorrelatedOverhead(&counters.updateSendPlusReceive, updateCorrelators)
	result.Timing.InputSendPlusReceive = input
	result.Timing.UpdateSendPlusReceive = update
	result.Validity.InputSampleBufferOverflow = input.BufferOverflow
	result.Validity.UpdateSampleBufferOverflow = update.BufferOverflow
}

type networkAccumulator struct {
	localDropMetricsAvailable                        bool
	rtt                                              durationHistogram
	localDatagramsDropped, coalescedDatagramsDropped uint64
	staleUpdatesDropped                              uint64
}

func (a *networkAccumulator) addSGSP(stats sgsp.Stats) {
	if a == nil {
		return
	}
	a.localDropMetricsAvailable = true
	if stats.TransportStatsAvailable && stats.RTT > 0 {
		a.rtt.Record(stats.RTT)
	}
	a.localDatagramsDropped += stats.LocalDatagramsDropped
	a.coalescedDatagramsDropped += stats.CoalescedDatagramsDropped
	a.staleUpdatesDropped += stats.StaleUpdatesDropped
}

func (a *networkAccumulator) addTransport(stats transport.Stats) {
	if a != nil && stats.RTT > 0 {
		a.rtt.Record(stats.RTT)
	}
}

func (a *networkAccumulator) Snapshot() networkMeasurement {
	if a == nil {
		return networkMeasurement{}
	}
	return networkMeasurement{LocalDropMetricsAvailable: a.localDropMetricsAvailable, RTT: a.rtt.Snapshot(), LocalDatagramsDropped: a.localDatagramsDropped, CoalescedDatagramsDropped: a.coalescedDatagramsDropped, StaleUpdatesDropped: a.staleUpdatesDropped}
}

// benchmarkClock provides a monotonic time origin shared by the local server
// and clients. Its elapsed stamps travel in payloads so update age does not
// depend on wall-clock synchronization.
type benchmarkClock struct{ started time.Time }

func newBenchmarkClock() benchmarkClock { return benchmarkClock{started: time.Now()} }

func (c benchmarkClock) Stamp() uint64 {
	if c.started.IsZero() {
		return 0
	}
	age := time.Since(c.started)
	if age <= 0 {
		return 0
	}
	return uint64(age)
}

func (c benchmarkClock) Age(stamp uint64) (time.Duration, bool) {
	if c.started.IsZero() || stamp == 0 || stamp > uint64(1<<63-1) {
		return 0, false
	}
	age := time.Since(c.started) - time.Duration(stamp)
	return age, age >= 0
}

type benchmarkAuthenticator struct{}

func (benchmarkAuthenticator) Authenticate(context.Context, sgsp.Credential) (sgsp.Principal, error) {
	return sgsp.Principal{Issuer: "sgspbench", Subject: "benchmark-client", ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func runTrial(ctx context.Context, cfg config) (measurement, error) {
	if cfg.Implementation == "quic" {
		return runQUICTrial(ctx, cfg)
	}
	return runSGSPTrial(ctx, cfg)
}

func trialStatus(err error) string {
	if errors.Is(err, udprelay.ErrHarnessOverload) {
		return "harness_overload"
	}
	return "failed"
}

func runSGSPTrial(parent context.Context, cfg config) (result measurement, resultErr error) {
	certificate, roots, err := benchmarkCertificate()
	if err != nil {
		return result, err
	}
	serverPacket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return result, err
	}
	counters := &trialCounters{}
	observer := &benchmarkObserver{}
	inputCorrelators := make([]*localOverheadCorrelator, cfg.Clients)
	updateCorrelators := make([]*localOverheadCorrelator, cfg.Clients)
	for index := range inputCorrelators {
		inputCorrelators[index] = newLocalOverheadCorrelator(correlatedSampleCapacity(cfg), &counters.inputSendPlusReceive)
		updateCorrelators[index] = newLocalOverheadCorrelator(correlatedSampleCapacity(cfg), &counters.updateSendPlusReceive)
	}
	clock := newBenchmarkClock()
	limits := sgsp.DefaultLimits()
	limits.MaxSessions = max(limits.MaxSessions, cfg.Clients)
	limits.MaxPendingHandshakes = max(limits.MaxPendingHandshakes, cfg.Clients)
	// Every benchmark client originates from loopback. Scale the admission
	// rate limits with the configured population so that an intentional
	// same-host setup does not mistake its single source address for an attack.
	// These remain bounded limits; production defaults are unchanged.
	limits.HandshakesPerIPPerSecond = max(limits.HandshakesPerIPPerSecond, cfg.Clients)
	limits.HandshakeBurstPerIP = max(limits.HandshakeBurstPerIP, cfg.Clients)
	serverRouter := benchmarkServerRouter(cfg, counters, inputCorrelators, updateCorrelators)
	server, err := sgsp.NewServer(sgsp.ServerConfig{
		TLS:      &tls.Config{Certificates: []tls.Certificate{certificate}},
		App:      sgsp.AppIdentity{ID: "sgspbench", Version: "1"},
		Owner:    sgsp.Owner{ID: "benchmark-owner", Endpoint: sgsp.Endpoint{Address: serverPacket.LocalAddr().String(), ServerName: "localhost"}},
		Auth:     benchmarkAuthenticator{},
		Limits:   limits,
		Dispatch: sgsp.DispatchConfig{Mode: sgsp.Handlers, Router: serverRouter},
		Observer: observer,
	})
	if err != nil {
		_ = serverPacket.Close()
		return result, err
	}
	serverCtx, stopServer := context.WithCancel(parent)
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serverCtx, serverPacket) }()
	defer func() {
		stopServer()
		if err := <-serveDone; resultErr == nil && err != nil {
			resultErr = err
		}
	}()

	type trialClient struct {
		client sgsp.Client
		relay  *udprelay.Relay
	}
	clients := make([]trialClient, cfg.Clients)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for _, client := range clients {
			if client.client != nil {
				_ = client.client.Close(closeCtx)
			}
			if client.relay != nil {
				_ = client.relay.Close()
			}
		}
	}()
	setupCtx, cancelSetup := context.WithCancel(parent)
	defer cancelSetup()
	var setupOnce sync.Once
	var setupErr error
	var setupWorkers sync.WaitGroup
	failSetup := func(err error) {
		setupOnce.Do(func() {
			setupErr = err
			cancelSetup()
		})
	}
	for index := 0; index < cfg.Clients; index++ {
		index := index
		setupWorkers.Add(1)
		go func() {
			defer setupWorkers.Done()
			relay, err := udprelay.New("127.0.0.1:0", "127.0.0.1:0", serverPacket.LocalAddr(), udprelay.Config{RTT: cfg.RTT, Jitter: cfg.Jitter, Loss: cfg.Loss, Reorder: cfg.Reorder, Seed: uint64(cfg.Seed) + uint64(index)})
			if err != nil {
				failSetup(err)
				return
			}
			client, err := sgsp.Dial(setupCtx, sgsp.Endpoint{Address: relay.ClientAddr().String(), ServerName: "localhost"}, sgsp.ClientConfig{
				TLS:              &tls.Config{RootCAs: roots},
				App:              sgsp.AppIdentity{ID: "sgspbench", Version: "1"},
				Limits:           limits,
				DisableReconnect: true,
				Credentials: func(context.Context) (sgsp.Credential, error) {
					return sgsp.Credential{Scheme: "benchmark", Data: []byte("benchmark")}, nil
				},
				// The client deliberately polls and continuously drains updates so
				// DecodedAt-to-Next return is the SGSP queue/dispatch boundary.
				Dispatch: sgsp.DispatchConfig{Mode: sgsp.Polling},
				Observer: observer,
			})
			if err != nil {
				_ = relay.Close()
				failSetup(err)
				return
			}
			clients[index] = trialClient{client: client, relay: relay}
		}()
	}
	setupWorkers.Wait()
	if setupErr != nil {
		return result, setupErr
	}

	trialCtx, cancelTrial := context.WithTimeout(parent, cfg.Warmup+cfg.Duration+10*time.Second)
	defer cancelTrial()

	startWorkers := func(measured bool, start <-chan struct{}, ready chan<- error) (context.CancelFunc, *sync.WaitGroup) {
		workCtx, stopWork := context.WithCancel(trialCtx)
		workers := &sync.WaitGroup{}
		for index, client := range clients {
			workers.Add(2)
			go func(client sgsp.Client, correlator *localOverheadCorrelator) {
				defer workers.Done()
				receiveSGSPUpdates(workCtx, client, counters, clock, correlator)
			}(client.client, updateCorrelators[index])
			go func(session sgsp.Session, clientIndex int, correlator *localOverheadCorrelator) {
				defer workers.Done()
				runSGSPClient(workCtx, cfg, session, counters, clock, clientIndex, correlator, measured, start, ready)
			}(client.client.Session(), index, inputCorrelators[index])
		}
		return stopWork, workers
	}
	warmupStop, warmupWorkers := startWorkers(false, nil, nil)
	if !waitTrial(trialCtx, cfg.Warmup) {
		warmupStop()
		warmupWorkers.Wait()
		return result, context.Cause(trialCtx)
	}
	// Stop and join the warmup generators before resetting counters. Individual
	// workload frames retain an explicit phase marker, so late warmup frames
	// cannot be attributed to the newly opened measurement window.
	warmupStop()
	warmupWorkers.Wait()
	counters.reset()
	observer.Reset()
	for _, correlator := range inputCorrelators {
		correlator.Reset()
	}
	for _, correlator := range updateCorrelators {
		correlator.Reset()
	}
	measurementStart := make(chan struct{})
	measurementReady := make(chan error, cfg.Clients)
	measurementStop, measurementWorkers := startWorkers(true, measurementStart, measurementReady)
	if err := waitForWorkloadSetup(trialCtx, measurementReady, cfg.Clients); err != nil {
		measurementStop()
		measurementWorkers.Wait()
		return result, err
	}
	// The preparation opens measurement-phase streams but does not emit
	// workload. Reset queue observations once that setup has drained, then
	// release every generator from the same measurement boundary.
	observer.Reset()
	started := time.Now()
	runtimeStarted := runtimeSnapshot()
	counters.startMeasurement()
	close(measurementStart)
	if !waitTrial(trialCtx, cfg.Duration) {
		counters.stopMeasurement()
		measurementStop()
		measurementWorkers.Wait()
		return result, context.Cause(trialCtx)
	}
	counters.stopMeasurement()
	measurementStop()
	measurementWorkers.Wait()
	result = counters.snapshot()
	applyCorrelatedOverhead(&result, counters, inputCorrelators, updateCorrelators)
	var network networkAccumulator
	for _, client := range clients {
		network.addSGSP(client.client.Session().Stats())
	}
	for _, session := range server.Sessions() {
		network.addSGSP(session.Stats())
	}
	result.Network = network.Snapshot()
	result.Queues = observer.Snapshot()
	result.Duration = time.Since(started)
	result.Runtime = runtimeDelta(runtimeStarted, runtimeSnapshot())
	result.Runtime.normalizeByOfferedOperations(result.offeredOperations())
	for _, client := range clients {
		stats := client.relay.Stats()
		result.Relay.Forwarded += stats.Forwarded
		result.Relay.Dropped += stats.Dropped
		result.Relay.PendingPackets += stats.PendingPackets
		result.Relay.PendingBytes += stats.PendingBytes
		result.Relay.PeakPendingPackets += stats.PeakPendingPackets
		result.Relay.PeakPendingBytes += stats.PeakPendingBytes
		result.Relay.Overloaded = result.Relay.Overloaded || stats.Overloaded
		if err := client.relay.Err(); err != nil {
			return result, err
		}
	}
	return result, nil
}

// runQUICTrial is the transport baseline for the same payload sizes and
// topology as runSGSPTrial. It keeps the SGSP wire envelopes but bypasses the
// endpoint/session/dispatch layers being compared.
func runQUICTrial(parent context.Context, cfg config) (result measurement, resultErr error) {
	certificate, roots, err := benchmarkCertificate()
	if err != nil {
		return result, err
	}
	serverPacket, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		return result, err
	}
	counters := &trialCounters{}
	inputCorrelators := make([]*localOverheadCorrelator, cfg.Clients)
	updateCorrelators := make([]*localOverheadCorrelator, cfg.Clients)
	for index := range inputCorrelators {
		inputCorrelators[index] = newLocalOverheadCorrelator(correlatedSampleCapacity(cfg), &counters.inputSendPlusReceive)
		updateCorrelators[index] = newLocalOverheadCorrelator(correlatedSampleCapacity(cfg), &counters.updateSendPlusReceive)
	}
	clock := newBenchmarkClock()
	transportConfig := benchmarkTransportConfig(&tls.Config{Certificates: []tls.Certificate{certificate}})
	listener, err := quictransport.Listen(serverPacket, transportConfig)
	if err != nil {
		_ = serverPacket.Close()
		return result, err
	}
	serverCtx, stopServer := context.WithCancel(parent)
	var serverWorkers sync.WaitGroup
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		for {
			connection, err := listener.Accept(serverCtx)
			if err != nil {
				return
			}
			serverWorkers.Add(1)
			go func() {
				defer serverWorkers.Done()
				serveBareQUICConnection(connection, cfg, counters, clock, inputCorrelators, updateCorrelators)
			}()
		}
	}()
	defer func() {
		stopServer()
		_ = listener.Close()
		_ = serverPacket.Close()
		<-serveDone
		serverWorkers.Wait()
	}()

	type trialClient struct {
		connection transport.Conn
		packet     net.PacketConn
		relay      *udprelay.Relay
	}
	clients := make([]trialClient, cfg.Clients)
	defer func() {
		for _, client := range clients {
			if client.connection != nil {
				_ = client.connection.Close(uint64(sgsp.Normal), "benchmark complete")
			}
			if client.packet != nil {
				_ = client.packet.Close()
			}
			if client.relay != nil {
				_ = client.relay.Close()
			}
		}
	}()
	setupCtx, cancelSetup := context.WithCancel(parent)
	defer cancelSetup()
	var setupOnce sync.Once
	var setupErr error
	var setupWorkers sync.WaitGroup
	failSetup := func(err error) {
		setupOnce.Do(func() {
			setupErr = err
			cancelSetup()
		})
	}
	for index := 0; index < cfg.Clients; index++ {
		index := index
		setupWorkers.Add(1)
		go func() {
			defer setupWorkers.Done()
			relay, err := udprelay.New("127.0.0.1:0", "127.0.0.1:0", serverPacket.LocalAddr(), udprelay.Config{RTT: cfg.RTT, Jitter: cfg.Jitter, Loss: cfg.Loss, Reorder: cfg.Reorder, Seed: uint64(cfg.Seed) + uint64(index)})
			if err != nil {
				failSetup(err)
				return
			}
			packet, err := net.ListenPacket("udp", "[::]:0")
			if err != nil {
				_ = relay.Close()
				failSetup(err)
				return
			}
			tlsConfig := &tls.Config{RootCAs: roots, ServerName: "localhost"}
			connection, err := quictransport.Dial(setupCtx, packet, relay.ClientAddr(), benchmarkTransportConfig(tlsConfig))
			if err != nil {
				_ = packet.Close()
				_ = relay.Close()
				failSetup(err)
				return
			}
			clients[index] = trialClient{connection: connection, packet: packet, relay: relay}
		}()
	}
	setupWorkers.Wait()
	if setupErr != nil {
		return result, setupErr
	}

	trialCtx, cancelTrial := context.WithTimeout(parent, cfg.Warmup+cfg.Duration+10*time.Second)
	defer cancelTrial()

	startWorkers := func(measured bool, start <-chan struct{}, ready chan<- error) (context.CancelFunc, *sync.WaitGroup) {
		workCtx, stopWork := context.WithCancel(trialCtx)
		workers := &sync.WaitGroup{}
		for index, client := range clients {
			workers.Add(2)
			go func(connection transport.Conn, correlator *localOverheadCorrelator) {
				defer workers.Done()
				receiveBareUpdates(workCtx, connection, cfg, counters, clock, correlator)
			}(client.connection, updateCorrelators[index])
			go func(connection transport.Conn, clientIndex int, correlator *localOverheadCorrelator) {
				defer workers.Done()
				runBareQUICClient(workCtx, cfg, connection, counters, clock, clientIndex, correlator, measured, start, ready)
			}(client.connection, index, inputCorrelators[index])
		}
		return stopWork, workers
	}
	warmupStop, warmupWorkers := startWorkers(false, nil, nil)
	if !waitTrial(trialCtx, cfg.Warmup) {
		warmupStop()
		warmupWorkers.Wait()
		return result, context.Cause(trialCtx)
	}
	warmupStop()
	warmupWorkers.Wait()
	counters.reset()
	for _, correlator := range inputCorrelators {
		correlator.Reset()
	}
	for _, correlator := range updateCorrelators {
		correlator.Reset()
	}
	measurementStart := make(chan struct{})
	measurementReady := make(chan error, cfg.Clients)
	measurementStop, measurementWorkers := startWorkers(true, measurementStart, measurementReady)
	if err := waitForWorkloadSetup(trialCtx, measurementReady, cfg.Clients); err != nil {
		measurementStop()
		measurementWorkers.Wait()
		return result, err
	}
	started := time.Now()
	runtimeStarted := runtimeSnapshot()
	counters.startMeasurement()
	close(measurementStart)
	if !waitTrial(trialCtx, cfg.Duration) {
		counters.stopMeasurement()
		measurementStop()
		measurementWorkers.Wait()
		return result, context.Cause(trialCtx)
	}
	counters.stopMeasurement()
	measurementStop()
	for _, client := range clients {
		_ = client.connection.Close(uint64(sgsp.Canceled), "benchmark complete")
	}
	measurementWorkers.Wait()
	result = counters.snapshot()
	applyCorrelatedOverhead(&result, counters, inputCorrelators, updateCorrelators)
	var network networkAccumulator
	for _, client := range clients {
		network.addTransport(client.connection.Stats())
	}
	result.Network = network.Snapshot()
	// The raw baseline deliberately has no SGSP endpoint, scheduler, or
	// dispatch queue. Report those library-only metrics as unavailable rather
	// than fabricating a zero-valued queue measurement.
	result.Queues = queueMeasurement{AgeBuckets: map[string]uint64{}}
	result.Duration = time.Since(started)
	result.Runtime = runtimeDelta(runtimeStarted, runtimeSnapshot())
	result.Runtime.normalizeByOfferedOperations(result.offeredOperations())
	for _, client := range clients {
		stats := client.relay.Stats()
		result.Relay.Forwarded += stats.Forwarded
		result.Relay.Dropped += stats.Dropped
		result.Relay.PendingPackets += stats.PendingPackets
		result.Relay.PendingBytes += stats.PendingBytes
		result.Relay.PeakPendingPackets += stats.PeakPendingPackets
		result.Relay.PeakPendingBytes += stats.PeakPendingBytes
		result.Relay.Overloaded = result.Relay.Overloaded || stats.Overloaded
		if err := client.relay.Err(); err != nil {
			return result, err
		}
	}
	return result, nil
}

func benchmarkTransportConfig(tlsConfig *tls.Config) quictransport.Config {
	limits := sgsp.DefaultLimits()
	return quictransport.Config{
		TLS: tlsConfig, HandshakeTimeout: limits.AuthTimeout, IdleTimeout: limits.IdleTimeout, KeepAlive: limits.KeepAlive,
		StreamReceiveWindow: limits.StreamReceiveWindow, ConnectionReceiveWindow: limits.ConnectionReceiveWindow,
		MaxIncomingBidi: int64(limits.Requests + limits.Streams + 1), MaxIncomingUni: int64(limits.ReliableChannels), EnableDatagrams: true,
	}
}

func serveBareQUICConnection(connection transport.Conn, cfg config, counters *trialCounters, clock benchmarkClock, inputCorrelators, updateCorrelators []*localOverheadCorrelator) {
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		serveBareDatagrams(connection, cfg, counters, clock, inputCorrelators, updateCorrelators)
	}()
	for {
		stream, err := connection.AcceptBidi(connection.Context())
		if err != nil {
			workers.Wait()
			return
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			serveBareStream(stream, cfg, counters)
		}()
	}
}

func serveBareDatagrams(connection transport.Conn, cfg config, counters *trialCounters, clock benchmarkClock, inputCorrelators, updateCorrelators []*localOverheadCorrelator) {
	var sequence uint64
	for {
		datagram, err := connection.ReceiveDatagram(connection.Context())
		if err != nil {
			return
		}
		event, err := wire.DecodeDatagram(datagram.Payload, cfg.InputBytes+32)
		decodedAt := time.Now()
		if err != nil || event.Kind != wire.SequencedKind || event.Channel != 1 || event.MessageType != uint64(benchmarkInputType) {
			continue
		}
		tick, inputSequence, stamp, payloadOK := benchmarkPayloadFields(event.Payload)
		measuring := counters.recording() && payloadOK && benchmarkPayloadIsMeasured(tick)
		clientIndex, clientKnown := benchmarkPayloadClientIndex(event.Payload)
		if measuring {
			counters.inputDelivered.Add(1)
			counters.updateOffered.Add(1)
			inputReceive := time.Since(decodedAt)
			counters.inputReceiveOverhead.Record(inputReceive)
			if clientKnown && clientIndex < len(inputCorrelators) {
				inputCorrelators[clientIndex].RecordReceive(inputSequence, inputReceive)
			}
		}
		sequence++
		update, err := wire.EncodeDatagram(wire.Event{Kind: wire.SequencedKind, Channel: 2, MessageType: uint64(benchmarkUpdateType), Sequence: sequence, Payload: benchmarkPayloadForClientPhase(cfg.UpdateBytes, tick, inputSequence, stamp, clientIndex, benchmarkPayloadIsMeasured(tick))}, cfg.UpdateBytes+32)
		started := time.Now()
		if err == nil {
			err = connection.SendDatagram(connection.Context(), update)
		}
		if measuring && (err == nil || connection.Context().Err() == nil) {
			counters.updateSendOverhead.Record(time.Since(started))
		}
		if err != nil {
			if measuring {
				counters.updateFailed.Add(1)
			}
			continue
		}
		if measuring {
			counters.updateAccepted.Add(1)
			if clientKnown && clientIndex < len(updateCorrelators) {
				updateCorrelators[clientIndex].RecordSend(inputSequence, time.Since(started))
			}
		}
	}
}

func serveBareStream(stream transport.BidiStream, cfg config, counters *trialCounters) {
	defer stream.Close()
	reader := bufio.NewReader(stream)
	kind, err := wire.ReadVarint(reader)
	if err != nil {
		return
	}
	switch kind {
	case wire.RequestStreamKind:
		prefix, _ := wire.AppendVarint(nil, kind)
		body, err := io.ReadAll(io.LimitReader(reader, int64(benchmarkRPCBytes+64)))
		if err != nil {
			return
		}
		request, err := wire.DecodeRequest(append(prefix, body...), benchmarkRPCBytes)
		if err != nil || request.MessageType != uint64(benchmarkRequestType) {
			return
		}
		measured := benchmarkRequestPayloadIsMeasured(request.Payload)
		if counters.recording() && measured {
			counters.requestDelivered.Add(1)
		}
		response, err := wire.EncodeResponse(wire.Response{Status: 0, Payload: make([]byte, benchmarkRPCBytes)}, benchmarkRPCBytes)
		if err != nil {
			return
		}
		if _, err := stream.Write(response); err != nil {
			if counters.recording() && measured {
				counters.requestFailed.Add(1)
			}
			return
		}
		_ = stream.CloseWrite()
	case wire.CustomStreamKind:
		messageType, err := wire.ReadVarint(reader)
		if err != nil || (messageType != uint64(benchmarkStreamType) && messageType != uint64(benchmarkMeasuredStreamType)) {
			return
		}
		acceptance, _ := wire.AppendVarint(nil, 0)
		if _, err := stream.Write(acceptance); err != nil {
			return
		}
		copyBenchmarkBulk(reader, counters, messageType == uint64(benchmarkMeasuredStreamType))
	}
}

func receiveBareUpdates(ctx context.Context, connection transport.Conn, cfg config, counters *trialCounters, clock benchmarkClock, correlator *localOverheadCorrelator) {
	for {
		datagram, err := connection.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		event, err := wire.DecodeDatagram(datagram.Payload, cfg.UpdateBytes+32)
		decodedAt := time.Now()
		if err == nil && event.Kind == wire.SequencedKind && event.Channel == 2 && event.MessageType == uint64(benchmarkUpdateType) {
			polledAt := time.Now()
			if counters.recording() && benchmarkPayloadIsMeasuredFromPayload(event.Payload) {
				receiveOverhead := polledAt.Sub(decodedAt)
				counters.updateDelivered.Add(1)
				counters.updateReceiveOverhead.Record(receiveOverhead)
				if _, sequence, stamp, ok := benchmarkPayloadFields(event.Payload); ok {
					correlator.RecordReceive(sequence, receiveOverhead)
					if age, ok := clock.Age(stamp); ok {
						counters.updateAge.Record(age)
					}
				}
			}
		}
	}
}

func runBareQUICClient(ctx context.Context, cfg config, connection transport.Conn, counters *trialCounters, clock benchmarkClock, clientIndex int, correlator *localOverheadCorrelator, measured bool, start <-chan struct{}, ready chan<- error) {
	var stream transport.BidiStream
	var stopStream func()
	var setupErr error
	if cfg.BulkBytes > 0 {
		opened, err := openBareBulkStream(ctx, connection, measured)
		if err != nil {
			setupErr = err
		} else {
			stream, stopStream = opened, abortBidiOnContext(ctx, opened)
		}
	}
	defer func() {
		if stream != nil {
			stopStream()
			stream.Abort(uint64(sgsp.Canceled))
			_ = stream.Close()
		}
	}()
	if ready != nil {
		select {
		case ready <- setupErr:
		case <-ctx.Done():
			return
		}
	}
	if setupErr != nil && measured {
		return
	}
	if start != nil {
		select {
		case <-start:
		case <-ctx.Done():
			return
		}
	}
	if !waitTrial(ctx, workloadStartPhase(cfg, clientIndex)) {
		return
	}
	inputTicker := time.NewTicker(time.Second / time.Duration(cfg.Hz))
	defer inputTicker.Stop()
	inputSchedule := scheduledTicker{interval: time.Second / time.Duration(cfg.Hz)}
	var rpcTicker *time.Ticker
	var rpc <-chan time.Time
	if cfg.RPCPerSecond > 0 {
		rpcTicker = time.NewTicker(time.Second / time.Duration(cfg.RPCPerSecond))
		defer rpcTicker.Stop()
		rpc = rpcTicker.C
	}
	rpcSchedule := scheduledTicker{interval: time.Second / time.Duration(max(cfg.RPCPerSecond, 1))}
	var bulkTicker *time.Ticker
	var bulk <-chan time.Time
	if stream != nil {
		bulkTicker = time.NewTicker(10 * time.Millisecond)
		defer bulkTicker.Stop()
		bulk = bulkTicker.C
	}
	bulkSchedule := scheduledTicker{interval: 10 * time.Millisecond}
	var tick, sequence uint64
	pacer := bulkPacer{perSecond: cfg.BulkBytes}
	for {
		select {
		case <-ctx.Done():
			return
		case tickAt := <-inputTicker.C:
			tick++
			sequence++
			measuring := measured && counters.recording()
			if measuring {
				counters.recordInputTick(inputSchedule.Observe(tickAt))
				counters.inputOffered.Add(1)
			}
			payload, err := wire.EncodeDatagram(wire.Event{Kind: wire.SequencedKind, Channel: 1, MessageType: uint64(benchmarkInputType), Sequence: sequence, Payload: benchmarkPayloadForClientPhase(cfg.InputBytes, tick, sequence, clock.Stamp(), clientIndex, measured)}, cfg.InputBytes+32)
			if err != nil {
				if measuring {
					counters.inputFailed.Add(1)
				}
				continue
			}
			started := time.Now()
			if err := connection.SendDatagram(ctx, payload); err != nil {
				if measuring && ctx.Err() == nil {
					counters.inputSendOverhead.Record(time.Since(started))
				}
				if measuring && countWorkloadFailure(ctx, err) {
					counters.inputFailed.Add(1)
				}
			} else if measuring {
				counters.inputSendOverhead.Record(time.Since(started))
				counters.inputAccepted.Add(1)
				correlator.RecordSend(sequence, time.Since(started))
			}
		case tickAt := <-rpc:
			measuring := measured && counters.recording()
			if measuring {
				counters.recordRequestTick(rpcSchedule.Observe(tickAt))
				counters.requestOffered.Add(1)
			}
			started := time.Now()
			unknown, err := callBareQUIC(ctx, connection, measured)
			if err != nil {
				if measuring && ctx.Err() == nil {
					if unknown {
						counters.requestUnknown.Add(1)
					} else {
						counters.requestFailed.Add(1)
					}
				}
			} else if measuring {
				counters.requestRoundTrip.Record(time.Since(started))
				counters.requestAccepted.Add(1)
			}
		case tickAt := <-bulk:
			payload := make([]byte, pacer.Next())
			if len(payload) == 0 {
				continue
			}
			measuring := measured && counters.recording()
			if measuring {
				counters.recordBulkTick(bulkSchedule.Observe(tickAt))
				counters.bulkOffered.Add(uint64(len(payload)))
			}
			written, err := stream.Write(payload)
			if measuring && written > 0 {
				counters.bulkAccepted.Add(uint64(written))
			}
			if err != nil || written != len(payload) {
				if measuring && ctx.Err() == nil {
					counters.bulkFailed.Add(1)
				}
				return
			}
		}
	}
}

func openBareBulkStream(ctx context.Context, connection transport.Conn, measured bool) (transport.BidiStream, error) {
	stream, err := connection.OpenBidi(ctx)
	if err != nil {
		return nil, err
	}
	stop := abortBidiOnContext(ctx, stream)
	defer stop()
	if err := stream.SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
		_ = stream.Close()
		return nil, err
	}
	streamType := benchmarkStreamType
	if measured {
		streamType = benchmarkMeasuredStreamType
	}
	header, err := wire.EncodeCustomStreamHeader(uint64(streamType))
	if err != nil {
		_ = stream.Close()
		return nil, err
	}
	if _, err := stream.Write(header); err != nil {
		_ = stream.Close()
		return nil, err
	}
	status, err := wire.ReadVarint(bufio.NewReader(stream))
	if err != nil || status != 0 {
		_ = stream.Close()
		if err == nil {
			err = errors.New("bare-QUIC stream rejected")
		}
		return nil, err
	}
	if err := stream.SetDeadline(time.Time{}); err != nil {
		_ = stream.Close()
		return nil, err
	}
	return stream, nil
}

// callBareQUIC returns outcomeUnknown when a failure occurs after any request
// bytes were handed to QUIC but before a complete response was received. This
// is the same request-outcome boundary used by SGSP's Call API, expressed for
// the raw transport baseline without adding an acknowledgement protocol.
func callBareQUIC(ctx context.Context, connection transport.Conn, measured bool) (outcomeUnknown bool, err error) {
	stream, err := connection.OpenBidi(ctx)
	if err != nil {
		return false, err
	}
	stop := abortBidiOnContext(ctx, stream)
	defer stop()
	defer stream.Close()
	request, err := wire.EncodeRequest(wire.Request{MessageType: uint64(benchmarkRequestType), TimeoutMS: 5_000, Payload: benchmarkRequestPayload(measured)}, benchmarkRPCBytes)
	if err != nil {
		return false, err
	}
	written, err := stream.Write(request)
	if err != nil {
		return bareRequestOutcomeUnknown(written, false), err
	}
	if written != len(request) {
		return bareRequestOutcomeUnknown(written, false), io.ErrShortWrite
	}
	if err := stream.CloseWrite(); err != nil {
		return true, err
	}
	body, err := io.ReadAll(io.LimitReader(stream, int64(benchmarkRPCBytes+64)))
	if err != nil {
		return true, err
	}
	response, err := wire.DecodeResponse(body, benchmarkRPCBytes)
	if err != nil || response.Status != 0 || len(response.Payload) != benchmarkRPCBytes {
		if err == nil {
			err = errors.New("invalid bare-QUIC request response")
		}
		// A full response establishes a known remote result even if it is
		// malformed or unsuccessful, so this is not outcome-unknown.
		return false, err
	}
	return false, nil
}

func abortBidiOnContext(ctx context.Context, stream transport.BidiStream) func() {
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			stream.Abort(uint64(sgsp.Canceled))
		case <-done:
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { close(done) }) }
}

func benchmarkServerRouter(cfg config, counters *trialCounters, inputCorrelators, updateCorrelators []*localOverheadCorrelator) *sgsp.Router {
	router := sgsp.NewRouter()
	_ = router.OnEvent(benchmarkInputType, func(ctx context.Context, incoming *sgsp.Incoming) {
		tick, sequence, stamp, payloadOK := benchmarkPayloadFields(incoming.Payload)
		measured := payloadOK && benchmarkPayloadIsMeasured(tick)
		measuring := counters.recording() && measured
		clientIndex, clientKnown := benchmarkPayloadClientIndex(incoming.Payload)
		if measuring {
			counters.inputDelivered.Add(1)
			counters.updateOffered.Add(1)
			if decodedAt := incoming.DecodedAt(); !decodedAt.IsZero() {
				inputReceive := time.Since(decodedAt)
				counters.inputReceiveOverhead.Record(inputReceive)
				if clientKnown && clientIndex < len(inputCorrelators) {
					inputCorrelators[clientIndex].RecordReceive(sequence, inputReceive)
				}
			}
		}
		update := benchmarkPayloadForClientPhase(cfg.UpdateBytes, tick, sequence, stamp, clientIndex, measured)
		started := time.Now()
		err := incoming.Session.Send(ctx, benchmarkUpdateType, update, sgsp.SendOptions{Channel: 2, Delivery: sgsp.UnreliableSequenced})
		if measuring && (err == nil || ctx.Err() == nil) {
			counters.updateSendOverhead.Record(time.Since(started))
		}
		if err != nil {
			if measuring {
				counters.updateFailed.Add(1)
			}
			return
		}
		if measuring {
			counters.updateAccepted.Add(1)
			if clientKnown && clientIndex < len(updateCorrelators) {
				updateCorrelators[clientIndex].RecordSend(sequence, time.Since(started))
			}
		}
	})
	_ = router.OnRequest(benchmarkRequestType, func(ctx context.Context, incoming *sgsp.Incoming) {
		measured := benchmarkRequestPayloadIsMeasured(incoming.Payload)
		if counters.recording() && measured {
			counters.requestDelivered.Add(1)
		}
		if err := incoming.Reply(ctx, make([]byte, benchmarkRPCBytes)); err != nil && counters.recording() && measured {
			counters.requestFailed.Add(1)
		}
	})
	_ = router.OnStream(benchmarkStreamType, func(_ context.Context, incoming *sgsp.Incoming) {
		if incoming.Stream == nil {
			return
		}
		copyBenchmarkBulk(incoming.Stream, counters, false)
	})
	_ = router.OnStream(benchmarkMeasuredStreamType, func(_ context.Context, incoming *sgsp.Incoming) {
		if incoming.Stream == nil {
			return
		}
		copyBenchmarkBulk(incoming.Stream, counters, true)
	})
	return router
}

func copyBenchmarkBulk(stream io.Reader, counters *trialCounters, measured bool) {
	buffer := make([]byte, 32<<10)
	for {
		count, err := stream.Read(buffer)
		if count > 0 && measured && counters.recording() {
			counters.bulkDelivered.Add(uint64(count))
		}
		if err != nil {
			if measured && counters.recording() && !errors.Is(err, io.EOF) {
				counters.bulkFailed.Add(1)
			}
			return
		}
	}
}

func receiveSGSPUpdates(ctx context.Context, client sgsp.Client, counters *trialCounters, clock benchmarkClock, correlator *localOverheadCorrelator) {
	for {
		incoming, err := client.Next(ctx)
		if err != nil {
			return
		}
		polledAt := time.Now()
		if incoming.Kind == sgsp.Event && incoming.Type == benchmarkUpdateType && incoming.Channel == 2 && incoming.Delivery == sgsp.UnreliableSequenced && counters.recording() && benchmarkPayloadIsMeasuredFromPayload(incoming.Payload) {
			if decodedAt := incoming.DecodedAt(); !decodedAt.IsZero() && !polledAt.Before(decodedAt) {
				receiveOverhead := polledAt.Sub(decodedAt)
				counters.updateReceiveOverhead.Record(receiveOverhead)
				if _, sequence, stamp, ok := benchmarkPayloadFields(incoming.Payload); ok {
					correlator.RecordReceive(sequence, receiveOverhead)
					if age, ok := clock.Age(stamp); ok {
						counters.updateAge.Record(age)
					}
				}
			}
			counters.updateDelivered.Add(1)
		}
		incoming.Release()
	}
}

func runSGSPClient(ctx context.Context, cfg config, session sgsp.Session, counters *trialCounters, clock benchmarkClock, clientIndex int, correlator *localOverheadCorrelator, measured bool, start <-chan struct{}, ready chan<- error) {
	var stream sgsp.Stream
	var streamStop chan struct{}
	var setupErr error
	if cfg.BulkBytes > 0 {
		streamType := benchmarkStreamType
		if measured {
			streamType = benchmarkMeasuredStreamType
		}
		opened, err := session.OpenStream(ctx, streamType)
		if err != nil {
			setupErr = err
		} else {
			stream = opened
			streamStop = make(chan struct{})
			go func() {
				select {
				case <-ctx.Done():
					// A raw-stream Write may be flow-control blocked and cannot
					// observe this workload context itself. Abort is concurrency
					// safe and makes the generator's shutdown bounded.
					stream.Abort(sgsp.Canceled)
				case <-streamStop:
				}
			}()
		}
	}
	defer func() {
		if stream != nil {
			close(streamStop)
			stream.Abort(sgsp.Canceled)
		}
	}()
	if ready != nil {
		select {
		case ready <- setupErr:
		case <-ctx.Done():
			return
		}
	}
	if setupErr != nil && measured {
		return
	}
	if start != nil {
		select {
		case <-start:
		case <-ctx.Done():
			return
		}
	}
	if !waitTrial(ctx, workloadStartPhase(cfg, clientIndex)) {
		return
	}
	inputTicker := time.NewTicker(time.Second / time.Duration(cfg.Hz))
	defer inputTicker.Stop()
	inputSchedule := scheduledTicker{interval: time.Second / time.Duration(cfg.Hz)}
	var rpcTicker *time.Ticker
	var rpc <-chan time.Time
	if cfg.RPCPerSecond > 0 {
		rpcTicker = time.NewTicker(time.Second / time.Duration(cfg.RPCPerSecond))
		defer rpcTicker.Stop()
		rpc = rpcTicker.C
	}
	rpcSchedule := scheduledTicker{interval: time.Second / time.Duration(max(cfg.RPCPerSecond, 1))}
	var bulkTicker *time.Ticker
	var bulk <-chan time.Time
	if stream != nil {
		bulkTicker = time.NewTicker(10 * time.Millisecond)
		defer bulkTicker.Stop()
		bulk = bulkTicker.C
	}
	bulkSchedule := scheduledTicker{interval: 10 * time.Millisecond}
	var tick, sequence uint64
	pacer := bulkPacer{perSecond: cfg.BulkBytes}
	for {
		select {
		case <-ctx.Done():
			return
		case tickAt := <-inputTicker.C:
			tick++
			sequence++
			measuring := measured && counters.recording()
			if measuring {
				counters.recordInputTick(inputSchedule.Observe(tickAt))
				counters.inputOffered.Add(1)
			}
			payload := benchmarkPayloadForClientPhase(cfg.InputBytes, tick, sequence, clock.Stamp(), clientIndex, measured)
			started := time.Now()
			if err := session.Send(ctx, benchmarkInputType, payload, sgsp.SendOptions{Channel: 1, Delivery: sgsp.UnreliableSequenced}); err != nil {
				if measuring && ctx.Err() == nil {
					counters.inputSendOverhead.Record(time.Since(started))
				}
				if measuring && countWorkloadFailure(ctx, err) {
					counters.inputFailed.Add(1)
				}
			} else if measuring {
				counters.inputSendOverhead.Record(time.Since(started))
				counters.inputAccepted.Add(1)
				correlator.RecordSend(sequence, time.Since(started))
			}
		case tickAt := <-rpc:
			measuring := measured && counters.recording()
			if measuring {
				counters.recordRequestTick(rpcSchedule.Observe(tickAt))
				counters.requestOffered.Add(1)
			}
			started := time.Now()
			response, err := session.Call(ctx, benchmarkRequestType, benchmarkRequestPayload(measured))
			if err != nil || len(response) != benchmarkRPCBytes {
				// The measurement cutoff can cancel a request after it has been
				// offered but before its reply arrives. Keep that as an unmatched
				// offered operation, not an application/transport failure.
				if measuring && ctx.Err() == nil {
					if requestOutcomeUnknown(err) {
						counters.requestUnknown.Add(1)
					} else {
						counters.requestFailed.Add(1)
					}
				}
			} else if measuring {
				counters.requestRoundTrip.Record(time.Since(started))
				counters.requestAccepted.Add(1)
			}
		case tickAt := <-bulk:
			payload := make([]byte, pacer.Next())
			if len(payload) == 0 {
				continue
			}
			measuring := measured && counters.recording()
			if measuring {
				counters.recordBulkTick(bulkSchedule.Observe(tickAt))
				counters.bulkOffered.Add(uint64(len(payload)))
			}
			written, err := stream.Write(payload)
			if measuring && written > 0 {
				counters.bulkAccepted.Add(uint64(written))
			}
			if err != nil || written != len(payload) {
				if measuring && ctx.Err() == nil {
					counters.bulkFailed.Add(1)
				}
				return
			}
		}
	}
}

func waitTrial(ctx context.Context, duration time.Duration) bool {
	if duration <= 0 {
		return true
	}
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

func waitForWorkloadSetup(ctx context.Context, ready <-chan error, clients int) error {
	for range clients {
		select {
		case err := <-ready:
			if err != nil {
				return fmt.Errorf("prepare measured workload: %w", err)
			}
		case <-ctx.Done():
			return context.Cause(ctx)
		}
	}
	return nil
}

func benchmarkPayload(size int, tick, sequence, stamp uint64) []byte {
	return benchmarkPayloadForClient(size, tick, sequence, stamp, 0)
}

func benchmarkPayloadForClient(size int, tick, sequence, stamp uint64, clientIndex int) []byte {
	return benchmarkPayloadForClientPhase(size, tick, sequence, stamp, clientIndex, false)
}

// benchmarkPayloadForClientPhase reserves the high bit of tick as a local
// benchmark phase marker. The configured rates cannot approach that value,
// and retaining the marker in server replies lets either endpoint discard a
// late warmup datagram after measurement has started.
func benchmarkPayloadForClientPhase(size int, tick, sequence, stamp uint64, clientIndex int, measured bool) []byte {
	if measured {
		tick |= benchmarkMeasuredTick
	} else {
		tick &^= benchmarkMeasuredTick
	}
	payload := make([]byte, size)
	if len(payload) >= 8 {
		binary.BigEndian.PutUint64(payload[:8], tick)
	}
	if len(payload) >= 16 {
		binary.BigEndian.PutUint64(payload[8:16], sequence)
	}
	if len(payload) >= 24 {
		binary.BigEndian.PutUint64(payload[16:24], stamp)
	}
	if len(payload) >= 32 && clientIndex >= 0 {
		binary.BigEndian.PutUint64(payload[24:32], uint64(clientIndex))
	}
	return payload
}

func benchmarkPayloadIsMeasured(tick uint64) bool {
	return tick&benchmarkMeasuredTick != 0
}

func benchmarkPayloadIsMeasuredFromPayload(payload []byte) bool {
	tick, _, _, ok := benchmarkPayloadFields(payload)
	return ok && benchmarkPayloadIsMeasured(tick)
}

func benchmarkRequestPayload(measured bool) []byte {
	payload := make([]byte, benchmarkRPCBytes)
	if measured {
		payload[0] = 1
	}
	return payload
}

func benchmarkRequestPayloadIsMeasured(payload []byte) bool {
	return len(payload) > 0 && payload[0] == 1
}

func benchmarkPayloadFields(payload []byte) (tick, sequence, stamp uint64, ok bool) {
	if len(payload) < 24 {
		return 0, 0, 0, false
	}
	return binary.BigEndian.Uint64(payload[:8]), binary.BigEndian.Uint64(payload[8:16]), binary.BigEndian.Uint64(payload[16:24]), true
}

func benchmarkPayloadClientIndex(payload []byte) (int, bool) {
	if len(payload) < 32 {
		return 0, false
	}
	value := binary.BigEndian.Uint64(payload[24:32])
	if value > uint64(^uint(0)>>1) {
		return 0, false
	}
	return int(value), true
}

func benchmarkCertificate() (tls.Certificate, *x509.CertPool, error) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"}, DNSNames: []string{"localhost"},
		NotBefore: time.Now().Add(-time.Minute), NotAfter: time.Now().Add(time.Hour),
		KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, public, private)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	roots := x509.NewCertPool()
	roots.AddCert(parsed)
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, roots, nil
}

func runtimeSnapshot() runtimeMeasurement {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	return runtimeMeasurement{GoMaxProcs: runtime.GOMAXPROCS(0), Goroutines: runtime.NumGoroutine(), HeapAlloc: memory.HeapAlloc, RSSBytes: processRSSBytes(), CPUSeconds: processCPUSeconds(), TotalAlloc: memory.TotalAlloc, Mallocs: memory.Mallocs, NumCPU: runtime.NumCPU(), OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH}
}

// runtimeDelta preserves the final live-memory snapshot but turns cumulative
// allocator counters into the work performed during the measured interval.
func runtimeDelta(start, end runtimeMeasurement) runtimeMeasurement {
	if end.TotalAlloc >= start.TotalAlloc {
		end.TotalAlloc -= start.TotalAlloc
	} else {
		end.TotalAlloc = 0
	}
	if end.Mallocs >= start.Mallocs {
		end.Mallocs -= start.Mallocs
	} else {
		end.Mallocs = 0
	}
	if end.CPUSeconds >= start.CPUSeconds {
		end.CPUSeconds -= start.CPUSeconds
	} else {
		end.CPUSeconds = 0
	}
	return end
}

// offeredOperations is the explicit denominator for interval allocator rates.
// It includes every workload operation recorded at API offer time: inputs,
// updates, requests, and paced bulk writes. The total allocation numerator
// covers the complete same-process measurement interval, including orderly
// finalization, so these values are campaign comparison rates rather than a
// claim about an individual operation type's allocation internals.
func (m measurement) offeredOperations() uint64 {
	return m.Inputs.Offered + m.Updates.Offered + m.Requests.Offered + m.Bulk.Offered
}

func (r *runtimeMeasurement) normalizeByOfferedOperations(operations uint64) {
	if r == nil {
		return
	}
	r.OfferedOperations = operations
	r.AllocatedBytesPerOperation = 0
	r.AllocationsPerOperation = 0
	if operations == 0 {
		return
	}
	r.AllocatedBytesPerOperation = float64(r.TotalAlloc) / float64(operations)
	r.AllocationsPerOperation = float64(r.Mallocs) / float64(operations)
}

// processRSSBytes records Linux resident memory when it is available. A zero
// value is explicitly "unavailable" on platforms without procfs rather than
// a claim of zero resident memory.
func processRSSBytes() uint64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return pages * uint64(os.Getpagesize())
}
