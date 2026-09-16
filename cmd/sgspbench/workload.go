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
	"io"
	"math/big"
	"math/bits"
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
	benchmarkInputType   sgsp.MessageType = 1
	benchmarkUpdateType  sgsp.MessageType = 2
	benchmarkRequestType sgsp.MessageType = 3
	benchmarkStreamType  sgsp.MessageType = 4
	benchmarkRPCBytes                     = 128
)

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

type operationCounts struct {
	Offered   uint64 `json:"offered,omitempty"`
	Accepted  uint64 `json:"accepted,omitempty"`
	Delivered uint64 `json:"delivered,omitempty"`
	Failed    uint64 `json:"failed,omitempty"`
}

type relayMeasurement struct {
	Forwarded      uint64 `json:"forwarded"`
	Dropped        uint64 `json:"dropped"`
	PendingPackets uint64 `json:"pending_packets"`
	PendingBytes   int64  `json:"pending_bytes"`
	Overloaded     bool   `json:"overloaded"`
}

type runtimeMeasurement struct {
	GoMaxProcs      int     `json:"gomaxprocs"`
	Goroutines      int     `json:"goroutines"`
	HeapAlloc       uint64  `json:"heap_alloc"`
	RSSBytes        uint64  `json:"rss_bytes"`
	CPUSeconds      float64 `json:"cpu_seconds"`
	TotalAlloc      uint64  `json:"allocated_bytes"`
	Mallocs         uint64  `json:"allocations"`
	NumCPU          int     `json:"num_cpu"`
	OperatingSystem string  `json:"operating_system"`
	Architecture    string  `json:"architecture"`
}

// durationSummary reports upper bounds from a fixed binary histogram. Keeping
// the samples in fixed buckets makes long trials bounded independently of the
// offered workload while retaining enough resolution to evaluate a 1 ms gate.
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

const durationHistogramBuckets = 64

type durationHistogram struct {
	buckets [durationHistogramBuckets]atomic.Uint64
}

func (h *durationHistogram) Record(value time.Duration) {
	if h == nil || value < 0 {
		return
	}
	bucket := 0
	if value > 0 {
		bucket = bits.Len64(uint64(value))
	}
	if bucket >= len(h.buckets) {
		bucket = len(h.buckets) - 1
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
	var total uint64
	for index := range h.buckets {
		total += h.buckets[index].Load()
	}
	if total == 0 {
		return durationSummary{}
	}
	quantile := func(numerator, denominator uint64) time.Duration {
		target := (total*numerator + denominator - 1) / denominator
		var seen uint64
		for index := range h.buckets {
			seen += h.buckets[index].Load()
			if seen >= target {
				return durationBucketUpperBound(index)
			}
		}
		return durationBucketUpperBound(len(h.buckets) - 1)
	}
	max := time.Duration(0)
	for index := len(h.buckets) - 1; index >= 0; index-- {
		if h.buckets[index].Load() > 0 {
			max = durationBucketUpperBound(index)
			break
		}
	}
	return durationSummary{Samples: total, P50UpperBound: quantile(50, 100), P95UpperBound: quantile(95, 100), P99UpperBound: quantile(99, 100), MaxUpperBound: max}
}

func durationBucketUpperBound(bucket int) time.Duration {
	if bucket <= 0 {
		return 0
	}
	if bucket >= 63 {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(1<<bucket - 1)
}

type trialCounters struct {
	inputOffered, inputAccepted, inputDelivered, inputFailed         atomic.Uint64
	updateOffered, updateAccepted, updateDelivered, updateFailed     atomic.Uint64
	requestOffered, requestAccepted, requestDelivered, requestFailed atomic.Uint64
	bulkOffered, bulkAccepted, bulkDelivered, bulkFailed             atomic.Uint64
	inputSendOverhead, requestRoundTrip, updateAge                   durationHistogram
}

// countWorkloadFailure reports whether an operation failed while the measured
// workload was still active. An error caused by the measurement cutoff leaves
// an offered operation unmatched; it is not a transport/application failure.
func countWorkloadFailure(ctx context.Context, err error) bool {
	return err != nil && ctx.Err() == nil
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
	for _, counter := range []*atomic.Uint64{
		&c.inputOffered, &c.inputAccepted, &c.inputDelivered, &c.inputFailed,
		&c.updateOffered, &c.updateAccepted, &c.updateDelivered, &c.updateFailed,
		&c.requestOffered, &c.requestAccepted, &c.requestDelivered, &c.requestFailed,
		&c.bulkOffered, &c.bulkAccepted, &c.bulkDelivered, &c.bulkFailed,
	} {
		counter.Store(0)
	}
	c.inputSendOverhead.Reset()
	c.requestRoundTrip.Reset()
	c.updateAge.Reset()
}

func (c *trialCounters) snapshot() measurement {
	if c == nil {
		return measurement{}
	}
	counts := func(offered, accepted, delivered, failed *atomic.Uint64) operationCounts {
		return operationCounts{Offered: offered.Load(), Accepted: accepted.Load(), Delivered: delivered.Load(), Failed: failed.Load()}
	}
	return measurement{
		Inputs:   counts(&c.inputOffered, &c.inputAccepted, &c.inputDelivered, &c.inputFailed),
		Updates:  counts(&c.updateOffered, &c.updateAccepted, &c.updateDelivered, &c.updateFailed),
		Requests: counts(&c.requestOffered, &c.requestAccepted, &c.requestDelivered, &c.requestFailed),
		Bulk:     counts(&c.bulkOffered, &c.bulkAccepted, &c.bulkDelivered, &c.bulkFailed),
		Timing: timingMeasurement{
			InputSendOverhead: c.inputSendOverhead.Snapshot(),
			RequestRoundTrip:  c.requestRoundTrip.Snapshot(),
			UpdateAge:         c.updateAge.Snapshot(),
		},
	}
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
	serverRouter := benchmarkServerRouter(cfg, counters)
	server, err := sgsp.NewServer(sgsp.ServerConfig{
		TLS:      &tls.Config{Certificates: []tls.Certificate{certificate}},
		App:      sgsp.AppIdentity{ID: "sgspbench", Version: "1"},
		Owner:    sgsp.Owner{ID: "benchmark-owner", Endpoint: sgsp.Endpoint{Address: serverPacket.LocalAddr().String(), ServerName: "localhost"}},
		Auth:     benchmarkAuthenticator{},
		Limits:   limits,
		Dispatch: sgsp.DispatchConfig{Mode: sgsp.Handlers, Router: serverRouter},
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
				Dispatch: sgsp.DispatchConfig{Mode: sgsp.Handlers, Router: benchmarkClientRouter(counters, clock)},
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

	workCtx, stopWork := context.WithCancel(trialCtx)
	var workers sync.WaitGroup
	for _, client := range clients {
		workers.Add(1)
		go func(session sgsp.Session) {
			defer workers.Done()
			runSGSPClient(workCtx, cfg, session, counters, clock)
		}(client.client.Session())
	}
	if !waitTrial(workCtx, cfg.Warmup) {
		stopWork()
		workers.Wait()
		return result, context.Cause(workCtx)
	}
	counters.reset()
	started := time.Now()
	runtimeStarted := runtimeSnapshot()
	if !waitTrial(workCtx, cfg.Duration) {
		stopWork()
		workers.Wait()
		return result, context.Cause(workCtx)
	}
	stopWork()
	workers.Wait()
	result = counters.snapshot()
	result.Duration = time.Since(started)
	result.Runtime = runtimeDelta(runtimeStarted, runtimeSnapshot())
	for _, client := range clients {
		stats := client.relay.Stats()
		result.Relay.Forwarded += stats.Forwarded
		result.Relay.Dropped += stats.Dropped
		result.Relay.PendingPackets += stats.PendingPackets
		result.Relay.PendingBytes += stats.PendingBytes
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
				serveBareQUICConnection(connection, cfg, counters, clock)
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

	workCtx, stopWork := context.WithCancel(trialCtx)
	var workers sync.WaitGroup
	for _, client := range clients {
		workers.Add(2)
		go func(connection transport.Conn) {
			defer workers.Done()
			receiveBareUpdates(workCtx, connection, cfg, counters, clock)
		}(client.connection)
		go func(connection transport.Conn) {
			defer workers.Done()
			runBareQUICClient(workCtx, cfg, connection, counters, clock)
		}(client.connection)
	}
	if !waitTrial(workCtx, cfg.Warmup) {
		stopWork()
		workers.Wait()
		return result, context.Cause(workCtx)
	}
	counters.reset()
	started := time.Now()
	runtimeStarted := runtimeSnapshot()
	if !waitTrial(workCtx, cfg.Duration) {
		stopWork()
		workers.Wait()
		return result, context.Cause(workCtx)
	}
	stopWork()
	for _, client := range clients {
		_ = client.connection.Close(uint64(sgsp.Canceled), "benchmark complete")
	}
	workers.Wait()
	result = counters.snapshot()
	result.Duration = time.Since(started)
	result.Runtime = runtimeDelta(runtimeStarted, runtimeSnapshot())
	for _, client := range clients {
		stats := client.relay.Stats()
		result.Relay.Forwarded += stats.Forwarded
		result.Relay.Dropped += stats.Dropped
		result.Relay.PendingPackets += stats.PendingPackets
		result.Relay.PendingBytes += stats.PendingBytes
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

func serveBareQUICConnection(connection transport.Conn, cfg config, counters *trialCounters, clock benchmarkClock) {
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		serveBareDatagrams(connection, cfg, counters, clock)
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

func serveBareDatagrams(connection transport.Conn, cfg config, counters *trialCounters, clock benchmarkClock) {
	var sequence uint64
	for {
		datagram, err := connection.ReceiveDatagram(connection.Context())
		if err != nil {
			return
		}
		event, err := wire.DecodeDatagram(datagram.Payload, cfg.InputBytes+32)
		if err != nil || event.Kind != wire.SequencedKind || event.Channel != 1 || event.MessageType != uint64(benchmarkInputType) {
			continue
		}
		if counters != nil {
			counters.inputDelivered.Add(1)
			counters.updateOffered.Add(1)
		}
		sequence++
		tick, inputSequence, stamp, _ := benchmarkPayloadFields(event.Payload)
		update, err := wire.EncodeDatagram(wire.Event{Kind: wire.SequencedKind, Channel: 2, MessageType: uint64(benchmarkUpdateType), Sequence: sequence, Payload: benchmarkPayload(cfg.UpdateBytes, tick, inputSequence, stamp)}, cfg.UpdateBytes+32)
		if err != nil || connection.SendDatagram(connection.Context(), update) != nil {
			if counters != nil {
				counters.updateFailed.Add(1)
			}
			continue
		}
		if counters != nil {
			counters.updateAccepted.Add(1)
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
		if counters != nil {
			counters.requestDelivered.Add(1)
		}
		response, err := wire.EncodeResponse(wire.Response{Status: 0, Payload: make([]byte, benchmarkRPCBytes)}, benchmarkRPCBytes)
		if err != nil {
			return
		}
		if _, err := stream.Write(response); err != nil {
			if counters != nil {
				counters.requestFailed.Add(1)
			}
			return
		}
		_ = stream.CloseWrite()
	case wire.CustomStreamKind:
		messageType, err := wire.ReadVarint(reader)
		if err != nil || messageType != uint64(benchmarkStreamType) {
			return
		}
		acceptance, _ := wire.AppendVarint(nil, 0)
		if _, err := stream.Write(acceptance); err != nil {
			return
		}
		copyBenchmarkBulk(reader, counters)
	}
}

func receiveBareUpdates(ctx context.Context, connection transport.Conn, cfg config, counters *trialCounters, clock benchmarkClock) {
	for {
		datagram, err := connection.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		event, err := wire.DecodeDatagram(datagram.Payload, cfg.UpdateBytes+32)
		if err == nil && event.Kind == wire.SequencedKind && event.Channel == 2 && event.MessageType == uint64(benchmarkUpdateType) {
			counters.updateDelivered.Add(1)
			if _, _, stamp, ok := benchmarkPayloadFields(event.Payload); ok {
				if age, ok := clock.Age(stamp); ok {
					counters.updateAge.Record(age)
				}
			}
		}
	}
}

func runBareQUICClient(ctx context.Context, cfg config, connection transport.Conn, counters *trialCounters, clock benchmarkClock) {
	inputTicker := time.NewTicker(time.Second / time.Duration(cfg.Hz))
	defer inputTicker.Stop()
	var rpcTicker *time.Ticker
	var rpc <-chan time.Time
	if cfg.RPCPerSecond > 0 {
		rpcTicker = time.NewTicker(time.Second / time.Duration(cfg.RPCPerSecond))
		defer rpcTicker.Stop()
		rpc = rpcTicker.C
	}
	var bulkTicker *time.Ticker
	var bulk <-chan time.Time
	var stream transport.BidiStream
	var stopStream func()
	if cfg.BulkBytes > 0 {
		opened, err := openBareBulkStream(ctx, connection)
		if err != nil {
			counters.bulkFailed.Add(1)
		} else {
			stream, stopStream = opened, abortBidiOnContext(ctx, opened)
			bulkTicker = time.NewTicker(10 * time.Millisecond)
			defer bulkTicker.Stop()
			bulk = bulkTicker.C
		}
	}
	defer func() {
		if stream != nil {
			stopStream()
			stream.Abort(uint64(sgsp.Canceled))
			_ = stream.Close()
		}
	}()
	var tick, sequence uint64
	pacer := bulkPacer{perSecond: cfg.BulkBytes}
	for {
		select {
		case <-ctx.Done():
			return
		case <-inputTicker.C:
			tick++
			sequence++
			counters.inputOffered.Add(1)
			payload, err := wire.EncodeDatagram(wire.Event{Kind: wire.SequencedKind, Channel: 1, MessageType: uint64(benchmarkInputType), Sequence: sequence, Payload: benchmarkPayload(cfg.InputBytes, tick, sequence, clock.Stamp())}, cfg.InputBytes+32)
			if err != nil {
				counters.inputFailed.Add(1)
				continue
			}
			started := time.Now()
			if err := connection.SendDatagram(ctx, payload); err != nil {
				if ctx.Err() == nil {
					counters.inputSendOverhead.Record(time.Since(started))
				}
				if countWorkloadFailure(ctx, err) {
					counters.inputFailed.Add(1)
				}
			} else {
				counters.inputSendOverhead.Record(time.Since(started))
				counters.inputAccepted.Add(1)
			}
		case <-rpc:
			counters.requestOffered.Add(1)
			started := time.Now()
			if err := callBareQUIC(ctx, connection); err != nil {
				if ctx.Err() == nil {
					counters.requestFailed.Add(1)
				}
			} else {
				counters.requestRoundTrip.Record(time.Since(started))
				counters.requestAccepted.Add(1)
			}
		case <-bulk:
			payload := make([]byte, pacer.Next())
			if len(payload) == 0 {
				continue
			}
			counters.bulkOffered.Add(uint64(len(payload)))
			written, err := stream.Write(payload)
			if written > 0 {
				counters.bulkAccepted.Add(uint64(written))
			}
			if err != nil || written != len(payload) {
				if ctx.Err() == nil {
					counters.bulkFailed.Add(1)
				}
				return
			}
		}
	}
}

func openBareBulkStream(ctx context.Context, connection transport.Conn) (transport.BidiStream, error) {
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
	header, err := wire.EncodeCustomStreamHeader(uint64(benchmarkStreamType))
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

func callBareQUIC(ctx context.Context, connection transport.Conn) error {
	stream, err := connection.OpenBidi(ctx)
	if err != nil {
		return err
	}
	stop := abortBidiOnContext(ctx, stream)
	defer stop()
	defer stream.Close()
	request, err := wire.EncodeRequest(wire.Request{MessageType: uint64(benchmarkRequestType), TimeoutMS: 5_000, Payload: make([]byte, benchmarkRPCBytes)}, benchmarkRPCBytes)
	if err != nil {
		return err
	}
	if _, err := stream.Write(request); err != nil {
		return err
	}
	if err := stream.CloseWrite(); err != nil {
		return err
	}
	body, err := io.ReadAll(io.LimitReader(stream, int64(benchmarkRPCBytes+64)))
	if err != nil {
		return err
	}
	response, err := wire.DecodeResponse(body, benchmarkRPCBytes)
	if err != nil || response.Status != 0 || len(response.Payload) != benchmarkRPCBytes {
		if err == nil {
			err = errors.New("invalid bare-QUIC request response")
		}
		return err
	}
	return nil
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

func benchmarkServerRouter(cfg config, counters *trialCounters) *sgsp.Router {
	router := sgsp.NewRouter()
	_ = router.OnEvent(benchmarkInputType, func(ctx context.Context, incoming *sgsp.Incoming) {
		if counters != nil {
			counters.inputDelivered.Add(1)
			counters.updateOffered.Add(1)
		}
		tick, sequence, stamp, _ := benchmarkPayloadFields(incoming.Payload)
		update := benchmarkPayload(cfg.UpdateBytes, tick, sequence, stamp)
		if err := incoming.Session.Send(ctx, benchmarkUpdateType, update, sgsp.SendOptions{Channel: 2, Delivery: sgsp.UnreliableSequenced}); err != nil {
			if counters != nil {
				counters.updateFailed.Add(1)
			}
			return
		}
		if counters != nil {
			counters.updateAccepted.Add(1)
		}
	})
	_ = router.OnRequest(benchmarkRequestType, func(ctx context.Context, incoming *sgsp.Incoming) {
		if counters != nil {
			counters.requestDelivered.Add(1)
		}
		if err := incoming.Reply(ctx, make([]byte, benchmarkRPCBytes)); err != nil && counters != nil {
			counters.requestFailed.Add(1)
		}
	})
	_ = router.OnStream(benchmarkStreamType, func(_ context.Context, incoming *sgsp.Incoming) {
		if incoming.Stream == nil {
			return
		}
		copyBenchmarkBulk(incoming.Stream, counters)
	})
	return router
}

func copyBenchmarkBulk(stream io.Reader, counters *trialCounters) {
	buffer := make([]byte, 32<<10)
	for {
		count, err := stream.Read(buffer)
		if count > 0 && counters != nil {
			counters.bulkDelivered.Add(uint64(count))
		}
		if err != nil {
			if counters != nil && !errors.Is(err, io.EOF) {
				counters.bulkFailed.Add(1)
			}
			return
		}
	}
}

func benchmarkClientRouter(counters *trialCounters, clock benchmarkClock) *sgsp.Router {
	router := sgsp.NewRouter()
	_ = router.OnEvent(benchmarkUpdateType, func(_ context.Context, incoming *sgsp.Incoming) {
		if counters != nil {
			counters.updateDelivered.Add(1)
			if _, _, stamp, ok := benchmarkPayloadFields(incoming.Payload); ok {
				if age, ok := clock.Age(stamp); ok {
					counters.updateAge.Record(age)
				}
			}
		}
	})
	return router
}

func runSGSPClient(ctx context.Context, cfg config, session sgsp.Session, counters *trialCounters, clock benchmarkClock) {
	inputTicker := time.NewTicker(time.Second / time.Duration(cfg.Hz))
	defer inputTicker.Stop()
	var rpcTicker *time.Ticker
	var rpc <-chan time.Time
	if cfg.RPCPerSecond > 0 {
		rpcTicker = time.NewTicker(time.Second / time.Duration(cfg.RPCPerSecond))
		defer rpcTicker.Stop()
		rpc = rpcTicker.C
	}
	var bulkTicker *time.Ticker
	var bulk <-chan time.Time
	var stream sgsp.Stream
	var streamStop chan struct{}
	if cfg.BulkBytes > 0 {
		opened, err := session.OpenStream(ctx, benchmarkStreamType)
		if err != nil {
			counters.bulkFailed.Add(1)
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
			bulkTicker = time.NewTicker(10 * time.Millisecond)
			defer bulkTicker.Stop()
			bulk = bulkTicker.C
		}
	}
	defer func() {
		if stream != nil {
			close(streamStop)
			stream.Abort(sgsp.Canceled)
		}
	}()
	var tick, sequence uint64
	pacer := bulkPacer{perSecond: cfg.BulkBytes}
	for {
		select {
		case <-ctx.Done():
			return
		case <-inputTicker.C:
			tick++
			sequence++
			counters.inputOffered.Add(1)
			payload := benchmarkPayload(cfg.InputBytes, tick, sequence, clock.Stamp())
			started := time.Now()
			if err := session.Send(ctx, benchmarkInputType, payload, sgsp.SendOptions{Channel: 1, Delivery: sgsp.UnreliableSequenced}); err != nil {
				if ctx.Err() == nil {
					counters.inputSendOverhead.Record(time.Since(started))
				}
				if countWorkloadFailure(ctx, err) {
					counters.inputFailed.Add(1)
				}
			} else {
				counters.inputSendOverhead.Record(time.Since(started))
				counters.inputAccepted.Add(1)
			}
		case <-rpc:
			counters.requestOffered.Add(1)
			started := time.Now()
			response, err := session.Call(ctx, benchmarkRequestType, make([]byte, benchmarkRPCBytes))
			if err != nil || len(response) != benchmarkRPCBytes {
				// The measurement cutoff can cancel a request after it has been
				// offered but before its reply arrives. Keep that as an unmatched
				// offered operation, not an application/transport failure.
				if ctx.Err() == nil {
					counters.requestFailed.Add(1)
				}
			} else {
				counters.requestRoundTrip.Record(time.Since(started))
				counters.requestAccepted.Add(1)
			}
		case <-bulk:
			payload := make([]byte, pacer.Next())
			if len(payload) == 0 {
				continue
			}
			counters.bulkOffered.Add(uint64(len(payload)))
			written, err := stream.Write(payload)
			if written > 0 {
				counters.bulkAccepted.Add(uint64(written))
			}
			if err != nil || written != len(payload) {
				if ctx.Err() == nil {
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

func benchmarkPayload(size int, tick, sequence, stamp uint64) []byte {
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
	return payload
}

func benchmarkPayloadFields(payload []byte) (tick, sequence, stamp uint64, ok bool) {
	if len(payload) < 24 {
		return 0, 0, 0, false
	}
	return binary.BigEndian.Uint64(payload[:8]), binary.BigEndian.Uint64(payload[8:16]), binary.BigEndian.Uint64(payload[16:24]), true
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
