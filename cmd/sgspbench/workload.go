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
	"net"
	"runtime"
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
	Forwarded, Dropped, PendingPackets uint64 `json:"forwarded" json:"dropped" json:"pending_packets"`
	PendingBytes                       int64  `json:"pending_bytes"`
	Overloaded                         bool   `json:"overloaded"`
}

type runtimeMeasurement struct {
	GoMaxProcs      int    `json:"gomaxprocs"`
	Goroutines      int    `json:"goroutines"`
	HeapAlloc       uint64 `json:"heap_alloc"`
	TotalAlloc      uint64 `json:"total_alloc"`
	Mallocs         uint64 `json:"mallocs"`
	NumCPU          int    `json:"num_cpu"`
	OperatingSystem string `json:"operating_system"`
	Architecture    string `json:"architecture"`
}

type trialCounters struct {
	inputOffered, inputAccepted, inputDelivered, inputFailed         atomic.Uint64
	updateOffered, updateAccepted, updateDelivered, updateFailed     atomic.Uint64
	requestOffered, requestAccepted, requestDelivered, requestFailed atomic.Uint64
	bulkOffered, bulkAccepted, bulkDelivered, bulkFailed             atomic.Uint64
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
	}
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
	limits := sgsp.DefaultLimits()
	limits.MaxSessions = max(limits.MaxSessions, cfg.Clients)
	limits.MaxPendingHandshakes = max(limits.MaxPendingHandshakes, cfg.Clients)
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

	trialCtx, cancelTrial := context.WithTimeout(parent, cfg.Warmup+cfg.Duration+10*time.Second)
	defer cancelTrial()
	type trialClient struct {
		client sgsp.Client
		relay  *udprelay.Relay
	}
	clients := make([]trialClient, 0, cfg.Clients)
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		for _, client := range clients {
			_ = client.client.Close(closeCtx)
			_ = client.relay.Close()
		}
	}()
	for index := 0; index < cfg.Clients; index++ {
		relay, err := udprelay.New("127.0.0.1:0", "127.0.0.1:0", serverPacket.LocalAddr(), udprelay.Config{RTT: cfg.RTT, Jitter: cfg.Jitter, Loss: cfg.Loss, Reorder: cfg.Reorder, Seed: uint64(cfg.Seed) + uint64(index)})
		if err != nil {
			return result, err
		}
		client, err := sgsp.Dial(trialCtx, sgsp.Endpoint{Address: relay.ClientAddr().String(), ServerName: "localhost"}, sgsp.ClientConfig{
			TLS:              &tls.Config{RootCAs: roots},
			App:              sgsp.AppIdentity{ID: "sgspbench", Version: "1"},
			Limits:           limits,
			DisableReconnect: true,
			Credentials: func(context.Context) (sgsp.Credential, error) {
				return sgsp.Credential{Scheme: "benchmark", Data: []byte("benchmark")}, nil
			},
			Dispatch: sgsp.DispatchConfig{Mode: sgsp.Handlers, Router: benchmarkClientRouter(counters)},
		})
		if err != nil {
			_ = relay.Close()
			return result, err
		}
		clients = append(clients, trialClient{client: client, relay: relay})
	}

	workCtx, stopWork := context.WithCancel(trialCtx)
	var workers sync.WaitGroup
	for _, client := range clients {
		workers.Add(1)
		go func(session sgsp.Session) {
			defer workers.Done()
			runSGSPClient(workCtx, cfg, session, counters)
		}(client.client.Session())
	}
	if !waitTrial(workCtx, cfg.Warmup) {
		stopWork()
		workers.Wait()
		return result, context.Cause(workCtx)
	}
	counters.reset()
	started := time.Now()
	if !waitTrial(workCtx, cfg.Duration) {
		stopWork()
		workers.Wait()
		return result, context.Cause(workCtx)
	}
	stopWork()
	workers.Wait()
	result = counters.snapshot()
	result.Duration = time.Since(started)
	result.Runtime = runtimeSnapshot()
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
				serveBareQUICConnection(connection, cfg, counters)
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

	trialCtx, cancelTrial := context.WithTimeout(parent, cfg.Warmup+cfg.Duration+10*time.Second)
	defer cancelTrial()
	type trialClient struct {
		connection transport.Conn
		packet     net.PacketConn
		relay      *udprelay.Relay
	}
	clients := make([]trialClient, 0, cfg.Clients)
	defer func() {
		for _, client := range clients {
			_ = client.connection.Close(uint64(sgsp.Normal), "benchmark complete")
			_ = client.packet.Close()
			_ = client.relay.Close()
		}
	}()
	for index := 0; index < cfg.Clients; index++ {
		relay, err := udprelay.New("127.0.0.1:0", "127.0.0.1:0", serverPacket.LocalAddr(), udprelay.Config{RTT: cfg.RTT, Jitter: cfg.Jitter, Loss: cfg.Loss, Reorder: cfg.Reorder, Seed: uint64(cfg.Seed) + uint64(index)})
		if err != nil {
			return result, err
		}
		packet, err := net.ListenPacket("udp", "[::]:0")
		if err != nil {
			_ = relay.Close()
			return result, err
		}
		tlsConfig := &tls.Config{RootCAs: roots, ServerName: "localhost"}
		connection, err := quictransport.Dial(trialCtx, packet, relay.ClientAddr(), benchmarkTransportConfig(tlsConfig))
		if err != nil {
			_ = packet.Close()
			_ = relay.Close()
			return result, err
		}
		clients = append(clients, trialClient{connection: connection, packet: packet, relay: relay})
	}

	workCtx, stopWork := context.WithCancel(trialCtx)
	var workers sync.WaitGroup
	for _, client := range clients {
		workers.Add(2)
		go func(connection transport.Conn) {
			defer workers.Done()
			receiveBareUpdates(workCtx, connection, cfg, counters)
		}(client.connection)
		go func(connection transport.Conn) {
			defer workers.Done()
			runBareQUICClient(workCtx, cfg, connection, counters)
		}(client.connection)
	}
	if !waitTrial(workCtx, cfg.Warmup) {
		stopWork()
		workers.Wait()
		return result, context.Cause(workCtx)
	}
	counters.reset()
	started := time.Now()
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
	result.Runtime = runtimeSnapshot()
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

func serveBareQUICConnection(connection transport.Conn, cfg config, counters *trialCounters) {
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		serveBareDatagrams(connection, cfg, counters)
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

func serveBareDatagrams(connection transport.Conn, cfg config, counters *trialCounters) {
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
		update, err := wire.EncodeDatagram(wire.Event{Kind: wire.SequencedKind, Channel: 2, MessageType: uint64(benchmarkUpdateType), Sequence: sequence, Payload: benchmarkPayload(cfg.UpdateBytes, sequence, sequence)}, cfg.UpdateBytes+32)
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

func receiveBareUpdates(ctx context.Context, connection transport.Conn, cfg config, counters *trialCounters) {
	for {
		datagram, err := connection.ReceiveDatagram(ctx)
		if err != nil {
			return
		}
		event, err := wire.DecodeDatagram(datagram.Payload, cfg.UpdateBytes+32)
		if err == nil && event.Kind == wire.SequencedKind && event.Channel == 2 && event.MessageType == uint64(benchmarkUpdateType) {
			counters.updateDelivered.Add(1)
		}
	}
}

func runBareQUICClient(ctx context.Context, cfg config, connection transport.Conn, counters *trialCounters) {
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
	bulkChunk := max(1, cfg.BulkBytes/100)
	for {
		select {
		case <-ctx.Done():
			return
		case <-inputTicker.C:
			tick++
			sequence++
			counters.inputOffered.Add(1)
			payload, err := wire.EncodeDatagram(wire.Event{Kind: wire.SequencedKind, Channel: 1, MessageType: uint64(benchmarkInputType), Sequence: sequence, Payload: benchmarkPayload(cfg.InputBytes, tick, sequence)}, cfg.InputBytes+32)
			if err != nil || connection.SendDatagram(ctx, payload) != nil {
				counters.inputFailed.Add(1)
			} else {
				counters.inputAccepted.Add(1)
			}
		case <-rpc:
			counters.requestOffered.Add(1)
			if err := callBareQUIC(ctx, connection); err != nil {
				if ctx.Err() == nil {
					counters.requestFailed.Add(1)
				}
			} else {
				counters.requestAccepted.Add(1)
			}
		case <-bulk:
			payload := make([]byte, bulkChunk)
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
	update := benchmarkPayload(cfg.UpdateBytes, 0, 0)
	_ = router.OnEvent(benchmarkInputType, func(ctx context.Context, incoming *sgsp.Incoming) {
		if counters != nil {
			counters.inputDelivered.Add(1)
			counters.updateOffered.Add(1)
		}
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

func benchmarkClientRouter(counters *trialCounters) *sgsp.Router {
	router := sgsp.NewRouter()
	_ = router.OnEvent(benchmarkUpdateType, func(context.Context, *sgsp.Incoming) {
		if counters != nil {
			counters.updateDelivered.Add(1)
		}
	})
	return router
}

func runSGSPClient(ctx context.Context, cfg config, session sgsp.Session, counters *trialCounters) {
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
	bulkChunk := max(1, cfg.BulkBytes/100)
	for {
		select {
		case <-ctx.Done():
			return
		case <-inputTicker.C:
			tick++
			sequence++
			counters.inputOffered.Add(1)
			if err := session.Send(ctx, benchmarkInputType, benchmarkPayload(cfg.InputBytes, tick, sequence), sgsp.SendOptions{Channel: 1, Delivery: sgsp.UnreliableSequenced}); err != nil {
				counters.inputFailed.Add(1)
			} else {
				counters.inputAccepted.Add(1)
			}
		case <-rpc:
			counters.requestOffered.Add(1)
			response, err := session.Call(ctx, benchmarkRequestType, make([]byte, benchmarkRPCBytes))
			if err != nil || len(response) != benchmarkRPCBytes {
				counters.requestFailed.Add(1)
			} else {
				counters.requestAccepted.Add(1)
			}
		case <-bulk:
			payload := make([]byte, bulkChunk)
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

func benchmarkPayload(size int, tick, sequence uint64) []byte {
	payload := make([]byte, size)
	if len(payload) >= 8 {
		binary.BigEndian.PutUint64(payload[:8], tick)
	}
	if len(payload) >= 16 {
		binary.BigEndian.PutUint64(payload[8:16], sequence)
	}
	return payload
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
	return runtimeMeasurement{GoMaxProcs: runtime.GOMAXPROCS(0), Goroutines: runtime.NumGoroutine(), HeapAlloc: memory.HeapAlloc, TotalAlloc: memory.TotalAlloc, Mallocs: memory.Mallocs, NumCPU: runtime.NumCPU(), OperatingSystem: runtime.GOOS, Architecture: runtime.GOARCH}
}
