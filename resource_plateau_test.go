package sgsp

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"net"
	"os"
	goruntime "runtime"
	"testing"
	"time"
)

const (
	resourcePlateauHeapTolerance       = 2 << 20
	resourcePlateauGoroutineTolerance  = 16
	resourcePlateauSampleInterval      = 30 * time.Second
	resourcePlateauCycleTimeout        = 3 * time.Second
	resourcePlateauSlowConsumerTimeout = 50 * time.Millisecond
)

type resourcePlateauSnapshot struct {
	ElapsedMilliseconds int64  `json:"elapsed_milliseconds"`
	HeapAlloc           uint64 `json:"heap_alloc"`
	Goroutines          int    `json:"goroutines"`
	ApplicationBytes    int64  `json:"application_bytes"`
	Sessions            int    `json:"sessions"`
}

type resourcePlateauResult struct {
	Schema             int                       `json:"schema"`
	Status             string                    `json:"status"`
	StartedUTC         time.Time                 `json:"started_utc"`
	CompletedUTC       time.Time                 `json:"completed_utc"`
	RequestedDuration  string                    `json:"requested_duration"`
	Cycles             int                       `json:"cycles"`
	HeapToleranceBytes uint64                    `json:"heap_tolerance_bytes"`
	GoroutineTolerance int                       `json:"goroutine_tolerance"`
	WarmedBaseline     resourcePlateauSnapshot   `json:"warmed_baseline"`
	Peak               resourcePlateauSnapshot   `json:"peak"`
	Teardown           resourcePlateauSnapshot   `json:"teardown"`
	Samples            []resourcePlateauSnapshot `json:"samples"`
	GoVersion          string                    `json:"go_version"`
}

func TestResourcePlateau(t *testing.T) {
	rawDuration := os.Getenv("SGSP_RESOURCE_PLATEAU_DURATION")
	if rawDuration == "" {
		t.Skip("set SGSP_RESOURCE_PLATEAU_DURATION for the retained M8 slow-consumer plateau campaign")
	}
	duration, err := time.ParseDuration(rawDuration)
	if err != nil || duration <= 0 {
		t.Fatalf("SGSP_RESOURCE_PLATEAU_DURATION must be a positive Go duration")
	}
	output := os.Getenv("SGSP_RESOURCE_PLATEAU_OUTPUT")
	if output == "" {
		t.Fatal("SGSP_RESOURCE_PLATEAU_OUTPUT is required to retain plateau evidence")
	}

	certificate, roots := endpointCertificate(t)
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	limits := DefaultLimits()
	limits.QueueMessages = 2
	limits.SlowConsumerTimeout = resourcePlateauSlowConsumerTimeout
	limits.HandshakesPerIPPerSecond = 100_000
	limits.HandshakeBurstPerIP = 100_000
	handlerStarted := make(chan struct{}, 1)
	router := NewRouter()
	if err := router.OnEvent(95, func(ctx context.Context, _ *Incoming) {
		select {
		case handlerStarted <- struct{}{}:
		case <-ctx.Done():
			return
		}
		<-ctx.Done()
	}); err != nil {
		t.Fatal(err)
	}
	server, err := NewServer(ServerConfig{
		TLS:    &tls.Config{Certificates: []tls.Certificate{certificate}},
		App:    AppIdentity{ID: "resource-plateau", Version: "1"},
		Owner:  Owner{ID: "server", Endpoint: Endpoint{Address: packet.LocalAddr().String(), ServerName: "localhost"}},
		Auth:   &testAuthenticator{},
		Limits: limits,
		Dispatch: DispatchConfig{
			Mode:   Handlers,
			Router: router,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	endpoint := server.(*serverEndpoint)
	serveContext, stop := context.WithCancel(context.Background())
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(serveContext, packet) }()
	<-endpoint.started
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			stop()
			<-serveDone
		}
	})
	clientConfig := ClientConfig{
		TLS:    &tls.Config{RootCAs: roots},
		App:    AppIdentity{ID: "resource-plateau", Version: "1"},
		Limits: limits,
		Credentials: func(context.Context) (Credential, error) {
			return Credential{Scheme: "test", Data: []byte("valid")}, nil
		},
		Dispatch: DispatchConfig{Mode: Handlers, Router: NewRouter()},
	}

	started := time.Now().UTC()
	deadline := started.Add(duration)
	warmAt := started.Add(duration / 10)
	nextSample := warmAt
	result := resourcePlateauResult{
		Schema:             1,
		Status:             "running",
		StartedUTC:         started,
		RequestedDuration:  rawDuration,
		HeapToleranceBytes: resourcePlateauHeapTolerance,
		GoroutineTolerance: resourcePlateauGoroutineTolerance,
		GoVersion:          goruntime.Version(),
	}
	warmed := false
	for time.Now().Before(deadline) {
		runSlowConsumerPlateauCycle(t, endpoint, packet.LocalAddr().String(), clientConfig, handlerStarted)
		result.Cycles++
		now := time.Now().UTC()
		if !warmed && !now.Before(warmAt) {
			goruntime.GC()
			result.WarmedBaseline = resourcePlateauSnapshotFor(endpoint, started)
			result.Peak = result.WarmedBaseline
			result.Samples = append(result.Samples, result.WarmedBaseline)
			warmed = true
			nextSample = now.Add(resourcePlateauSampleInterval)
		}
		if warmed && !now.Before(nextSample) {
			goruntime.GC()
			sample := resourcePlateauSnapshotFor(endpoint, started)
			assertResourcePlateauBounds(t, result.WarmedBaseline, sample)
			result.Peak = resourcePlateauPeak(result.Peak, sample)
			result.Samples = append(result.Samples, sample)
			nextSample = nextSample.Add(resourcePlateauSampleInterval)
		}
	}
	if !warmed {
		goruntime.GC()
		result.WarmedBaseline = resourcePlateauSnapshotFor(endpoint, started)
		result.Peak = result.WarmedBaseline
		result.Samples = append(result.Samples, result.WarmedBaseline)
	}
	stop()
	if err := <-serveDone; err != nil {
		t.Fatal(err)
	}
	stopped = true
	goruntime.GC()
	result.Teardown = resourcePlateauSnapshotFor(endpoint, started)
	assertResourcePlateauBounds(t, result.WarmedBaseline, result.Teardown)
	result.Status = "completed"
	result.CompletedUTC = time.Now().UTC()
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, append(data, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func runSlowConsumerPlateauCycle(t *testing.T, endpoint *serverEndpoint, address string, config ClientConfig, handlerStarted <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), resourcePlateauCycleTimeout)
	defer cancel()
	client, err := Dial(ctx, Endpoint{Address: address, ServerName: "localhost"}, config)
	if err != nil {
		t.Fatal(err)
	}
	concrete := client.(*clientEndpoint)
	sessions := endpoint.Sessions()
	if len(sessions) != 1 {
		t.Fatalf("server sessions = %d, want 1", len(sessions))
	}
	serverSession := sessions[0].(*sessionRecord)
	payload := []byte("queued")
	if err := client.Session().Send(ctx, 95, payload, SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerStarted:
	case <-ctx.Done():
		t.Fatal("first reliable handler did not start")
	}
	charge := int64(len(payload) + 32)
	for messages := int64(2); messages <= 3; messages++ {
		if err := client.Session().Send(ctx, 95, payload, SendOptions{Delivery: ReliableOrdered}); err != nil {
			t.Fatal(err)
		}
		waitForResourcePlateauBudget(t, ctx, endpoint, messages*charge)
	}
	if err := client.Session().Send(ctx, 95, payload, SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Session().Context().Done():
	case <-ctx.Done():
		t.Fatal("slow consumer did not close the peer session")
	}
	if code := concrete.session.terminalCode(); code != SlowConsumer {
		t.Fatalf("client slow-consumer terminal code = %v, want %v", code, SlowConsumer)
	}
	if code := serverSession.terminalCode(); code != SlowConsumer {
		t.Fatalf("server slow-consumer terminal code = %v, want %v", code, SlowConsumer)
	}
	_ = client.Close(context.Background())
	for len(endpoint.Sessions()) != 0 {
		select {
		case <-endpoint.changed:
		case <-ctx.Done():
			t.Fatalf("slow consumer retained %d active sessions", len(endpoint.Sessions()))
		}
	}
	if used := endpoint.applicationBudget.Used(); used != 0 {
		t.Fatalf("slow-consumer cleanup retained %d application bytes", used)
	}
}

func waitForResourcePlateauBudget(t *testing.T, ctx context.Context, endpoint *serverEndpoint, want int64) {
	t.Helper()
	for endpoint.applicationBudget.Used() < want {
		select {
		case <-ctx.Done():
			t.Fatalf("reliable queue charge = %d, want at least %d", endpoint.applicationBudget.Used(), want)
		case <-time.After(time.Millisecond):
		}
	}
}

func resourcePlateauSnapshotFor(endpoint *serverEndpoint, started time.Time) resourcePlateauSnapshot {
	var memory goruntime.MemStats
	goruntime.ReadMemStats(&memory)
	return resourcePlateauSnapshot{
		ElapsedMilliseconds: time.Since(started).Milliseconds(),
		HeapAlloc:           memory.HeapAlloc,
		Goroutines:          goruntime.NumGoroutine(),
		ApplicationBytes:    endpoint.applicationBudget.Used(),
		Sessions:            len(endpoint.Sessions()),
	}
}

func resourcePlateauPeak(left, right resourcePlateauSnapshot) resourcePlateauSnapshot {
	if right.HeapAlloc > left.HeapAlloc {
		left.HeapAlloc = right.HeapAlloc
	}
	if right.Goroutines > left.Goroutines {
		left.Goroutines = right.Goroutines
	}
	if right.ApplicationBytes > left.ApplicationBytes {
		left.ApplicationBytes = right.ApplicationBytes
	}
	if right.Sessions > left.Sessions {
		left.Sessions = right.Sessions
	}
	left.ElapsedMilliseconds = right.ElapsedMilliseconds
	return left
}

func assertResourcePlateauBounds(t *testing.T, baseline, observed resourcePlateauSnapshot) {
	t.Helper()
	if observed.ApplicationBytes != 0 || observed.Sessions != 0 {
		t.Fatalf("resource plateau retained application_bytes=%d sessions=%d", observed.ApplicationBytes, observed.Sessions)
	}
	if observed.HeapAlloc > baseline.HeapAlloc+resourcePlateauHeapTolerance {
		t.Fatalf("resource plateau heap=%d, warmed baseline=%d, tolerance=%d", observed.HeapAlloc, baseline.HeapAlloc, resourcePlateauHeapTolerance)
	}
	if observed.Goroutines > baseline.Goroutines+resourcePlateauGoroutineTolerance {
		t.Fatalf("resource plateau goroutines=%d, warmed baseline=%d, tolerance=%d", observed.Goroutines, baseline.Goroutines, resourcePlateauGoroutineTolerance)
	}
}
