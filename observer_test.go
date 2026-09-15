package sgsp

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"qattidev/sgsp/internal/runtime"
	"qattidev/sgsp/internal/transport"
)

type blockingObserver struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (o *blockingObserver) Observe(Observation) { o.once.Do(func() { close(o.started) }); <-o.release }

func TestSlowObserver(t *testing.T) {
	observer := &blockingObserver{started: make(chan struct{}), release: make(chan struct{})}
	queue := newObserverQueue(observer, 1, time.Millisecond)
	defer queue.Close()
	queue.Observe(Observation{Name: "first"})
	select {
	case <-observer.started:
	case <-time.After(time.Second):
		t.Fatal("observer did not receive first flush")
	}
	queue.Observe(Observation{Name: "queued"})
	queue.Observe(Observation{Name: "dropped"})
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for queue.Dropped() == 0 {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatal("slow observer did not drop a bounded flush")
		}
	}
	close(observer.release)
}

type recordingObserver struct{ values chan Observation }

func (o *recordingObserver) Observe(value Observation) { o.values <- value }

type staticStatsConnection struct {
	transport.Conn
	stats transport.Stats
}

func (c staticStatsConnection) Stats() transport.Stats { return c.stats }

func TestObserverAggregation(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 1)}
	queue := newObserverQueue(observer, 4, time.Millisecond)
	defer queue.Close()
	queue.Observe(Observation{Name: "messages", Kind: "in", Value: 2})
	queue.Observe(Observation{Name: "messages", Kind: "in", Value: 3})
	select {
	case value := <-observer.values:
		if value.Name != "messages" || value.Kind != "in" || value.Value != 5 {
			t.Fatalf("aggregated observation = %#v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("aggregate was not flushed")
	}
}

func TestHistogramKindUsesFixedBoundedBuckets(t *testing.T) {
	for duration, want := range map[time.Duration]string{
		0:                             "latency_le_1us",
		time.Microsecond:              "latency_le_1us",
		1500 * time.Nanosecond:        "latency_le_2us",
		2 * time.Microsecond:          "latency_le_2us",
		3 * time.Microsecond:          "latency_le_4us",
		524288 * time.Microsecond:     "latency_le_524288us",
		time.Second:                   "latency_le_1s",
		time.Second + time.Nanosecond: "latency_overflow",
	} {
		if got := histogramKind("latency", duration); got != want {
			t.Fatalf("histogramKind(%s) = %q, want %q", duration, got, want)
		}
	}
}

func TestObserveDurationAggregatesHistogramBucket(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 1)}
	queue := newObserverQueue(observer, 4, time.Millisecond)
	defer queue.Close()
	operations := &connectionOperations{observer: queue}
	operations.observeDuration("requests", "latency", Normal, 3*time.Microsecond)
	select {
	case value := <-observer.values:
		if value.Name != "requests" || value.Kind != "latency_le_4us" || value.Code != Normal || value.Value != 1 {
			t.Fatalf("histogram observation = %#v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("histogram observation was not flushed")
	}
}

func TestTransportObservationsUseBoundedCountersAndRTTBuckets(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 4)}
	queue := newObserverQueue(observer, 4, time.Millisecond)
	defer queue.Close()
	operations := &connectionOperations{observer: queue}
	operations.observeTransportSample(
		transport.Stats{BytesSent: 100, BytesReceived: 200},
		transport.Stats{RTT: 3 * time.Microsecond, BytesSent: 125, BytesReceived: 240, PacketsLost: 2},
	)
	got := make(map[string]Observation, 4)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(got) < 4 {
		select {
		case value := <-observer.values:
			got[value.Kind] = value
		case <-deadline.C:
			t.Fatalf("transport observations = %#v", got)
		}
	}
	if value := got["rtt_le_4us"]; value.Name != "transport" || value.Code != Normal || value.Value != 1 {
		t.Fatalf("RTT observation = %#v", value)
	}
	if value := got["bytes_sent"]; value.Name != "transport" || value.Value != 25 {
		t.Fatalf("sent observation = %#v", value)
	}
	if value := got["bytes_received"]; value.Name != "transport" || value.Value != 40 {
		t.Fatalf("received observation = %#v", value)
	}
	if value := got["packets_lost"]; value.Name != "transport" || value.Code != Normal || value.Value != 2 {
		t.Fatalf("packet-loss observation = %#v", value)
	}
}

func TestDatagramDropAccounting(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 2)}
	queue := newObserverQueue(observer, 4, time.Millisecond)
	defer queue.Close()
	operations := &connectionOperations{observer: queue, connection: staticStatsConnection{stats: transport.Stats{BytesSent: 7, BytesReceived: 11}}}
	operations.dropDatagram("pressure", Backpressure, false)
	operations.dropDatagram("stale", Normal, true)
	if got := operations.localDatagramDrops.Load(); got != 2 {
		t.Fatalf("local datagram drops = %d", got)
	}
	if got := operations.staleUpdatesDropped.Load(); got != 1 {
		t.Fatalf("stale datagram drops = %d", got)
	}
	stats := operations.stats()
	if stats.LocalDatagramsDropped != 2 || stats.StaleUpdatesDropped != 1 || stats.BytesSent != 7 || stats.BytesReceived != 11 {
		t.Fatalf("session stats = %#v", stats)
	}
	got := make(map[string]Observation, 2)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(got) < 2 {
		select {
		case value := <-observer.values:
			got[value.Kind] = value
		case <-deadline.C:
			t.Fatalf("datagram drop observations = %#v", got)
		}
	}
	if value := got["pressure"]; value.Name != "datagram_drop" || value.Code != Backpressure || value.Value != 1 {
		t.Fatalf("pressure observation = %#v", value)
	}
	if value := got["stale"]; value.Name != "datagram_drop" || value.Code != Normal || value.Value != 1 {
		t.Fatalf("stale observation = %#v", value)
	}
}

func TestMessageObservationsSeparateCountsAndBytes(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 2)}
	queue := newObserverQueue(observer, 2, time.Millisecond)
	defer queue.Close()
	operations := &connectionOperations{observer: queue}
	operations.observeMessage("in_reliable", 17)
	got := make(map[string]Observation, 2)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(got) < 2 {
		select {
		case observation := <-observer.values:
			got[observation.Kind] = observation
		case <-deadline.C:
			t.Fatalf("message observations = %#v", got)
		}
	}
	if value := got["in_reliable_count"]; value.Name != "messages" || value.Code != Normal || value.Value != 1 {
		t.Fatalf("message count = %#v", value)
	}
	if value := got["in_reliable_bytes"]; value.Name != "messages" || value.Code != Normal || value.Value != 17 {
		t.Fatalf("message bytes = %#v", value)
	}
}

func TestOutboundRequestObservationsIncludeResultAndLatency(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 2)}
	queue := newObserverQueue(observer, 2, time.Millisecond)
	defer queue.Close()
	operations := &connectionOperations{observer: queue, epoch: 1}
	operations.session = newSessionRecord(SessionID{1}, Owner{}, "", Principal{}, DefaultLimits(), false, operations)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Millisecond))
	defer cancel()
	if _, err := operations.call(ctx, 1, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired Call = %v", err)
	}
	got := make(map[string]Observation, 2)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(got) < 2 {
		select {
		case observation := <-observer.values:
			got[observation.Kind] = observation
		case <-deadline.C:
			t.Fatalf("outbound request observations = %#v", got)
		}
	}
	if value := got["outbound_result"]; value.Name != "requests" || value.Code != DeadlineExceeded || value.Value != 1 {
		t.Fatalf("outbound request result = %#v", value)
	}
	for kind, value := range got {
		if strings.HasPrefix(kind, "outbound_latency_") && value.Name == "requests" && value.Code == DeadlineExceeded && value.Value == 1 {
			return
		}
	}
	t.Fatalf("outbound request latency missing from %#v", got)
}

func TestActiveOperationObservationsUseBoundedInitiatorKinds(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 6)}
	queue := newObserverQueue(observer, 8, time.Millisecond)
	defer queue.Close()
	operations := &connectionOperations{observer: queue, inRequests: 2, outRequests: 3, inStreams: 5, outStreams: 7}
	operations.observeActiveOperations()
	got := make(map[string]Observation, 6)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(got) < 6 {
		select {
		case value := <-observer.values:
			got[value.Name+"/"+value.Kind] = value
		case <-deadline.C:
			t.Fatalf("active operation observations = %#v", got)
		}
	}
	for key, want := range map[string]float64{
		"requests/active_inbound":  2,
		"requests/active_outbound": 3,
		"streams/request_inbound":  2,
		"streams/request_outbound": 3,
		"streams/custom_inbound":   5,
		"streams/custom_outbound":  7,
	} {
		if value := got[key]; value.Code != Normal || value.Value != want {
			t.Fatalf("%s observation = %#v, want %v", key, value, want)
		}
	}
}

func TestServerSessionStateObservationsIncludeSuspendedGauge(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 5)}
	queue := newObserverQueue(observer, 5, time.Millisecond)
	defer queue.Close()
	limits := DefaultLimits()
	active := newSessionRecord(SessionID{1}, Owner{}, "", Principal{}, limits, false, nil)
	suspended := newSessionRecord(SessionID{2}, Owner{}, "", Principal{}, limits, false, nil)
	suspended.setState(Suspended, 2)
	server := &serverEndpoint{
		sessions: map[SessionID]*sessionRecord{active.ID(): active, suspended.ID(): suspended},
		observer: queue,
	}
	server.observeSessionStatesOnce()
	got := make(map[string]Observation, 5)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(got) < 5 {
		select {
		case observation := <-observer.values:
			got[observation.Kind] = observation
		case <-deadline.C:
			t.Fatalf("session-state observations = %#v", got)
		}
	}
	for kind, want := range map[string]float64{
		"state_connecting":     0,
		"state_authenticating": 0,
		"state_active":         1,
		"state_suspended":      1,
		"state_closed":         0,
	} {
		if observation := got[kind]; observation.Name != "sessions" || observation.Code != Normal || observation.Value != want {
			t.Fatalf("%s session gauge = %#v, want %v", kind, observation, want)
		}
	}
}

func TestUnknownRequestOutcomeObservation(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 1)}
	queue := newObserverQueue(observer, 2, time.Millisecond)
	defer queue.Close()
	operations := &connectionOperations{observer: queue}
	result := operations.unknownRequestOutcome(context.Canceled)
	var outcome *Error
	if !errors.As(result, &outcome) || !outcome.OutcomeUnknown {
		t.Fatalf("unknown outcome = %v", result)
	}
	select {
	case value := <-observer.values:
		if value.Name != "requests" || value.Kind != "unknown_outbound" || value.Code != OutcomeUnknown || value.Value != 1 {
			t.Fatalf("unknown outcome observation = %#v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("missing unknown outcome observation")
	}
}

func TestQueueObservationsCoverHandlerUsageAndDequeueAge(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 3)}
	queue := newObserverQueue(observer, 4, time.Millisecond)
	defer queue.Close()
	handlerQueue := runtime.NewQueue[*Incoming](128, 2)
	if !handlerQueue.TryPush(runtime.Item[*Incoming]{Value: &Incoming{}, Bytes: 23}) {
		t.Fatal("could not seed handler queue")
	}
	operations := &connectionOperations{observer: queue, handlerQueue: handlerQueue}
	operations.observeHandlerQueueUsage()
	operations.observeQueuedIncoming(&Incoming{Kind: Event, enqueuedAt: time.Now().Add(-time.Millisecond)})
	got := make(map[string]Observation, 3)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(got) < 3 {
		select {
		case value := <-observer.values:
			got[value.Name+"/"+value.Kind] = value
		case <-deadline.C:
			t.Fatalf("queue observations = %#v", got)
		}
	}
	if value := got["queue_bytes/in_handler"]; value.Value != 23 || value.Code != Normal {
		t.Fatalf("queue byte gauge = %#v", value)
	}
	if value := got["queue_items/in_handler"]; value.Value != 1 || value.Code != Normal {
		t.Fatalf("queue item gauge = %#v", value)
	}
	for key, value := range got {
		if strings.HasPrefix(key, "queue_age/") {
			if !strings.HasPrefix(value.Kind, "in_event_le_") || value.Value != 1 || value.Code != Normal {
				t.Fatalf("queue age observation = %#v", value)
			}
			return
		}
	}
	t.Fatalf("queue age observation missing from %#v", got)
}

func TestPollingQueueUsageObservation(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 2)}
	queue := newObserverQueue(observer, 4, time.Millisecond)
	defer queue.Close()
	polling := newIncomingQueue(128, 2, nil)
	incoming := &Incoming{Payload: make([]byte, 5)}
	if !polling.Push(incoming, 37) {
		t.Fatal("could not seed polling queue")
	}
	defer incoming.Release()
	polling.observeUsage(queue)
	got := make(map[string]Observation, 2)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for len(got) < 2 {
		select {
		case value := <-observer.values:
			got[value.Name+"/"+value.Kind] = value
		case <-deadline.C:
			t.Fatalf("polling queue observations = %#v", got)
		}
	}
	if value := got["queue_bytes/in_polling"]; value.Value != 37 || value.Code != Normal {
		t.Fatalf("polling queue byte gauge = %#v", value)
	}
	if value := got["queue_items/in_polling"]; value.Value != 1 || value.Code != Normal {
		t.Fatalf("polling queue item gauge = %#v", value)
	}
}
