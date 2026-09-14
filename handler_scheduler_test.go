package sgsp

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"qattidev/sgsp/internal/runtime"
	"qattidev/sgsp/internal/wire"
)

func TestHandlerSessionsShareApplicationBudget(t *testing.T) {
	limits := DefaultLimits()
	limits.QueueBytes = 1024
	limits.QueueMessages = 4
	scheduler := newHandlerScheduler(2, 2)
	defer scheduler.close()
	budget := runtime.NewBudget(64)
	leftStarted := make(chan struct{})
	releaseLeft := make(chan struct{})
	rightStarted := make(chan struct{})
	router := NewRouter()
	if err := router.OnEvent(1, func(_ context.Context, incoming *Incoming) {
		if incoming.Session.ID()[0] == 1 {
			close(leftStarted)
			<-releaseLeft
			return
		}
		close(rightStarted)
	}); err != nil {
		t.Fatal(err)
	}
	left := scheduledOperations(limits, scheduler, router, 1)
	right := scheduledOperations(limits, scheduler, router, 2)
	left.applicationBudget, right.applicationBudget = budget, budget
	if !left.dispatch(&Incoming{Kind: Event, Session: left.session, Epoch: 1, Type: 1, Payload: make([]byte, 16), ctx: left.context()}) {
		t.Fatal("first session could not acquire global budget")
	}
	select {
	case <-leftStarted:
	case <-time.After(time.Second):
		t.Fatal("first callback did not start")
	}
	if right.dispatch(&Incoming{Kind: Event, Session: right.session, Epoch: 1, Type: 1, Payload: []byte("x"), ctx: right.context()}) {
		t.Fatal("second session exceeded the shared application budget")
	}
	close(releaseLeft)
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for !right.dispatch(&Incoming{Kind: Event, Session: right.session, Epoch: 1, Type: 1, Payload: []byte("x"), ctx: right.context()}) {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatal("budget was not released when handler work completed")
		}
	}
	select {
	case <-rightStarted:
	case <-time.After(time.Second):
		t.Fatal("second session did not progress after budget release")
	}
	if used := budget.Used(); used != 0 {
		t.Fatalf("application budget retained %d bytes", used)
	}
}

func TestPollingReliableQueueWaitsForRelease(t *testing.T) {
	global := runtime.NewBudget(64)
	queue := newIncomingQueue(64, 1, global)
	first := &Incoming{Kind: Event, Payload: []byte("x")}
	if !queue.Push(first, incomingCharge(first)) {
		t.Fatal("first polling item was rejected")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	second := &Incoming{Kind: Event, Payload: []byte("y")}
	if err := queue.PushWait(ctx, second, incomingCharge(second)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("full polling queue error = %v, want deadline exceeded", err)
	}
	item, err := queue.Next(context.Background())
	if err != nil {
		t.Fatalf("first Next = %v", err)
	}
	item.Release()
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := queue.PushWait(ctx, second, incomingCharge(second)); err != nil {
		t.Fatalf("polling queue did not resume after Release: %v", err)
	}
	item, err = queue.Next(context.Background())
	if err != nil {
		t.Fatalf("second Next = %v", err)
	}
	item.Release()
	if used := global.Used(); used != 0 {
		t.Fatalf("global polling budget retained %d bytes", used)
	}
}

func TestReliableHandlerQueueWaitsOnlyThroughSlowConsumerTimeout(t *testing.T) {
	limits := DefaultLimits()
	limits.QueueMessages = 1
	limits.QueueBytes = 1024
	limits.SlowConsumerTimeout = 25 * time.Millisecond
	scheduler := newHandlerScheduler(1, 1)
	defer scheduler.close()
	started := make(chan struct{})
	release := make(chan struct{})
	router := NewRouter()
	if err := router.OnEvent(1, func(_ context.Context, incoming *Incoming) {
		if string(incoming.Payload) == "first" {
			close(started)
			<-release
		}
	}); err != nil {
		t.Fatal(err)
	}
	operations := scheduledOperations(limits, scheduler, router, 1)
	first := wire.Event{Channel: 1, MessageType: 1, Payload: []byte("first")}
	if err := operations.deliverReliableEvent(first); err != nil {
		t.Fatalf("first reliable delivery = %v", err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first reliable callback did not start")
	}
	if err := operations.deliverReliableEvent(wire.Event{Channel: 1, MessageType: 1, Payload: []byte("queued")}); err != nil {
		t.Fatalf("queued reliable delivery = %v", err)
	}
	if err := operations.deliverReliableEvent(wire.Event{Channel: 1, MessageType: 1, Payload: []byte("overflow")}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("overflow reliable delivery = %v, want deadline exceeded", err)
	}
	close(release)
}

// Scheduler tests use a real session record but no transport: they exercise
// only queue ownership and callback scheduling.
func TestHandlerSchedulerSerialPerSessionAndFairAcrossSessions(t *testing.T) {
	limits := DefaultLimits()
	scheduler := newHandlerScheduler(2, 2)
	defer scheduler.close()

	var leftCalls atomic.Int32
	var leftActive atomic.Int32
	var leftMax atomic.Int32
	leftStarted := make(chan struct{})
	leftSecond := make(chan struct{})
	rightStarted := make(chan struct{})
	releaseLeft := make(chan struct{})
	router := NewRouter()
	if err := router.OnEvent(1, func(_ context.Context, incoming *Incoming) {
		if incoming.Session.ID()[0] == 1 {
			active := leftActive.Add(1)
			for {
				maximum := leftMax.Load()
				if active <= maximum || leftMax.CompareAndSwap(maximum, active) {
					break
				}
			}
			defer leftActive.Add(-1)
			if leftCalls.Add(1) == 1 {
				close(leftStarted)
				<-releaseLeft
				return
			}
			close(leftSecond)
			return
		}
		close(rightStarted)
	}); err != nil {
		t.Fatal(err)
	}
	left := scheduledOperations(limits, scheduler, router, 1)
	right := scheduledOperations(limits, scheduler, router, 2)
	if !left.dispatch(&Incoming{Kind: Event, Session: left.session, Epoch: 1, Type: 1, Payload: []byte("first"), ctx: left.context()}) {
		t.Fatal("first left event was rejected")
	}
	select {
	case <-leftStarted:
	case <-time.After(time.Second):
		t.Fatal("first left callback did not start")
	}
	if !left.dispatch(&Incoming{Kind: Event, Session: left.session, Epoch: 1, Type: 1, Payload: []byte("second"), ctx: left.context()}) {
		t.Fatal("second left event was rejected")
	}
	if !right.dispatch(&Incoming{Kind: Event, Session: right.session, Epoch: 1, Type: 1, Payload: []byte("other"), ctx: right.context()}) {
		t.Fatal("right event was rejected")
	}
	select {
	case <-rightStarted:
	case <-time.After(time.Second):
		t.Fatal("a different session did not progress while left was blocked")
	}
	select {
	case <-leftSecond:
		t.Fatal("callbacks for one session overlapped")
	default:
	}
	close(releaseLeft)
	select {
	case <-leftSecond:
	case <-time.After(time.Second):
		t.Fatal("second left callback did not run")
	}
	if leftMax.Load() != 1 {
		t.Fatalf("same-session callback overlap = %d", leftMax.Load())
	}
}

func TestHandlerSchedulerCoalescesQueuedSequencedEvents(t *testing.T) {
	limits := DefaultLimits()
	scheduler := newHandlerScheduler(1, 1)
	defer scheduler.close()
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	delivered := make(chan string, 2)
	router := NewRouter()
	if err := router.OnEvent(1, func(_ context.Context, incoming *Incoming) {
		if string(incoming.Payload) == "block" {
			close(firstStarted)
			<-releaseFirst
			return
		}
		delivered <- string(incoming.Payload)
	}); err != nil {
		t.Fatal(err)
	}
	operations := scheduledOperations(limits, scheduler, router, 1)
	if !operations.dispatch(&Incoming{Kind: Event, Session: operations.session, Epoch: 1, Type: 1, Payload: []byte("block"), ctx: operations.context()}) {
		t.Fatal("blocking event was rejected")
	}
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("blocking callback did not start")
	}
	for _, payload := range []string{"one", "three", "four"} {
		if !operations.dispatch(&Incoming{Kind: Event, Session: operations.session, Epoch: 1, Type: 1, Channel: 3, Delivery: UnreliableSequenced, Payload: []byte(payload), ctx: operations.context()}) {
			t.Fatalf("sequenced event %q was rejected", payload)
		}
	}
	close(releaseFirst)
	select {
	case got := <-delivered:
		if got != "four" {
			t.Fatalf("queued sequenced delivery = %q, want four", got)
		}
	case <-time.After(time.Second):
		t.Fatal("replacement event was not dispatched")
	}
	select {
	case extra := <-delivered:
		t.Fatalf("stale queued event was dispatched: %q", extra)
	case <-time.After(25 * time.Millisecond):
	}
}

func TestHandlerSchedulerDeliversTerminalAfterLogicalClose(t *testing.T) {
	limits := DefaultLimits()
	scheduler := newHandlerScheduler(1, 1)
	defer scheduler.close()
	lifecycle := make(chan *Lifecycle, 1)
	after := make(chan struct{})
	router := NewRouter()
	if err := router.OnLifecycle(func(_ context.Context, incoming *Incoming) {
		lifecycle <- incoming.Lifecycle
	}); err != nil {
		t.Fatal(err)
	}
	operations := scheduledOperations(limits, scheduler, router, 1)
	operations.onTerminal = func() { close(after) }
	operations.remoteClose(Normal)
	select {
	case got := <-lifecycle:
		if got == nil || got.Kind != SessionEnded || got.Reason != Normal {
			t.Fatalf("terminal lifecycle = %#v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("terminal lifecycle was not delivered")
	}
	select {
	case <-after:
	case <-time.After(time.Second):
		t.Fatal("terminal completion callback did not follow lifecycle delivery")
	}
}

func scheduledOperations(limits Limits, scheduler *handlerScheduler, router *Router, marker byte) *connectionOperations {
	operations := &connectionOperations{mode: Handlers, scheduler: scheduler}
	var id SessionID
	id[0] = marker
	session := newSessionRecord(id, Owner{}, "", Principal{ExpiresAt: time.Now().Add(time.Minute)}, limits, false, operations)
	operations.session, operations.router, operations.epoch = session, router, 1
	operations.startEpoch()
	return operations
}
