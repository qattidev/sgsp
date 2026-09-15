package sgsp

// This file is intentionally test-only. M3 exercises public session semantics
// without creating a public unauthenticated endpoint; M4 wires equivalent
// records only after TLS/HELLO/authentication admission succeeds.

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"qattidev/sgsp/internal/runtime"
)

type acceptedSession struct {
	mu         sync.Mutex
	dispatchMu sync.Mutex
	id         SessionID
	owner      Owner
	principal  Principal
	ctx        context.Context
	cancel     context.CancelFunc
	state      State
	epoch      uint64
	attachment any
	peer       *acceptedSession
	router     *Router
	mode       DispatchMode
	queue      *runtime.Queue[*Incoming]
	hold       bool
	sequences  map[ChannelID]uint64
	received   map[ChannelID]uint64
}

func newAcceptedPair(t *testing.T, leftRouter, rightRouter *Router, limits Limits) (*acceptedSession, *acceptedSession) {
	t.Helper()
	if limits == (Limits{}) {
		limits = DefaultLimits()
	}
	leftCtx, leftCancel := context.WithCancel(context.Background())
	rightCtx, rightCancel := context.WithCancel(context.Background())
	left := &acceptedSession{ctx: leftCtx, cancel: leftCancel, state: Active, epoch: 1, router: leftRouter, mode: Handlers, queue: runtime.NewQueue[*Incoming](limits.QueueBytes, limits.QueueMessages), sequences: map[ChannelID]uint64{}, received: map[ChannelID]uint64{}}
	right := &acceptedSession{ctx: rightCtx, cancel: rightCancel, state: Active, epoch: 1, router: rightRouter, mode: Handlers, queue: runtime.NewQueue[*Incoming](limits.QueueBytes, limits.QueueMessages), sequences: map[ChannelID]uint64{}, received: map[ChannelID]uint64{}}
	left.peer, right.peer = right, left
	return left, right
}

func (s *acceptedSession) ID() SessionID    { return s.id }
func (s *acceptedSession) Owner() Owner     { return s.owner }
func (s *acceptedSession) GroupKey() string { return "" }
func (s *acceptedSession) State() State     { s.mu.Lock(); defer s.mu.Unlock(); return s.state }
func (s *acceptedSession) Epoch() uint64    { s.mu.Lock(); defer s.mu.Unlock(); return s.epoch }
func (s *acceptedSession) Principal() Principal {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.principal
	out.Attributes = mapsClone(out.Attributes)
	return out
}
func (s *acceptedSession) Context() context.Context { return s.ctx }
func (s *acceptedSession) Attachment() any          { s.mu.Lock(); defer s.mu.Unlock(); return s.attachment }
func (s *acceptedSession) SetAttachment(value any)  { s.mu.Lock(); s.attachment = value; s.mu.Unlock() }
func mapsClone(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}

func (s *acceptedSession) Send(ctx context.Context, typ MessageType, payload []byte, options SendOptions) error {
	return s.send(ctx, typ, payload, options, false)
}
func (s *acceptedSession) TrySend(typ MessageType, payload []byte, options SendOptions) error {
	return s.send(context.Background(), typ, payload, options, true)
}
func (s *acceptedSession) send(ctx context.Context, typ MessageType, payload []byte, options SendOptions, immediate bool) error {
	if typ == 0 || len(payload) > DefaultLimits().MessageBytes {
		return ErrInvalidArgument
	}
	if s.State() != Active {
		return ErrSessionClosed
	}
	copyPayload := append([]byte(nil), payload...)
	if immediate {
		if !s.peer.enqueueEvent(typ, copyPayload, options) {
			return ErrBackpressure
		}
		return nil
	}
	if s.peer.enqueueEvent(typ, copyPayload, options) {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}
func (s *acceptedSession) enqueueEvent(typ MessageType, payload []byte, options SendOptions) bool {
	s.mu.Lock()
	if s.state != Active {
		s.mu.Unlock()
		return false
	}
	sequence := uint64(0)
	if options.Delivery == UnreliableSequenced {
		s.received[options.Channel]++
		sequence = s.received[options.Channel]
	}
	incoming := &Incoming{Kind: Event, Session: s, Epoch: s.epoch, Type: typ, Channel: options.Channel, Delivery: options.Delivery, Sequence: sequence, Payload: payload, ctx: s.ctx}
	hold := s.hold
	s.mu.Unlock()
	item := runtime.Item[*Incoming]{Value: incoming, Bytes: len(payload) + 32}
	if options.Delivery == UnreliableSequenced {
		item.Key = uint64(options.Channel) + 1
		accepted, _ := s.queue.Replace(item)
		if !accepted {
			return false
		}
	} else if !s.queue.TryPush(item) {
		return false
	}
	if !hold && s.mode == Handlers {
		s.dispatch()
	}
	return true
}
func (s *acceptedSession) dispatch() {
	s.dispatchMu.Lock()
	defer s.dispatchMu.Unlock()
	for {
		item, ok := s.queue.Pop()
		if !ok {
			return
		}
		incoming := item.Value
		s.invoke(incoming)
	}
}
func (s *acceptedSession) invoke(incoming *Incoming) {
	defer incoming.Release()
	defer func() {
		if recover() != nil {
			_ = s.Close(context.Background(), Internal)
		}
	}()
	handler := s.router.handler(incoming)
	if handler == nil {
		if incoming.Kind == RequestMessage {
			_ = incoming.Fail(s.ctx, UnsupportedMessage, "unsupported message")
		}
		return
	}
	handler(incoming.Context(), incoming)
	if incoming.Kind == RequestMessage && !incoming.responded {
		_ = incoming.Fail(s.ctx, Internal, "handler returned without response")
	}
}
func (s *acceptedSession) Call(ctx context.Context, typ MessageType, payload []byte) ([]byte, error) {
	if typ == 0 || s.State() != Active {
		return nil, ErrSessionClosed
	}
	result := make(chan struct {
		payload []byte
		err     error
	}, 1)
	incoming := &Incoming{Kind: RequestMessage, Session: s.peer, Epoch: s.peer.Epoch(), Type: typ, Payload: append([]byte(nil), payload...), ctx: s.peer.Context()}
	incoming.reply = func(_ context.Context, response []byte) error {
		result <- struct {
			payload []byte
			err     error
		}{append([]byte(nil), response...), nil}
		return nil
	}
	incoming.fail = func(_ context.Context, code Code, message string) error {
		result <- struct {
			payload []byte
			err     error
		}{nil, &Error{Code: code, Message: message}}
		return nil
	}
	if !s.peer.queue.TryPush(runtime.Item[*Incoming]{Value: incoming, Bytes: len(payload) + 32}) {
		return nil, ErrBackpressure
	}
	if s.peer.mode == Handlers {
		// The accepted-session fixture models the transport handoff; production
		// dispatch uses the bounded worker scheduler introduced with endpoints.
		go s.peer.dispatch()
	}
	select {
	case response := <-result:
		return response.payload, response.err
	case <-ctx.Done():
		return nil, &Error{Code: OutcomeUnknown, OutcomeUnknown: true, Cause: ctx.Err()}
	}
}
func (s *acceptedSession) OpenStream(_ context.Context, typ MessageType) (Stream, error) {
	if typ == 0 || s.State() != Active {
		return nil, ErrSessionClosed
	}
	local, remote := newTestStreamPair()
	incoming := &Incoming{Kind: StreamMessage, Session: s.peer, Epoch: s.peer.Epoch(), Type: typ, Stream: remote, ctx: s.peer.Context()}
	if !s.peer.queue.TryPush(runtime.Item[*Incoming]{Value: incoming, Bytes: 32}) {
		_ = local.Close()
		_ = remote.Close()
		return nil, ErrBackpressure
	}
	if s.peer.mode == Handlers {
		s.peer.dispatch()
	}
	return local, nil
}
func (s *acceptedSession) RefreshAuth(context.Context, Credential) error { return ErrWrongMode }
func (s *acceptedSession) Close(context.Context, Code) error {
	s.mu.Lock()
	if s.state != Closed {
		s.state = Closed
		s.cancel()
	}
	s.mu.Unlock()
	return nil
}
func (s *acceptedSession) Stats() Stats {
	_, bytes := s.queue.Len()
	return Stats{SendQueuedBytes: int64(bytes)}
}
func (s *acceptedSession) Next(ctx context.Context) (*Incoming, error) {
	if s.mode != Polling {
		return nil, ErrWrongMode
	}
	item, err := s.queue.WaitPop(ctx)
	if err != nil {
		return nil, err
	}
	return item.Value, nil
}

type testStream struct {
	reader *io.PipeReader
	writer *io.PipeWriter
	once   sync.Once
}

func newTestStreamPair() (*testStream, *testStream) {
	leftRead, rightWrite := io.Pipe()
	rightRead, leftWrite := io.Pipe()
	return &testStream{reader: leftRead, writer: leftWrite}, &testStream{reader: rightRead, writer: rightWrite}
}
func (s *testStream) Read(payload []byte) (int, error)  { return s.reader.Read(payload) }
func (s *testStream) Write(payload []byte) (int, error) { return s.writer.Write(payload) }
func (s *testStream) CloseWrite() error                 { return s.writer.Close() }
func (s *testStream) CloseRead() error                  { return s.reader.Close() }
func (s *testStream) Close() error {
	var err error
	s.once.Do(func() {
		err = s.writer.Close()
		if closeErr := s.reader.Close(); err == nil {
			err = closeErr
		}
	})
	return err
}
func (*testStream) SetDeadline(time.Time) error { return nil }
func (s *testStream) Abort(Code)                { _ = s.Close() }

func TestSendOwnership(t *testing.T) {
	received := make(chan []byte, 1)
	router := NewRouter()
	if err := router.OnEvent(1, func(_ context.Context, incoming *Incoming) { received <- append([]byte(nil), incoming.Payload...) }); err != nil {
		t.Fatal(err)
	}
	client, _ := newAcceptedPair(t, nil, router, DefaultLimits())
	payload := []byte("before")
	if err := client.Send(context.Background(), 1, payload, SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	copy(payload, "after!")
	if got := string(<-received); got != "before" {
		t.Fatalf("receiver observed %q", got)
	}
}

func TestBidirectionalRequests(t *testing.T) {
	leftRouter, rightRouter := NewRouter(), NewRouter()
	if err := leftRouter.OnRequest(1, func(_ context.Context, in *Incoming) {
		_ = in.Reply(context.Background(), append([]byte("l:"), in.Payload...))
	}); err != nil {
		t.Fatal(err)
	}
	if err := rightRouter.OnRequest(1, func(_ context.Context, in *Incoming) {
		_ = in.Reply(context.Background(), append([]byte("r:"), in.Payload...))
	}); err != nil {
		t.Fatal(err)
	}
	left, right := newAcceptedPair(t, leftRouter, rightRouter, DefaultLimits())
	leftResult := make(chan string, 1)
	rightResult := make(chan string, 1)
	go func() {
		response, err := left.Call(context.Background(), 1, []byte("x"))
		if err != nil {
			t.Error(err)
		}
		leftResult <- string(response)
	}()
	go func() {
		response, err := right.Call(context.Background(), 1, []byte("y"))
		if err != nil {
			t.Error(err)
		}
		rightResult <- string(response)
	}()
	if got := <-leftResult; got != "r:x" {
		t.Fatalf("left response = %q", got)
	}
	if got := <-rightResult; got != "l:y" {
		t.Fatalf("right response = %q", got)
	}
}

func TestQueueBackpressure(t *testing.T) {
	limits := DefaultLimits()
	limits.QueueBytes = 82
	limits.QueueMessages = 2
	client, server := newAcceptedPair(t, nil, NewRouter(), limits)
	server.mu.Lock()
	server.hold = true
	server.mu.Unlock()
	if err := client.TrySend(1, []byte("123456789"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	if err := client.TrySend(1, []byte("123456789"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	if err := client.TrySend(1, []byte("123456789"), SendOptions{Delivery: ReliableOrdered}); !errors.Is(err, ErrBackpressure) {
		t.Fatalf("TrySend = %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Millisecond)
	defer cancel()
	if err := client.Send(ctx, 1, []byte("123456789"), SendOptions{Delivery: ReliableOrdered}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send = %v", err)
	}
}

func TestSequencedCoalescing(t *testing.T) {
	seen := make(chan string, 1)
	router := NewRouter()
	if err := router.OnEvent(1, func(_ context.Context, in *Incoming) { seen <- string(in.Payload) }); err != nil {
		t.Fatal(err)
	}
	client, server := newAcceptedPair(t, nil, router, DefaultLimits())
	server.mu.Lock()
	server.hold = true
	server.mu.Unlock()
	for _, payload := range []string{"one", "two", "three"} {
		if err := client.TrySend(1, []byte(payload), SendOptions{Channel: 1, Delivery: UnreliableSequenced}); err != nil {
			t.Fatal(err)
		}
	}
	server.mu.Lock()
	server.hold = false
	server.mu.Unlock()
	server.dispatch()
	if got := <-seen; got != "three" {
		t.Fatalf("coalesced event = %q", got)
	}
}

func TestReceiveOwnership(t *testing.T) {
	client, server := newAcceptedPair(t, nil, nil, DefaultLimits())
	server.mode = Polling
	payload := []byte("borrowed")
	if err := client.Send(context.Background(), 1, payload, SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	incoming, err := server.Next(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	copyPayload := append([]byte(nil), incoming.Payload...)
	incoming.Release()
	incoming.Release()
	if string(copyPayload) != "borrowed" || incoming.Payload != nil {
		t.Fatal("polling ownership was not enforced")
	}
}

func TestWrongDispatchMode(t *testing.T) {
	client, _ := newAcceptedPair(t, nil, nil, DefaultLimits())
	if _, err := client.Next(context.Background()); !errors.Is(err, ErrWrongMode) {
		t.Fatalf("handler-mode Next = %v", err)
	}
}

func TestCustomStreamDelivery(t *testing.T) {
	streams := make(chan Stream, 1)
	router := NewRouter()
	if err := router.OnStream(9, func(_ context.Context, incoming *Incoming) { streams <- incoming.Stream }); err != nil {
		t.Fatal(err)
	}
	client, _ := newAcceptedPair(t, nil, router, DefaultLimits())
	local, err := client.OpenStream(context.Background(), 9)
	if err != nil {
		t.Fatal(err)
	}
	remote := <-streams
	written := make(chan error, 1)
	go func() {
		_, err := local.Write([]byte("raw bytes"))
		if err == nil {
			err = local.CloseWrite()
		}
		written <- err
	}()
	got, err := io.ReadAll(remote)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if string(got) != "raw bytes" {
		t.Fatalf("stream payload = %q", got)
	}
}

func TestUnknownRoutes(t *testing.T) {
	unknown := make(chan IncomingKind, 1)
	router := NewRouter()
	if err := router.OnUnknown(func(_ context.Context, incoming *Incoming) { unknown <- incoming.Kind }); err != nil {
		t.Fatal(err)
	}
	client, _ := newAcceptedPair(t, nil, router, DefaultLimits())
	if err := client.Send(context.Background(), 99, []byte("event"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	if kind := <-unknown; kind != Event {
		t.Fatalf("fallback kind = %v", kind)
	}

	client, _ = newAcceptedPair(t, nil, NewRouter(), DefaultLimits())
	if _, err := client.Call(context.Background(), 99, nil); !errors.Is(err, ErrUnsupportedMessage) {
		t.Fatalf("unknown request = %v", err)
	}
}

func TestUnknownRequestOutcomeAcceptedSession(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	router := NewRouter()
	if err := router.OnRequest(3, func(context.Context, *Incoming) { close(started); <-release }); err != nil {
		t.Fatal(err)
	}
	client, _ := newAcceptedPair(t, nil, router, DefaultLimits())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := client.Call(ctx, 3, []byte("operation")); result <- err }()
	<-started
	err := <-result
	var protocol *Error
	if !errors.As(err, &protocol) || !protocol.OutcomeUnknown || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Call error = %#v", err)
	}
	close(release)
}

func TestPreSendFailure(t *testing.T) {
	limits := DefaultLimits()
	limits.QueueBytes, limits.QueueMessages = 82, 2
	client, server := newAcceptedPair(t, nil, nil, limits)
	server.mode = Polling
	if err := client.TrySend(1, []byte("123456789"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	if err := client.TrySend(1, []byte("123456789"), SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	_, err := client.Call(context.Background(), 1, []byte("request"))
	var protocol *Error
	if errors.As(err, &protocol) && protocol.OutcomeUnknown {
		t.Fatalf("pre-send failure reported an unknown outcome: %#v", err)
	}
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("pre-send failure = %v", err)
	}
}

func TestHandlerPanic(t *testing.T) {
	panicRouter := NewRouter()
	if err := panicRouter.OnEvent(1, func(context.Context, *Incoming) { panic("game bug") }); err != nil {
		t.Fatal(err)
	}
	client, affected := newAcceptedPair(t, nil, panicRouter, DefaultLimits())
	if err := client.Send(context.Background(), 1, nil, SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	if affected.State() != Closed {
		t.Fatal("panic did not close only the affected session")
	}

}

func TestDispatchOrdering(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	serialRouter := NewRouter()
	if err := serialRouter.OnEvent(1, func(context.Context, *Incoming) { entered <- struct{}{}; <-release }); err != nil {
		t.Fatal(err)
	}
	left, _ := newAcceptedPair(t, nil, serialRouter, DefaultLimits())
	firstDone := make(chan error, 1)
	go func() { firstDone <- left.Send(context.Background(), 1, nil, SendOptions{Delivery: ReliableOrdered}) }()
	<-entered
	secondDone := make(chan error, 1)
	go func() { secondDone <- left.Send(context.Background(), 1, nil, SendOptions{Delivery: ReliableOrdered}) }()
	select {
	case <-entered:
		t.Fatal("callbacks overlapped within one session")
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err := <-secondDone; err != nil {
		t.Fatal(err)
	}
}
