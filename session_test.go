package sgsp

import (
	"context"
	"sync"
	"testing"
	"time"
)

type recordingOperations struct {
	sent       []byte
	credential []byte
	closed     Code
}

func (o *recordingOperations) send(_ context.Context, _ MessageType, payload []byte, _ SendOptions) error {
	o.sent = append([]byte(nil), payload...)
	return nil
}
func (*recordingOperations) call(_ context.Context, _ MessageType, payload []byte) ([]byte, error) {
	return append([]byte("reply:"), payload...), nil
}
func (*recordingOperations) openStream(context.Context, MessageType) (Stream, error) {
	return nil, ErrUnsupportedMessage
}
func (o *recordingOperations) refresh(_ context.Context, credential Credential) error {
	o.credential = append([]byte(nil), credential.Data...)
	return nil
}
func (o *recordingOperations) close(_ context.Context, code Code) error { o.closed = code; return nil }
func (*recordingOperations) stats() Stats {
	return Stats{RTT: time.Millisecond, TransportStatsAvailable: true}
}

func TestSessionRecordOwnershipAndLifecycle(t *testing.T) {
	operations := &recordingOperations{}
	principal := Principal{Issuer: "issuer", Subject: "subject", Attributes: map[string]string{"role": "player"}}
	session := newSessionRecord(SessionID{1}, Owner{ID: "owner"}, "group", principal, DefaultLimits(), true, operations)
	payload := []byte("input")
	if err := session.Send(context.Background(), 1, payload, SendOptions{Delivery: ReliableOrdered}); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	if string(operations.sent) != "input" {
		t.Fatalf("operation saw mutable payload %q", operations.sent)
	}
	copyPrincipal := session.Principal()
	copyPrincipal.Attributes["role"] = "admin"
	if session.Principal().Attributes["role"] != "player" {
		t.Fatal("principal attributes were not copied")
	}
	credential := Credential{Scheme: "test", Data: []byte("secret")}
	if err := session.RefreshAuth(context.Background(), credential); err != nil {
		t.Fatal(err)
	}
	credential.Data[0] = 'X'
	if string(operations.credential) != "secret" {
		t.Fatal("credential was not copied")
	}
	if err := session.Close(context.Background(), Normal); err != nil {
		t.Fatal(err)
	}
	if operations.closed != Normal || session.State() != Closed {
		t.Fatal("close transition failed")
	}
	select {
	case <-session.Context().Done():
	default:
		t.Fatal("logical context was not canceled")
	}
	if err := session.Send(context.Background(), 1, nil, SendOptions{}); err != ErrSessionClosed {
		t.Fatalf("send after close = %v", err)
	}
}

func TestSessionRecordRejectsInvalidCalls(t *testing.T) {
	session := newSessionRecord(SessionID{}, Owner{}, "", Principal{}, DefaultLimits(), true, &recordingOperations{})
	if err := session.Close(context.Background(), SessionSuperseded); err != ErrInvalidArgument {
		t.Fatalf("reserved close code = %v", err)
	}
	if err := session.Send(context.Background(), 0, nil, SendOptions{}); err != ErrInvalidArgument {
		t.Fatalf("zero type = %v", err)
	}
	if err := session.Send(context.Background(), 1, make([]byte, DefaultLimits().DatagramBytes+1), SendOptions{Delivery: Unreliable}); err == nil {
		t.Fatal("oversized datagram accepted")
	}
	session.setState(Suspended, 2)
	if _, err := session.Call(context.Background(), 1, nil); err != ErrSessionSuspended {
		t.Fatalf("suspended call = %v", err)
	}
	serverSession := newSessionRecord(SessionID{}, Owner{}, "", Principal{}, DefaultLimits(), false, &recordingOperations{})
	if err := serverSession.RefreshAuth(context.Background(), Credential{}); err != ErrWrongMode {
		t.Fatalf("server refresh = %v", err)
	}
}

func TestConcurrentResume(t *testing.T) {
	principal := Principal{Issuer: "issuer", Subject: "subject", ExpiresAt: time.Now().Add(time.Minute)}
	initial := &connectionOperations{}
	session := newSessionRecord(SessionID{1}, Owner{ID: "owner"}, "", principal, DefaultLimits(), false, initial)
	initial.session, initial.epoch = session, 1

	const candidates = 100
	start := make(chan struct{})
	results := make(chan *connectionOperations, candidates)
	errs := make(chan error, candidates)
	var workers sync.WaitGroup
	for range candidates {
		workers.Add(1)
		go func() {
			defer workers.Done()
			operations := &connectionOperations{session: session}
			<-start
			previous, _, err := session.replaceConnection(operations, principal)
			if err != nil {
				errs <- err
				return
			}
			if previous == nil {
				errs <- ErrSessionNotFound
				return
			}
			results <- operations
		}()
	}
	close(start)
	workers.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatalf("resume commit failed: %v", err)
	}
	committed := make(map[*connectionOperations]struct{}, candidates)
	for operations := range results {
		committed[operations] = struct{}{}
	}
	if len(committed) != candidates || session.Epoch() != candidates+1 || session.State() != Active {
		t.Fatalf("commits/epoch/state = %d/%d/%v", len(committed), session.Epoch(), session.State())
	}
	session.mu.RLock()
	current, ok := session.operations.(*connectionOperations)
	session.mu.RUnlock()
	if !ok || current == nil {
		t.Fatal("final resume did not install an operations attachment")
	}
	if _, exists := committed[current]; !exists || !session.isCurrent(current) {
		t.Fatal("more than one resume attachment can appear current")
	}
	for operations := range committed {
		if operations != current && session.isCurrent(operations) {
			t.Fatal("superseded resume attachment remained current")
		}
	}
}
