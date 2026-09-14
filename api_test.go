package sgsp

import (
	"context"
	"errors"
	"testing"
)

func TestTypedCodecs(t *testing.T) {
	router := NewRouter()
	typed := Message[int]{ID: 10, Codec: JSON[int]()}
	seen := 0
	if err := OnMessage(router, typed, func(_ context.Context, _ Session, value int) error { seen = value; return nil }); err != nil {
		t.Fatal(err)
	}
	payload, err := typed.Codec.Encode(7)
	if err != nil {
		t.Fatal(err)
	}
	incoming := &Incoming{Kind: Event, Type: typed.ID, Payload: payload}
	handler := router.handler(incoming)
	if handler == nil {
		t.Fatal("typed route was not registered")
	}
	handler(context.Background(), incoming)
	if seen != 7 {
		t.Fatalf("decoded value = %d", seen)
	}
	if err := router.OnEvent(typed.ID, func(context.Context, *Incoming) {}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("duplicate route = %v", err)
	}

	request := Request[int, string]{ID: 11, Input: JSON[int](), Output: JSON[string]()}
	if err := OnCall(router, request, func(_ context.Context, _ Session, value int) (string, error) { return "ok", nil }); err != nil {
		t.Fatal(err)
	}
	requestPayload, _ := request.Input.Encode(3)
	var response []byte
	call := &Incoming{Kind: RequestMessage, Type: request.ID, Payload: requestPayload, reply: func(_ context.Context, value []byte) error { response = append([]byte(nil), value...); return nil }, fail: func(context.Context, Code, string) error { t.Fatal("unexpected failure"); return nil }}
	router.handler(call)(context.Background(), call)
	decoded, err := request.Output.Decode(response)
	if err != nil || decoded != "ok" {
		t.Fatalf("response = %q, %v", decoded, err)
	}
	if err := call.Reply(context.Background(), nil); !errors.Is(err, ErrSessionClosed) {
		t.Fatalf("second reply = %v", err)
	}
}

func TestIncomingReleaseAndResponseValidation(t *testing.T) {
	called := 0
	incoming := &Incoming{Kind: RequestMessage, Payload: []byte("payload"), release: func() { called++ }, reply: func(context.Context, []byte) error { return nil }, fail: func(context.Context, Code, string) error { return nil }}
	incoming.Release()
	incoming.Release()
	if called != 1 || incoming.Payload != nil {
		t.Fatal("Release was not idempotent")
	}
	if err := incoming.Fail(context.Background(), Normal, ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("normal failure = %v", err)
	}
	if err := (&Incoming{Kind: Event}).Reply(context.Background(), nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("event reply = %v", err)
	}
}

func TestRouterSnapshotIsIndependent(t *testing.T) {
	router := NewRouter()
	if err := router.OnEvent(1, func(context.Context, *Incoming) {}); err != nil {
		t.Fatal(err)
	}
	snapshot := router.snapshot()
	if err := router.OnEvent(2, func(context.Context, *Incoming) {}); err != nil {
		t.Fatal(err)
	}
	if snapshot.handler(&Incoming{Kind: Event, Type: 1}) == nil {
		t.Fatal("snapshot lost existing route")
	}
	if snapshot.handler(&Incoming{Kind: Event, Type: 2}) != nil {
		t.Fatal("snapshot observed post-construction route")
	}
}
