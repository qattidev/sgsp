package server

import (
	"context"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"qattidev/sgsp"
	"qattidev/sgsp/examples/pong/internal/dev"
	"qattidev/sgsp/examples/pong/internal/game"
	"qattidev/sgsp/examples/pong/internal/netclient"
	"qattidev/sgsp/examples/pong/internal/protocol"
	"qattidev/sgsp/internal/udprelay"
)

func startServer(t *testing.T) (dev.Material, string) {
	t.Helper()
	material, err := dev.Ensure(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	packet, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = packet.Close() })
	s, err := New(material, packet.LocalAddr().String(), nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, packet) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("server shutdown: %v", err)
			}
		case <-time.After(3 * time.Second):
			t.Error("server did not stop")
		}
	})
	return material, packet.LocalAddr().String()
}

func dial(t *testing.T, ctx context.Context, address string, material dev.Material) (*netclient.Client, protocol.JoinReply) {
	t.Helper()
	client, joined, err := netclient.Dial(ctx, address, material)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, joined
}

func snapshot(t *testing.T, ctx context.Context, c *netclient.Client, accept func(protocol.State) bool) protocol.State {
	t.Helper()
	for {
		select {
		case state := <-c.Updates:
			if accept(state) {
				return state
			}
		case <-ctx.Done():
			t.Fatalf("waiting for snapshot: %v", ctx.Err())
			return protocol.State{}
		}
	}
}

func sharedSnapshot(t *testing.T, ctx context.Context, a, b *netclient.Client, accept func(protocol.State) bool) protocol.State {
	t.Helper()
	var states [2]protocol.State
	for {
		select {
		case states[0] = <-a.Updates:
		case states[1] = <-b.Updates:
		case <-ctx.Done():
			t.Fatalf("waiting for shared snapshot: %v", ctx.Err())
			return protocol.State{}
		}
		if states[0].Tick != 0 && states[0].Tick == states[1].Tick && accept(states[0]) {
			if states[0] != states[1] {
				t.Fatalf("clients disagree at tick %d: %+v / %+v", states[0].Tick, states[0], states[1])
			}
			return states[0]
		}
	}
}

func TestTwoClientsPlayAndReplacePlayer(t *testing.T) {
	material, address := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	left, first := dial(t, ctx, address, material)
	if first.Side != protocol.Left || first.State.Playing() {
		t.Fatalf("first join: %+v", first)
	}
	right, second := dial(t, ctx, address, material)
	if second.Side != protocol.Right || !second.State.Playing() {
		t.Fatalf("second join: %+v", second)
	}
	again, err := sgsp.Call(ctx, left.Connection.Session(), protocol.Join, struct{}{})
	if err != nil || again.Side != first.Side {
		t.Fatalf("idempotent join: %+v, %v", again, err)
	}
	if left.Connection.Session().Principal().Subject == right.Connection.Session().Principal().Subject {
		t.Fatal("clients share an identity")
	}
	if err := left.Send(ctx, 0); err != nil {
		t.Fatal(err)
	}
	shared := sharedSnapshot(t, ctx, left, right, func(s protocol.State) bool { return s.Playing() && s.Paddles[0] < first.State.Paddles[0] && !s.Serving })
	if shared.Paddles[1] != second.State.Paddles[1] {
		t.Fatal("left player's input moved right paddle")
	}
	encoded, err := protocol.Snapshots.Codec.Encode(shared)
	if err != nil || len(encoded) > sgsp.DefaultLimits().DatagramBytes {
		t.Fatalf("snapshot exceeds datagram budget: %d, %v", len(encoded), err)
	}
	third, _, err := netclient.Dial(ctx, address, material)
	if third != nil {
		third.Close()
		t.Fatal("third player was accepted")
	}
	if !errors.Is(err, sgsp.ErrResourceExhausted) || !strings.Contains(err.Error(), "game full") {
		t.Fatalf("third player error: %v", err)
	}
	left.Close()
	paused := snapshot(t, ctx, right, func(s protocol.State) bool { return !s.Occupied[0] })
	later := snapshot(t, ctx, right, func(s protocol.State) bool { return s.Tick >= paused.Tick+6 })
	if later.Playing() || later.Ball != paused.Ball || later.Scores != paused.Scores {
		t.Fatal("game did not pause after player departure")
	}
	replacement, joined := dial(t, ctx, address, material)
	if joined.Side != protocol.Left || joined.State.Scores != paused.Scores {
		t.Fatalf("replacement join: %+v", joined)
	}
	sharedSnapshot(t, ctx, replacement, right, func(s protocol.State) bool { return s.Playing() })
}

func TestReconnectKeepsSlot(t *testing.T) {
	material, address := startServer(t)
	upstream, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		t.Fatal(err)
	}
	relay, err := udprelay.New("127.0.0.1:0", "127.0.0.1:0", upstream, udprelay.Config{Seed: 42})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = relay.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	left, joined := dial(t, ctx, relay.ClientAddr().String(), material)
	right, _ := dial(t, ctx, address, material)
	sharedSnapshot(t, ctx, left, right, func(s protocol.State) bool { return s.Playing() })
	id, epoch := left.Connection.Session().ID(), left.Connection.Session().Epoch()
	relay.SetBlackhole(udprelay.ClientToServer, true)
	relay.SetBlackhole(udprelay.ServerToClient, true)
	paused := snapshot(t, ctx, right, func(s protocol.State) bool { return !s.Connected[0] && s.Occupied[0] })
	later := snapshot(t, ctx, right, func(s protocol.State) bool { return s.Tick >= paused.Tick+6 })
	if later.Ball != paused.Ball || later.Scores != paused.Scores {
		t.Fatal("lost connection did not pause game")
	}
	third, _, err := netclient.Dial(ctx, address, material)
	if third != nil {
		third.Close()
	}
	if !errors.Is(err, sgsp.ErrResourceExhausted) {
		t.Fatalf("suspended slot was not reserved: %v", err)
	}
	relay.SetBlackhole(udprelay.ClientToServer, false)
	relay.SetBlackhole(udprelay.ServerToClient, false)
	sharedSnapshot(t, ctx, left, right, func(s protocol.State) bool { return s.Tick > later.Tick && s.Playing() })
	if left.Connection.Session().ID() != id || left.Connection.Session().Epoch() <= epoch {
		t.Fatal("client did not resume the same session with a new epoch")
	}
	again, err := sgsp.Call(ctx, left.Connection.Session(), protocol.Join, struct{}{})
	if err != nil || again.Side != joined.Side {
		t.Fatalf("resumed assignment: %+v, %v", again, err)
	}
	if err := left.Send(ctx, 0); err != nil {
		t.Fatal(err)
	}
	snapshot(t, ctx, right, func(s protocol.State) bool { return s.Paddles[0] < paused.Paddles[0] })
}

// Embedding the interface keeps this fake focused on the state/epoch boundary;
// any accidental network operation in applyInput would panic during the test.
type fakeSession struct {
	sgsp.Session
	id    sgsp.SessionID
	epoch uint64
	state sgsp.State
}

func (s *fakeSession) ID() sgsp.SessionID { return s.id }
func (s *fakeSession) Epoch() uint64      { return s.epoch }
func (s *fakeSession) State() sgsp.State  { return s.state }

func TestInputEpochFence(t *testing.T) {
	session := &fakeSession{id: sgsp.SessionID{1}, epoch: 2, state: sgsp.Active}
	s := &Server{game: game.New(), players: [2]*player{{session: session, epoch: 2}, nil}}
	s.game.SetPlayer(protocol.Left, true, true)
	s.game.SetPlayer(protocol.Right, true, true)
	before := s.game.State.Paddles
	s.applyInput(inputWork{session: session, epoch: 1, input: protocol.Input{Target: 0}})
	s.game.Step()
	if s.game.State.Paddles != before {
		t.Fatal("stale epoch moved paddle")
	}
	session.state = sgsp.Suspended
	s.applyInput(inputWork{session: session, epoch: 2, input: protocol.Input{Target: 0}})
	s.game.Step()
	if s.game.State.Paddles != before {
		t.Fatal("suspended session moved paddle")
	}
	session.state = sgsp.Active
	s.applyInput(inputWork{session: session, epoch: 2, input: protocol.Input{Target: 0}})
	s.game.Step()
	if s.game.State.Paddles[0] >= before[0] {
		t.Fatal("current epoch input was not applied")
	}
}

func TestSlowSnapshotConsumerDoesNotBlockSimulation(t *testing.T) {
	s := &Server{game: game.New()}
	for side := range s.players {
		s.players[side] = &player{session: &fakeSession{id: sgsp.SessionID{byte(side + 1)}, epoch: 1, state: sgsp.Active},
			epoch: 1, snapshots: make(chan snapshotWork, 1)}
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for range 100 {
			s.game.Step()
			s.broadcast()
		}
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("broadcast blocked behind a slow consumer")
	}
	for _, p := range s.players {
		if newest := <-p.snapshots; newest.state.Tick != 100 {
			t.Fatalf("queued stale tick %d", newest.state.Tick)
		}
	}
}
