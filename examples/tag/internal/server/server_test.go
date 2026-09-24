package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"qattidev/sgsp"
	"qattidev/sgsp/examples/tag/internal/bot"
	"qattidev/sgsp/examples/tag/internal/dev"
	"qattidev/sgsp/examples/tag/internal/netclient"
	"qattidev/sgsp/examples/tag/internal/protocol"
	"testing"
	"time"
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

func TestAutoMultiplayer120Hz(t *testing.T) {
	material, address := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	a, ja := dial(t, ctx, address, material)
	b, jb := dial(t, ctx, address, material)
	if ja.Side == jb.Side {
		t.Fatal("duplicate slots")
	}
	third, _, err := netclient.Dial(ctx, address, material)
	if third != nil {
		third.Close()
		t.Fatal("third player admitted")
	}
	if !errors.Is(err, sgsp.ErrResourceExhausted) {
		t.Fatal(err)
	}
	sharedSnapshot(t, ctx, a, b, func(s protocol.State) bool { return s.Playing() })
	states := [2]protocol.State{ja.State, jb.State}
	clients := [2]*netclient.Client{a, b}
	controllers := [2]bot.Controller{}
	tick := time.NewTicker(time.Second / 120)
	defer tick.Stop()
	start := time.Now()
	counts := [2]int{}
	seen := map[uint64]protocol.State{}
	matched := 0
	for time.Since(start) < 8*time.Second {
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case s := <-a.Updates:
			states[0] = s
			counts[0]++
			seen[s.Tick] = s
		case s := <-b.Updates:
			states[1] = s
			counts[1]++
			if other, ok := seen[s.Tick]; ok {
				if other != s {
					t.Fatal("shared state differs")
				}
				matched++
			}
		case <-tick.C:
			for i, c := range clients {
				if err := c.Send(ctx, controllers[i].Input(states[i], protocol.Side(i))); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
	for i, s := range states {
		hz := float64(counts[i]) / time.Since(start).Seconds()
		t.Logf("client %d: %.1f updates/s, server %.1f Hz, scores %v", i, hz, s.Hz, s.Scores)
		if hz < 100 || s.Hz < 110 || s.Hz > 130 {
			t.Fatal("120 Hz loop not observed")
		}
		if s.Scores[0]+s.Scores[1] < 2 {
			t.Fatal("tags not frequent")
		}
	}
	if matched < 100 {
		t.Fatal("insufficient identical snapshots")
	}
	a.Close()
	paused := snapshot(t, ctx, b, func(s protocol.State) bool { return !s.Occupied[0] })
	later := snapshot(t, ctx, b, func(s protocol.State) bool { return s.Tick > paused.Tick+10 })
	if paused.Positions != later.Positions {
		t.Fatal("disconnected game moved")
	}
	_, replacement := dial(t, ctx, address, material)
	if replacement.Side != ja.Side {
		t.Fatal("slot not reusable")
	}
}

func TestManualAndAutoCombinations(t *testing.T) {
	for _, modes := range [][2]bool{{false, false}, {false, true}, {true, false}} {
		t.Run(fmt.Sprint(modes), func(t *testing.T) {
			material, address := startServer(t)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			a, ja := dial(t, ctx, address, material)
			b, jb := dial(t, ctx, address, material)
			controllers := [2]bot.Controller{}
			inputs := [2]protocol.Input{{X: 1}, {Y: -1}}
			states := [2]protocol.State{ja.State, jb.State}
			for i := range 2 {
				if modes[i] {
					inputs[i] = controllers[i].Input(states[i], protocol.Side(i))
				}
			}
			if err := a.Send(ctx, inputs[0]); err != nil {
				t.Fatal(err)
			}
			if err := b.Send(ctx, inputs[1]); err != nil {
				t.Fatal(err)
			}
			shared := sharedSnapshot(t, ctx, a, b, func(s protocol.State) bool {
				return s.Auto == modes && s.Positions[0] != ja.State.Positions[0] && s.Positions[1] != jb.State.Positions[1]
			})
			if !shared.Playing() {
				t.Fatal("not playing")
			}
		})
	}
}
