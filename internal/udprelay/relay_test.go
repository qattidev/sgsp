package udprelay

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

func TestRelayForwardsAndBlackholesDirections(t *testing.T) {
	upstream, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	go func() {
		buffer := make([]byte, 256)
		for {
			count, source, err := upstream.ReadFrom(buffer)
			if err != nil {
				return
			}
			_, _ = upstream.WriteTo(append([]byte("echo:"), buffer[:count]...), source)
		}
	}()
	relay, err := New("127.0.0.1:0", "127.0.0.1:0", upstream.LocalAddr(), Config{RTT: 4 * time.Millisecond, Seed: 7})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	client, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.WriteTo([]byte("first"), relay.ClientAddr()); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, 256)
	if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	count, _, err := client.ReadFrom(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if got := string(buffer[:count]); got != "echo:first" {
		t.Fatalf("relay response = %q", got)
	}
	relay.SetBlackhole(ClientToServer, true)
	if _, err := client.WriteTo([]byte("lost"), relay.ClientAddr()); err != nil {
		t.Fatal(err)
	}
	if err := client.SetReadDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := client.ReadFrom(buffer); err == nil {
		t.Fatal("blackholed packet was forwarded")
	} else if network, ok := err.(net.Error); !ok || !network.Timeout() {
		t.Fatalf("blackhole read error = %v", err)
	}
	if stats := relay.Stats(); stats.Forwarded < 2 || stats.Dropped < 1 || stats.PendingPackets != 0 || stats.PendingBytes != 0 {
		t.Fatalf("relay stats = %#v", stats)
	}
}

func TestRelayRebindsServerFacingSourcePort(t *testing.T) {
	upstream, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	sources := make(chan string, 2)
	go func() {
		buffer := make([]byte, 256)
		for {
			count, source, err := upstream.ReadFrom(buffer)
			if err != nil {
				return
			}
			sources <- source.String()
			_, _ = upstream.WriteTo(buffer[:count], source)
		}
	}()
	relay, err := New("127.0.0.1:0", "127.0.0.1:0", upstream.LocalAddr(), Config{Seed: 9})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	client, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	exchange := func(payload string) string {
		t.Helper()
		if _, err := client.WriteTo([]byte(payload), relay.ClientAddr()); err != nil {
			t.Fatal(err)
		}
		if err := client.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		buffer := make([]byte, 256)
		if _, _, err := client.ReadFrom(buffer); err != nil {
			t.Fatal(err)
		}
		select {
		case source := <-sources:
			return source
		case <-time.After(time.Second):
			t.Fatal("upstream did not observe forwarded packet")
			return ""
		}
	}
	first := exchange("first")
	if err := relay.RebindUpstream("127.0.0.1:0"); err != nil {
		t.Fatal(err)
	}
	second := exchange("second")
	if first == second {
		t.Fatalf("rebind retained server-facing source %q", first)
	}
}

func TestRelayCloseRacesRebind(t *testing.T) {
	upstream, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	relay, err := New("127.0.0.1:0", "127.0.0.1:0", upstream.LocalAddr(), Config{Seed: 19})
	if err != nil {
		t.Fatal(err)
	}
	start := make(chan struct{})
	rebound := make(chan error, 1)
	closed := make(chan error, 1)
	go func() {
		<-start
		rebound <- relay.RebindUpstream("127.0.0.1:0")
	}()
	go func() {
		<-start
		closed <- relay.Close()
	}()
	close(start)
	select {
	case err := <-rebound:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("rebind racing Close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("rebind racing Close did not return")
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not return")
	}
}

func TestRelayReportsBoundedHarnessOverload(t *testing.T) {
	upstream, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer upstream.Close()
	relay, err := New("127.0.0.1:0", "127.0.0.1:0", upstream.LocalAddr(), Config{RTT: time.Second, Seed: 1, MaxPackets: 1, MaxBytes: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer relay.Close()
	client, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.WriteTo([]byte("one"), relay.ClientAddr()); err != nil {
		t.Fatal(err)
	}
	if _, err := client.WriteTo([]byte("two"), relay.ClientAddr()); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for !relay.Stats().Overloaded {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatal("relay did not report scheduled-packet overload")
		}
	}
	if !errors.Is(relay.Err(), ErrHarnessOverload) {
		t.Fatalf("relay error = %v", relay.Err())
	}
}
