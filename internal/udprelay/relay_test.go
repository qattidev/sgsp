package udprelay

import (
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
