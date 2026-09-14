package testutil

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClockAndBarrier(t *testing.T) {
	clock := NewClock(time.Unix(100, 0))
	after := clock.After(time.Second)
	clock.Advance(999 * time.Millisecond)
	select {
	case <-after:
		t.Fatal("timer fired early")
	default:
	}
	clock.Advance(time.Millisecond)
	if got := <-after; !got.Equal(time.Unix(101, 0)) {
		t.Fatalf("timer = %v", got)
	}

	barrier := NewBarrier(2)
	done := make(chan error, 1)
	go func() { done <- barrier.Wait(context.Background()) }()
	select {
	case <-done:
		t.Fatal("barrier released early")
	default:
	}
	if err := barrier.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestFakeTransportFaultControlsAndCounters(t *testing.T) {
	fake := NewFakeTransport(4)
	fake.SetWritesBlocked(true)
	done := make(chan error, 1)
	go func() { done <- fake.SendDatagram(context.Background(), []byte{1}) }()
	<-fake.WriteStarted()
	fake.SetWritesBlocked(false)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if got, err := fake.NextSent(context.Background()); err != nil || got[0] != 1 {
		t.Fatalf("sent = %v, %v", got, err)
	}

	fake.DropDatagrams(1)
	if err := fake.SendDatagram(context.Background(), []byte{2}); err != nil {
		t.Fatal(err)
	}
	fake.SetReorder(true)
	_ = fake.SendDatagram(context.Background(), []byte{3})
	_ = fake.SendDatagram(context.Background(), []byte{4})
	fake.FlushReordered()
	if got, _ := fake.NextSent(context.Background()); got[0] != 4 {
		t.Fatalf("first reordered = %v", got)
	}
	if got, _ := fake.NextSent(context.Background()); got[0] != 3 {
		t.Fatalf("second reordered = %v", got)
	}

	if err := fake.Deliver([]byte{9}); err != nil {
		t.Fatal(err)
	}
	got, err := fake.ReceiveDatagram(context.Background())
	if err != nil || got.Payload[0] != 9 {
		t.Fatalf("received = %v, %v", got, err)
	}
	stop := fake.StartTask()
	stop()
	stop()
	if fake.LiveTasks() != 0 || !fake.Reserve(4) || fake.Reserve(1) {
		t.Fatal("counter accounting failed")
	}
	fake.Release(4)
	fake.Reset()
	_, err = fake.ReceiveDatagram(context.Background())
	if !errors.Is(err, ErrReset) {
		t.Fatalf("reset error = %v", err)
	}
}
