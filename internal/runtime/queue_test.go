package runtime

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestQueueBoundsAndWait(t *testing.T) {
	queue := NewQueue[int](3, 2)
	if !queue.TryPush(Item[int]{Value: 1, Bytes: 2}) || queue.TryPush(Item[int]{Value: 2, Bytes: 2}) {
		t.Fatal("byte bound was not enforced")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := queue.Push(ctx, Item[int]{Value: 2, Bytes: 2}); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting push = %v", err)
	}
	item, ok := queue.Pop()
	if !ok || item.Value != 1 {
		t.Fatal("unexpected pop")
	}
	if !queue.TryPush(Item[int]{Value: 2, Bytes: 2}) {
		t.Fatal("released capacity was not reusable")
	}
}

func TestQueueSequencedReplacement(t *testing.T) {
	queue := NewQueue[string](5, 2)
	if ok, replaced := queue.Replace(Item[string]{Key: 1, Value: "one", Bytes: 3}); !ok || replaced {
		t.Fatal("initial insert failed")
	}
	if ok, replaced := queue.Replace(Item[string]{Key: 1, Value: "too-large", Bytes: 6}); ok || !replaced {
		t.Fatal("oversized replacement should retain old item")
	}
	item, ok := queue.Pop()
	if !ok || item.Value != "one" {
		t.Fatalf("old item was not retained: %#v", item)
	}
	if ok, replaced := queue.Replace(Item[string]{Key: 1, Value: "three", Bytes: 2}); !ok || replaced {
		t.Fatal("replacement after pop failed")
	}
	if ok, replaced := queue.Replace(Item[string]{Key: 1, Value: "four", Bytes: 4}); !ok || !replaced {
		t.Fatal("in-place replacement failed")
	}
	item, _ = queue.Pop()
	if item.Value != "four" {
		t.Fatalf("replacement = %#v", item)
	}
}

func TestQueueWaitPop(t *testing.T) {
	queue := NewQueue[string](10, 1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result := make(chan Item[string], 1)
	go func() {
		item, err := queue.WaitPop(ctx)
		if err != nil {
			t.Error(err)
			return
		}
		result <- item
	}()
	if !queue.TryPush(Item[string]{Value: "ready", Bytes: 1}) {
		t.Fatal("push failed")
	}
	if item := <-result; item.Value != "ready" {
		t.Fatalf("item = %#v", item)
	}
}

func TestQueueRemoveRetainsOtherItems(t *testing.T) {
	queue := NewQueue[int](10, 3)
	for _, value := range []int{1, 2, 3} {
		if !queue.TryPush(Item[int]{Value: value, Bytes: 1}) {
			t.Fatalf("push %d failed", value)
		}
	}
	removed := queue.Remove(func(item Item[int]) bool { return item.Value%2 != 0 })
	if len(removed) != 2 || removed[0].Value != 1 || removed[1].Value != 3 {
		t.Fatalf("removed = %#v", removed)
	}
	if items, bytes := queue.Len(); items != 1 || bytes != 1 {
		t.Fatalf("remaining queue = %d items, %d bytes", items, bytes)
	}
	item, ok := queue.Pop()
	if !ok || item.Value != 2 {
		t.Fatalf("remaining item = %#v", item)
	}
}

func TestQueueReservationHoldsCapacityUntilCommitOrCancel(t *testing.T) {
	queue := NewQueue[string](4, 2)
	reservation := queue.TryReserve(3)
	if reservation == nil {
		t.Fatal("reserve failed")
	}
	if queue.TryPush(Item[string]{Value: "too-large", Bytes: 2}) {
		t.Fatal("push consumed reserved bytes")
	}
	if !reservation.Commit(Item[string]{Value: "reserved", Bytes: 3}) {
		t.Fatal("commit failed")
	}
	item, ok := queue.Pop()
	if !ok || item.Value != "reserved" {
		t.Fatalf("committed item = %#v", item)
	}
	reservation = queue.TryReserve(4)
	if reservation == nil {
		t.Fatal("second reserve failed")
	}
	reservation.Cancel()
	if !queue.TryPush(Item[string]{Value: "after-cancel", Bytes: 4}) {
		t.Fatal("cancel did not release capacity")
	}
}
