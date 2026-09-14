// Package runtime provides bounded scheduling primitives shared by sessions.
package runtime

import (
	"context"
	"sync"
)

// Item carries the allocation charge owned by a bounded queue. Key is used by
// Replace to coalesce replaceable (sequenced) work without losing an accepted
// older item when a larger replacement cannot fit.
type Item[T any] struct {
	Value T
	Bytes int
	Key   uint64
}

type Queue[T any] struct {
	mu                           sync.Mutex
	items                        []Item[T]
	maxBytes                     int
	maxItems                     int
	bytes                        int
	reservedBytes, reservedItems int
	closed                       bool
	notify                       chan struct{}
}

// Reservation holds one queue item and its byte charge before its value has
// been allocated. It is single-use: Commit transfers the held capacity to an
// item, while Cancel returns it to waiting producers.
type Reservation[T any] struct {
	queue *Queue[T]
	bytes int
	mu    sync.Mutex
	done  bool
}

func NewQueue[T any](maxBytes, maxItems int) *Queue[T] {
	if maxBytes < 0 || maxItems < 0 {
		panic("runtime: negative queue limit")
	}
	return &Queue[T]{maxBytes: maxBytes, maxItems: maxItems, notify: make(chan struct{}, 1)}
}

func (q *Queue[T]) TryPush(item Item[T]) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || item.Bytes < 0 || len(q.items)+q.reservedItems >= q.maxItems || item.Bytes > q.maxBytes-q.bytes-q.reservedBytes {
		return false
	}
	q.items = append(q.items, item)
	q.bytes += item.Bytes
	q.signal()
	return true
}

// TryReserve claims capacity without accepting a value. It lets framed
// readers wait before allocating a declared body rather than allocating first
// and discovering a full application queue afterwards.
func (q *Queue[T]) TryReserve(bytes int) *Reservation[T] {
	if q == nil || bytes < 0 {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || len(q.items)+q.reservedItems >= q.maxItems || bytes > q.maxBytes-q.bytes-q.reservedBytes {
		return nil
	}
	q.reservedItems++
	q.reservedBytes += bytes
	return &Reservation[T]{queue: q, bytes: bytes}
}

// Reserve waits for capacity to hold one future item. Callers must Commit or
// Cancel the returned reservation exactly once.
func (q *Queue[T]) Reserve(ctx context.Context, bytes int) (*Reservation[T], error) {
	for {
		if reservation := q.TryReserve(bytes); reservation != nil {
			return reservation, nil
		}
		q.mu.Lock()
		closed := q.closed
		notify := q.notify
		q.mu.Unlock()
		if closed {
			return nil, context.Canceled
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-notify:
		}
	}
}

// Commit transfers the held capacity to item. A closed queue rejects the
// item and releases the reservation.
func (r *Reservation[T]) Commit(item Item[T]) bool {
	if r == nil || item.Bytes != r.bytes {
		return false
	}
	return r.finish(item, true)
}

// Cancel releases capacity held by a reservation. It is safe to call after a
// Commit or a previous Cancel.
func (r *Reservation[T]) Cancel() {
	if r == nil {
		return
	}
	var zero Item[T]
	_ = r.finish(zero, false)
}

func (r *Reservation[T]) finish(item Item[T], commit bool) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.done || r.queue == nil {
		return false
	}
	r.done = true
	q := r.queue
	q.mu.Lock()
	defer q.mu.Unlock()
	q.reservedItems--
	q.reservedBytes -= r.bytes
	if commit && !q.closed {
		q.items = append(q.items, item)
		q.bytes += item.Bytes
		q.signal()
		return true
	}
	q.signal()
	return false
}

// Push waits only for local capacity. A false result means the queue closed;
// context cancellation is returned unchanged so callers preserve their own
// deadline/cancellation semantics.
func (q *Queue[T]) Push(ctx context.Context, item Item[T]) error {
	for {
		if q.TryPush(item) {
			return nil
		}
		q.mu.Lock()
		closed := q.closed
		notify := q.notify
		q.mu.Unlock()
		if closed {
			return context.Canceled
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
		}
	}
}

func (q *Queue[T]) Pop() (Item[T], bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.items) == 0 {
		var zero Item[T]
		return zero, false
	}
	item := q.items[0]
	copy(q.items, q.items[1:])
	var zero Item[T]
	q.items[len(q.items)-1] = zero
	q.items = q.items[:len(q.items)-1]
	q.bytes -= item.Bytes
	q.signal()
	return item, true
}

// Remove discards every item selected by keep out of the queue and returns
// them to the caller. The caller remains responsible for releasing any
// resources owned by the returned values. It is used for epoch cleanup where
// a shared polling queue must retain work belonging to other sessions.
func (q *Queue[T]) Remove(remove func(Item[T]) bool) []Item[T] {
	if q == nil || remove == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	items := q.items
	kept := items[:0]
	bytes := 0
	var removed []Item[T]
	for _, item := range items {
		if remove(item) {
			removed = append(removed, item)
			continue
		}
		kept = append(kept, item)
		bytes += item.Bytes
	}
	for index := len(kept); index < len(items); index++ {
		var zero Item[T]
		items[index] = zero
	}
	q.items, q.bytes = kept, bytes
	if len(removed) > 0 {
		q.signal()
	}
	return removed
}

// WaitPop blocks until an item becomes available, the queue closes, or ctx
// ends. The item remains charged until Pop removes it, so callers must not
// decode by retaining an alias after their owning layer releases it.
func (q *Queue[T]) WaitPop(ctx context.Context) (Item[T], error) {
	for {
		if item, ok := q.Pop(); ok {
			return item, nil
		}
		q.mu.Lock()
		closed := q.closed
		notify := q.notify
		q.mu.Unlock()
		if closed {
			var zero Item[T]
			return zero, context.Canceled
		}
		select {
		case <-ctx.Done():
			var zero Item[T]
			return zero, ctx.Err()
		case <-notify:
		}
	}
}

// Replace inserts a new keyed item or atomically replaces the existing keyed
// item. If a replacement cannot acquire the extra budget, the old item stays.
func (q *Queue[T]) Replace(item Item[T]) (accepted, replaced bool) {
	accepted, _, replaced = q.ReplaceWith(item)
	return accepted, replaced
}

// ReplaceWith has the same admission semantics as Replace and, when it
// succeeds at replacing an existing item, returns that item to its owner for
// prompt resource release. A failed larger replacement leaves the old item
// untouched and does not return it.
func (q *Queue[T]) ReplaceWith(item Item[T]) (accepted bool, previous Item[T], replaced bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || item.Bytes < 0 {
		return false, previous, false
	}
	for i, old := range q.items {
		if old.Key != item.Key {
			continue
		}
		if item.Bytes > q.maxBytes-(q.bytes-old.Bytes)-q.reservedBytes {
			return false, previous, true
		}
		q.items[i] = item
		q.bytes += item.Bytes - old.Bytes
		q.signal()
		return true, old, true
	}
	if len(q.items)+q.reservedItems >= q.maxItems || item.Bytes > q.maxBytes-q.bytes-q.reservedBytes {
		return false, previous, false
	}
	q.items = append(q.items, item)
	q.bytes += item.Bytes
	q.signal()
	return true, previous, false
}

func (q *Queue[T]) Close() { q.mu.Lock(); q.closed = true; q.signal(); q.mu.Unlock() }
func (q *Queue[T]) Len() (items, bytes int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items), q.bytes
}
func (q *Queue[T]) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
