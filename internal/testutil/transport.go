package testutil

import (
	"context"
	"errors"
	"sync"

	"qattidev/sgsp/internal/transport"
)

var ErrReset = errors.New("testutil: transport reset")

// FakeTransport is a deterministic, bounded adapter test double. Tests drive
// each fault independently: write blocking, delivery, reset, datagram loss,
// and reordering. Its counters make leak and budget assertions observable.
type FakeTransport struct {
	mu            sync.Mutex
	closed        bool
	writesBlocked bool
	dropNext      int
	reorder       bool
	pending       [][]byte
	inbound       chan []byte
	sent          chan []byte
	writeStarted  chan struct{}
	writeGate     chan struct{}
	reset         chan struct{}
	tasks         int
	budget        int64
	maxBudget     int64
}

func NewFakeTransport(maxBudget int64) *FakeTransport {
	return &FakeTransport{
		inbound: make(chan []byte, 128), sent: make(chan []byte, 128),
		writeStarted: make(chan struct{}, 1), writeGate: make(chan struct{}), reset: make(chan struct{}),
		maxBudget: maxBudget,
	}
}

func (f *FakeTransport) SendDatagram(ctx context.Context, payload []byte) error {
	f.mu.Lock()
	blocked, closed, gate, reset := f.writesBlocked, f.closed, f.writeGate, f.reset
	f.mu.Unlock()
	if closed {
		return ErrReset
	}
	if blocked {
		select {
		case f.writeStarted <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-reset:
			return ErrReset
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	copyPayload := append([]byte(nil), payload...)
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed {
		return ErrReset
	}
	if f.dropNext > 0 {
		f.dropNext--
		return nil
	}
	if f.reorder {
		f.pending = append(f.pending, copyPayload)
		return nil
	}
	select {
	case f.sent <- copyPayload:
		return nil
	default:
		return errors.New("testutil: sent queue full")
	}
}

func (f *FakeTransport) ReceiveDatagram(ctx context.Context) (transport.Datagram, error) {
	select {
	case payload := <-f.inbound:
		return transport.Datagram{Payload: payload}, nil
	case <-f.reset:
		return transport.Datagram{}, ErrReset
	case <-ctx.Done():
		return transport.Datagram{}, ctx.Err()
	}
}

func (f *FakeTransport) Close() error { f.Reset(); return nil }

func (f *FakeTransport) SetWritesBlocked(blocked bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.closed || f.writesBlocked == blocked {
		return
	}
	f.writesBlocked = blocked
	if blocked {
		f.writeGate = make(chan struct{})
		return
	}
	close(f.writeGate)
}
func (f *FakeTransport) WriteStarted() <-chan struct{} { return f.writeStarted }
func (f *FakeTransport) DropDatagrams(count int)       { f.mu.Lock(); f.dropNext += count; f.mu.Unlock() }
func (f *FakeTransport) SetReorder(reorder bool)       { f.mu.Lock(); f.reorder = reorder; f.mu.Unlock() }

func (f *FakeTransport) FlushReordered() {
	f.mu.Lock()
	pending := f.pending
	f.pending = nil
	f.mu.Unlock()
	for i := len(pending) - 1; i >= 0; i-- {
		f.sent <- pending[i]
	}
}

func (f *FakeTransport) Deliver(payload []byte) error {
	f.mu.Lock()
	closed := f.closed
	f.mu.Unlock()
	if closed {
		return ErrReset
	}
	copyPayload := append([]byte(nil), payload...)
	select {
	case f.inbound <- copyPayload:
		return nil
	default:
		return errors.New("testutil: inbound queue full")
	}
}
func (f *FakeTransport) NextSent(ctx context.Context) ([]byte, error) {
	select {
	case payload := <-f.sent:
		return payload, nil
	case <-f.reset:
		return nil, ErrReset
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (f *FakeTransport) Reset() {
	f.mu.Lock()
	if !f.closed {
		f.closed = true
		close(f.reset)
		if f.writesBlocked {
			close(f.writeGate)
		}
	}
	f.mu.Unlock()
}

// StartTask increments the live task count and returns its idempotent stop function.
func (f *FakeTransport) StartTask() func() {
	f.mu.Lock()
	f.tasks++
	f.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { f.mu.Lock(); f.tasks--; f.mu.Unlock() }) }
}
func (f *FakeTransport) Reserve(bytes int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	if bytes < 0 || f.budget+bytes > f.maxBudget {
		return false
	}
	f.budget += bytes
	return true
}
func (f *FakeTransport) Release(bytes int64) { f.mu.Lock(); f.budget -= bytes; f.mu.Unlock() }
func (f *FakeTransport) LiveTasks() int      { f.mu.Lock(); defer f.mu.Unlock(); return f.tasks }
func (f *FakeTransport) Budget() int64       { f.mu.Lock(); defer f.mu.Unlock(); return f.budget }
