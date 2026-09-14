package runtime

import (
	"context"
	"sync"
)

// Budget is a process-wide allocation ledger. It deliberately tracks only
// library-owned bytes; callers must charge before copying a payload and release
// exactly once when its envelope leaves the library.
type Budget struct {
	mu        sync.Mutex
	max, used int64
	notify    chan struct{}
}

func NewBudget(max int64) *Budget {
	if max < 0 {
		panic("runtime: negative budget")
	}
	return &Budget{max: max, notify: make(chan struct{}, 1)}
}
func (b *Budget) Acquire(bytes int64) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if bytes < 0 || bytes > b.max-b.used {
		return false
	}
	b.used += bytes
	return true
}
func (b *Budget) Release(bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if bytes < 0 || bytes > b.used {
		panic("runtime: invalid budget release")
	}
	b.used -= bytes
	b.signal()
}
func (b *Budget) Used() int64 { b.mu.Lock(); defer b.mu.Unlock(); return b.used }
func (b *Budget) Max() int64  { return b.max }

// Wait blocks until a release may have made capacity available. Callers must
// retry Acquire after it returns because other waiters can win the capacity.
func (b *Budget) Wait(ctx context.Context) error {
	if b == nil {
		return context.Canceled
	}
	b.mu.Lock()
	notify := b.notify
	b.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-notify:
		return nil
	}
}
func (b *Budget) signal() {
	select {
	case b.notify <- struct{}{}:
	default:
	}
}
