// Package testutil provides deterministic test-only clocks, barriers, and
// transports. Production packages must not depend on it.
package testutil

import (
	"context"
	"sync"
	"time"
)

// Clock advances only when Advance is called. It is safe for concurrent tests.
type Clock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []clockWaiter
}

type clockWaiter struct {
	at time.Time
	ch chan time.Time
}

func NewClock(at time.Time) *Clock { return &Clock{now: at} }

func (c *Clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *Clock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	c.waiters = append(c.waiters, clockWaiter{at: c.now.Add(d), ch: ch})
	return ch
}

func (c *Clock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	ready := make([]clockWaiter, 0, len(c.waiters))
	pending := c.waiters[:0]
	for _, waiter := range c.waiters {
		if !waiter.at.After(c.now) {
			ready = append(ready, waiter)
		} else {
			pending = append(pending, waiter)
		}
	}
	c.waiters = pending
	now := c.now
	c.mu.Unlock()
	for _, waiter := range ready {
		waiter.ch <- now
		close(waiter.ch)
	}
}

// Barrier blocks callers until the configured number of arrivals is reached.
// It is one-shot; construct a new barrier for each deterministic phase.
type Barrier struct {
	mu      sync.Mutex
	needed  int
	arrived int
	done    chan struct{}
}

func NewBarrier(participants int) *Barrier {
	if participants < 1 {
		panic("testutil: barrier needs at least one participant")
	}
	return &Barrier{needed: participants, done: make(chan struct{})}
}

func (b *Barrier) Wait(ctx context.Context) error {
	b.mu.Lock()
	if b.arrived < b.needed {
		b.arrived++
		if b.arrived == b.needed {
			close(b.done)
		}
	}
	done := b.done
	b.mu.Unlock()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
