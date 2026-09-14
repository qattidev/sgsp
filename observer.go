package sgsp

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// observerQueue aggregates bounded-label observations off the gameplay path,
// then flushes them through a bounded queue to one possibly slow host task.
// A slow observer loses observations, never gameplay messages.
type observerQueue struct {
	observer Observer
	queue    chan Observation
	cancel   context.CancelFunc
	done     chan struct{}
	dropped  atomic.Uint64
	once     sync.Once
	mu       sync.Mutex
	pending  map[observationKey]float64
	interval time.Duration
}

type observationKey struct {
	name, kind string
	code       Code
}

func newObserverQueue(observer Observer, capacity int, interval time.Duration) *observerQueue {
	if observer == nil {
		return nil
	}
	if capacity < 1 {
		capacity = 1
	}
	if interval <= 0 {
		interval = time.Second
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := &observerQueue{observer: observer, queue: make(chan Observation, capacity), cancel: cancel, done: make(chan struct{}), pending: make(map[observationKey]float64), interval: interval}
	go func() {
		defer close(result.done)
		for {
			select {
			case observation := <-result.queue:
				result.observer.Observe(observation)
			case <-ctx.Done():
				return
			}
		}
	}()
	go result.flush(ctx)
	return result
}
func (q *observerQueue) Observe(observation Observation) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.pending[observationKey{name: observation.Name, kind: observation.Kind, code: observation.Code}] += observation.Value
	q.mu.Unlock()
}
func (q *observerQueue) Dropped() uint64 {
	if q == nil {
		return 0
	}
	return q.dropped.Load()
}
func (q *observerQueue) Close() {
	if q == nil {
		return
	}
	// Host callbacks may ignore cancellation. Do not make session shutdown wait
	// forever for observer code that SGSP cannot preempt.
	q.once.Do(q.cancel)
}

func (q *observerQueue) flush(ctx context.Context) {
	ticker := time.NewTicker(q.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			q.flushOnce()
		case <-ctx.Done():
			return
		}
	}
}
func (q *observerQueue) flushOnce() {
	q.mu.Lock()
	pending := q.pending
	q.pending = make(map[observationKey]float64)
	q.mu.Unlock()
	for key, value := range pending {
		select {
		case q.queue <- Observation{Name: key.name, Kind: key.kind, Code: key.code, Value: value}:
		default:
			q.dropped.Add(1)
		}
	}
}
