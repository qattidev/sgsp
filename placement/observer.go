package placement

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"qattidev/sgsp"
)

// observerQueue keeps placement observation delivery out of Resolve and store
// calls. Its labels are internal fixed strings and sanitized SGSP codes, so a
// host-provided store cannot create an unbounded metric cardinality.
type observerQueue struct {
	observer sgsp.Observer
	queue    chan sgsp.Observation
	cancel   context.CancelFunc
	dropped  atomic.Uint64
	once     sync.Once

	mu      sync.Mutex
	pending map[observationKey]float64
}

type observationKey struct {
	kind string
	code sgsp.Code
}

func newObserverQueue(observer sgsp.Observer) *observerQueue {
	if observer == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	queue := &observerQueue{
		observer: observer,
		queue:    make(chan sgsp.Observation, 4096),
		cancel:   cancel,
		pending:  make(map[observationKey]float64),
	}
	go func() {
		for {
			select {
			case observation := <-queue.queue:
				queue.observer.Observe(observation)
			case <-ctx.Done():
				return
			}
		}
	}()
	go queue.flush(ctx)
	return queue
}

func (q *observerQueue) Observe(kind string, code sgsp.Code, value float64) {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.pending[observationKey{kind: kind, code: code}] += value
	q.mu.Unlock()
}

func (q *observerQueue) Dropped() uint64 {
	if q == nil {
		return 0
	}
	return q.dropped.Load()
}

// Close stops aggregate flushing and returns without waiting on a host
// observer that ignores cancellation.
func (q *observerQueue) Close() {
	if q != nil {
		q.once.Do(q.cancel)
	}
}

func (q *observerQueue) flush(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
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
		case q.queue <- sgsp.Observation{Name: "placement", Kind: key.kind, Code: key.code, Value: value}:
		default:
			q.dropped.Add(1)
		}
	}
}

func placementHistogramKind(kind string, duration time.Duration) string {
	if duration < 0 {
		duration = 0
	}
	for bucket := int64(1); bucket <= 524_288; bucket <<= 1 {
		if duration <= time.Duration(bucket)*time.Microsecond {
			return kind + "_le_" + strconv.FormatInt(bucket, 10) + "us"
		}
	}
	if duration <= time.Second {
		return kind + "_le_1s"
	}
	return kind + "_overflow"
}
