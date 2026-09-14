package sgsp

import (
	"context"
	"sync"
)

// handlerScheduler is an endpoint-owned, bounded worker pool. A scheduled
// connection owns one ready token at a time, so workers can run different
// sessions concurrently without overlapping callbacks for any one session.
type handlerScheduler struct {
	ctx    context.Context
	cancel context.CancelFunc
	ready  chan *connectionOperations

	mu        sync.Mutex
	accepting bool
	work      sync.WaitGroup
	workers   sync.WaitGroup
	closeOnce sync.Once
}

func newHandlerScheduler(workerCount, maxReady int) *handlerScheduler {
	if workerCount < 1 {
		workerCount = 1
	}
	if maxReady < 1 {
		maxReady = 1
	}
	ctx, cancel := context.WithCancel(context.Background())
	scheduler := &handlerScheduler{
		ctx:       ctx,
		cancel:    cancel,
		ready:     make(chan *connectionOperations, maxReady),
		accepting: true,
	}
	for range workerCount {
		scheduler.workers.Add(1)
		go scheduler.run()
	}
	return scheduler
}

// enqueue transfers a session's single scheduling token to the ready queue.
// The queue is bounded to the endpoint admission limit, so a full queue is
// backpressure instead of hidden goroutine or memory growth.
func (s *handlerScheduler) enqueue(operations *connectionOperations) bool {
	if s == nil || operations == nil {
		return false
	}
	s.mu.Lock()
	if !s.accepting {
		s.mu.Unlock()
		return false
	}
	s.work.Add(1)
	s.mu.Unlock()
	select {
	case s.ready <- operations:
		return true
	case <-s.ctx.Done():
		s.work.Done()
		return false
	default:
		s.work.Done()
		return false
	}
}

func (s *handlerScheduler) run() {
	defer s.workers.Done()
	for {
		select {
		case <-s.ctx.Done():
			return
		case operations := <-s.ready:
			if operations == nil {
				continue
			}
			operations.runHandlerQuantum(s)
		}
	}
}

// close rejects new scheduling tokens but lets already queued callbacks drain.
// A host callback can ignore cancellation, so callers intentionally do not
// wait forever for the worker goroutines here.
func (s *handlerScheduler) close() {
	if s == nil {
		return
	}
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.accepting = false
		s.mu.Unlock()
		go func() {
			s.work.Wait()
			s.cancel()
		}()
	})
}
