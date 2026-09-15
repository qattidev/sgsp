package sgsp

import (
	"sync"
	"testing"
	"time"
)

type blockingObserver struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (o *blockingObserver) Observe(Observation) { o.once.Do(func() { close(o.started) }); <-o.release }

func TestSlowObserver(t *testing.T) {
	observer := &blockingObserver{started: make(chan struct{}), release: make(chan struct{})}
	queue := newObserverQueue(observer, 1, time.Millisecond)
	defer queue.Close()
	queue.Observe(Observation{Name: "first"})
	select {
	case <-observer.started:
	case <-time.After(time.Second):
		t.Fatal("observer did not receive first flush")
	}
	queue.Observe(Observation{Name: "queued"})
	queue.Observe(Observation{Name: "dropped"})
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for queue.Dropped() == 0 {
		select {
		case <-time.After(time.Millisecond):
		case <-deadline.C:
			t.Fatal("slow observer did not drop a bounded flush")
		}
	}
	close(observer.release)
}

type recordingObserver struct{ values chan Observation }

func (o *recordingObserver) Observe(value Observation) { o.values <- value }

func TestObserverAggregation(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 1)}
	queue := newObserverQueue(observer, 4, time.Millisecond)
	defer queue.Close()
	queue.Observe(Observation{Name: "messages", Kind: "in", Value: 2})
	queue.Observe(Observation{Name: "messages", Kind: "in", Value: 3})
	select {
	case value := <-observer.values:
		if value.Name != "messages" || value.Kind != "in" || value.Value != 5 {
			t.Fatalf("aggregated observation = %#v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("aggregate was not flushed")
	}
}

func TestHistogramKindUsesFixedBoundedBuckets(t *testing.T) {
	for duration, want := range map[time.Duration]string{
		0:                             "latency_le_1us",
		time.Microsecond:              "latency_le_1us",
		1500 * time.Nanosecond:        "latency_le_2us",
		2 * time.Microsecond:          "latency_le_2us",
		3 * time.Microsecond:          "latency_le_4us",
		524288 * time.Microsecond:     "latency_le_524288us",
		time.Second:                   "latency_le_1s",
		time.Second + time.Nanosecond: "latency_overflow",
	} {
		if got := histogramKind("latency", duration); got != want {
			t.Fatalf("histogramKind(%s) = %q, want %q", duration, got, want)
		}
	}
}

func TestObserveDurationAggregatesHistogramBucket(t *testing.T) {
	observer := &recordingObserver{values: make(chan Observation, 1)}
	queue := newObserverQueue(observer, 4, time.Millisecond)
	defer queue.Close()
	operations := &connectionOperations{observer: queue}
	operations.observeDuration("requests", "latency", Normal, 3*time.Microsecond)
	select {
	case value := <-observer.values:
		if value.Name != "requests" || value.Kind != "latency_le_4us" || value.Code != Normal || value.Value != 1 {
			t.Fatalf("histogram observation = %#v", value)
		}
	case <-time.After(time.Second):
		t.Fatal("histogram observation was not flushed")
	}
}
