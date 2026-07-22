// Package timer provides the engine's durable-timer scheduler: an in-memory
// min-heap of pending timers with one background goroutine that sleeps until the
// earliest deadline, then fires every timer that is due.
//
// Like the task queue, the Service is ephemeral — it holds no state that must
// survive a crash. Durability lives in the history log: a Sleep records a
// TimerStarted with an absolute fire_at, and crash recovery reconstructs the
// pending timers (TimerStarted without a matching TimerFired) and re-Adds them to a
// fresh Service. A timer whose deadline passed while the engine was down simply
// fires immediately on restart. This split — durable deadlines in the log,
// disposable scheduling in memory — is the same pattern the whole engine uses.
package timer

import (
	"container/heap"
	"sync"
	"time"
)

// Timer is one pending timer: fire callback target for a specific workflow step.
type Timer struct {
	WorkflowID string
	StepName   string
	Sequence   int64
	FireAt     time.Time
}

// Service schedules timers and fires them in deadline order. Safe for concurrent
// use. Create with New, then Start; Add timers; Stop to shut down.
type Service struct {
	mu   sync.Mutex
	heap timerHeap
	fire func(Timer)

	wake chan struct{}
	stop chan struct{}
	done chan struct{}
}

// New builds a Service that invokes fire for each timer once its deadline passes.
// fire runs on the Service's single goroutine, so it should return quickly (the
// engine's callback just appends TimerFired and advances the workflow).
func New(fire func(Timer)) *Service {
	return &Service{
		fire: fire,
		wake: make(chan struct{}, 1),
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// Start launches the background goroutine. Call exactly once, before Add.
func (s *Service) Start() { go s.run() }

// Add arms a timer. If it becomes the earliest deadline, the loop is woken to
// recompute its sleep. A timer already past its deadline fires on the next tick.
func (s *Service) Add(t Timer) {
	s.mu.Lock()
	heap.Push(&s.heap, t)
	s.mu.Unlock()
	// Non-blocking nudge: if a wake is already queued, one is enough.
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

// Len reports the number of pending timers (used by tests).
func (s *Service) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.heap.Len()
}

// Stop shuts the goroutine down and waits for it to exit. Idempotent-unsafe: call once.
func (s *Service) Stop() {
	close(s.stop)
	<-s.done
}

func (s *Service) run() {
	defer close(s.done)
	for {
		s.mu.Lock()
		wait := time.Hour // nothing pending: park until an Add wakes us
		if s.heap.Len() > 0 {
			wait = time.Until(s.heap[0].FireAt)
		}
		s.mu.Unlock()
		if wait < 0 {
			wait = 0
		}
		// Go 1.23+ garbage-collects the unreferenced timer from time.After even
		// before it fires, so re-arming it every wake does not leak.
		select {
		case <-s.stop:
			return
		case <-s.wake:
			// Heap changed (a possibly-earlier timer was added); recompute the sleep.
		case <-time.After(wait):
			s.fireDue()
		}
	}
}

// fireDue pops and fires every timer whose deadline has passed, in order.
func (s *Service) fireDue() {
	now := time.Now()
	var due []Timer
	s.mu.Lock()
	for s.heap.Len() > 0 && !s.heap[0].FireAt.After(now) {
		due = append(due, heap.Pop(&s.heap).(Timer))
	}
	s.mu.Unlock()
	// Fire outside the lock: the callback re-enters the engine (and may Add more).
	for _, t := range due {
		s.fire(t)
	}
}

// timerHeap is a min-heap of Timers ordered by FireAt, implementing heap.Interface.
// The earliest deadline is always at index 0, so "what fires next" is O(1) to read
// and O(log n) to insert/remove — the standard scheduler data structure.
type timerHeap []Timer

func (h timerHeap) Len() int           { return len(h) }
func (h timerHeap) Less(i, j int) bool { return h[i].FireAt.Before(h[j].FireAt) }
func (h timerHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *timerHeap) Push(x any) { *h = append(*h, x.(Timer)) }

func (h *timerHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	*h = old[:n-1]
	return t
}
