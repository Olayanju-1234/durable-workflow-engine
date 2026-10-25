package server

import (
	"errors"
	"sync"
	"sync/atomic"
)

// The change feed lets a live view follow the engine instead of re-polling it:
// the visibility index publishes every summary it records (and every removal),
// and schedule changes publish a notice. Publishing never blocks the engine: a
// subscriber that falls behind loses changes and is told to resync instead.

// Change is one thing a live view should redraw: a workflow's new summary, a
// workflow that is gone, or (Schedules) the namespace's schedules.
type Change struct {
	// Key is the workflow's store key.
	Key       string
	Summary   *WorkflowSummary // nil when Deleted or Schedules
	Deleted   bool
	Schedules bool
}

// ErrTooManySubscribers refuses a live stream beyond the engine's limit.
var ErrTooManySubscribers = errors.New("server: too many live streams")

const (
	// subscriptionBuffer is how many changes a subscriber may fall behind by.
	subscriptionBuffer = 512
	// maxSubscriptions bounds open live streams (each holds a goroutine).
	maxSubscriptions = 256
)

type feed struct {
	mu   sync.Mutex
	subs map[*Subscription]struct{}
	n    atomic.Int32 // len(subs), read without the lock on the hot path
}

// Subscription receives one namespace's changes until it is closed or the
// engine closes (Done).
type Subscription struct {
	ns     string
	ch     chan Change
	lagged atomic.Bool
	done   <-chan struct{}
	f      *feed
	once   sync.Once
}

// C delivers the namespace's changes in the order they were recorded.
func (s *Subscription) C() <-chan Change { return s.ch }

// Done is closed when the engine closes: the stream should end, and the
// client reconnect (in a cluster, to the next leader).
func (s *Subscription) Done() <-chan struct{} { return s.done }

// Lagged reports whether changes were dropped since the last call, and clears
// it: the subscriber must reload what it shows.
func (s *Subscription) Lagged() bool { return s.lagged.Swap(false) }

// Close stops the subscription.
func (s *Subscription) Close() {
	s.once.Do(func() {
		s.f.mu.Lock()
		delete(s.f.subs, s)
		s.f.n.Store(int32(len(s.f.subs)))
		s.f.mu.Unlock()
	})
}

func (e *Engine) subscribe(ns string) (*Subscription, error) {
	f := &e.feed
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.subs) >= maxSubscriptions {
		return nil, ErrTooManySubscribers
	}
	if f.subs == nil {
		f.subs = make(map[*Subscription]struct{})
	}
	s := &Subscription{ns: ns, ch: make(chan Change, subscriptionBuffer), done: e.done, f: f}
	f.subs[s] = struct{}{}
	f.n.Store(int32(len(f.subs)))
	return s, nil
}

// publish hands c to every subscriber of ns, dropping it for any that is full.
func (f *feed) publish(ns string, c Change) {
	if f.n.Load() == 0 {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for s := range f.subs {
		if s.ns != ns {
			continue
		}
		select {
		case s.ch <- c:
		default:
			s.lagged.Store(true)
		}
	}
}

// Subscribe follows the namespace's workflows and schedules (see Change).
func (s *Scope) Subscribe() (*Subscription, error) {
	if err := checkNamespace(s.ns); err != nil {
		return nil, err
	}
	return s.e.subscribe(s.ns)
}
