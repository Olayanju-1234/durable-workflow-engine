package server

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// Quotas (ADR-0018) bound how much a namespace can start: workflows running
// or waiting at once, and starts per minute. The engine checks them on every
// start, from any client or schedule, so no route around them exists; a run
// that continues as new is not a start. Concurrent starts reserve a slot
// before they run, so a namespace never exceeds its limit, however many race.

// ErrQuotaExceeded: a start over its namespace's quota (HTTP 429).
var ErrQuotaExceeded = errors.New("server: quota exceeded")

// WithGuardrails sets the engine's default quotas for every namespace that
// sets none of its own (Policy.MaxActiveWorkflows, MaxStartsPerMinute).
func WithGuardrails(p Policy) Option {
	return func(e *Engine) { e.guardrails = p }
}

type quotas struct {
	mu       sync.Mutex
	reserved map[string]int      // namespace -> starts admitted, not yet finished
	starts   map[string]*limiter // namespace -> starts-per-minute bucket
}

// activeWaitHint is what a refusal at the active limit suggests waiting: a
// slot frees when any workflow finishes, which nothing predicts.
const activeWaitHint = 5 * time.Second

// admitStart checks a namespace's quotas for one start and reserves its slot.
// release must be called when the start is over, saying whether a run began.
func (e *Engine) admitStart(ns string) (release func(started bool), err error) {
	p := e.namespaces.Policy(ns, e.guardrails)
	if p.MaxActiveWorkflows <= 0 && p.MaxStartsPerMinute <= 0 {
		return func(bool) {}, nil
	}
	if p.MaxActiveWorkflows > 0 {
		if err := e.ensureIndex(); err != nil {
			return nil, err
		}
	}
	q := &e.quota
	q.mu.Lock()
	defer q.mu.Unlock()
	if p.MaxActiveWorkflows > 0 {
		if active := e.activeIn(ns) + q.reserved[ns]; active >= p.MaxActiveWorkflows {
			e.metrics.startsRefused.With("max_active_workflows").Inc()
			return nil, &RetryAfter{After: activeWaitHint, Err: fmt.Errorf(
				"%w: namespace %q has %d workflows running or waiting, its limit (max_active_workflows); start more as some finish",
				ErrQuotaExceeded, ns, active)}
		}
	}
	var bucket *limiter
	if p.MaxStartsPerMinute > 0 {
		bucket = q.starts[ns]
		if bucket == nil || bucket.perMinute != p.MaxStartsPerMinute {
			bucket = newLimiter(p.MaxStartsPerMinute, p.MaxStartsPerMinute)
			q.starts[ns] = bucket
		}
		if ok, wait := bucket.take("starts", time.Now()); !ok {
			e.metrics.startsRefused.With("max_starts_per_minute").Inc()
			return nil, &RetryAfter{After: wait, Err: fmt.Errorf(
				"%w: namespace %q started its limit of %d workflows in the last minute (max_starts_per_minute); retry in %s",
				ErrQuotaExceeded, ns, p.MaxStartsPerMinute, wait.Round(time.Second))}
		}
	}
	q.reserved[ns]++
	return func(started bool) {
		q.mu.Lock()
		defer q.mu.Unlock()
		q.reserved[ns]--
		if !started && bucket != nil {
			bucket.refund("starts")
		}
	}, nil
}

// refund gives back a token a refused or idle start took.
func (l *limiter) refund(key string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if b := l.buckets[key]; b != nil {
		b.tokens = min(l.burst, b.tokens+1)
	}
}

// activeIn is how many workflows in a namespace are running or waiting, by
// the visibility index.
func (e *Engine) activeIn(ns string) int {
	e.idx.mu.Lock()
	defer e.idx.mu.Unlock()
	return e.idx.active[ns]
}
