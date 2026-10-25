package server

import (
	"context"
	"slices"
	"sync"
)

// routedQueue is an in-memory FIFO whose pollers can ask only for the items they
// can handle: an activity worker names the steps it implements, an SDK worker
// names the workflow types it can replay. route extracts that key from an item,
// and scope where it belongs (its namespace): a poller only ever sees its own
// scope's items, whatever names it asks for.
//
// Each item is handed to exactly one poller: removal happens under mu, so two
// pollers can never both take the same item. That is the "no duplicate
// dispatch" guarantee.
//
// The queue is deliberately ephemeral. On restart it is empty; the source of
// truth is the on-disk history, and crash recovery re-derives any outstanding
// work by replaying the log. Nothing here is durable.
type routedQueue[T any] struct {
	route func(T) string
	scope func(T) string

	mu      sync.Mutex
	pending []T
	// ready is closed (and replaced) on every enqueue, waking every poller so
	// each can re-check for an item matching its own routes.
	ready chan struct{}
}

func newRoutedQueue[T any](route, scope func(T) string) *routedQueue[T] {
	return &routedQueue[T]{route: route, scope: scope, ready: make(chan struct{})}
}

func (q *routedQueue[T]) enqueue(item T) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = append(q.pending, item)
	close(q.ready)
	q.ready = make(chan struct{})
}

// dropWhere removes every pending item matching fn, so a stale item is never
// handed out.
func (q *routedQueue[T]) dropWhere(fn func(T) bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.pending = slices.DeleteFunc(q.pending, fn)
}

// depth is how many items are pending right now (a point-in-time read).
func (q *routedQueue[T]) depth() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.pending)
}

// depthBy counts the pending items by key (a namespace, say).
func (q *routedQueue[T]) depthBy(key func(T) string) map[string]int {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := map[string]int{}
	for _, item := range q.pending {
		out[key(item)]++
	}
	return out
}

// poll blocks until an item in scope whose route is in routes is available,
// or ctx ends. Passing no routes accepts any item in scope. Long-poll is a
// worker's normal idle state.
func (q *routedQueue[T]) poll(ctx context.Context, scope string, routes []string) (T, error) {
	for {
		q.mu.Lock()
		for i, item := range q.pending {
			if q.scope(item) == scope && (len(routes) == 0 || slices.Contains(routes, q.route(item))) {
				q.pending = slices.Delete(q.pending, i, i+1)
				q.mu.Unlock()
				return item, nil
			}
		}
		wait := q.ready
		q.mu.Unlock()

		select {
		case <-wait:
		case <-ctx.Done():
			var zero T
			return zero, ctx.Err()
		}
	}
}

// taskQueue holds activity tasks, routed by step name within a namespace's
// task queue.
// Tasks name their workflow by its store key until they leave the engine.
type taskQueue = routedQueue[*Task]

func newTaskQueue() *taskQueue {
	return newRoutedQueue((*Task).GetStepName, func(t *Task) string {
		return routeScope(nsOf(t.GetWorkflowId()), t.GetTaskQueue())
	})
}
