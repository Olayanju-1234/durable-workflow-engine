package server

import "context"

// taskQueue is an in-memory FIFO of pending tasks awaiting a worker.
//
// It is a buffered channel: a channel receive delivers each value to exactly
// one goroutine, so an enqueued Task is handed to exactly one PollTask caller.
// That single-delivery property *is* the "no duplicate dispatch" guarantee — we
// get it from the language, not from bookkeeping.
//
// The queue is deliberately ephemeral. On restart it is empty; the source of
// truth is the on-disk history, and crash recovery (Week 4) re-derives any
// outstanding tasks by replaying the log. Nothing here is durable.
type taskQueue struct {
	ch chan *Task
}

// taskQueueCapacity bounds buffered tasks. Phase 1 keeps one step in flight per
// workflow, so this only fills under an extreme backlog of unworked workflows;
// a durable, back-pressured queue arrives in Phase 4.
const taskQueueCapacity = 1 << 16

func newTaskQueue() *taskQueue {
	return &taskQueue{ch: make(chan *Task, taskQueueCapacity)}
}

// enqueue adds a task for the next polling worker. It does not block until the
// buffer is full (see taskQueueCapacity).
func (q *taskQueue) enqueue(t *Task) {
	q.ch <- t
}

// depth is how many tasks are buffered right now (a point-in-time read; the
// value can change immediately after). Feeds the queue-depth gauge.
func (q *taskQueue) depth() int {
	return len(q.ch)
}

// poll blocks until a task is available, returning ctx.Err() if the caller's
// context is cancelled first (worker disconnect or server shutdown). Long-poll
// is the worker's normal idle state.
func (q *taskQueue) poll(ctx context.Context) (*Task, error) {
	select {
	case t := <-q.ch:
		return t, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
