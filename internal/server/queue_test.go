package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestQueueNoDuplicateDispatch is the Week-3 queue invariant: every enqueued
// task is handed to exactly one poller. We enqueue N distinct tasks, drain them
// with many concurrent pollers, and assert each task came out once and only once.
func TestQueueNoDuplicateDispatch(t *testing.T) {
	const (
		tasks   = 1000
		pollers = 16
	)
	q := newTaskQueue()
	for i := range tasks {
		q.enqueue(&Task{WorkflowId: fmt.Sprintf("wf-%d", i), Sequence: int64(i)})
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		mu    sync.Mutex
		seen  = make(map[string]int)
		count int
		wg    sync.WaitGroup
	)
	for range pollers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				task, err := q.poll(ctx)
				if err != nil {
					return // drained: ctx cancelled once all tasks are out
				}
				mu.Lock()
				seen[task.GetWorkflowId()]++
				count++
				if count == tasks {
					cancel() // everything dispatched; release the other pollers
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if count != tasks {
		t.Fatalf("dispatched %d tasks, want %d", count, tasks)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("task %s dispatched %d times, want exactly 1", id, n)
		}
	}
}

// TestQueuePollBlocksUntilCancel proves an empty queue parks the poller and that
// a cancelled context unblocks it with the context error (worker disconnect /
// server shutdown), rather than returning a phantom task.
func TestQueuePollBlocksUntilCancel(t *testing.T) {
	q := newTaskQueue()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		_, err := q.poll(ctx)
		done <- err
	}()

	select {
	case <-done:
		t.Fatal("poll returned before a task was enqueued or ctx cancelled")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocked
	}

	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("poll error: got %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("poll did not unblock after ctx cancel")
	}
}
