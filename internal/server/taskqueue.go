package server

import (
	"fmt"

	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// Task queues (ADR-0016). Workflow and activity tasks are routed by queue, then
// by workflow type or step name within it: a worker polls one named queue and
// sees nothing on any other, so two workers can implement the same activity
// name for different apps. A workflow runs on the queue it was started on, and
// its activities do too unless the code names another. The default queue,
// recorded as "", is what workers and starts that name none use: every engine
// before 0.4.0 behaved as if it were the only queue.
const DefaultTaskQueue = state.DefaultTaskQueue

// checkQueue refuses a queue name that is not empty, "default", or 1 to 128
// letters, digits, dots, underscores and hyphens starting with a letter or
// digit.
func checkQueue(q string) error {
	if q == "" {
		return nil
	}
	ok := len(q) <= 128 && isAlnum(q[0])
	for i := 0; ok && i < len(q); i++ {
		ok = isAlnum(q[i]) || q[i] == '.' || q[i] == '_' || q[i] == '-'
	}
	if !ok {
		return fmt.Errorf("%w: invalid task queue %q: use up to 128 letters, digits, dots, underscores and hyphens", ErrInvalidArgument, q)
	}
	return nil
}

func isAlnum(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9'
}

// recordedQueue is how history records a queue: the default one as "".
func recordedQueue(q string) string {
	if q == DefaultTaskQueue {
		return ""
	}
	return q
}

// routeScope is the part of a task's route that a poller must match exactly:
// its namespace and its queue.
func routeScope(ns, queue string) string {
	return ns + "\x00" + state.QueueName(queue)
}
