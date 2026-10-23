package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// deliver calls a webhook with a generous retry policy, as liftoff does, and
// reports how its last attempt failed.
func deliver(policy workflow.RetryPolicy, opts ...workflow.ActivityOption) workflow.Func {
	return func(ctx workflow.Context, in []byte) ([]byte, error) {
		_, err := ctx.ExecuteActivity("deliver", in, append(opts, workflow.WithRetryPolicy(policy))...)
		var ae *workflow.ActivityError
		if !errors.As(err, &ae) {
			return nil, errors.New("want an *ActivityError")
		}
		return []byte(ae.Type + "|" + ae.Message), nil
	}
}

func failOnce(t *testing.T, e *Engine, id string, f TaskFailure) {
	t.Helper()
	task := mustPoll(t, e, "w", 2*time.Second)
	if err := e.FailTask(id, task.GetStepName(), task.GetSequence(), task.GetLeaseToken(), f); err != nil {
		t.Fatal(err)
	}
}

// TestNonRetryableFailsAtOnce: a 4xx answer flagged non-retryable is the
// step's result on the first attempt, whatever the policy allows.
func TestNonRetryableFailsAtOnce(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Webhook": deliver(workflow.RetryPolicy{MaxAttempts: 6, InitialBackoff: 10 * time.Millisecond}),
	})
	id, err := e.StartWorkflow("Webhook", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	failOnce(t, e, id, TaskFailure{Message: "webhook answered 404", Type: "HTTP4xx", NonRetryable: true})
	st := waitCompleted(t, e, id, 3*time.Second)
	if string(st.Output) != "HTTP4xx|webhook answered 404" || st.Steps[0].Attempts != 1 {
		t.Fatalf("output %q after %d attempts, want the 404 on the first", st.Output, st.Steps[0].Attempts)
	}
	evs, _ := e.store.ReadAll(id)
	sf := evs[2].GetStepFailed()
	if !sf.GetNonRetryable() || sf.GetWillRetry() || sf.GetErrorType() != "HTTP4xx" {
		t.Fatalf("recorded %v", sf)
	}
	if e.queue.depth() != 0 {
		t.Fatal("a retry was dispatched")
	}
}

// TestNonRetryableErrorTypes: the policy lists the types not worth retrying;
// other failures retry as usual, and the engine's own failure types count.
func TestNonRetryableErrorTypes(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Webhook": deliver(workflow.RetryPolicy{
			MaxAttempts: 6, InitialBackoff: 10 * time.Millisecond, NonRetryableErrorTypes: []string{"BadRequest"},
		}),
		"Hang": deliver(workflow.RetryPolicy{
			MaxAttempts: 6, NonRetryableErrorTypes: []string{ErrorTypeStartToCloseTimeout},
		}, workflow.WithStartToCloseTimeout(100*time.Millisecond)),
	})
	id, err := e.StartWorkflow("Webhook", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	failOnce(t, e, id, TaskFailure{Message: "503", Type: "Unavailable"})
	failOnce(t, e, id, TaskFailure{Message: "400 bad payload", Type: "BadRequest"})
	st := waitCompleted(t, e, id, 3*time.Second)
	if string(st.Output) != "BadRequest|400 bad payload" || st.Steps[0].Attempts != 2 {
		t.Fatalf("output %q after %d attempts, want BadRequest on the second", st.Output, st.Steps[0].Attempts)
	}

	hung, err := e.StartWorkflow("Hang", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	mustPoll(t, e, "silent", time.Second) // never reports
	st = waitCompleted(t, e, hung, 3*time.Second)
	if !strings.HasPrefix(string(st.Output), ErrorTypeStartToCloseTimeout+"|activity timed out") || st.Steps[0].Attempts != 1 {
		t.Fatalf("output %q after %d attempts, want one start-to-close timeout", st.Output, st.Steps[0].Attempts)
	}
}

// TestNonRetryableSurvivesRecovery: an engine that crashed after recording a
// non-retryable failure but before deciding must not retry it on recovery
// (it would look like a failure from before will_retry existed).
func TestNonRetryableSurvivesRecovery(t *testing.T) {
	store := newStore(t, t.TempDir())
	id := "crashed-mid-decision"
	policy := &history.RetryPolicy{MaxAttempts: 5}
	for i, ev := range []*history.HistoryEvent{
		{Attributes: &history.HistoryEvent_WorkflowStarted{WorkflowStarted: &history.WorkflowStartedEvent{WorkflowId: id, WorkflowType: "Webhook"}}},
		{Attributes: &history.HistoryEvent_StepScheduled{StepScheduled: &history.StepScheduledEvent{StepName: "deliver", Sequence: 1, RetryPolicy: policy}}},
		{Attributes: &history.HistoryEvent_StepFailed{StepFailed: &history.StepFailedEvent{
			StepName: "deliver", Sequence: 1, Error: "410 gone", ErrorType: "Gone", NonRetryable: true, RetryCount: 1}}},
	} {
		ev.EventId, ev.Timestamp = int64(i+1), time.Now().UnixNano()
		if err := store.Append(id, ev); err != nil {
			t.Fatal(err)
		}
	}
	e := timeoutEngine(t, store, map[string]workflow.Func{"Webhook": deliver(workflow.RetryPolicy{MaxAttempts: 5})})
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	st := waitCompleted(t, e, id, 3*time.Second)
	if string(st.Output) != "Gone|410 gone" || st.Steps[0].Attempts != 1 {
		t.Fatalf("output %q after %d attempts, want the recorded failure, not a retry", st.Output, st.Steps[0].Attempts)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if task, err := e.PollTask(ctx, "w"); err == nil {
		t.Fatalf("recovery dispatched %v", task)
	}
}
