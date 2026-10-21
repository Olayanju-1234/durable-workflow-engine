package server

import (
	"context"
	"errors"
	"testing"
	"time"
)

// A signal can land while an SDK is deciding. It does not satisfy any wait (the
// workflow is not waiting yet), but it moves the history on, so the decision
// comes back stale. The engine must then hand out a fresh task: before, it only
// dropped the stale one and the workflow had nothing left to drive it.
func TestStaleDecisionAfterEarlySignalIsRedispatched(t *testing.T) {
	e := newTestEngine(t, t.TempDir())
	if err := e.RegisterWorkflowTypes([]string{"Remote"}, "sdk", "test"); err != nil {
		t.Fatal(err)
	}
	id, err := e.StartWorkflow("Remote", nil, "wf-1")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task, err := e.PollWorkflowTask(ctx, "sdk", []string{"Remote"})
	if err != nil {
		t.Fatal(err)
	}

	if err := e.SignalWorkflow(id, "approve", []byte("yes")); err != nil {
		t.Fatal(err)
	}
	cmd := &WorkflowCommand{Command: &WorkflowCommand_AwaitSignal{AwaitSignal: &AwaitSignalCommand{
		StepName: "approval", Sequence: 1, SignalName: "approve",
	}}}
	if err := e.CompleteWorkflowTask(id, task.GetTaskToken(), []*WorkflowCommand{cmd}, ""); !errors.Is(err, ErrStaleDecision) {
		t.Fatalf("decision on the pre-signal history: got %v, want ErrStaleDecision", err)
	}

	fresh, err := e.PollWorkflowTask(ctx, "sdk", []string{"Remote"})
	if err != nil {
		t.Fatalf("no fresh workflow task after the stale one: %v", err)
	}
	if fresh.GetHistoryLength() != 2 {
		t.Fatalf("fresh task covers %d events, want 2 (start + signal)", fresh.GetHistoryLength())
	}
	if err := e.CompleteWorkflowTask(id, fresh.GetTaskToken(), []*WorkflowCommand{cmd}, ""); err != nil {
		t.Fatal(err)
	}
	evs, _ := e.store.ReadAll(id)
	if n := len(evs); n != 4 || evs[3].GetStepCompleted() == nil {
		t.Fatalf("history has %d events; want the buffered signal delivered to the wait", n)
	}
}
