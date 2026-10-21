package server

import (
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

func assertOpenHandles(t *testing.T, e *Engine, want int, when string) {
	t.Helper()
	if got := e.store.OpenHandles(); got != want {
		t.Fatalf("open history handles %s = %d, want %d", when, got, want)
	}
}

// TestFinishedWorkflowReleasesHandle: once a workflow reaches a terminal event
// (completed, failed or terminated) its append handle is closed, so a
// long-running server holds descriptors only for live workflows.
func TestFinishedWorkflowReleasesHandle(t *testing.T) {
	e := newTestEngine(t, t.TempDir(),
		state.WorkflowDefinition{Type: "Gated", Steps: []state.StepDef{state.AwaitSignal("gate", "go")}},
		state.WorkflowDefinition{Type: "Work", Steps: state.Activities("do")},
	)
	start := func(typ, id string) {
		t.Helper()
		if _, err := e.StartWorkflow(typ, nil, id); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}

	start("Gated", "completes")
	assertOpenHandles(t, e, 1, "while waiting on a signal")
	if err := e.SignalWorkflow("completes", "go", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	assertOpenHandles(t, e, 0, "after completion")

	start("Work", "fails")
	task := pollWithin(t, e, time.Second)
	if err := e.CompleteTask("fails", task.GetStepName(), task.GetSequence(), task.GetLeaseToken(), nil, "boom", true); err != nil {
		t.Fatalf("fail task: %v", err)
	}
	assertOpenHandles(t, e, 0, "after failure")

	start("Work", "killed")
	start("Work", "live")
	assertOpenHandles(t, e, 2, "with two live workflows")
	if err := e.TerminateWorkflow("killed", "test"); err != nil {
		t.Fatal(err)
	}
	assertOpenHandles(t, e, 1, "after terminate (only the live one)")

	// A released workflow's log is intact and still readable.
	for id, want := range map[string]WorkflowStatus{
		"completes": WorkflowStatus_WORKFLOW_STATUS_COMPLETED,
		"fails":     WorkflowStatus_WORKFLOW_STATUS_FAILED,
		"killed":    WorkflowStatus_WORKFLOW_STATUS_TERMINATED,
	} {
		resp, err := e.GetWorkflow(id)
		if err != nil || resp.GetSummary().GetStatus() != want {
			t.Errorf("GetWorkflow(%s) = %v, %v; want %v", id, resp.GetSummary().GetStatus(), err, want)
		}
	}
}

// TestRecoverHoldsNoHandlesForFinishedWorkflows: after a restart, recovery reads
// finished workflows without caching a handle, and one that finishes during
// recovery (it crashed mid-decision) is released like any other.
func TestRecoverHoldsNoHandlesForFinishedWorkflows(t *testing.T) {
	dir := t.TempDir()
	def := state.WorkflowDefinition{Type: "One", Steps: state.Activities("only")}

	pre := newTestEngine(t, dir, def)
	writeHistory(t, pre.store, "already-done",
		startedEv("already-done", "One", nil),
		scheduledEv("only", 1, nil),
		completedEv("only", 1, []byte("ok")),
		&history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowCompleted{
			WorkflowCompleted: &history.WorkflowCompletedEvent{Output: []byte("ok")},
		}},
	)
	writeHistory(t, pre.store, "finishes-on-recover",
		startedEv("finishes-on-recover", "One", nil),
		scheduledEv("only", 1, nil),
		completedEv("only", 1, []byte("ok")),
	)

	e := newTestEngine(t, dir, def)
	if err := e.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	resp, err := e.GetWorkflow("finishes-on-recover")
	if err != nil || resp.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Fatalf("recovered workflow = %v, %v; want COMPLETED", resp.GetSummary().GetStatus(), err)
	}
	assertOpenHandles(t, e, 0, "after recovery")
}
