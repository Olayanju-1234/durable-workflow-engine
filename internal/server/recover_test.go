package server

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// These tests drive recovery directly against the Engine (no gRPC): they build a
// history on disk, simulate a restart by opening a fresh Engine over the same data
// dir, and assert Recover puts the workflow back where it belongs. The engine is
// the unit under test, so the checks read the durable log and the rebuilt queue.

func newTestEngine(t *testing.T, dir string, defs ...state.WorkflowDefinition) *Engine {
	t.Helper()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	e := NewEngine(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(e.Close)
	for _, d := range defs {
		e.Register(d)
	}
	return e
}

// writeHistory appends hand-built events to a workflow's log, stamping contiguous
// ids — the shape a real crash would leave on disk, without running the engine.
func writeHistory(t *testing.T, store *history.HistoryStore, wfID string, evs ...*history.HistoryEvent) {
	t.Helper()
	for i, ev := range evs {
		ev.EventId = int64(i + 1)
		ev.Timestamp = time.Now().UnixNano()
		if err := store.Append(wfID, ev); err != nil {
			t.Fatalf("write history %s event %d: %v", wfID, i, err)
		}
	}
}

func startedEv(wfID, wfType string, input []byte) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{WorkflowId: wfID, WorkflowType: wfType, Input: input},
	}}
}

func scheduledEv(name string, seq int64, input []byte) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
		StepScheduled: &history.StepScheduledEvent{StepName: name, Sequence: seq, Input: input},
	}}
}

func completedEv(name string, seq int64, output []byte) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
		StepCompleted: &history.StepCompletedEvent{StepName: name, Sequence: seq, Output: output},
	}}
}

// pollWithin polls the engine's rebuilt queue, failing if no task arrives in time.
func pollWithin(t *testing.T, e *Engine, d time.Duration) *Task {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	task, err := e.PollTask(ctx, "recover-test")
	if err != nil {
		t.Fatalf("poll after recovery: %v", err)
	}
	return task
}

// assertNothingQueued proves recovery enqueued no task: a short poll must time out.
func assertNothingQueued(t *testing.T, e *Engine) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if task, err := e.PollTask(ctx, "recover-test"); err == nil {
		t.Fatalf("expected empty queue after recovery, got task %v", task)
	} else if err != context.DeadlineExceeded {
		t.Fatalf("poll error: got %v, want DeadlineExceeded", err)
	}
}

// TestRecoverReenqueuesWaitingStep: a step was in flight when the engine died.
// The ephemeral queue is gone on restart, so Recover must rebuild it from the log
// and hand the same step to the next worker.
func TestRecoverReenqueuesWaitingStep(t *testing.T) {
	dir := t.TempDir()
	const wfID = "wf-waiting"
	def := state.WorkflowDefinition{Type: "Two", Steps: state.Activities("reserve", "ship")}

	// On-disk state left by a crash while step 1 was awaiting a worker (WAITING).
	pre := newTestEngine(t, dir, def)
	writeHistory(t, pre.store, wfID,
		startedEv(wfID, "Two", []byte("order-9")),
		scheduledEv("reserve", 1, []byte("order-9")),
	)

	// Restart: a fresh engine with an empty queue over the same data dir.
	e := newTestEngine(t, dir, def)
	if err := e.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	task := pollWithin(t, e, time.Second)
	if task.GetWorkflowId() != wfID || task.GetStepName() != "reserve" || task.GetSequence() != 1 {
		t.Fatalf("recovered task: got %s/%s/seq%d, want %s/reserve/seq1",
			task.GetWorkflowId(), task.GetStepName(), task.GetSequence(), wfID)
	}
	if string(task.GetInput()) != "order-9" {
		t.Errorf("recovered task input: got %q, want order-9", task.GetInput())
	}
}

// TestRecoverAdvancesRunningWorkflow: the engine died mid-decision — step 1's
// completion is recorded but step 2 was never scheduled (RUNNING). Recover must
// make the next decision, scheduling step 2 and enqueuing it.
func TestRecoverAdvancesRunningWorkflow(t *testing.T) {
	dir := t.TempDir()
	const wfID = "wf-running"
	def := state.WorkflowDefinition{Type: "Two", Steps: state.Activities("reserve", "ship")}

	pre := newTestEngine(t, dir, def)
	writeHistory(t, pre.store, wfID,
		startedEv(wfID, "Two", []byte("order-9")),
		scheduledEv("reserve", 1, []byte("order-9")),
		completedEv("reserve", 1, []byte("reserved")),
	)

	e := newTestEngine(t, dir, def)
	if err := e.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// Recover appended the StepScheduled for step 2 and enqueued its task.
	task := pollWithin(t, e, time.Second)
	if task.GetStepName() != "ship" || task.GetSequence() != 2 {
		t.Fatalf("recovered task: got %s/seq%d, want ship/seq2", task.GetStepName(), task.GetSequence())
	}
	if string(task.GetInput()) != "reserved" {
		t.Errorf("step-2 input should chain step-1 output: got %q, want reserved", task.GetInput())
	}

	events, _ := e.store.ReadAll(wfID)
	if len(events) != 4 { // started + sched1 + done1 + sched2
		t.Fatalf("history length after recovery: got %d, want 4", len(events))
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if st.Status != state.StatusWaiting {
		t.Errorf("status after advancing: got %s, want WAITING", st.Status)
	}
}

// TestRecoverSkipsTerminalWorkflow: a completed workflow needs no recovery — it
// must not be re-scheduled.
func TestRecoverSkipsTerminalWorkflow(t *testing.T) {
	dir := t.TempDir()
	const wfID = "wf-done"
	def := state.WorkflowDefinition{Type: "One", Steps: state.Activities("only")}

	pre := newTestEngine(t, dir, def)
	writeHistory(t, pre.store, wfID,
		startedEv(wfID, "One", nil),
		scheduledEv("only", 1, nil),
		completedEv("only", 1, []byte("ok")),
		&history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowCompleted{
			WorkflowCompleted: &history.WorkflowCompletedEvent{Output: []byte("ok")},
		}},
	)

	e := newTestEngine(t, dir, def)
	if err := e.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	assertNothingQueued(t, e)
}

// TestRecoverResumesStepsOfUnregisteredType: SDK workers register their types
// only after the engine has recovered, so in-flight steps must resume without a
// planner: the activity is re-enqueued and an overdue timer fires. Registering
// the type then resumes the decisions.
func TestRecoverResumesStepsOfUnregisteredType(t *testing.T) {
	dir := t.TempDir()
	pre := newTestEngine(t, dir) // nothing registered
	writeHistory(t, pre.store, "wf-activity",
		startedEv("wf-activity", "Remote", nil),
		scheduledEv("charge", 1, nil),
	)
	writeHistory(t, pre.store, "wf-timer",
		startedEv("wf-timer", "Remote", nil),
		&history.HistoryEvent{Attributes: &history.HistoryEvent_TimerStarted{
			TimerStarted: &history.TimerStartedEvent{StepName: "wait", Sequence: 1, FireAt: time.Now().Add(-time.Second).UnixNano()},
		}},
	)

	e := newTestEngine(t, dir) // still nothing registered
	if err := e.Recover(); err != nil {
		t.Fatalf("recover must not fail on an unregistered type: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	task, err := e.PollTask(ctx, "w", "charge")
	if err != nil || task.GetWorkflowId() != "wf-activity" {
		t.Fatalf("in-flight activity not re-enqueued: %v %v", task, err)
	}
	for {
		evs, err := e.store.ReadAll("wf-timer")
		if err != nil {
			t.Fatal(err)
		}
		if len(evs) == 3 && evs[2].GetTimerFired() != nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("overdue timer never fired; history has %d events", len(evs))
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := e.RegisterWorkflowTypes([]string{"Remote"}, "sdk", "test"); err != nil {
		t.Fatal(err)
	}
	wt, err := e.PollWorkflowTask(ctx, "sdk", []string{"Remote"})
	if err != nil || wt.GetWorkflowId() != "wf-timer" {
		t.Fatalf("registration did not resume the fired timer's workflow: %v %v", wt, err)
	}
}
