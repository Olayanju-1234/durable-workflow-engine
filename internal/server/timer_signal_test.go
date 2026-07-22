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

func discardLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// driveWorker completes every activity task the engine dispatches, echoing the
// step name, until ctx is cancelled. Timer and signal steps have no task, so they
// resolve on their own (a fired timer / a delivered signal) — this worker only ever
// sees activities.
func driveWorker(ctx context.Context, e *Engine) {
	for {
		task, err := e.PollTask(ctx, "w")
		if err != nil {
			return
		}
		_ = e.CompleteTask(task.GetWorkflowId(), task.GetStepName(), task.GetSequence(),
			task.GetLeaseToken(), []byte("done:"+task.GetStepName()), "", false)
	}
}

// completeNext completes exactly one in-flight activity, synchronously.
func completeNext(t *testing.T, e *Engine, output string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	task, err := e.PollTask(ctx, "w")
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if err := e.CompleteTask(task.GetWorkflowId(), task.GetStepName(), task.GetSequence(), task.GetLeaseToken(), []byte(output), "", false); err != nil {
		t.Fatalf("complete %s: %v", task.GetStepName(), err)
	}
}

// TestSleepStepRunsToCompletion: a workflow with a Sleep step pauses on a durable
// timer, then resumes on its own when the timer fires and runs to completion. The
// timer step's output is its input passed through, so the pipeline continues.
func TestSleepStepRunsToCompletion(t *testing.T) {
	dir := t.TempDir()
	def := state.WorkflowDefinition{Type: "Sleeper", Steps: []state.StepDef{
		state.Activity("prep"),
		state.Sleep("wait", 40*time.Millisecond),
		state.Activity("finish"),
	}}
	e := newTestEngine(t, dir, def)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go driveWorker(ctx, e)

	id, err := e.StartWorkflow("Sleeper", []byte("in"), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	final := waitCompleted(t, e, id, 3*time.Second)
	if final.Status != state.StatusCompleted {
		t.Fatalf("status: got %s, want COMPLETED", final.Status)
	}
	if string(final.Output) != "done:finish" {
		t.Errorf("output: got %q, want done:finish", final.Output)
	}
	if len(final.Steps) != 3 {
		t.Fatalf("steps: got %d, want 3", len(final.Steps))
	}
	timerStep := final.Steps[1]
	if timerStep.Name != "wait" || timerStep.Status != state.StepCompleted || string(timerStep.Output) != "done:prep" {
		t.Errorf("timer step: got {%s %s %q}, want {wait COMPLETED done:prep}", timerStep.Name, timerStep.Status, timerStep.Output)
	}
}

// TestTimerSurvivesRestart is the durable-timer headline: a timer armed before a
// crash still fires after a restart. Engine 1 arms the timer and "crashes" (stops)
// before it fires; a fresh Engine 2 recovers, re-arms the timer from the durable
// deadline in the log, and the workflow completes.
func TestTimerSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	def := state.WorkflowDefinition{Type: "Sleeper", Steps: []state.StepDef{
		state.Activity("prep"),
		state.Sleep("wait", 80*time.Millisecond),
		state.Activity("finish"),
	}}

	// --- Engine 1: arm the timer, then crash before it fires. ---
	store1, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("store1: %v", err)
	}
	e1 := NewEngine(store1, discardLogger())
	e1.Register(def)
	id, err := e1.StartWorkflow("Sleeper", []byte("in"), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	completeNext(t, e1, "prepped") // prep done → timer armed (80ms)

	// Confirm the crash point: WAITING on the timer step, not yet fired.
	evs, _ := store1.ReadAll(id)
	st, _ := state.Reconstruct(evs)
	if st.Status != state.StatusWaiting || st.Tail().Name != "wait" || st.Tail().FireAt == 0 {
		t.Fatalf("pre-crash state: got %s tail=%+v, want WAITING on 'wait' with FireAt set", st.Status, st.Tail())
	}
	e1.Close() // stop timer 1 — it must NOT fire
	store1.Close()

	// --- Engine 2: restart over the same data dir and recover. ---
	store2, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("store2: %v", err)
	}
	t.Cleanup(func() { store2.Close() })
	e2 := NewEngine(store2, discardLogger())
	t.Cleanup(e2.Close)
	e2.Register(def)
	if err := e2.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go driveWorker(ctx, e2)

	final := waitCompleted(t, e2, id, 3*time.Second)
	if final.Status != state.StatusCompleted {
		t.Fatalf("status after restart: got %s, want COMPLETED", final.Status)
	}
	if final.Steps[1].Status != state.StepCompleted {
		t.Errorf("timer step did not fire after restart: %+v", final.Steps[1])
	}
	if string(final.Output) != "done:finish" {
		t.Errorf("output: got %q, want done:finish", final.Output)
	}
}

// TestSignalBufferedBeforeWait proves signals are buffered: a signal delivered
// while the workflow is busy with an earlier step is matched to the later wait,
// with no second delivery needed.
func TestSignalBufferedBeforeWait(t *testing.T) {
	dir := t.TempDir()
	def := state.WorkflowDefinition{Type: "Buf", Steps: []state.StepDef{
		state.Activity("prep"),
		state.AwaitSignal("gate", "go"),
		state.Activity("finish"),
	}}
	e := newTestEngine(t, dir, def)

	id, err := e.StartWorkflow("Buf", []byte("in"), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// Deliver the signal while prep is still in flight — before the wait exists.
	if err := e.SignalWorkflow(id, "go", []byte("early")); err != nil {
		t.Fatalf("signal: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go driveWorker(ctx, e) // completes prep and finish; the wait resolves from the buffer

	final := waitCompleted(t, e, id, 3*time.Second)
	if final.Status != state.StatusCompleted {
		t.Fatalf("status: got %s, want COMPLETED", final.Status)
	}
	if string(final.Steps[1].Output) != "early" {
		t.Errorf("signal step output: got %q, want the buffered payload %q", final.Steps[1].Output, "early")
	}
	if string(final.Output) != "done:finish" {
		t.Errorf("output: got %q, want done:finish", final.Output)
	}
}
