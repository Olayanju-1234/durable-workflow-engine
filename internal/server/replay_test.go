package server

import (
	"context"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/sdk"
)

// orderWorkflow is a code-based workflow (Week 6): the data flow between steps is
// expressed in ordinary Go, not auto-chained by the engine. It exercises all three
// primitives — an activity, a durable Sleep, and a WaitForSignal — then a final
// activity whose input the workflow composes itself.
func orderWorkflow(ctx sdk.Context, input []byte) ([]byte, error) {
	return orderWorkflowWithCooldown(40*time.Millisecond)(ctx, input)
}

func orderWorkflowWithCooldown(cooldown time.Duration) sdk.WorkflowFunc {
	return func(ctx sdk.Context, input []byte) ([]byte, error) {
		reserved, err := ctx.ExecuteActivity("reserve", input)
		if err != nil {
			return nil, err
		}
		ctx.Sleep("cooldown", cooldown)
		approval := ctx.WaitForSignal("await-approval", "approve")
		shipped, err := ctx.ExecuteActivity("ship", append(append([]byte{}, reserved...), approval...))
		if err != nil {
			return nil, err
		}
		return shipped, nil
	}
}

func newFuncEngine(t *testing.T, dir string, w sdk.Workflow) *Engine {
	t.Helper()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	e := NewEngine(store, discardLogger())
	t.Cleanup(e.Close)
	e.RegisterWorkflow(w.Type, w)
	return e
}

// waitForStepEngine polls the engine's log until the tail step matches.
func waitForStepEngine(t *testing.T, e *Engine, id, stepName string, want state.Status) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		events, _ := e.store.ReadAll(id)
		st, err := state.Reconstruct(events)
		if err == nil && st.Status == want && st.Tail() != nil && st.Tail().Name == stepName {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("workflow %s did not reach %s at step %q", id, want, stepName)
}

// TestReplayWorkflowRunsToCompletion drives a code-based workflow end to end through
// the engine: activity → sleep (fires itself) → await signal (delivered externally)
// → activity. It proves the replay engine and the Week-4/5 machinery compose.
func TestReplayWorkflowRunsToCompletion(t *testing.T) {
	dir := t.TempDir()
	e := newFuncEngine(t, dir, sdk.Workflow{Type: "Order", Fn: orderWorkflow})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go driveWorker(ctx, e) // completes reserve & ship with "done:<step>"

	id, err := e.StartWorkflow("Order", []byte("order"), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// reserve runs, the timer fires on its own; then the workflow parks on the signal.
	waitForStepEngine(t, e, id, "await-approval", state.StatusWaiting)
	if err := e.SignalWorkflow(id, "approve", []byte("|ok")); err != nil {
		t.Fatalf("signal: %v", err)
	}

	final := waitCompleted(t, e, id, 3*time.Second)
	if final.Status != state.StatusCompleted {
		t.Fatalf("status: got %s, want COMPLETED", final.Status)
	}
	if string(final.Output) != "done:ship" {
		t.Errorf("output: got %q, want done:ship", final.Output)
	}
	if len(final.Steps) != 4 {
		t.Fatalf("steps: got %d, want 4 (reserve, cooldown, await, ship)", len(final.Steps))
	}
	// The ship step's input is composed by the workflow code: reserve output + signal.
	if got := string(final.Steps[3].Input); got != "done:reserve|ok" {
		t.Errorf("ship input: got %q, want done:reserve|ok (workflow-composed data flow)", got)
	}
	// Step kinds are reconstructed from history, independent of the workflow code.
	if final.Steps[1].Kind != state.KindTimer || final.Steps[2].Kind != state.KindSignal {
		t.Errorf("step kinds: got [%s %s], want [TIMER SIGNAL]", final.Steps[1].Kind, final.Steps[2].Kind)
	}
}

// TestReplayWorkflowSurvivesRestart: a code-based workflow paused on its timer is
// recovered by a fresh engine and finishes — recovery drives replay workflows too.
func TestReplayWorkflowSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	// Long enough that engine 1 is reliably observed paused on the timer under -race load;
	// with 40 ms the timer could fire before the poll saw it, and the wait timed out.
	w := sdk.Workflow{Type: "Order", Fn: orderWorkflowWithCooldown(750 * time.Millisecond)}

	// Engine 1: run reserve, arm the cooldown timer, then crash before it fires.
	store1, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("store1: %v", err)
	}
	e1 := NewEngine(store1, discardLogger())
	e1.RegisterWorkflow(w.Type, w)
	id, err := e1.StartWorkflow("Order", []byte("order"), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	completeNext(t, e1, "done:reserve") // reserve → timer armed
	waitForStepEngine(t, e1, id, "cooldown", state.StatusWaiting)
	e1.Close()
	store1.Close()

	// Engine 2: recover; the timer re-arms and the workflow continues to the signal.
	e2 := newFuncEngine(t, dir, w)
	if err := e2.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go driveWorker(ctx, e2)

	waitForStepEngine(t, e2, id, "await-approval", state.StatusWaiting)
	if err := e2.SignalWorkflow(id, "approve", []byte("|ok")); err != nil {
		t.Fatalf("signal: %v", err)
	}
	final := waitCompleted(t, e2, id, 3*time.Second)
	if final.Status != state.StatusCompleted || string(final.Output) != "done:ship" {
		t.Fatalf("after restart: status %s output %q, want COMPLETED done:ship", final.Status, final.Output)
	}
}
