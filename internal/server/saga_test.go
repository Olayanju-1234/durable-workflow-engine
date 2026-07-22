package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/sdk"
)

// runEngineWorker is a direct-engine worker whose exec decides success or failure,
// so tests can drive retry and compensation paths.
func runEngineWorker(ctx context.Context, e *Engine, exec func(step string, input []byte) ([]byte, error)) {
	for {
		task, err := e.PollTask(ctx, "w")
		if err != nil {
			return
		}
		out, execErr := exec(task.GetStepName(), task.GetInput())
		if execErr != nil {
			_ = e.CompleteTask(task.GetWorkflowId(), task.GetStepName(), task.GetSequence(), task.GetLeaseToken(), nil, execErr.Error(), true)
		} else {
			_ = e.CompleteTask(task.GetWorkflowId(), task.GetStepName(), task.GetSequence(), task.GetLeaseToken(), out, "", false)
		}
	}
}

func waitForState(t *testing.T, e *Engine, id string, pred func(*state.State) bool, d time.Duration) *state.State {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		evs, _ := e.store.ReadAll(id)
		if st, err := state.Reconstruct(evs); err == nil && pred(st) {
			return st
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("condition not met for %s within %s", id, d)
	return nil
}

// TestRetryThenSucceed: a flaky activity fails twice, then succeeds. The retry
// policy re-runs it transparently — the workflow never sees the transient errors —
// and completes on the third attempt.
func TestRetryThenSucceed(t *testing.T) {
	dir := t.TempDir()
	def := state.WorkflowDefinition{Type: "Flaky", Steps: state.Activities("charge")}
	e := newTestEngine(t, dir, def)
	e.SetRetryPolicy("charge", RetryPolicy{MaxAttempts: 3, Backoff: 15 * time.Millisecond})

	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runEngineWorker(ctx, e, func(step string, _ []byte) ([]byte, error) {
		if n := attempts.Add(1); n < 3 {
			return nil, fmt.Errorf("transient failure %d", n)
		}
		return []byte("charged"), nil
	})

	id, err := e.StartWorkflow("Flaky", []byte("in"), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	final := waitCompleted(t, e, id, 5*time.Second)
	if final.Status != state.StatusCompleted {
		t.Fatalf("status: got %s, want COMPLETED", final.Status)
	}
	if string(final.Output) != "charged" {
		t.Errorf("output: got %q, want charged", final.Output)
	}
	if final.Steps[0].Attempts != 3 {
		t.Errorf("attempts: got %d, want 3", final.Steps[0].Attempts)
	}
	if attempts.Load() != 3 {
		t.Errorf("worker ran %d times, want 3", attempts.Load())
	}
}

// TestRetryExhaustedFailsWorkflow: when every attempt fails, the failure finally
// surfaces to the workflow (here, a linear definition fails).
func TestRetryExhaustedFailsWorkflow(t *testing.T) {
	dir := t.TempDir()
	def := state.WorkflowDefinition{Type: "Doomed", Steps: state.Activities("charge")}
	e := newTestEngine(t, dir, def)
	e.SetRetryPolicy("charge", RetryPolicy{MaxAttempts: 2, Backoff: 10 * time.Millisecond})

	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runEngineWorker(ctx, e, func(step string, _ []byte) ([]byte, error) {
		attempts.Add(1)
		return nil, errors.New("always down")
	})

	id, err := e.StartWorkflow("Doomed", []byte("in"), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	final := waitForState(t, e, id, func(st *state.State) bool { return st.Status == state.StatusFailed }, 5*time.Second)
	if final.Error != "always down" {
		t.Errorf("failure error: got %q, want always down", final.Error)
	}
	if attempts.Load() != 2 {
		t.Errorf("worker ran %d times, want 2 (MaxAttempts)", attempts.Load())
	}
	if final.Steps[0].Attempts != 2 {
		t.Errorf("attempts: got %d, want 2", final.Steps[0].Attempts)
	}
}

// sagaWorkflow is a code-based Saga: if charging fails, it releases the reservation
// (a compensating activity) and fails with a wrapped error.
func sagaWorkflow(ctx sdk.Context, input []byte) ([]byte, error) {
	reserved, err := ctx.ExecuteActivity("reserve", input)
	if err != nil {
		return nil, err
	}
	if _, err := ctx.ExecuteActivity("charge", reserved); err != nil {
		ctx.ExecuteActivity("release", reserved) // compensate
		return nil, fmt.Errorf("charge failed, reservation released: %w", err)
	}
	return ctx.ExecuteActivity("ship", reserved)
}

// TestSagaCompensationOnFailure: a failed activity returns a Go error the workflow
// catches, runs a compensating activity, and fails cleanly — the replay engine makes
// Saga just ordinary error handling.
func TestSagaCompensationOnFailure(t *testing.T) {
	dir := t.TempDir()
	e := newFuncEngine(t, dir, sdk.Workflow{Type: "Saga", Fn: sagaWorkflow})
	// No retry on charge: it fails once and compensation runs.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runEngineWorker(ctx, e, func(step string, _ []byte) ([]byte, error) {
		switch step {
		case "charge":
			return nil, errors.New("card declined")
		default:
			return []byte("ok:" + step), nil
		}
	})

	id, err := e.StartWorkflow("Saga", []byte("order"), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	final := waitForState(t, e, id, func(st *state.State) bool { return st.Status == state.StatusFailed }, 5*time.Second)
	if !strings.Contains(final.Error, "card declined") || !strings.Contains(final.Error, "released") {
		t.Errorf("failure error: got %q, want it to mention the decline and the release", final.Error)
	}
	// Steps: reserve (done), charge (failed), release (compensation, done).
	if len(final.Steps) != 3 {
		t.Fatalf("steps: got %d, want 3 (reserve, charge, release)", len(final.Steps))
	}
	if final.Steps[1].Name != "charge" || final.Steps[1].Status != state.StepFailed {
		t.Errorf("step 2: got {%s %s}, want charge FAILED", final.Steps[1].Name, final.Steps[1].Status)
	}
	if final.Steps[2].Name != "release" || final.Steps[2].Status != state.StepCompleted {
		t.Errorf("compensation: got {%s %s}, want release COMPLETED", final.Steps[2].Name, final.Steps[2].Status)
	}
}

// TestRetrySurvivesRestart: an activity that failed with retries pending is retried
// after a crash — the engine re-arms the retry from history on recovery.
func TestRetrySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	def := state.WorkflowDefinition{Type: "Flaky", Steps: state.Activities("charge")}

	// Engine 1: charge fails; a retry is armed with a long backoff, so it stays
	// pending when we crash.
	store1, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("store1: %v", err)
	}
	e1 := NewEngine(store1, discardLogger())
	e1.Register(def)
	e1.SetRetryPolicy("charge", RetryPolicy{MaxAttempts: 5, Backoff: 30 * time.Second})
	ctx1, cancel1 := context.WithCancel(context.Background())
	go runEngineWorker(ctx1, e1, func(step string, _ []byte) ([]byte, error) {
		return nil, errors.New("down")
	})
	id, err := e1.StartWorkflow("Flaky", []byte("in"), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitForState(t, e1, id, func(st *state.State) bool {
		return st.Tail() != nil && st.Tail().Status == state.StepFailed
	}, 3*time.Second)
	cancel1()
	e1.Close()
	store1.Close()

	// Engine 2: recover re-arms the pending retry (promptly); a healthy worker
	// finishes it.
	store2, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("store2: %v", err)
	}
	t.Cleanup(func() { store2.Close() })
	e2 := NewEngine(store2, discardLogger())
	t.Cleanup(e2.Close)
	e2.Register(def)
	e2.SetRetryPolicy("charge", RetryPolicy{MaxAttempts: 5, Backoff: 10 * time.Millisecond})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go runEngineWorker(ctx2, e2, func(step string, _ []byte) ([]byte, error) {
		return []byte("charged"), nil
	})
	if err := e2.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	final := waitCompleted(t, e2, id, 5*time.Second)
	if final.Status != state.StatusCompleted || string(final.Output) != "charged" {
		t.Fatalf("after restart: status %s output %q, want COMPLETED charged", final.Status, final.Output)
	}
	if final.Steps[0].Attempts < 2 {
		t.Errorf("attempts: got %d, want >= 2 (one failure + one retry)", final.Steps[0].Attempts)
	}
}
