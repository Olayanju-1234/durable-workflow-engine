package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// newLeaseEngine builds an engine with a short lease for expiry tests.
func newLeaseEngine(t *testing.T, dir string, leaseDur time.Duration, defs ...state.WorkflowDefinition) *Engine {
	t.Helper()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	e := NewEngine(store, discardLogger(), WithLeaseDuration(leaseDur))
	t.Cleanup(e.Close)
	for _, d := range defs {
		e.Register(d)
	}
	return e
}

func mustPoll(t *testing.T, e *Engine, workerID string, d time.Duration) *Task {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	task, err := e.PollTask(ctx, workerID)
	if err != nil {
		t.Fatalf("poll as %s: %v", workerID, err)
	}
	return task
}

// TestLeaseExpiryReassignsTaskAndRejectsZombie is the Week-8 deliverable: a
// worker takes a task and dies (never completes, never heartbeats). The lease
// expires, the failure is recorded, and the retry machinery hands the same step
// to a healthy worker, who finishes the workflow. The dead worker coming back
// to life ("zombie") is turned away — its stale token can't record anything.
func TestLeaseExpiryReassignsTaskAndRejectsZombie(t *testing.T) {
	def := state.WorkflowDefinition{Type: "One", Steps: state.Activities("work")}
	e := newLeaseEngine(t, t.TempDir(), 100*time.Millisecond, def)
	e.SetRetryPolicy("work", RetryPolicy{MaxAttempts: 2, Backoff: 10 * time.Millisecond})

	id, err := e.StartWorkflow("One", []byte("in"), "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	// The zombie claims the task and goes dark.
	zombieTask := mustPoll(t, e, "zombie", time.Second)

	// The lease expires and the retry re-dispatches: a healthy worker gets the
	// same step as a fresh attempt with a fresh token.
	healthyTask := mustPoll(t, e, "healthy", 3*time.Second)
	if healthyTask.GetStepName() != "work" || healthyTask.GetSequence() != 1 {
		t.Fatalf("reassigned task: got %s/seq%d, want work/seq1", healthyTask.GetStepName(), healthyTask.GetSequence())
	}
	if healthyTask.GetLeaseToken() == zombieTask.GetLeaseToken() {
		t.Fatal("reassigned task reused the zombie's lease token")
	}

	if err := e.CompleteTask(id, "work", 1, healthyTask.GetLeaseToken(), []byte("done"), "", false); err != nil {
		t.Fatalf("healthy complete: %v", err)
	}
	final := waitCompleted(t, e, id, 3*time.Second)
	if string(final.Output) != "done" {
		t.Errorf("output: got %q, want done", final.Output)
	}
	if final.Steps[0].Attempts != 2 {
		t.Errorf("attempts: got %d, want 2 (zombie's expiry + healthy's run)", final.Steps[0].Attempts)
	}

	// The zombie wakes up and reports its result: rejected, never recorded.
	err = e.CompleteTask(id, "work", 1, zombieTask.GetLeaseToken(), []byte("stale"), "", false)
	if err == nil || !strings.Contains(err.Error(), "lease") {
		t.Fatalf("zombie complete: got %v, want a lease rejection", err)
	}

	// The expiry left an audit trail: a StepFailed naming the dead worker.
	events, _ := e.store.ReadAll(id)
	found := false
	for _, ev := range events {
		if sf, ok := ev.GetAttributes().(*history.HistoryEvent_StepFailed); ok {
			if strings.Contains(sf.StepFailed.GetError(), "lease expired") &&
				strings.Contains(sf.StepFailed.GetError(), "zombie") {
				found = true
			}
		}
	}
	if !found {
		t.Error("history has no StepFailed recording the lease expiry")
	}
}

// TestLeaseExpiryWithoutRetryFailsWorkflow: with no retry policy (MaxAttempts 1),
// a dead worker's lease expiry surfaces to the workflow as the step's failure —
// explicit and terminal, instead of the pre-Phase-3 behaviour of stranding the
// workflow in WAITING forever.
func TestLeaseExpiryWithoutRetryFailsWorkflow(t *testing.T) {
	def := state.WorkflowDefinition{Type: "One", Steps: state.Activities("work")}
	e := newLeaseEngine(t, t.TempDir(), 80*time.Millisecond, def)

	id, err := e.StartWorkflow("One", nil, "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	mustPoll(t, e, "doomed-worker", time.Second) // claim and go dark

	final := waitForState(t, e, id, func(st *state.State) bool { return st.Status == state.StatusFailed }, 3*time.Second)
	if !strings.Contains(final.Error, "lease expired") || !strings.Contains(final.Error, "doomed-worker") {
		t.Errorf("workflow error: got %q, want it to name the lease expiry and the worker", final.Error)
	}
}

// TestHeartbeatKeepsLongTaskAlive: a task that legitimately runs much longer
// than the lease survives by heartbeating. Teeth: there is no retry policy, so
// if the lease expired the workflow would FAIL (per the test above) — reaching
// COMPLETED with Attempts==1 proves the heartbeats actually extended the lease
// across three full lease windows.
func TestHeartbeatKeepsLongTaskAlive(t *testing.T) {
	def := state.WorkflowDefinition{Type: "Slow", Steps: state.Activities("crunch")}
	e := newLeaseEngine(t, t.TempDir(), 500*time.Millisecond, def)

	id, err := e.StartWorkflow("Slow", nil, "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	task := mustPoll(t, e, "slow-worker", time.Second)

	// Simulate 1.5s of work (3× the lease), heartbeating every 100ms.
	workDone := time.After(1500 * time.Millisecond)
	tick := time.NewTicker(100 * time.Millisecond)
	defer tick.Stop()
work:
	for {
		select {
		case <-tick.C:
			if err := e.Heartbeat(id, task.GetLeaseToken()); err != nil {
				t.Fatalf("heartbeat: %v", err)
			}
		case <-workDone:
			break work
		}
	}

	if err := e.CompleteTask(id, "crunch", 1, task.GetLeaseToken(), []byte("ok"), "", false); err != nil {
		t.Fatalf("complete after long work: %v", err)
	}
	final := waitCompleted(t, e, id, 3*time.Second)
	if final.Steps[0].Attempts != 1 {
		t.Errorf("attempts: got %d, want 1 (never reassigned)", final.Steps[0].Attempts)
	}
}

// TestHeartbeatStaleLeaseRejected: once a lease has expired and been reclaimed,
// a heartbeat with the old token is refused — the zombie worker's cue to stop.
func TestHeartbeatStaleLeaseRejected(t *testing.T) {
	def := state.WorkflowDefinition{Type: "One", Steps: state.Activities("work")}
	e := newLeaseEngine(t, t.TempDir(), 60*time.Millisecond, def)
	e.SetRetryPolicy("work", RetryPolicy{MaxAttempts: 2, Backoff: 10 * time.Millisecond})

	id, err := e.StartWorkflow("One", nil, "")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	task := mustPoll(t, e, "zombie", time.Second)

	// Wait for the reaper to reclaim the lease (a StepFailed lands in history).
	waitForState(t, e, id, func(st *state.State) bool {
		return st.Tail() != nil && (st.Tail().Attempts > 1 || st.Tail().Status == state.StepFailed)
	}, 3*time.Second)

	if err := e.Heartbeat(id, task.GetLeaseToken()); err == nil {
		t.Fatal("heartbeat on an expired lease succeeded; the zombie was never told to stop")
	}
	if err := e.Heartbeat("no-such-workflow", "bogus"); err == nil {
		t.Fatal("heartbeat on an unknown workflow succeeded")
	}
}
