package server

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/internal/timer"
)

// A lease is a worker's time-boxed claim on a dispatched task. PollTask grants
// one; the worker must CompleteTask or Heartbeat before the deadline or the
// engine declares the worker dead and reassigns the task. The token is the
// proof of ownership: a result or heartbeat without the current token is
// rejected, so a "zombie" worker whose task was reassigned can never record a
// stale result over a live attempt's.
//
// Leases are ephemeral, like the task queue: an engine restart drops them all,
// and recovery re-enqueues in-flight steps for fresh claims. The durable fact
// is the step being scheduled, not who holds it.
type lease struct {
	workflowID string
	stepName   string
	sequence   int64
	workerID   string
	token      string
	claimedAt  time.Time // when the task was dispatched; basis for step latency
	deadline   time.Time
}

// defaultLeaseDuration is how long a worker may hold a task without a
// heartbeat. Generous for a Phase-3 default; tune per deployment via
// WithLeaseDuration.
const defaultLeaseDuration = 30 * time.Second

// newLeaseToken returns a 128-bit random token. Uniqueness, not secrecy, is the
// requirement: it must never collide with the token of another attempt.
func newLeaseToken() string {
	return fmt.Sprintf("%016x%016x", rand.Uint64(), rand.Uint64())
}

// claimLocked grants a lease on a just-dequeued task: mints the token, stamps
// it onto the task the worker receives, and arms the expiry timer. The caller
// must hold e.mu. One in-flight step per workflow means one lease per workflow,
// so the map is keyed by workflow id.
func (e *Engine) claimLocked(t *Task, workerID string) *lease {
	now := time.Now()
	le := &lease{
		workflowID: t.GetWorkflowId(),
		stepName:   t.GetStepName(),
		sequence:   t.GetSequence(),
		workerID:   workerID,
		token:      newLeaseToken(),
		claimedAt:  now,
		deadline:   now.Add(e.leaseDuration),
	}
	e.leases[le.workflowID] = le
	e.metrics.tasksDispatched.Inc()
	t.LeaseToken = le.token
	t.LeaseDurationMs = e.leaseDuration.Milliseconds()
	e.leaseTimers.Add(timer.Timer{
		WorkflowID: le.workflowID, StepName: le.stepName,
		Sequence: le.sequence, FireAt: le.deadline,
	})
	return le
}

// checkLeaseLocked verifies that token is the current lease on the workflow's
// in-flight task. The caller must hold e.mu.
func (e *Engine) checkLeaseLocked(workflowID, token string) error {
	le, ok := e.leases[workflowID]
	if !ok {
		return fmt.Errorf("server: no active lease for workflow %q (lease expired, task reassigned, or engine restarted)", workflowID)
	}
	if le.token != token {
		return fmt.Errorf("server: stale lease token for workflow %q: the task was reassigned to worker %q", workflowID, le.workerID)
	}
	return nil
}

// Heartbeat extends the caller's lease, proving the worker is still alive. It
// fails if the lease is gone or held by someone else — the worker's cue to
// abandon the work, because another worker now owns (or will own) the task.
// Expiry is judged when the reaper runs, so a heartbeat that lands after the
// deadline but before reclamation still rescues the lease.
func (e *Engine) Heartbeat(workflowID, token string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.checkLeaseLocked(workflowID, token); err != nil {
		return err
	}
	le := e.leases[workflowID]
	le.deadline = time.Now().Add(e.leaseDuration)
	e.log.Debug("lease extended", "workflow_id", workflowID, "worker_id", le.workerID,
		"step", le.stepName, "sequence", le.sequence, "deadline", le.deadline)
	return nil
}

// onLeaseExpired is the lease timer's callback: decide whether the lease it was
// armed for is actually dead, and if so reclaim the task. It acts on the
// *current* lease state under e.mu, not on the timer's snapshot, so stale fires
// self-correct:
//
//   - lease gone (completed, or already reclaimed): no-op.
//   - deadline moved (a heartbeat, or a newer lease on a retry of the same
//     step): re-arm at the new deadline and check again then.
//   - deadline passed: the worker is dead. Record StepFailed("lease expired")
//     and hand the failure to the same retry machinery a worker-reported error
//     uses — attempts remaining ⇒ backoff + reassign; exhausted ⇒ the workflow
//     sees the error (and can compensate).
//
// Treating a dead worker as a failed attempt (rather than silently re-queueing)
// is deliberate: it is durable, it counts against MaxAttempts so a task that
// kills its workers can't loop forever, and it leaves an audit trail in history.
func (e *Engine) onLeaseExpired(tm timer.Timer) {
	e.mu.Lock()
	defer e.mu.Unlock()

	le, ok := e.leases[tm.WorkflowID]
	if !ok || le.stepName != tm.StepName || le.sequence != tm.Sequence {
		return // resolved or superseded; stale fire
	}
	if now := time.Now(); now.Before(le.deadline) {
		e.leaseTimers.Add(timer.Timer{
			WorkflowID: le.workflowID, StepName: le.stepName,
			Sequence: le.sequence, FireAt: le.deadline,
		})
		return
	}

	// Read history before dropping the lease: on a transient I/O failure the
	// lease stays armed and the re-check below retries the whole reclaim.
	events, err := e.store.ReadAll(tm.WorkflowID)
	if err != nil {
		e.log.Error("lease expired: read history", "workflow_id", tm.WorkflowID, "err", err)
		e.rearmCallback(e.leaseTimers, tm)
		return
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		e.log.Error("lease expired: reconstruct", "workflow_id", tm.WorkflowID, "err", err)
		return
	}
	delete(e.leases, tm.WorkflowID)

	tail := st.Tail()
	// Belt and braces: only reclaim if history still shows this exact step in
	// flight. Anything else means the workflow moved on without the lease map
	// noticing (shouldn't happen; dropping the lease above is the safe recovery).
	if st.Status != state.StatusWaiting || tail == nil ||
		tail.Sequence != le.sequence || tail.Name != le.stepName || tail.Status != state.StepScheduled {
		return
	}
	failedEv := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
		StepFailed: &history.StepFailedEvent{
			StepName: le.stepName, Sequence: le.sequence,
			Error:      fmt.Sprintf("lease expired: worker %q missed its deadline", le.workerID),
			RetryCount: int32(tail.Attempts),
		},
	}}
	if err := e.appendLocked(tm.WorkflowID, failedEv, int64(len(events)+1)); err != nil {
		// The StepFailed event never landed, so history still shows this attempt
		// as scheduled with no lease and no queue entry. Re-enqueue it: a worker
		// gets a fresh lease and the attempt proceeds as if nothing happened.
		e.log.Error("lease expired: append failed; re-enqueueing task", "workflow_id", tm.WorkflowID, "err", err)
		e.queue.enqueue(&Task{
			WorkflowId: le.workflowID, StepName: le.stepName,
			Sequence: le.sequence, Input: tail.Input,
		})
		return
	}
	e.metrics.leasesExpired.Inc()
	e.log.Warn("lease expired; reclaiming task", "workflow_id", tm.WorkflowID,
		"worker_id", le.workerID, "step", le.stepName, "sequence", le.sequence, "attempt", tail.Attempts)

	policy := e.retryPolicy(tail)
	if tail.Kind == state.KindActivity && tail.Attempts < policy.MaxAttempts {
		e.scheduleRetryLocked(tm.WorkflowID, tail, policy.Delay(tail.Attempts))
		return
	}
	e.advanceOrRetry(tm, tm.WorkflowID, st.WorkflowType)
}
