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

// leaseKey names one in-flight step: a workflow can have several activities
// in flight at once, each under its own lease.
type leaseKey struct {
	workflowID string
	sequence   int64
}

// claim grants a lease on a just-dequeued task: mints the token, stamps it onto
// the task the worker receives, and arms the expiry timer. The caller holds the
// workflow's lock.
func (e *Engine) claim(t *Task, workerID string) *lease {
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
	e.mu.Lock()
	key := leaseKey{le.workflowID, le.sequence}
	if old, ok := e.leases[key]; ok {
		delete(e.leaseTokens, old.token)
	}
	e.leases[key] = le
	e.leaseTokens[le.token] = le
	e.mu.Unlock()
	e.metrics.tasksDispatched.Inc()
	t.LeaseToken = le.token
	t.LeaseDurationMs = e.leaseDuration.Milliseconds()
	e.leaseTimers.Add(timer.Timer{
		WorkflowID: le.workflowID, StepName: le.stepName,
		Sequence: le.sequence, FireAt: le.deadline,
	})
	return le
}

// checkLeaseLocked verifies that token is the current lease on the step's
// task and returns it. The caller must hold e.mu.
func (e *Engine) checkLeaseLocked(workflowID string, sequence int64, token string) (*lease, error) {
	le, ok := e.leases[leaseKey{workflowID, sequence}]
	if !ok {
		return nil, fmt.Errorf("server: no active lease for workflow %q step %d (lease expired, task reassigned, or engine restarted)", workflowID, sequence)
	}
	if le.token != token {
		return nil, fmt.Errorf("server: stale lease token for workflow %q: the task was reassigned to worker %q", workflowID, le.workerID)
	}
	return le, nil
}

// dropLease forgets a step's lease once its attempt is over.
func (e *Engine) dropLease(workflowID string, sequence int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if le, ok := e.leases[leaseKey{workflowID, sequence}]; ok {
		delete(e.leaseTokens, le.token)
		delete(e.leases, leaseKey{workflowID, sequence})
	}
}

// dropLeases forgets every lease of a finished workflow, so workers still
// running its steps are turned away when they report.
func (e *Engine) dropLeases(workflowID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for key, le := range e.leases {
		if key.workflowID == workflowID {
			delete(e.leaseTokens, le.token)
			delete(e.leases, key)
		}
	}
}

// Heartbeat extends the caller's lease, proving the worker is still alive. It
// fails if the lease is gone or held by someone else — the worker's cue to
// abandon the work, because another worker now owns (or will own) the task.
// Expiry is judged when the reaper runs, so a heartbeat that lands after the
// deadline but before reclamation still rescues the lease.
func (e *Engine) Heartbeat(workflowID, token string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	le, ok := e.leaseTokens[token]
	if !ok || le.workflowID != workflowID {
		return fmt.Errorf("server: no active lease for workflow %q with this token (lease expired, task reassigned, or engine restarted)", workflowID)
	}
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
	w := e.lockWorkflow(tm.WorkflowID)
	defer e.unlockWorkflow(w)
	key := leaseKey{tm.WorkflowID, tm.Sequence}

	e.mu.Lock()
	cur, ok := e.leases[key]
	var le lease
	if ok {
		le = *cur
	}
	e.mu.Unlock()
	if !ok || le.stepName != tm.StepName {
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
	events, st, err := e.historyLocked(w)
	if err != nil {
		e.log.Error("lease expired", "workflow_id", tm.WorkflowID, "err", err)
		if events == nil {
			e.rearmCallback(e.leaseTimers, tm)
		}
		return
	}
	e.mu.Lock()
	// A heartbeat may have landed since the copy above; it rescues the lease.
	if cur := e.leases[key]; cur == nil || cur.token != le.token || time.Now().Before(cur.deadline) {
		e.mu.Unlock()
		if cur != nil && cur.token == le.token {
			e.leaseTimers.Add(timer.Timer{WorkflowID: le.workflowID, StepName: le.stepName, Sequence: le.sequence, FireAt: cur.deadline})
		}
		return
	}
	delete(e.leaseTokens, le.token)
	delete(e.leases, key)
	e.mu.Unlock()

	// Belt and braces: only reclaim if history still shows this exact step in
	// flight. Anything else means the workflow moved on without the lease map
	// noticing (shouldn't happen; dropping the lease above is the safe recovery).
	step := st.Step(le.sequence)
	if st.Status.IsTerminal() || step == nil || step.Name != le.stepName ||
		step.Kind != state.KindActivity || step.Status != state.StepScheduled {
		return
	}
	attempt := *step
	workflowType := st.WorkflowType
	policy := e.retryPolicy(&attempt)
	willRetry := attempt.Attempts < policy.MaxAttempts
	failedEv := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
		StepFailed: &history.StepFailedEvent{
			StepName: le.stepName, Sequence: le.sequence,
			Error:      fmt.Sprintf("lease expired: worker %q missed its deadline", le.workerID),
			RetryCount: int32(attempt.Attempts), WillRetry: willRetry,
		},
	}}
	if err := e.appendLocked(w, failedEv, int64(len(events)+1)); err != nil {
		// The StepFailed event never landed, so history still shows this attempt
		// as scheduled with no lease and no queue entry. Re-enqueue it: a worker
		// gets a fresh lease and the attempt proceeds as if nothing happened.
		e.log.Error("lease expired: append failed; re-enqueueing task", "workflow_id", tm.WorkflowID, "err", err)
		e.queue.enqueue(&Task{
			WorkflowId: le.workflowID, StepName: le.stepName,
			Sequence: le.sequence, Input: attempt.Input,
		})
		return
	}
	e.metrics.leasesExpired.Inc()
	e.log.Warn("lease expired; reclaiming task", "workflow_id", tm.WorkflowID,
		"worker_id", le.workerID, "step", le.stepName, "sequence", le.sequence, "attempt", attempt.Attempts)
	if willRetry {
		e.scheduleRetryLocked(tm.WorkflowID, &attempt, policy.Delay(attempt.Attempts))
		return
	}
	e.advanceOrRetry(w, workflowType)
}
