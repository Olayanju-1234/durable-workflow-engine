package server

import (
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/internal/timer"
)

// RetryPolicy controls automatic re-execution of a failed activity. Retries are an
// engine concern, handled transparently: the workflow only sees an activity error
// once attempts are exhausted. See state.RetryPolicy.
type RetryPolicy = state.RetryPolicy

// noRetry is the default: run once, surface any failure to the workflow immediately.
var noRetry = RetryPolicy{MaxAttempts: 1}

// SetRetryPolicy registers a retry policy for an activity by step name. Call before
// serving. A policy the workflow code sets on the call itself (recorded with the
// step) takes precedence.
func (e *Engine) SetRetryPolicy(stepName string, p RetryPolicy) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policies[stepName] = p
}

// retryPolicy is the policy that governs a step: the one recorded with it,
// else the engine's for its name, else no retries.
func (e *Engine) retryPolicy(step *state.Step) RetryPolicy {
	if step.Retry != nil {
		return *step.Retry
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if p, ok := e.policies[step.Name]; ok {
		return p
	}
	return noRetry
}

// scheduleRetryLocked arms a backoff timer to re-run a failed activity. The failure
// is already recorded; when the timer fires, onRetryDue re-schedules the same step
// sequence (another attempt). A zero delay fires promptly
// (used on recovery, where the original backoff window is lost).
func (e *Engine) scheduleRetryLocked(workflowID string, step *state.Step, delay time.Duration) {
	e.retryTimers.Add(timer.Timer{
		WorkflowID: workflowID, StepName: step.Name,
		Sequence: step.Sequence, FireAt: time.Now().Add(delay),
	})
	e.metrics.retriesScheduled.Inc()
	e.log.Info("retry scheduled", "workflow_id", workflowID, "step", step.Name,
		"sequence", step.Sequence, "attempt", step.Attempts+1, "delay", delay)
}

// onRetryDue is the retry timer's callback: re-schedule the failed activity as a new
// attempt (same sequence) and dispatch it. Guarded so a stale fire is a no-op.
func (e *Engine) onRetryDue(tm timer.Timer) {
	w := e.lockWorkflow(tm.WorkflowID)
	defer e.unlockWorkflow(w)

	events, st, err := e.historyLocked(w)
	if err != nil {
		e.log.Error("retry", "workflow_id", tm.WorkflowID, "err", err)
		if events == nil {
			e.rearmCallback(e.retryTimers, tm)
		}
		return
	}
	// Only fire if the target step is still failed and awaiting its retry: a
	// failure recorded as will_retry, or (histories from before that flag) the
	// latest step's failure with nothing else in flight.
	target := st.Step(tm.Sequence)
	if st.Status.IsTerminal() || target == nil || target.Name != tm.StepName || target.Status != state.StepFailed ||
		!(target.Retrying || (target == st.Tail() && st.Status == state.StatusRunning)) {
		return
	}
	step := *target
	retry := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
		StepScheduled: &history.StepScheduledEvent{
			StepName: step.Name, Sequence: step.Sequence, Input: step.Input,
			RetryPolicy: step.Retry.ToProto(),
		},
	}}
	if err := e.appendLocked(w, retry, int64(len(events)+1)); err != nil {
		e.log.Error("retry: append", "workflow_id", tm.WorkflowID, "err", err)
		e.rearmCallback(e.retryTimers, tm)
		return
	}
	e.queue.enqueue(&Task{
		WorkflowId: tm.WorkflowID, StepName: step.Name,
		Sequence: step.Sequence, Input: step.Input,
	})
	e.log.Info("retry dispatched", "workflow_id", tm.WorkflowID, "step", step.Name,
		"sequence", step.Sequence, "attempt", step.Attempts+1)
}
