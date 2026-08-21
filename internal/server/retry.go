package server

import (
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/internal/timer"
)

// RetryPolicy controls automatic re-execution of a failed activity. Retries are an
// engine concern, handled transparently: the workflow only sees an activity error
// once attempts are exhausted. MaxAttempts counts the first try (1 = no retry);
// Backoff is the base delay, doubled each subsequent attempt (exponential), capped
// at MaxBackoff.
type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
	MaxBackoff  time.Duration
}

// noRetry is the default: run once, surface any failure to the workflow immediately.
var noRetry = RetryPolicy{MaxAttempts: 1}

// SetRetryPolicy registers a retry policy for an activity by step name. Call before
// serving. Keying by step name is a Phase-2 simplification; per-call activity options
// arrive with the richer SDK in Phase 5.
func (e *Engine) SetRetryPolicy(stepName string, p RetryPolicy) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.policies[stepName] = p
}

func (e *Engine) retryPolicy(stepName string) RetryPolicy {
	if p, ok := e.policies[stepName]; ok {
		return p
	}
	return noRetry
}

// backoff returns the delay before the given attempt's retry (attempt is the number
// that just failed: 1 → first backoff). Exponential, capped at MaxBackoff.
func (p RetryPolicy) backoff(attempt int) time.Duration {
	d := p.Backoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if p.MaxBackoff > 0 && d >= p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	return d
}

// scheduleRetryLocked arms a backoff timer to re-run a failed activity. The failure
// is already recorded; when the timer fires, onRetryDue re-schedules the same step
// sequence (another attempt). The caller must hold e.mu. A zero delay fires promptly
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
	e.mu.Lock()
	defer e.mu.Unlock()

	events, err := e.store.ReadAll(tm.WorkflowID)
	if err != nil {
		e.log.Error("retry: read history", "workflow_id", tm.WorkflowID, "err", err)
		e.rearmCallback(e.retryTimers, tm)
		return
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		e.log.Error("retry: reconstruct", "workflow_id", tm.WorkflowID, "err", err)
		return
	}
	tail := st.Tail()
	// Only fire if the target step is still failed and awaiting its retry.
	if tail == nil || tail.Sequence != tm.Sequence || tail.Name != tm.StepName || tail.Status != state.StepFailed {
		return
	}
	retry := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
		StepScheduled: &history.StepScheduledEvent{
			StepName: tail.Name, Sequence: tail.Sequence, Input: tail.Input,
		},
	}}
	if err := e.appendLocked(tm.WorkflowID, retry, int64(len(events)+1)); err != nil {
		e.log.Error("retry: append", "workflow_id", tm.WorkflowID, "err", err)
		e.rearmCallback(e.retryTimers, tm)
		return
	}
	e.queue.enqueue(&Task{
		WorkflowId: tm.WorkflowID, StepName: tail.Name,
		Sequence: tail.Sequence, Input: tail.Input,
	})
	e.log.Info("retry dispatched", "workflow_id", tm.WorkflowID, "step", tail.Name,
		"sequence", tail.Sequence, "attempt", tail.Attempts+1)
}
