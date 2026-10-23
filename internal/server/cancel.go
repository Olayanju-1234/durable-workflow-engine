package server

import (
	"fmt"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// Cancellation, two ways (ADR-0009):
//
//   - CancelWorkflow ends a workflow as CANCELLED: a WorkflowFailed event with
//     cancelled set. Like any terminal event it abandons what is in flight.
//   - A cancel_step command closes one open step the workflow code no longer
//     wants, the loser of a race say: a StepFailed event with cancelled set.
//     A timer stops mattering, a signal wait stops consuming signals, and a
//     queued or retrying activity never runs again.
//
// Either way a worker still running an abandoned activity is told so: its
// lease is replaced by a tombstone, and its next heartbeat or completion fails
// with ErrActivityCancelled instead of a plain lost lease.

// cancelStepLocked records the cancellation of an open step and reports
// whether there was one to cancel. The caller must hold w.mu.
func (e *Engine) cancelStepLocked(w *wfState, sequence int64, reason string) bool {
	events, st, err := e.historyLocked(w)
	if err != nil {
		return false
	}
	step := st.Step(sequence)
	if step == nil || !step.Open() {
		return false
	}
	msg := "cancelled"
	if reason != "" {
		msg += ": " + reason
	}
	name, kind, attempts := step.Name, step.Kind, step.Attempts
	cancelled := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
		StepFailed: &history.StepFailedEvent{
			StepName: name, Sequence: sequence, Error: msg, Cancelled: true, RetryCount: int32(attempts),
		},
	}}
	if err := e.appendLocked(w, cancelled, int64(len(events)+1)); err != nil {
		e.log.Error("cancel step: append", "workflow_id", w.id, "step", name, "err", err)
		return false
	}
	if kind == state.KindActivity {
		e.cancelLease(w.id, sequence, msg)
		e.queue.dropWhere(func(t *Task) bool { return t.GetWorkflowId() == w.id && t.GetSequence() == sequence })
	}
	e.metrics.stepsCancelled.Inc()
	e.log.Info("step cancelled", "workflow_id", w.id, "step", name, "sequence", sequence, "kind", kind, "reason", reason)
	return true
}

// CancelWorkflow ends a running workflow as CANCELLED, recording the reason.
// Workers still running its activities are told to stop at their next
// heartbeat or completion.
func (e *Engine) CancelWorkflow(workflowID, reason string) error {
	msg := "cancelled"
	if reason != "" {
		msg += ": " + reason
	}
	return e.endWorkflow(workflowID, "cancelled", reason,
		&history.WorkflowFailedEvent{Error: msg, Cancelled: true})
}

// endWorkflow durably ends a non-terminal workflow with a WorkflowFailed event
// (cancelled or terminated, as ev says) on a client's request.
func (e *Engine) endWorkflow(workflowID, how, reason string, ev *history.WorkflowFailedEvent) error {
	if err := validateID(workflowID); err != nil {
		return fmt.Errorf("%w: %q", ErrNotFound, workflowID)
	}
	w := e.lockWorkflow(workflowID)
	defer e.unlockWorkflow(w)

	events, st, err := e.historyLocked(w)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return fmt.Errorf("%w: %q", ErrNotFound, workflowID)
	}
	if st.Status.IsTerminal() {
		return fmt.Errorf("%w: %q is %s", ErrAlreadyTerminal, workflowID, st.Status)
	}
	end := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowFailed{WorkflowFailed: ev}}
	if err := e.appendLocked(w, end, int64(len(events)+1)); err != nil {
		return err
	}
	e.metrics.workflowsFinished.With(how).Inc()
	e.metrics.workflowsActive.Dec()
	e.log.Warn("workflow "+how, "workflow_id", workflowID, "reason", reason)
	return nil
}
