package server

import (
	"fmt"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/internal/timer"
)

// Timeouts. Every limit is recorded in history as a duration next to the
// event it is measured from, so the deadline is a pure function of history and
// recovery re-arms the same one:
//
//   - execution timeout: WorkflowStarted + execution_timeout_ms. Passing it
//     ends the workflow as TIMED_OUT, abandoning whatever is in flight.
//   - schedule-to-close: the step's first StepScheduled + its limit. Passing it
//     fails the step for good, retries or not; a retry is only planned if it
//     can start before then.
//   - start-to-close: one attempt, from dispatch. It rides on the attempt's
//     lease (the lease timer fires at the earlier of the two), because both
//     are about one worker's claim and both end in the same retryable failure.
//
// The deadlines service holds both durable deadlines: Sequence 0 is a
// workflow's execution timeout, anything else a step's schedule-to-close.

func (e *Engine) armExecutionDeadline(workflowID, runID string, at time.Time) {
	e.deadlines.Add(timer.Timer{WorkflowID: workflowID, RunID: runID, FireAt: at})
}

// armStepDeadline arms an activity's schedule-to-close timeout, if it has one.
func (e *Engine) armStepDeadline(workflowID, runID string, s *state.Step) {
	if at, ok := scheduleToCloseDeadline(s); ok {
		e.deadlines.Add(timer.Timer{WorkflowID: workflowID, RunID: runID, StepName: s.Name, Sequence: s.Sequence, FireAt: at})
	}
}

// staleRun reports whether a timer was armed for an earlier run of the
// workflow than the one now current.
func staleRun(tm timer.Timer, st *state.State) bool {
	return tm.RunID != "" && tm.RunID != st.RunID
}

func scheduleToCloseDeadline(s *state.Step) (time.Time, bool) {
	if s.ScheduleToClose <= 0 {
		return time.Time{}, false
	}
	return time.Unix(0, s.ScheduledAt).Add(s.ScheduleToClose), true
}

// activityTask is the task a worker receives for an open activity step.
func activityTask(workflowID string, s *state.Step) *Task {
	return &Task{
		WorkflowId: workflowID, StepName: s.Name, Sequence: s.Sequence, Input: s.Input,
		StartToCloseTimeoutMs: s.StartToClose.Milliseconds(),
		Attempt:               int32(max(s.Attempts, 1)),
		ActivityId:            fmt.Sprintf("%s/%d", localID(workflowID), s.Sequence),
	}
}

func (e *Engine) onDeadline(tm timer.Timer) {
	if tm.Sequence == 0 {
		e.onExecutionDeadline(tm)
	} else {
		e.onStepDeadline(tm)
	}
}

// onExecutionDeadline times the workflow out if it is still running at its
// recorded deadline. Like every callback it re-checks history first, so a
// stale fire (the workflow finished, or a re-armed duplicate) does nothing.
func (e *Engine) onExecutionDeadline(tm timer.Timer) {
	w := e.lockWorkflow(tm.WorkflowID)
	defer e.unlockWorkflow(w)
	events, st, err := e.historyLocked(w)
	if err != nil {
		e.log.Error("execution timeout", "workflow_id", tm.WorkflowID, "err", err)
		if events == nil {
			e.rearmCallback(e.deadlines, tm)
		}
		return
	}
	if staleRun(tm, st) || st.Status.IsTerminal() || st.Status == state.StatusNotStarted || st.ExecutionTimeout <= 0 {
		return
	}
	if deadline := time.Unix(0, st.StartedAt).Add(st.ExecutionTimeout); time.Now().Before(deadline) {
		e.armExecutionDeadline(tm.WorkflowID, st.RunID, deadline)
		return
	}
	msg := fmt.Sprintf("workflow timed out: execution timeout of %s exceeded", st.ExecutionTimeout)
	timedOut := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowFailed{
		WorkflowFailed: &history.WorkflowFailedEvent{Error: msg, TimedOut: true},
	}}
	if err := e.appendLocked(w, timedOut, int64(len(events)+1)); err != nil {
		e.log.Error("execution timeout: append", "workflow_id", tm.WorkflowID, "err", err)
		e.rearmCallback(e.deadlines, tm)
		return
	}
	e.metrics.workflowsFinished.With("timed_out").Inc()
	e.metrics.workflowsActive.Dec()
	e.log.Warn("workflow timed out", "workflow_id", tm.WorkflowID, "execution_timeout", st.ExecutionTimeout)
}

// onStepDeadline fails an activity that is still open (in flight, queued or
// between attempts) at its schedule-to-close deadline.
func (e *Engine) onStepDeadline(tm timer.Timer) {
	w := e.lockWorkflow(tm.WorkflowID)
	defer e.unlockWorkflow(w)
	events, st, err := e.historyLocked(w)
	if err != nil {
		e.log.Error("schedule-to-close timeout", "workflow_id", tm.WorkflowID, "err", err)
		if events == nil {
			e.rearmCallback(e.deadlines, tm)
		}
		return
	}
	step := st.Step(tm.Sequence)
	if staleRun(tm, st) || st.Status.IsTerminal() || step == nil || step.Kind != state.KindActivity || step.Name != tm.StepName || !step.Open() {
		return
	}
	deadline, ok := scheduleToCloseDeadline(step)
	if !ok {
		return
	}
	if time.Now().Before(deadline) {
		e.deadlines.Add(timer.Timer{WorkflowID: tm.WorkflowID, RunID: st.RunID, StepName: step.Name, Sequence: step.Sequence, FireAt: deadline})
		return
	}
	s := *step
	e.timeOutStepLocked(w, &s)
}

// timeOutStepLocked records the final failure of an activity whose
// schedule-to-close deadline passed, turns away its worker and advances. The
// caller must hold w.mu.
func (e *Engine) timeOutStepLocked(w *wfState, step *state.Step) {
	events, st, err := e.historyLocked(w)
	if err != nil {
		e.log.Error("schedule-to-close timeout", "workflow_id", w.id, "err", err)
		return
	}
	failed := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
		StepFailed: &history.StepFailedEvent{
			StepName: step.Name, Sequence: step.Sequence, RetryCount: int32(step.Attempts),
			Error:     fmt.Sprintf("activity timed out: schedule-to-close timeout of %s exceeded", step.ScheduleToClose),
			ErrorType: ErrorTypeScheduleToCloseTimeout,
		},
	}}
	if err := e.appendLocked(w, failed, int64(len(events)+1)); err != nil {
		e.log.Error("schedule-to-close timeout: append", "workflow_id", w.id, "err", err)
		e.deadlines.Add(timer.Timer{WorkflowID: w.id, RunID: st.RunID, StepName: step.Name, Sequence: step.Sequence,
			FireAt: time.Now().Add(callbackRetryDelay)})
		return
	}
	e.dropLease(w.id, step.Sequence)
	e.queue.dropWhere(func(t *Task) bool { return t.GetWorkflowId() == w.id && t.GetSequence() == step.Sequence })
	e.metrics.activityTimeouts.With("schedule_to_close").Inc()
	e.log.Warn("activity timed out", "workflow_id", w.id, "step", step.Name, "sequence", step.Sequence,
		"timeout", "schedule_to_close", "limit", step.ScheduleToClose, "attempts", step.Attempts)
	e.advanceOrRetry(w, st.WorkflowType)
}
