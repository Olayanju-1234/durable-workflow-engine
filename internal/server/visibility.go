package server

import (
	"errors"
	"fmt"
	"sort"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// Sentinel errors the gRPC layer maps to status codes; everything else is a
// server-side fault.
var (
	ErrNotFound        = errors.New("server: workflow not found")
	ErrAlreadyTerminal = errors.New("server: workflow already terminal")
)

// ListWorkflows summarizes every workflow in the store, sorted by id. When
// filter is not UNSPECIFIED only matching workflows are returned.
//
// This is a full scan — read and replay every history file — which is fine at
// Phase-3 scale and honest about the storage model: the data dir *is* the
// index. A dedicated visibility store lands if/when this becomes the
// bottleneck (same upgrade path as ADR-0001).
func (e *Engine) ListWorkflows(filter WorkflowStatus) ([]*WorkflowSummary, error) {
	ids, err := e.store.ListWorkflows()
	if err != nil {
		return nil, fmt.Errorf("server: list workflows: %w", err)
	}
	sort.Strings(ids)

	summaries := make([]*WorkflowSummary, 0, len(ids))
	for _, id := range ids {
		sum := e.summarize(id)
		if filter != WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED && sum.GetStatus() != filter {
			continue
		}
		summaries = append(summaries, sum)
	}
	return summaries, nil
}

// GetWorkflow returns one workflow in full reconstructed detail: its summary
// plus every step as replay sees it. Unknown ids return ErrNotFound.
func (e *Engine) GetWorkflow(workflowID string) (*GetWorkflowResponse, error) {
	events, err := e.store.ReadAll(workflowID)
	if err != nil {
		return nil, fmt.Errorf("server: read history %s: %w", workflowID, err)
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, workflowID)
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		return nil, fmt.Errorf("server: reconstruct %s: %w", workflowID, err)
	}

	steps := make([]*StepInfo, 0, len(st.Steps))
	for _, s := range st.Steps {
		steps = append(steps, &StepInfo{
			StepName:   s.Name,
			Sequence:   s.Sequence,
			Kind:       s.Kind.String(),
			Status:     s.Status.String(),
			Attempts:   int32(s.Attempts),
			Error:      s.Error,
			Output:     s.Output,
			FireAt:     s.FireAt,
			SignalName: s.Signal,
		})
	}
	return &GetWorkflowResponse{
		Summary: summaryFrom(workflowID, events, st),
		Steps:   steps,
		Input:   st.Input,
		Output:  st.Output,
	}, nil
}

// TerminateWorkflow is the operator kill switch: it durably closes a
// non-terminal workflow by recording a WorkflowFailed event flagged as
// terminated, with the reason in the error text. The current lease (if any) is
// dropped, so a worker still running the in-flight step is rejected when it
// reports back — same zombie-fencing as lease expiry. Leftover timers are
// harmless: every timer callback re-checks history and no-ops on a terminal
// workflow.
func (e *Engine) TerminateWorkflow(workflowID, reason string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	events, err := e.store.ReadAll(workflowID)
	if err != nil {
		return fmt.Errorf("server: read history %s: %w", workflowID, err)
	}
	if len(events) == 0 {
		return fmt.Errorf("%w: %q", ErrNotFound, workflowID)
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		return fmt.Errorf("server: reconstruct %s: %w", workflowID, err)
	}
	if st.Status.IsTerminal() {
		return fmt.Errorf("%w: %q is %s", ErrAlreadyTerminal, workflowID, st.Status)
	}

	msg := "terminated by operator"
	if reason != "" {
		msg += ": " + reason
	}
	failedEv := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowFailed{
		WorkflowFailed: &history.WorkflowFailedEvent{Error: msg, Terminated: true},
	}}
	if err := e.appendLocked(workflowID, failedEv, int64(len(events)+1)); err != nil {
		return err
	}
	delete(e.leases, workflowID)
	e.metrics.workflowsFinished.With("terminated").Inc()
	e.metrics.workflowsActive.Dec()
	e.log.Warn("workflow terminated", "workflow_id", workflowID, "reason", reason)
	return nil
}

// summarize builds the list-view row for one workflow. A workflow whose
// history cannot be read or replayed is reported with UNSPECIFIED status and
// the failure in Error — an operator must see a broken log, not a shorter list.
func (e *Engine) summarize(workflowID string) *WorkflowSummary {
	sum := &WorkflowSummary{WorkflowId: workflowID}
	events, err := e.store.ReadAll(workflowID)
	if err != nil {
		sum.Error = fmt.Sprintf("history unreadable: %v", err)
		return sum
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		sum.Error = fmt.Sprintf("history invalid: %v", err)
		return sum
	}
	return summaryFrom(workflowID, events, st)
}

func summaryFrom(workflowID string, events []*history.HistoryEvent, st *state.State) *WorkflowSummary {
	sum := &WorkflowSummary{
		WorkflowId:    workflowID,
		WorkflowType:  st.WorkflowType,
		Status:        protoStatus(st),
		Error:         st.Error,
		HistoryLength: int64(len(events)),
	}
	if len(events) > 0 {
		sum.StartedAt = events[0].GetTimestamp()
		sum.UpdatedAt = events[len(events)-1].GetTimestamp()
	}
	return sum
}

// protoStatus maps the engine's reconstructed status onto the API enum. The
// one divergence: a FAILED workflow whose terminal event was an operator
// termination presents as TERMINATED.
func protoStatus(st *state.State) WorkflowStatus {
	if st.Status == state.StatusFailed && st.Terminated {
		return WorkflowStatus_WORKFLOW_STATUS_TERMINATED
	}
	switch st.Status {
	case state.StatusRunning:
		return WorkflowStatus_WORKFLOW_STATUS_RUNNING
	case state.StatusWaiting:
		return WorkflowStatus_WORKFLOW_STATUS_WAITING
	case state.StatusCompleted:
		return WorkflowStatus_WORKFLOW_STATUS_COMPLETED
	case state.StatusFailed:
		return WorkflowStatus_WORKFLOW_STATUS_FAILED
	case state.StatusTimedOut:
		return WorkflowStatus_WORKFLOW_STATUS_TIMED_OUT
	default:
		return WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED
	}
}
