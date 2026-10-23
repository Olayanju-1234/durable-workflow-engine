package server

import (
	"errors"
	"fmt"
	"strings"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// Sentinel errors the gRPC layer maps to status codes; everything else is a
// server-side fault.
var (
	ErrNotFound        = errors.New("server: workflow not found")
	ErrAlreadyTerminal = errors.New("server: workflow already terminal")
	// ErrAlreadyExists: StartWorkflow with a workflow id that already has history.
	ErrAlreadyExists = errors.New("server: workflow already exists")
	// ErrUnknownType: StartWorkflow for a type no planner claims on this node.
	ErrUnknownType = errors.New("server: unknown workflow type")
	// ErrInvalidArgument: a request field is structurally invalid (empty signal
	// name, ...). A client bug, not a transient condition.
	ErrInvalidArgument = errors.New("server: invalid argument")
	// ErrStaleDecision: an SDK returned a command for a position the workflow
	// has already moved past. Expected under re-dispatch; the worker should
	// simply poll again rather than treat it as a failure.
	ErrStaleDecision = errors.New("server: stale workflow decision")
	// ErrInvalidCommand: an SDK returned a structurally invalid command (bad
	// step sequence, empty name). A client bug, not a transient condition.
	ErrInvalidCommand = errors.New("server: invalid workflow command")
	// ErrActivityCancelled: a worker reported on an activity whose step was
	// cancelled or whose workflow finished. It should stop the work.
	ErrActivityCancelled = errors.New("server: activity cancelled")
)

// ListWorkflows summarizes every workflow, sorted by id. When filter is not
// UNSPECIFIED only matching workflows are returned. It reads the in-memory
// visibility index (index.go), not the history files. A workflow whose history
// cannot be read or replayed is listed with UNSPECIFIED status and the reason:
// a broken log is surfaced, never hidden.
func (e *Engine) ListWorkflows(filter WorkflowStatus) ([]*WorkflowSummary, error) {
	return e.indexed(func(s *WorkflowSummary) bool {
		return filter == WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED || s.GetStatus() == filter
	})
}

// GetWorkflow returns one workflow in full reconstructed detail: its summary
// plus every step as replay sees it. Unknown ids return ErrNotFound.
func (e *Engine) GetWorkflow(workflowID string) (*GetWorkflowResponse, error) {
	if err := validateID(workflowID); err != nil {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, workflowID)
	}
	w := e.lockWorkflow(workflowID)
	defer e.unlockWorkflow(w)
	events, st, err := e.historyLocked(w)
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, workflowID)
	}

	steps := make([]*StepInfo, 0, len(st.Steps))
	for _, s := range st.Steps {
		status, stepErr := s.Status.String(), s.Error
		if st.Status.IsTerminal() && s.Open() {
			// Abandoned: a race's loser, say. Nothing will ever use it.
			status, stepErr = state.StepCancelled.String(), "not needed: the workflow is "+protoStatusName(st)
		}
		steps = append(steps, &StepInfo{
			StepName:   s.Name,
			Sequence:   s.Sequence,
			Kind:       s.Kind.String(),
			Status:     status,
			Attempts:   int32(s.Attempts),
			Error:      stepErr,
			Output:     s.Output,
			FireAt:     s.FireAt,
			SignalName: s.Signal,
			Marker:     s.Marker,
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
	msg := "terminated by operator"
	if reason != "" {
		msg += ": " + reason
	}
	return e.endWorkflow(workflowID, "terminated", reason,
		&history.WorkflowFailedEvent{Error: msg, Terminated: true})
}

// protoStatusName is the status as the API spells it ("COMPLETED").
func protoStatusName(st *state.State) string {
	return strings.TrimPrefix(protoStatus(st).String(), "WORKFLOW_STATUS_")
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
	case state.StatusCancelled:
		return WorkflowStatus_WORKFLOW_STATUS_CANCELLED
	default:
		return WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED
	}
}
