package client

import (
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// Status is a workflow's lifecycle state.
type Status string

const (
	StatusRunning    Status = "RUNNING"    // the engine owes it a decision
	StatusWaiting    Status = "WAITING"    // a step is in flight: an activity, timer or signal wait
	StatusCompleted  Status = "COMPLETED"  // finished with an output
	StatusFailed     Status = "FAILED"     // workflow code returned an error
	StatusTerminated Status = "TERMINATED" // ended by TerminateWorkflow
	StatusTimedOut   Status = "TIMED_OUT"  // its execution timeout passed (WithExecutionTimeout)
	StatusCancelled  Status = "CANCELLED"  // ended by CancelWorkflow
	// An earlier run that continued as new (workflow.ContinueAsNew); the
	// workflow id went on as a new run. Only DescribeWorkflowRun shows it.
	StatusContinuedAsNew Status = "CONTINUED_AS_NEW"
	StatusUnknown        Status = "UNKNOWN" // the history could not be read
)

// Done reports whether the workflow has finished, successfully or not.
func (s Status) Done() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusTerminated || s == StatusTimedOut || s == StatusCancelled ||
		s == StatusContinuedAsNew
}

// WorkflowDescription is a workflow as replayed from its history.
type WorkflowDescription struct {
	ID            string
	Type          string
	Status        Status
	StartedAt     time.Time
	UpdatedAt     time.Time // time of the latest event
	Input         []byte
	Output        []byte // set when Status is StatusCompleted
	Error         string // set when the workflow failed or was terminated
	HistoryLength int64
	Steps         []Step
	// RunID names the run described. A workflow that continues as new has a
	// chain of runs: ContinuedFromRunID is the one before this, and
	// ContinuedAsNewRunID (for an earlier run) the one after.
	RunID               string
	ContinuedFromRunID  string
	ContinuedAsNewRunID string
	// HistorySizeBytes is the run's history size on disk.
	HistorySizeBytes int64
}

// Run is one run of a workflow id (see ListWorkflowRuns).
type Run struct {
	RunID     string
	Status    Status
	StartedAt time.Time
	ClosedAt  time.Time // zero while the run is open
	Current   bool
}

// Step is one durable call the workflow made.
type Step struct {
	Name     string
	Sequence int64
	Kind     string // "ACTIVITY", "TIMER", "SIGNAL" or "MARKER"
	// "SCHEDULED", "COMPLETED", "FAILED" or "CANCELLED" (cancelled by the
	// workflow code, or still open when the workflow finished).
	Status   string
	Attempts int // more than 1 means the engine retried it
	Output   []byte
	Error    string
	FireAt   time.Time // timers only
	Signal   string    // signal waits only
}

var statuses = map[server.WorkflowStatus]Status{
	server.WorkflowStatus_WORKFLOW_STATUS_RUNNING:    StatusRunning,
	server.WorkflowStatus_WORKFLOW_STATUS_WAITING:    StatusWaiting,
	server.WorkflowStatus_WORKFLOW_STATUS_COMPLETED:  StatusCompleted,
	server.WorkflowStatus_WORKFLOW_STATUS_FAILED:     StatusFailed,
	server.WorkflowStatus_WORKFLOW_STATUS_TERMINATED: StatusTerminated,
	server.WorkflowStatus_WORKFLOW_STATUS_TIMED_OUT:  StatusTimedOut,
	server.WorkflowStatus_WORKFLOW_STATUS_CANCELLED:  StatusCancelled,

	server.WorkflowStatus_WORKFLOW_STATUS_CONTINUED_AS_NEW: StatusContinuedAsNew,
}

func describe(r *server.GetWorkflowResponse) *WorkflowDescription {
	sum := r.GetSummary()
	st, ok := statuses[sum.GetStatus()]
	if !ok {
		st = StatusUnknown
	}
	d := &WorkflowDescription{
		ID: sum.GetWorkflowId(), Type: sum.GetWorkflowType(), Status: st,
		StartedAt: unixNanos(sum.GetStartedAt()), UpdatedAt: unixNanos(sum.GetUpdatedAt()),
		Input: r.GetInput(), Output: r.GetOutput(), Error: sum.GetError(),
		HistoryLength: sum.GetHistoryLength(),

		RunID: sum.GetRunId(), ContinuedFromRunID: r.GetContinuedFromRunId(),
		ContinuedAsNewRunID: r.GetContinuedAsNewRunId(), HistorySizeBytes: sum.GetHistorySizeBytes(),
	}
	for _, s := range r.GetSteps() {
		d.Steps = append(d.Steps, Step{
			Name: s.GetStepName(), Sequence: s.GetSequence(), Kind: s.GetKind(), Status: s.GetStatus(),
			Attempts: int(s.GetAttempts()), Output: s.GetOutput(), Error: s.GetError(),
			FireAt: unixNanos(s.GetFireAt()), Signal: s.GetSignalName(),
		})
	}
	return d
}

func unixNanos(n int64) time.Time {
	if n == 0 {
		return time.Time{}
	}
	return time.Unix(0, n)
}
