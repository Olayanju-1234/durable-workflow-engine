package sdk

import (
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// Context is the durable API a workflow function calls.
//
// Deprecated: use workflow.Context.
type Context = workflow.Context

// WorkflowFunc is a workflow written as ordinary Go.
//
// Deprecated: use workflow.Func.
type WorkflowFunc = workflow.Func

// Workflow adapts a WorkflowFunc into an engine planner.
//
// Deprecated: run workflows with package worker.
type Workflow struct {
	Type string
	Fn   WorkflowFunc
}

// Decide replays the workflow function against history and returns the next
// decision.
func (w Workflow) Decide(events []*history.HistoryEvent) (state.Decision, error) {
	return replay.Decide(w.Fn, events, nil)
}

// DecideAll returns every decision the workflow made (parallel steps).
func (w Workflow) DecideAll(events []*history.HistoryEvent) ([]state.Decision, error) {
	return replay.DecideAll(w.Fn, events, nil)
}
