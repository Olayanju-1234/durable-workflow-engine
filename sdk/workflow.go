package sdk

import (
	"errors"
	"fmt"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// Context is the durable API a workflow function calls. Every call maps to one
// step in the workflow's history. During replay the call returns the step's
// recorded result instead of doing the work again — so the workflow function is
// re-run from the top on every decision, but each call is a fast lookup until it
// reaches the first step with no result yet, which is what to do next.
//
// Workflow code MUST be deterministic: it may only interact with the outside world
// through this Context. No time.Now, no rand, no network or disk, no goroutines —
// those would make a replay diverge from the recorded run. Use ExecuteActivity for
// side effects (they run once, on a worker) and Sleep for time.
type Context interface {
	// ExecuteActivity runs a named activity on a worker and returns its result. A
	// failed activity returns an error the workflow can handle (e.g. compensate) or
	// propagate (return it) to fail the workflow.
	ExecuteActivity(name string, input []byte) ([]byte, error)
	// Sleep pauses the workflow durably for d (a timer that survives restarts).
	Sleep(name string, d time.Duration)
	// WaitForSignal blocks until a matching external signal arrives, returning its
	// payload. A signal delivered earlier is buffered and returned immediately.
	WaitForSignal(name, signal string) []byte
	// Input is the workflow's start input.
	Input() []byte
}

// WorkflowFunc is a workflow written as ordinary Go: orchestration logic that calls
// the Context's durable primitives. Its return value is the workflow output; a
// returned error fails the workflow.
type WorkflowFunc func(ctx Context, input []byte) ([]byte, error)

// Workflow adapts a WorkflowFunc into an engine planner: given a history, Decide
// replays the function and returns the single next action. Register it with the
// engine via RegisterWorkflow(w.Type, w).
type Workflow struct {
	Type string
	Fn   WorkflowFunc
}

// Decide replays the workflow function against history and returns the next
// decision. It satisfies the engine's Planner interface, so the engine drives a
// code-based workflow exactly like a data-based one.
func (w Workflow) Decide(events []*history.HistoryEvent) (state.Decision, error) {
	st, err := state.Reconstruct(events)
	if err != nil {
		return state.Decision{}, err
	}
	// Terminal, not-started, or a step in flight: nothing to decide (mirrors the
	// data-driven planner). We only re-run the function when it can make progress.
	if st.Status != state.StatusRunning {
		return state.Decision{Type: state.DecideNoop}, nil
	}

	ctx := &replayContext{st: st}
	decision := runToYield(w.Fn, ctx)
	if ctx.err != nil {
		return state.Decision{}, ctx.err
	}
	return decision, nil
}

// runToYield runs the workflow function until a Context call yields (a step that
// has no result yet, or an in-flight step), recovering the yield to get the
// decision. If the function instead returns, the workflow is complete or failed.
func runToYield(fn WorkflowFunc, ctx *replayContext) (decision state.Decision) {
	defer func() {
		if r := recover(); r != nil {
			y, ok := r.(yield)
			if !ok {
				panic(r) // a real panic in workflow code: re-raise
			}
			decision = y.decision
		}
	}()
	out, err := fn(ctx, ctx.st.Input)
	if err != nil {
		return state.Decision{Type: state.DecideFailWorkflow, Error: err.Error()}
	}
	return state.Decision{Type: state.DecideCompleteWorkflow, Output: out}
}

// yield is thrown (panic) by a Context call to unwind the workflow function once
// it reaches the point where the engine must act. recover in runToYield catches it.
type yield struct{ decision state.Decision }

// replayContext walks a workflow's steps in call order. next is the 1-based
// sequence of the command currently being issued; it must line up with the steps
// recorded in history, which is exactly the determinism check.
type replayContext struct {
	st   *state.State
	next int64
	err  error // set on a determinism violation
}

func (c *replayContext) Input() []byte { return c.st.Input }

func (c *replayContext) ExecuteActivity(name string, input []byte) ([]byte, error) {
	return c.command(state.KindActivity, name, "", input, 0)
}

func (c *replayContext) Sleep(name string, d time.Duration) {
	_, _ = c.command(state.KindTimer, name, "", nil, d)
}

func (c *replayContext) WaitForSignal(name, signal string) []byte {
	out, _ := c.command(state.KindSignal, name, signal, nil, 0)
	return out
}

// command is the heart of replay. For the next step in call order it either
// returns the recorded result (replay), blocks (yield NOOP) if the step is still
// in flight, or yields the decision to create the step if it isn't in history yet.
func (c *replayContext) command(kind state.StepKind, name, signal string, input []byte, delay time.Duration) ([]byte, error) {
	c.next++
	seq := c.next

	if int(seq) <= len(c.st.Steps) {
		rec := c.st.Steps[seq-1]
		// Determinism: the command the code issues now must be the step history
		// recorded at this position. A mismatch means the code changed in a way that
		// diverges from the durable run — refuse rather than corrupt the workflow.
		if rec.Kind != kind || rec.Name != name {
			c.err = fmt.Errorf("sdk: non-determinism at step %d: workflow issued %s %q but history has %s %q",
				seq, kind, name, rec.Kind, rec.Name)
			panic(yield{state.Decision{Type: state.DecideNoop}})
		}
		switch rec.Status {
		case state.StepCompleted:
			return rec.Output, nil
		case state.StepFailed:
			return nil, errors.New(rec.Error)
		default:
			panic(yield{state.Decision{Type: state.DecideNoop}}) // in flight: wait
		}
	}

	// No history for this step yet: this is the next action.
	d := state.Decision{StepName: name, Sequence: seq, Input: input}
	switch kind {
	case state.KindActivity:
		d.Type = state.DecideScheduleStep
	case state.KindTimer:
		d.Type = state.DecideStartTimer
		d.Delay = delay
	case state.KindSignal:
		d.Type = state.DecideAwaitSignal
		d.Signal = signal
	}
	panic(yield{d})
}
