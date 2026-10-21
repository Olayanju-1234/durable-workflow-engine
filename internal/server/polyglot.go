package server

import (
	"context"
	"fmt"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/internal/timer"
)

// Polyglot workflow execution: running workflow code in any language.
//
// The engine drives every workflow through the Planner seam — "given this
// history, what happens next?". An in-process Go workflow answers that by
// replaying a function. A remote SDK answers it over gRPC instead:
//
//	engine: needs a decision for workflow W
//	     → enqueues a WorkflowTask carrying W's full history
//	SDK:  PollWorkflowTask → replays the workflow in ITS runtime
//	     → CompleteWorkflowTask with the single next command
//	engine: applies the command exactly like a local decision
//
// Two properties make this safe and simple:
//
//   - The SDK is a PURE FUNCTION of history. It holds no state, so a crashed
//     SDK worker costs nothing — the task is just re-dispatched to another.
//   - Decisions are FENCED BY HISTORY LENGTH. A task records how many events it
//     was based on; if the history moved on before the command came back, the
//     command is stale and rejected. That is what makes re-dispatch safe: two
//     workers may both decide, but only one command can ever apply.

// defaultDecisionTimeout bounds how long an SDK worker may hold a workflow task
// before the engine assumes it died and re-dispatches. Deciding is pure
// computation (no I/O), so this is generous rather than tight. Tune with
// WithDecisionTimeout.
const defaultDecisionTimeout = 30 * time.Second

// remotePlanner marks a workflow type as executed by an external SDK. It
// satisfies Planner so it can live in the same registry as in-process
// workflows, but Decide is never called — advanceLocked recognises the type and
// dispatches a workflow task instead.
type remotePlanner struct {
	workflowType string
}

func (p *remotePlanner) Decide([]*history.HistoryEvent) (state.Decision, error) {
	return state.Decision{}, fmt.Errorf(
		"server: workflow type %q runs on a remote SDK; decisions arrive via CompleteWorkflowTask, not Decide", p.workflowType)
}

// decisionTask is an outstanding workflow task: one workflow's decision, handed
// to an SDK worker and not yet returned.
type decisionTask struct {
	token         string
	historyLength int64
	dispatchedAt  time.Time
}

// workflowTaskQueue holds workflow tasks awaiting an SDK worker, routed by
// workflow type: a TypeScript worker must not receive a Python workflow's task.
type workflowTaskQueue = routedQueue[*WorkflowTask]

func newWorkflowTaskQueue() *workflowTaskQueue {
	return newRoutedQueue((*WorkflowTask).GetWorkflowType)
}

// dropWorkflow removes any pending task for a workflow — used when a decision
// is applied or superseded, so a stale task is never handed out.
func dropWorkflow(q *workflowTaskQueue, workflowID string) {
	q.dropWhere(func(t *WorkflowTask) bool { return t.GetWorkflowId() == workflowID })
}

// RegisterWorkflowTypes marks workflow types as executed by an external SDK and
// makes them startable. SDK workers call this on connect (and on reconnect,
// since registrations are in-memory and an engine restart clears them).
//
// Registering also kicks any workflow of that type that was waiting for a
// decision: the engine may have restarted while no SDK worker was connected, in
// which case recovery skipped those workflows as "unregistered type".
func (e *Engine) RegisterWorkflowTypes(workflowTypes []string, workerID, sdk string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	for _, wt := range workflowTypes {
		if wt == "" {
			return fmt.Errorf("server: empty workflow type in registration from %q", workerID)
		}
		if existing, ok := e.planners[wt]; ok {
			// An in-process Go workflow already owns this type. Refuse rather
			// than silently letting a remote SDK shadow compiled-in code.
			if _, isRemote := existing.(*remotePlanner); !isRemote {
				return fmt.Errorf("server: workflow type %q is already registered in-process", wt)
			}
			continue
		}
		e.planners[wt] = &remotePlanner{workflowType: wt}
		e.log.Info("workflow type registered", "type", wt, "worker_id", workerID, "sdk", sdk)
	}

	// Pick up anything that was stranded while this type had no runner.
	for _, wt := range workflowTypes {
		if err := e.resumeTypeLocked(wt); err != nil {
			return err
		}
	}
	return nil
}

// resumeTypeLocked advances every non-terminal workflow of a type that is
// waiting on a decision. The caller must hold e.mu.
func (e *Engine) resumeTypeLocked(workflowType string) error {
	ids, err := e.store.ListWorkflows()
	if err != nil {
		return fmt.Errorf("server: list workflows to resume %s: %w", workflowType, err)
	}
	planner := e.planners[workflowType]
	for _, id := range ids {
		events, err := e.store.ReadAll(id)
		if err != nil {
			return fmt.Errorf("server: resume read %s: %w", id, err)
		}
		st, err := state.Reconstruct(events)
		if err != nil {
			// A corrupt log must not block registration of healthy workflows.
			e.log.Error("resume: skipping unreadable workflow", "workflow_id", id, "err", err)
			continue
		}
		if st.WorkflowType != workflowType || st.Status != state.StatusRunning {
			continue // wrong type, terminal, or already waiting on a step
		}
		if err := e.advanceLocked(id, planner); err != nil {
			return err
		}
		e.log.Info("resumed workflow after SDK registration", "workflow_id", id, "type", workflowType)
	}
	return nil
}

// dispatchWorkflowTaskLocked hands a workflow's history to an SDK worker for a
// decision. It is idempotent per (workflow, history length): if a task for this
// exact position is already outstanding, it is not duplicated. The caller must
// hold e.mu.
func (e *Engine) dispatchWorkflowTaskLocked(workflowID, workflowType string, events []*history.HistoryEvent) error {
	historyLength := int64(len(events))
	if p, ok := e.pendingDecisions[workflowID]; ok && p.historyLength == historyLength {
		return nil // already being decided at this position
	}

	task := &WorkflowTask{
		WorkflowId:    workflowID,
		WorkflowType:  workflowType,
		History:       events,
		TaskToken:     newLeaseToken(),
		HistoryLength: historyLength,
	}
	e.pendingDecisions[workflowID] = &decisionTask{
		token:         task.TaskToken,
		historyLength: historyLength,
		dispatchedAt:  time.Now(),
	}
	// Replace any task from an earlier position; only the newest can apply.
	dropWorkflow(e.workflowQueue, workflowID)
	e.workflowQueue.enqueue(task)
	e.metrics.workflowTasksDispatched.Inc()

	// Guard against an SDK worker that takes the task and dies.
	e.decisionTimers.Add(timer.Timer{
		WorkflowID: workflowID,
		Sequence:   historyLength,
		FireAt:     time.Now().Add(e.decisionTimeout),
	})
	e.log.Debug("workflow task dispatched", "workflow_id", workflowID,
		"type", workflowType, "history_length", historyLength)
	return nil
}

// onDecisionTimeout re-dispatches a workflow task whose SDK worker never
// answered. Re-deciding is always safe: replay is a pure function of history,
// and the history-length fence means a late answer from the original worker is
// rejected rather than applied twice.
func (e *Engine) onDecisionTimeout(tm timer.Timer) {
	e.mu.Lock()
	defer e.mu.Unlock()

	p, ok := e.pendingDecisions[tm.WorkflowID]
	if !ok || p.historyLength != tm.Sequence {
		return // answered, or superseded by a newer position
	}
	events, err := e.store.ReadAll(tm.WorkflowID)
	if err != nil {
		e.log.Error("decision timeout: read history", "workflow_id", tm.WorkflowID, "err", err)
		e.rearmCallback(e.decisionTimers, tm)
		return
	}
	st, err := state.Reconstruct(events)
	if err != nil || st.Status != state.StatusRunning {
		delete(e.pendingDecisions, tm.WorkflowID)
		return // moved on; nothing to decide
	}
	delete(e.pendingDecisions, tm.WorkflowID) // force a fresh dispatch below
	e.metrics.workflowTaskTimeouts.Inc()
	e.log.Warn("workflow task timed out; re-dispatching",
		"workflow_id", tm.WorkflowID, "history_length", tm.Sequence)
	if err := e.dispatchWorkflowTaskLocked(tm.WorkflowID, st.WorkflowType, events); err != nil {
		e.log.Error("decision timeout: re-dispatch", "workflow_id", tm.WorkflowID, "err", err)
	}
}

// PollWorkflowTask blocks until a workflow of one of the given types needs a
// decision. The returned task carries the workflow's entire history — the SDK
// replays it to work out what comes next.
func (e *Engine) PollWorkflowTask(ctx context.Context, workerID string, workflowTypes []string) (*WorkflowTask, error) {
	ctx, cancel := e.pollContext(ctx)
	defer cancel()
	task, err := e.workflowQueue.poll(ctx, workflowTypes)
	if err != nil {
		return nil, e.pollError(err)
	}
	if ctx.Err() != nil {
		e.workflowQueue.enqueue(task) // the poller left; keep the task for the next one
		return nil, e.pollError(ctx.Err())
	}
	e.log.Debug("workflow task delivered", "worker_id", workerID,
		"workflow_id", task.GetWorkflowId(), "type", task.GetWorkflowType())
	return task, nil
}

// CompleteWorkflowTask applies the command an SDK worker decided on. A nil
// command means "nothing to do yet" (the workflow is blocked on a step already
// in flight). A non-empty replayError fails the workflow durably — an SDK that
// cannot replay its own history will never succeed on a retry, so looping would
// be worse than surfacing it.
func (e *Engine) CompleteWorkflowTask(workflowID, taskToken string, cmd *WorkflowCommand, replayError string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	pending, ok := e.pendingDecisions[workflowID]
	if !ok {
		return fmt.Errorf("%w: no workflow task outstanding for %q (it was already answered, or the engine restarted)", ErrStaleDecision, workflowID)
	}
	if pending.token != taskToken {
		return fmt.Errorf("%w: workflow task for %q was re-dispatched to another worker", ErrStaleDecision, workflowID)
	}

	events, err := e.store.ReadAll(workflowID)
	if err != nil {
		return fmt.Errorf("server: read history %s: %w", workflowID, err)
	}
	// The fence: the decision was computed from a snapshot of history. If a
	// timer fired or a signal landed since, the command may be based on a stale
	// view — reject it. Clearing the pending decision is safe here precisely
	// because whatever advanced the history also dispatched a fresh task.
	if int64(len(events)) != pending.historyLength {
		delete(e.pendingDecisions, workflowID)
		return fmt.Errorf("%w: history for %q advanced from %d to %d events while the task was outstanding",
			ErrStaleDecision, workflowID, pending.historyLength, len(events))
	}

	if replayError != "" {
		delete(e.pendingDecisions, workflowID)
		dropWorkflow(e.workflowQueue, workflowID)
		e.metrics.workflowTaskReplayErrors.Inc()
		e.log.Error("sdk replay failed", "workflow_id", workflowID, "err", replayError)
		return e.applyDecisionLocked(workflowID, state.Decision{
			Type:  state.DecideFailWorkflow,
			Error: "workflow replay failed in SDK: " + replayError,
		}, int64(len(events)+1))
	}

	// A malformed command deliberately LEAVES the decision pending. Clearing it
	// would strand the workflow forever: no task in the queue and nothing to
	// re-dispatch it. Instead the decision timer re-offers the task, so a fixed
	// or redeployed SDK heals the workflow on its own, and a persistently buggy
	// one merely spins slowly and visibly (workflow_task_timeouts_total).
	decision, err := decisionFromCommand(cmd)
	if err != nil {
		return err
	}
	if err := validateDecision(events, decision); err != nil {
		return err
	}

	delete(e.pendingDecisions, workflowID)
	dropWorkflow(e.workflowQueue, workflowID)
	if decision.Type == state.DecideNoop {
		return nil // SDK says the workflow is blocked; nothing to record
	}
	e.metrics.workflowTasksCompleted.Inc()
	return e.applyDecisionLocked(workflowID, decision, int64(len(events)+1))
}

// decisionFromCommand translates the wire command into the engine's internal
// decision. A nil command (or an empty oneof) is a no-op.
func decisionFromCommand(cmd *WorkflowCommand) (state.Decision, error) {
	if cmd == nil || cmd.GetCommand() == nil {
		return state.Decision{Type: state.DecideNoop}, nil
	}
	switch c := cmd.GetCommand().(type) {
	case *WorkflowCommand_ScheduleActivity:
		a := c.ScheduleActivity
		return state.Decision{
			Type: state.DecideScheduleStep, StepName: a.GetStepName(),
			Sequence: a.GetSequence(), Input: a.GetInput(),
		}, nil
	case *WorkflowCommand_StartTimer:
		t := c.StartTimer
		return state.Decision{
			Type: state.DecideStartTimer, StepName: t.GetStepName(),
			Sequence: t.GetSequence(), Input: t.GetInput(),
			Delay: time.Duration(t.GetDelayMs()) * time.Millisecond,
		}, nil
	case *WorkflowCommand_AwaitSignal:
		s := c.AwaitSignal
		return state.Decision{
			Type: state.DecideAwaitSignal, StepName: s.GetStepName(),
			Sequence: s.GetSequence(), Signal: s.GetSignalName(), Input: s.GetInput(),
		}, nil
	case *WorkflowCommand_CompleteWorkflow:
		return state.Decision{Type: state.DecideCompleteWorkflow, Output: c.CompleteWorkflow.GetOutput()}, nil
	case *WorkflowCommand_FailWorkflow:
		return state.Decision{Type: state.DecideFailWorkflow, Error: c.FailWorkflow.GetError()}, nil
	default:
		return state.Decision{}, fmt.Errorf("server: unknown workflow command %T", c)
	}
}

// CommandFromDecision is the inverse of decisionFromCommand: what a Go SDK
// worker sends back for a decision its replay produced. A noop is a nil command.
func CommandFromDecision(d state.Decision) *WorkflowCommand {
	switch d.Type {
	case state.DecideScheduleStep:
		return &WorkflowCommand{Command: &WorkflowCommand_ScheduleActivity{ScheduleActivity: &ScheduleActivityCommand{
			StepName: d.StepName, Sequence: d.Sequence, Input: d.Input,
		}}}
	case state.DecideStartTimer:
		return &WorkflowCommand{Command: &WorkflowCommand_StartTimer{StartTimer: &StartTimerCommand{
			StepName: d.StepName, Sequence: d.Sequence, Input: d.Input, DelayMs: d.Delay.Milliseconds(),
		}}}
	case state.DecideAwaitSignal:
		return &WorkflowCommand{Command: &WorkflowCommand_AwaitSignal{AwaitSignal: &AwaitSignalCommand{
			StepName: d.StepName, Sequence: d.Sequence, SignalName: d.Signal, Input: d.Input,
		}}}
	case state.DecideCompleteWorkflow:
		return &WorkflowCommand{Command: &WorkflowCommand_CompleteWorkflow{CompleteWorkflow: &CompleteWorkflowCommand{Output: d.Output}}}
	case state.DecideFailWorkflow:
		return &WorkflowCommand{Command: &WorkflowCommand_FailWorkflow{FailWorkflow: &FailWorkflowCommand{Error: d.Error}}}
	default:
		return nil
	}
}

// validateDecision rejects a malformed command before it reaches the log. The
// engine must never trust an SDK to number its own steps correctly: a bad
// sequence would produce a history that Reconstruct later refuses to read,
// turning a client bug into an unreadable workflow.
func validateDecision(events []*history.HistoryEvent, d state.Decision) error {
	switch d.Type {
	case state.DecideCompleteWorkflow, state.DecideFailWorkflow, state.DecideNoop:
		return nil // terminal commands carry no step; noop carries nothing at all
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		return fmt.Errorf("server: reconstruct before applying command: %w", err)
	}
	if want := int64(len(st.Steps) + 1); d.Sequence != want {
		return fmt.Errorf("%w: command has sequence %d, expected %d — the SDK's replay is out of step with history",
			ErrInvalidCommand, d.Sequence, want)
	}
	if d.StepName == "" {
		return fmt.Errorf("%w: command has an empty step name", ErrInvalidCommand)
	}
	if d.Type == state.DecideAwaitSignal && d.Signal == "" {
		return fmt.Errorf("%w: await-signal command has an empty signal name", ErrInvalidCommand)
	}
	return nil
}
