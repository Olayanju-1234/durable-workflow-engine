package server

import (
	"context"
	"errors"
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
	return newRoutedQueue((*WorkflowTask).GetWorkflowType, func(t *WorkflowTask) string {
		return routeScope(nsOf(t.GetWorkflowId()), t.GetTaskQueue())
	})
}

// dropWorkflow removes any pending task for a workflow — used when a decision
// is applied or superseded, so a stale task is never handed out.
func dropWorkflow(q *workflowTaskQueue, workflowID string) {
	q.dropWhere(func(t *WorkflowTask) bool { return t.GetWorkflowId() == workflowID })
}

// RegisterWorkflowTypes marks workflow types as executed by an external SDK and
// makes them startable, in the default namespace. SDK workers call this on
// connect (and on reconnect, since registrations are in-memory and an engine
// restart clears them).
//
// Registering also kicks any workflow of that type that was waiting for a
// decision: the engine may have restarted while no SDK worker was connected, in
// which case recovery skipped those workflows as "unregistered type".
func (e *Engine) RegisterWorkflowTypes(workflowTypes []string, workerID, sdk string) error {
	return e.registerWorkflowTypes(DefaultNamespace, workflowTypes, workerID, sdk)
}

func (e *Engine) registerWorkflowTypes(ns string, workflowTypes []string, workerID, sdk string) error {
	var added []string
	e.mu.Lock()
	for _, wt := range workflowTypes {
		if wt == "" {
			e.mu.Unlock()
			return fmt.Errorf("server: empty workflow type in registration from %q", workerID)
		}
		if existing, ok := e.planners[nsName{ns, wt}]; ok {
			// An in-process Go workflow already owns this type. Refuse rather
			// than silently letting a remote SDK shadow compiled-in code.
			if _, isRemote := existing.(*remotePlanner); !isRemote {
				e.mu.Unlock()
				return fmt.Errorf("server: workflow type %q is already registered in-process", wt)
			}
			continue
		}
		e.planners[nsName{ns, wt}] = &remotePlanner{workflowType: wt}
		added = append(added, wt)
		e.log.Info("workflow type registered", "namespace", ns, "type", wt, "worker_id", workerID, "sdk", sdk)
	}
	e.mu.Unlock()

	// Pick up anything that was stranded while this type had no runner. A
	// type already registered has nothing stranded: its decisions dispatch.
	for _, wt := range added {
		if err := e.resumeType(ns, wt); err != nil {
			return err
		}
	}
	return nil
}

// resumeType advances every non-terminal workflow of a type in a namespace
// that is waiting on a decision.
func (e *Engine) resumeType(ns, workflowType string) error {
	waiting, err := e.indexed(func(s *WorkflowSummary) bool {
		return nsOf(s.GetWorkflowId()) == ns && s.GetWorkflowType() == workflowType &&
			(s.GetStatus() == WorkflowStatus_WORKFLOW_STATUS_RUNNING || s.GetStatus() == WorkflowStatus_WORKFLOW_STATUS_WAITING)
	})
	if err != nil {
		return fmt.Errorf("server: list workflows to resume %s: %w", workflowType, err)
	}
	planner, _ := e.planner(ns, workflowType)
	for _, s := range waiting {
		id := s.GetWorkflowId()
		w := e.lockWorkflow(id)
		resumed, err := e.resumeLocked(w, workflowType, planner)
		e.unlockWorkflow(w)
		if err != nil {
			return err
		}
		if resumed {
			e.log.Info("resumed workflow after SDK registration", "workflow_id", id, "type", workflowType)
		}
	}
	return nil
}

func (e *Engine) resumeLocked(w *wfState, workflowType string, planner Planner) (bool, error) {
	_, st, err := e.historyLocked(w)
	if err != nil {
		// A corrupt log must not block registration of healthy workflows.
		e.log.Error("resume: skipping unreadable workflow", "workflow_id", w.id, "err", err)
		return false, nil
	}
	if st.WorkflowType != workflowType || !st.OwesDecision() {
		return false, nil // wrong type, terminal, or waiting on steps with nothing new
	}
	return true, e.advanceLocked(w, planner)
}

// dispatchWorkflowTaskLocked hands a workflow's history to an SDK worker for a
// decision. It is idempotent per (workflow, history length): if a task for this
// exact position is already outstanding, it is not duplicated. The caller must
// hold w.mu.
func (e *Engine) dispatchWorkflowTaskLocked(w *wfState, workflowType string, events []*history.HistoryEvent) error {
	workflowID := w.id
	historyLength := int64(len(events))
	e.mu.Lock()
	defer e.mu.Unlock()
	if p, ok := e.pendingDecisions[workflowID]; ok && p.historyLength == historyLength {
		return nil // already being decided at this position
	}

	task := &WorkflowTask{
		WorkflowId:    workflowID,
		WorkflowType:  workflowType,
		History:       events,
		TaskToken:     newLeaseToken(),
		HistoryLength: historyLength,

		RunId:                  w.runID(),
		SuggestContinueAsNewAt: w.suggestAt,
		HistorySizeBytes:       w.bytes,
		TaskQueue:              state.QueueName(w.taskQueue()),
	}
	if w.fold != nil {
		task.Traceparent, task.Tracestate = runTraceparent(w.fold.State().Trace)
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
	w := e.lockWorkflow(tm.WorkflowID)
	defer e.unlockWorkflow(w)

	e.mu.Lock()
	p, ok := e.pendingDecisions[tm.WorkflowID]
	e.mu.Unlock()
	if !ok || p.historyLength != tm.Sequence {
		return // answered, or superseded by a newer position
	}
	events, st, err := e.historyLocked(w)
	if err != nil && events == nil {
		e.log.Error("decision timeout: read history", "workflow_id", tm.WorkflowID, "err", err)
		e.rearmCallback(e.decisionTimers, tm)
		return
	}
	e.mu.Lock()
	delete(e.pendingDecisions, tm.WorkflowID) // force a fresh dispatch below
	e.mu.Unlock()
	if err != nil || st.Status.IsTerminal() {
		return // finished; nothing to decide
	}
	e.metrics.workflowTaskTimeouts.Inc()
	e.log.Warn("workflow task timed out; re-dispatching",
		"workflow_id", tm.WorkflowID, "history_length", tm.Sequence)
	if err := e.dispatchWorkflowTaskLocked(w, st.WorkflowType, events); err != nil {
		e.log.Error("decision timeout: re-dispatch", "workflow_id", tm.WorkflowID, "err", err)
	}
}

// PollWorkflowTask blocks until a workflow of one of the given types needs a
// decision. The returned task carries the workflow's entire history — the SDK
// replays it to work out what comes next.
func (e *Engine) PollWorkflowTask(ctx context.Context, workerID string, workflowTypes []string) (*WorkflowTask, error) {
	return e.pollWorkflowTask(ctx, DefaultNamespace, "", workerID, workflowTypes)
}

// pollWorkflowTask is PollWorkflowTask for a namespace's task queue; the task
// names its workflow by store key.
func (e *Engine) pollWorkflowTask(ctx context.Context, ns, queue, workerID string, workflowTypes []string) (*WorkflowTask, error) {
	ctx, cancel := e.pollContext(ctx)
	defer cancel()
	task, err := e.workflowQueue.poll(ctx, routeScope(ns, queue), workflowTypes)
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

// CompleteWorkflowTask applies the commands an SDK worker decided on, in order.
// No commands (or a single noop) means "nothing to do yet": every step the
// workflow waits on is in flight. A non-empty replayError fails the workflow
// durably — an SDK that cannot replay its own history will never succeed on a
// retry, so looping would be worse than surfacing it.
func (e *Engine) CompleteWorkflowTask(workflowID, taskToken string, cmds []*WorkflowCommand, replayError string) error {
	if err := validateID(workflowID); err != nil {
		return err
	}
	w := e.lockWorkflow(workflowID)
	defer e.unlockWorkflow(w)

	e.mu.Lock()
	pending, ok := e.pendingDecisions[workflowID]
	e.mu.Unlock()
	if !ok {
		return fmt.Errorf("%w: no workflow task outstanding for %q (it was already answered, or the engine restarted)", ErrStaleDecision, workflowID)
	}
	if pending.token != taskToken {
		return fmt.Errorf("%w: workflow task for %q was re-dispatched to another worker", ErrStaleDecision, workflowID)
	}

	events, st, err := e.historyLocked(w)
	if err != nil {
		return err
	}
	// The fence: the decision was computed from a snapshot of history. If a
	// signal or a step result landed since, the commands may be based on a
	// stale view, so reject them, and hand out a fresh task for the new
	// position (nothing else would: a signal that satisfies no wait dispatches
	// nothing).
	if int64(len(events)) != pending.historyLength {
		e.dropDecision(workflowID)
		stale := fmt.Errorf("%w: history for %q advanced from %d to %d events while the task was outstanding",
			ErrStaleDecision, workflowID, pending.historyLength, len(events))
		if !st.Status.IsTerminal() {
			if err := e.dispatchWorkflowTaskLocked(w, st.WorkflowType, events); err != nil {
				return errors.Join(stale, err)
			}
		}
		return stale
	}

	if replayError != "" {
		e.dropDecision(workflowID)
		dropWorkflow(e.workflowQueue, workflowID)
		e.metrics.workflowTaskReplayErrors.Inc()
		e.log.Error("sdk replay failed", "workflow_id", workflowID, "err", replayError)
		return e.applyDecisionsLocked(w, []state.Decision{{
			Type:  state.DecideFailWorkflow,
			Error: "workflow replay failed in SDK: " + replayError,
		}})
	}

	// A malformed command deliberately LEAVES the decision pending. Clearing it
	// would strand the workflow forever: no task in the queue and nothing to
	// re-dispatch it. Instead the decision timer re-offers the task, so a fixed
	// or redeployed SDK heals the workflow on its own, and a persistently buggy
	// one merely spins slowly and visibly (workflow_task_timeouts_total).
	decisions := make([]state.Decision, 0, len(cmds))
	for _, cmd := range cmds {
		d, err := decisionFromCommand(cmd)
		if err != nil {
			return err
		}
		decisions = append(decisions, d)
	}
	if err := validateDecisions(st, decisions); err != nil {
		return err
	}

	e.dropDecision(workflowID)
	dropWorkflow(e.workflowQueue, workflowID)
	e.exportDecision(workflowID, st, pending, len(decisions))
	if len(decisions) == 0 || (len(decisions) == 1 && decisions[0].Type == state.DecideNoop) {
		return nil // SDK says the workflow is blocked; nothing to record
	}
	e.metrics.workflowTasksCompleted.Inc()
	return e.applyDecisionsLocked(w, decisions)
}

// dropDecision forgets the workflow's outstanding decision.
func (e *Engine) dropDecision(workflowID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.pendingDecisions, workflowID)
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
		retry, err := retryFromCommand(a.GetRetryPolicy())
		if err != nil {
			return state.Decision{}, err
		}
		if a.GetStartToCloseTimeoutMs() < 0 || a.GetScheduleToCloseTimeoutMs() < 0 {
			return state.Decision{}, fmt.Errorf("%w: activity %q has a negative timeout", ErrInvalidCommand, a.GetStepName())
		}
		if err := checkQueue(a.GetTaskQueue()); err != nil {
			return state.Decision{}, fmt.Errorf("%w: activity %q: %v", ErrInvalidCommand, a.GetStepName(), err)
		}
		return state.Decision{
			Type: state.DecideScheduleStep, StepName: a.GetStepName(),
			Sequence: a.GetSequence(), Input: a.GetInput(), Retry: retry,
			StartToClose:    time.Duration(a.GetStartToCloseTimeoutMs()) * time.Millisecond,
			ScheduleToClose: time.Duration(a.GetScheduleToCloseTimeoutMs()) * time.Millisecond,
			TaskQueue:       a.GetTaskQueue(),
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
		return state.Decision{Type: state.DecideFailWorkflow, Error: c.FailWorkflow.GetError(), Cancelled: c.FailWorkflow.GetCancelled()}, nil
	case *WorkflowCommand_RecordMarker:
		m := c.RecordMarker
		return state.Decision{
			Type: state.DecideRecordMarker, StepName: m.GetStepName(),
			Sequence: m.GetSequence(), Marker: m.GetMarker(), Output: m.GetData(),
		}, nil
	case *WorkflowCommand_CancelStep:
		s := c.CancelStep
		return state.Decision{
			Type: state.DecideCancelStep, StepName: s.GetStepName(), Sequence: s.GetSequence(), Error: s.GetReason(),
		}, nil
	case *WorkflowCommand_ContinueAsNew:
		n := c.ContinueAsNew
		if n.GetExecutionTimeoutMs() < 0 {
			return state.Decision{}, fmt.Errorf("%w: continue-as-new has a negative execution timeout", ErrInvalidCommand)
		}
		if err := checkQueue(n.GetTaskQueue()); err != nil {
			return state.Decision{}, fmt.Errorf("%w: continue-as-new: %v", ErrInvalidCommand, err)
		}
		return state.Decision{
			Type: state.DecideContinueAsNew, Input: n.GetInput(), WorkflowType: n.GetWorkflowType(),
			ExecutionTimeout: time.Duration(n.GetExecutionTimeoutMs()) * time.Millisecond,
			TaskQueue:        n.GetTaskQueue(),
		}, nil
	default:
		return state.Decision{}, fmt.Errorf("server: unknown workflow command %T", c)
	}
}

// kindOfDecision is the kind of step a decision opens.
func kindOfDecision(t state.DecisionType) state.StepKind {
	switch t {
	case state.DecideStartTimer:
		return state.KindTimer
	case state.DecideAwaitSignal:
		return state.KindSignal
	case state.DecideRecordMarker:
		return state.KindMarker
	default:
		return state.KindActivity
	}
}

// retryFromCommand validates an SDK-chosen retry policy. A malformed one is
// refused like any malformed command, before it can reach the log.
func retryFromCommand(p *history.RetryPolicy) (*state.RetryPolicy, error) {
	if p == nil {
		return nil, nil
	}
	if p.GetMaxAttempts() < 1 || p.GetInitialBackoffMs() < 0 || p.GetMaxBackoffMs() < 0 {
		return nil, fmt.Errorf("%w: retry policy needs max_attempts >= 1 and non-negative backoffs, got %v",
			ErrInvalidCommand, p)
	}
	return state.RetryPolicyFromProto(p), nil
}

// CommandFromDecision is the inverse of decisionFromCommand: what a Go SDK
// worker sends back for a decision its replay produced. A noop is a nil command.
func CommandFromDecision(d state.Decision) *WorkflowCommand {
	switch d.Type {
	case state.DecideScheduleStep:
		return &WorkflowCommand{Command: &WorkflowCommand_ScheduleActivity{ScheduleActivity: &ScheduleActivityCommand{
			StepName: d.StepName, Sequence: d.Sequence, Input: d.Input, RetryPolicy: d.Retry.ToProto(),
			StartToCloseTimeoutMs:    d.StartToClose.Milliseconds(),
			ScheduleToCloseTimeoutMs: d.ScheduleToClose.Milliseconds(),
			TaskQueue:                d.TaskQueue,
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
		return &WorkflowCommand{Command: &WorkflowCommand_FailWorkflow{FailWorkflow: &FailWorkflowCommand{Error: d.Error, Cancelled: d.Cancelled}}}
	case state.DecideRecordMarker:
		return &WorkflowCommand{Command: &WorkflowCommand_RecordMarker{RecordMarker: &RecordMarkerCommand{
			StepName: d.StepName, Sequence: d.Sequence, Marker: d.Marker, Data: d.Output,
		}}}
	case state.DecideCancelStep:
		return &WorkflowCommand{Command: &WorkflowCommand_CancelStep{CancelStep: &CancelStepCommand{
			StepName: d.StepName, Sequence: d.Sequence, Reason: d.Error,
		}}}
	case state.DecideContinueAsNew:
		return &WorkflowCommand{Command: &WorkflowCommand_ContinueAsNew{ContinueAsNew: &ContinueAsNewCommand{
			Input: d.Input, WorkflowType: d.WorkflowType, ExecutionTimeoutMs: d.ExecutionTimeout.Milliseconds(),
			TaskQueue: d.TaskQueue,
		}}}
	default:
		return nil
	}
}

// validateDecisions rejects malformed commands before any reaches the log. The
// engine must never trust an SDK to number its own steps correctly: a bad
// sequence would produce a history that Reconstruct later refuses to read,
// turning a client bug into an unreadable workflow. Steps the batch opens take
// consecutive sequences after the history's; a terminal command comes last; a
// cancel names a step that exists by then, recorded or opened earlier in the batch.
func validateDecisions(st *state.State, ds []state.Decision) error {
	want := int64(len(st.Steps) + 1)
	opened := map[int64]state.Decision{} // steps this batch opens, by sequence
	for i, d := range ds {
		switch d.Type {
		case state.DecideCancelStep:
			name, kind := "", state.StepKind(-1)
			if s := st.Step(d.Sequence); s != nil {
				name, kind = s.Name, s.Kind
			} else if o, ok := opened[d.Sequence]; ok {
				name, kind = o.StepName, kindOfDecision(o.Type)
			} else {
				return fmt.Errorf("%w: command %d cancels step %d, which does not exist", ErrInvalidCommand, i+1, d.Sequence)
			}
			if name != d.StepName || kind == state.KindMarker {
				return fmt.Errorf("%w: command %d cancels %q at step %d, but that step is %s %q — the SDK's replay is out of step with history",
					ErrInvalidCommand, i+1, d.StepName, d.Sequence, kind, name)
			}
			continue
		case state.DecideNoop:
			if len(ds) > 1 {
				return fmt.Errorf("%w: a noop cannot be part of a batch of commands", ErrInvalidCommand)
			}
			continue
		case state.DecideCompleteWorkflow, state.DecideFailWorkflow, state.DecideContinueAsNew:
			if i != len(ds)-1 {
				return fmt.Errorf("%w: command %d ends the workflow but more commands follow it", ErrInvalidCommand, i+1)
			}
			continue
		}
		if d.Sequence != want {
			return fmt.Errorf("%w: command %d has sequence %d, expected %d — the SDK's replay is out of step with history",
				ErrInvalidCommand, i+1, d.Sequence, want)
		}
		opened[want] = d
		want++
		if d.StepName == "" {
			return fmt.Errorf("%w: command has an empty step name", ErrInvalidCommand)
		}
		if d.Type == state.DecideAwaitSignal && d.Signal == "" {
			return fmt.Errorf("%w: await-signal command has an empty signal name", ErrInvalidCommand)
		}
		if d.Type == state.DecideRecordMarker && d.Marker != state.MarkerVersion && d.Marker != state.MarkerSideEffect &&
			d.Marker != state.MarkerCancel {
			return fmt.Errorf("%w: unknown marker kind %q", ErrInvalidCommand, d.Marker)
		}
	}
	return nil
}
