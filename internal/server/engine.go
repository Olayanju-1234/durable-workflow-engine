package server

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/metrics"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/internal/timer"
)

// Planner produces the next Decision for a workflow from its history. It is the
// seam between the engine and "what the workflow is": a data-driven
// state.WorkflowDefinition (Phase 1) and the replay engine over real Go code
// (Phase 2, package sdk) both satisfy it, so the engine drives either without
// caring which. The decision it returns is applied by advanceLocked unchanged.
type Planner interface {
	Decide(events []*history.HistoryEvent) (state.Decision, error)
}

// Engine is the running heart of the workflow service: it owns the history
// store and the task queue, and turns external events (a client starting a
// workflow, a worker completing a step) into the same decision loop the Week-2
// state machine defines — read history, Decide, record the resulting event,
// dispatch the next task. It is the gRPC-driven sibling of cmd/workflowd's
// runDemo, with real external workers in place of the simulated inline one.
//
// All history mutation runs under mu, so events for a given workflow are
// recorded one at a time and their ids stay contiguous. The task queue is
// separately concurrent (a channel), so polling never contends with recording.
type Engine struct {
	store         *history.HistoryStore
	queue         *taskQueue
	workflowQueue *workflowTaskQueue // polyglot: decisions awaiting an SDK worker

	timers         *timer.Service // durable Sleep steps
	retryTimers    *timer.Service // activity retry backoffs (not history steps)
	leaseTimers    *timer.Service // lease expiry checks (dead-worker detection)
	decisionTimers *timer.Service // polyglot: SDK worker died holding a workflow task
	advanceRetries *timer.Service // decision loops that failed mid-advance (transient I/O)
	log            *slog.Logger
	registry       *metrics.Registry
	metrics        *engineMetrics

	leaseDuration   time.Duration // immutable after NewEngine
	decisionTimeout time.Duration // immutable after NewEngine

	closeOnce sync.Once

	mu               sync.Mutex
	planners         map[string]Planner       // workflow type -> planner
	policies         map[string]RetryPolicy   // activity step name -> retry policy
	leases           map[string]*lease        // workflow id -> current task lease
	pendingDecisions map[string]*decisionTask // workflow id -> outstanding SDK decision
}

// Option configures an Engine at construction (the functional-options pattern:
// optional named parameters without breaking NewEngine's signature).
type Option func(*Engine)

// WithLeaseDuration sets how long a worker may hold a task without completing
// or heartbeating before the engine reassigns it. Short in tests, generous in
// production — it bounds how long a dead worker can stall a workflow.
func WithLeaseDuration(d time.Duration) Option {
	return func(e *Engine) { e.leaseDuration = d }
}

// WithDecisionTimeout sets how long an SDK worker may hold a workflow task
// before the engine re-dispatches it. Re-deciding is always safe (replay is
// pure and stale commands are fenced), so this only bounds how long a dead SDK
// worker can stall a workflow.
func WithDecisionTimeout(d time.Duration) Option {
	return func(e *Engine) { e.decisionTimeout = d }
}

// NewEngine builds an Engine over an open history store and starts its timer
// services. Register the workflow types it can run before serving traffic, and
// Close it on shutdown to stop the timer goroutines.
func NewEngine(store *history.HistoryStore, logger *slog.Logger, opts ...Option) *Engine {
	e := &Engine{
		store:           store,
		queue:           newTaskQueue(),
		log:             logger,
		leaseDuration:   defaultLeaseDuration,
		decisionTimeout: defaultDecisionTimeout,
		workflowQueue:   newWorkflowTaskQueue(),

		planners:         make(map[string]Planner),
		policies:         make(map[string]RetryPolicy),
		leases:           make(map[string]*lease),
		pendingDecisions: make(map[string]*decisionTask),
	}
	for _, opt := range opts {
		opt(e)
	}
	e.registry = metrics.NewRegistry()
	e.metrics = newEngineMetrics(e.registry,
		func() float64 { return float64(e.queue.depth()) },
		func() float64 { return float64(e.workflowQueue.depth()) })
	e.timers = timer.New(e.onTimerFired)
	e.timers.Start()
	e.retryTimers = timer.New(e.onRetryDue)
	e.retryTimers.Start()
	e.leaseTimers = timer.New(e.onLeaseExpired)
	e.leaseTimers.Start()
	e.decisionTimers = timer.New(e.onDecisionTimeout)
	e.decisionTimers.Start()
	e.advanceRetries = timer.New(e.onAdvanceRetry)
	e.advanceRetries.Start()
	return e
}

// Close stops the engine's timer services. The history store is owned by the
// caller and closed separately. Safe to call once.
func (e *Engine) Close() {
	e.closeOnce.Do(func() {
		e.timers.Stop()
		e.retryTimers.Stop()
		e.leaseTimers.Stop()
		e.decisionTimers.Stop()
		e.advanceRetries.Stop()
	})
}

// Metrics is the engine's Prometheus registry — mount its Handler at /metrics.
func (e *Engine) Metrics() *metrics.Registry {
	return e.registry
}

// Register makes a data-driven workflow definition runnable (the Phase-1 model).
func (e *Engine) Register(def state.WorkflowDefinition) {
	e.RegisterWorkflow(def.Type, def)
}

// RegisterWorkflow makes any planner runnable under a workflow type — a
// state.WorkflowDefinition, or a replay-engine workflow over real Go code
// (package sdk). Call before serving traffic.
func (e *Engine) RegisterWorkflow(workflowType string, planner Planner) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.planners[workflowType] = planner
}

// StartWorkflow records WorkflowStarted for a new execution and schedules its
// first step, returning the workflow id. A blank workflowID is auto-generated;
// a supplied one must not already exist.
func (e *Engine) StartWorkflow(workflowType string, input []byte, workflowID string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	planner, ok := e.planners[workflowType]
	if !ok {
		return "", fmt.Errorf("%w: %q is not registered on this node", ErrUnknownType, workflowType)
	}
	if workflowID == "" {
		// Timestamp alone can collide at coarse clock resolution or under rapid
		// calls; the random suffix makes a generated id effectively unique.
		workflowID = fmt.Sprintf("%s-%d-%08x", workflowType, time.Now().UnixNano(), rand.Uint32())
	}

	existing, err := e.store.ReadAll(workflowID)
	if err != nil {
		return "", fmt.Errorf("server: read history %s: %w", workflowID, err)
	}
	if len(existing) > 0 {
		return "", fmt.Errorf("%w: %q already has history", ErrAlreadyExists, workflowID)
	}

	started := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{
			WorkflowId: workflowID, WorkflowType: workflowType, Input: input,
		},
	}}
	if err := e.appendLocked(workflowID, started, 1); err != nil {
		return "", err
	}
	e.metrics.workflowsStarted.Inc()
	e.metrics.workflowsActive.Inc()
	e.log.Info("workflow started", "workflow_id", workflowID, "type", workflowType)

	if err := e.advanceLocked(workflowID, planner); err != nil {
		return "", err
	}
	return workflowID, nil
}

// Recover rebuilds runtime state from durable history after a restart. The task
// queue is ephemeral — on startup it is empty — but the on-disk log is the source
// of truth, so recovery replays every workflow and puts each non-terminal one
// back where it belongs:
//
//   - WAITING (a step was scheduled but its task died with the old queue): the
//     in-flight step is re-enqueued so a worker picks it up. This is at-least-once
//     — if the worker had actually finished but the result was never recorded, the
//     step re-runs. Phase 1 is engine-crash-safe, not exactly-once.
//   - RUNNING (the engine crashed mid-decision — after recording a step's result
//     but before scheduling the next, or before scheduling the very first step):
//     advance one decision, which schedules the next step (or reaches terminal).
//   - Terminal / not-started / unregistered type: nothing to do.
//
// Completed steps are never re-run: Decide derives the next action from history,
// and history already records them as done. Call Recover once, before serving.
func (e *Engine) Recover() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	ids, err := e.store.ListWorkflows()
	if err != nil {
		return fmt.Errorf("server: list workflows for recovery: %w", err)
	}

	var requeued, rearmed, advanced int
	for _, id := range ids {
		events, err := e.store.ReadAll(id)
		if err != nil {
			return fmt.Errorf("server: recover read %s: %w", id, err)
		}
		st, err := state.Reconstruct(events)
		if err != nil {
			return fmt.Errorf("server: recover reconstruct %s: %w", id, err)
		}
		if st.Status.IsTerminal() || st.Status == state.StatusNotStarted {
			continue
		}
		// Recovered live workflows re-enter the active gauge (it is in-memory and
		// died with the old process); advancing below may immediately retire one.
		e.metrics.workflowsActive.Inc()
		planner, ok := e.planners[st.WorkflowType]
		if !ok {
			// We can't drive a workflow whose code isn't registered on this node.
			// Leave it durable; a later deploy that registers the type recovers it.
			e.log.Warn("recovery: skipping unregistered workflow type",
				"workflow_id", id, "type", st.WorkflowType)
			continue
		}

		switch st.Status {
		case state.StatusWaiting:
			// The in-flight step's kind is reconstructed from history, so recovery
			// needs no definition to know how to resume it.
			tail := st.Tail()
			switch tail.Kind {
			case state.KindTimer:
				// Re-arm against the durable deadline. If it already passed while we
				// were down, the timer service fires it immediately.
				e.timers.Add(timer.Timer{
					WorkflowID: id, StepName: tail.Name,
					Sequence: tail.Sequence, FireAt: time.Unix(0, tail.FireAt),
				})
				rearmed++
				e.log.Info("recovery: re-armed timer", "workflow_id", id,
					"step", tail.Name, "sequence", tail.Sequence, "fire_at", time.Unix(0, tail.FireAt))
			case state.KindSignal:
				// A signal may have been buffered before the crash but not yet
				// delivered; try now. Otherwise the wait stands until a new signal.
				if err := e.tryDeliverSignalLocked(id); err != nil {
					return fmt.Errorf("server: recover deliver signal %s: %w", id, err)
				}
			default: // activity
				e.queue.enqueue(&Task{
					WorkflowId: id, StepName: tail.Name,
					Sequence: tail.Sequence, Input: tail.Input,
				})
				requeued++
				e.log.Info("recovery: re-enqueued in-flight step", "workflow_id", id,
					"step", tail.Name, "sequence", tail.Sequence)
			}
		case state.StatusRunning:
			// A failed activity with retries left (the engine died during the backoff
			// window) must retry, not fail. The backoff timer was in memory, so re-arm
			// it — promptly, since the original window is unknown.
			if tail := st.Tail(); tail != nil && tail.Status == state.StepFailed && tail.Kind == state.KindActivity {
				if p := e.retryPolicy(tail.Name); tail.Attempts < p.MaxAttempts {
					e.scheduleRetryLocked(id, tail, 0)
					rearmed++
					continue
				}
			}
			if err := e.advanceLocked(id, planner); err != nil {
				return fmt.Errorf("server: recover advance %s: %w", id, err)
			}
			advanced++
			e.log.Info("recovery: advanced mid-decision workflow", "workflow_id", id)
		}
	}
	e.log.Info("recovery complete", "workflows", len(ids),
		"requeued", requeued, "rearmed", rearmed, "advanced", advanced)
	return nil
}

// CompleteTask records a worker's result for the in-flight step and advances the
// workflow. failed selects StepFailed (error) over StepCompleted (output). It
// rejects a result that does not match the step currently in flight, which makes
// a duplicate or stale CompleteTask a clean error rather than a corrupt log.
// leaseToken must be the current lease on the task (from the dispatched Task):
// a worker whose lease expired — its task reassigned — is turned away here, so
// two workers can never both record a result for the same attempt.
func (e *Engine) CompleteTask(workflowID, stepName string, sequence int64, leaseToken string, output []byte, stepErr string, failed bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	if err := e.checkLeaseLocked(workflowID, leaseToken); err != nil {
		return err
	}
	claimedAt := e.leases[workflowID].claimedAt

	events, err := e.store.ReadAll(workflowID)
	if err != nil {
		return fmt.Errorf("server: read history %s: %w", workflowID, err)
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		return fmt.Errorf("server: reconstruct %s: %w", workflowID, err)
	}
	if st.Status != state.StatusWaiting {
		return fmt.Errorf("server: workflow %q has no step in flight (status %s)", workflowID, st.Status)
	}
	tail := st.Tail()
	if tail.Sequence != sequence || tail.Name != stepName {
		return fmt.Errorf("server: result %s/seq%d does not match in-flight step %s/seq%d",
			stepName, sequence, tail.Name, tail.Sequence)
	}
	planner, ok := e.planners[st.WorkflowType]
	if !ok {
		return fmt.Errorf("server: unknown workflow type %q", st.WorkflowType)
	}

	if failed {
		failedEv := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
			StepFailed: &history.StepFailedEvent{
				StepName: stepName, Sequence: sequence, Error: stepErr, RetryCount: int32(tail.Attempts),
			},
		}}
		if err := e.appendLocked(workflowID, failedEv, int64(len(events)+1)); err != nil {
			return err
		}
		// The attempt is over; its lease is spent (a retry gets a fresh one).
		delete(e.leases, workflowID)
		e.metrics.taskResults.With("failed").Inc()
		e.metrics.stepDuration.Observe(time.Since(claimedAt).Seconds())
		e.log.Warn("step failed", "workflow_id", workflowID, "step", stepName,
			"sequence", sequence, "error", stepErr, "attempt", tail.Attempts)

		// Retry transparently if attempts remain (only activities retry); the
		// workflow sees the error only once the policy is exhausted.
		policy := e.retryPolicy(stepName)
		if tail.Kind == state.KindActivity && tail.Attempts < policy.MaxAttempts {
			e.scheduleRetryLocked(workflowID, tail, policy.backoff(tail.Attempts))
			return nil
		}
		return e.advanceLocked(workflowID, planner)
	}

	completed := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
		StepCompleted: &history.StepCompletedEvent{StepName: stepName, Sequence: sequence, Output: output},
	}}
	if err := e.appendLocked(workflowID, completed, int64(len(events)+1)); err != nil {
		return err
	}
	delete(e.leases, workflowID)
	e.metrics.taskResults.With("completed").Inc()
	e.metrics.stepDuration.Observe(time.Since(claimedAt).Seconds())
	e.log.Info("step completed", "workflow_id", workflowID, "step", stepName, "sequence", sequence)
	return e.advanceLocked(workflowID, planner)
}

// PollTask blocks until a task is ready for a worker (or ctx is cancelled).
// A worker that names stepNames only receives tasks for those activities, so
// workers with different activity sets can share one engine; none means any.
// Dispatch grants the worker a lease on the task: the returned Task carries a
// token that CompleteTask and Heartbeat must present, and a deadline after
// which the engine assumes the worker died and reassigns the work.
func (e *Engine) PollTask(ctx context.Context, workerID string, stepNames ...string) (*Task, error) {
	t, err := e.queue.poll(ctx, stepNames)
	if err != nil {
		return nil, err
	}
	e.mu.Lock()
	le := e.claimLocked(t, workerID)
	e.mu.Unlock()
	e.log.Info("task dispatched", "worker_id", workerID, "workflow_id", t.GetWorkflowId(),
		"step", t.GetStepName(), "sequence", t.GetSequence(), "lease_deadline", le.deadline)
	return t, nil
}

// SignalWorkflow delivers an external signal to a running workflow. The signal is
// recorded durably (buffered in the log), then, if the workflow is currently
// waiting for a matching signal, the wait is satisfied and the workflow advances.
// A signal to a not-yet-started or terminal workflow is rejected; so is an empty
// signal name — it would be recorded forever and could never match a real wait.
func (e *Engine) SignalWorkflow(workflowID, signalName string, payload []byte) error {
	if signalName == "" {
		return fmt.Errorf("%w: signal name must not be empty", ErrInvalidArgument)
	}
	e.mu.Lock()
	defer e.mu.Unlock()

	events, err := e.store.ReadAll(workflowID)
	if err != nil {
		return fmt.Errorf("server: read history %s: %w", workflowID, err)
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		return fmt.Errorf("server: reconstruct %s: %w", workflowID, err)
	}
	if st.Status == state.StatusNotStarted {
		return fmt.Errorf("%w: %q", ErrNotFound, workflowID)
	}
	if st.Status.IsTerminal() {
		return fmt.Errorf("%w: %q is %s; cannot signal a terminal workflow", ErrAlreadyTerminal, workflowID, st.Status)
	}

	sig := &history.HistoryEvent{Attributes: &history.HistoryEvent_SignalReceived{
		SignalReceived: &history.SignalReceivedEvent{SignalName: signalName, Payload: payload},
	}}
	if err := e.appendLocked(workflowID, sig, int64(len(events)+1)); err != nil {
		return err
	}
	e.metrics.signalsReceived.Inc()
	e.log.Info("signal received", "workflow_id", workflowID, "signal", signalName)
	return e.tryDeliverSignalLocked(workflowID)
}

// tryDeliverSignalLocked completes the in-flight step if it is a signal wait with a
// matching, not-yet-consumed buffered signal, then advances. It is a no-op when the
// workflow isn't waiting on a signal or no matching signal has arrived. The caller
// must hold e.mu. Called both when a signal arrives and when a wait is first reached
// (in case the signal was buffered earlier).
func (e *Engine) tryDeliverSignalLocked(workflowID string) error {
	events, err := e.store.ReadAll(workflowID)
	if err != nil {
		return fmt.Errorf("server: read history %s: %w", workflowID, err)
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		return fmt.Errorf("server: reconstruct %s: %w", workflowID, err)
	}
	planner, ok := e.planners[st.WorkflowType]
	if !ok {
		return nil // no plan on this node: leave the signal buffered
	}
	payload, ok := deliverableSignal(st)
	if !ok {
		return nil // not waiting on a signal, or none has matched yet
	}
	tail := st.Tail()
	completed := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
		StepCompleted: &history.StepCompletedEvent{StepName: tail.Name, Sequence: tail.Sequence, Output: payload},
	}}
	if err := e.appendLocked(workflowID, completed, int64(len(events)+1)); err != nil {
		return err
	}
	e.log.Info("signal delivered", "workflow_id", workflowID, "step", tail.Name, "sequence", tail.Sequence)
	return e.advanceLocked(workflowID, planner)
}

// deliverableSignal reports the payload that satisfies the in-flight step, if that
// step is a signal wait and a matching buffered signal is available. Matching is by
// arrival order: the k-th wait for signal S consumes the k-th received S, so a
// signal that arrived before the wait is delivered correctly (buffering).
func deliverableSignal(st *state.State) ([]byte, bool) {
	tail := st.Tail()
	if tail == nil || tail.Status != state.StepScheduled || tail.Kind != state.KindSignal {
		return nil, false // no signal wait in flight
	}

	// Count already-completed waits for this signal name: that many matching signals
	// are spoken for. The next matching signal (if any) satisfies this wait.
	consumed := 0
	for i := 0; i < len(st.Steps)-1; i++ {
		s := st.Steps[i]
		if s.Kind == state.KindSignal && s.Signal == tail.Signal && s.Status == state.StepCompleted {
			consumed++
		}
	}
	seen := 0
	for _, sig := range st.Signals {
		if sig.Name == tail.Signal {
			if seen == consumed {
				return sig.Payload, true
			}
			seen++
		}
	}
	return nil, false
}

// onTimerFired is the timer service's callback: when a timer's deadline passes, the
// engine records TimerFired for the still-waiting timer step and advances. It runs
// on the timer goroutine, so it takes e.mu like any other mutation. The status guard
// makes a stale fire (e.g. a recovery-rearmed timer that raced) a harmless no-op.
func (e *Engine) onTimerFired(tm timer.Timer) {
	e.mu.Lock()
	defer e.mu.Unlock()

	events, err := e.store.ReadAll(tm.WorkflowID)
	if err != nil {
		e.log.Error("timer fired: read history", "workflow_id", tm.WorkflowID, "err", err)
		e.rearmCallback(e.timers, tm)
		return
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		e.log.Error("timer fired: reconstruct", "workflow_id", tm.WorkflowID, "err", err)
		return
	}
	tail := st.Tail()
	if st.Status != state.StatusWaiting || tail == nil || tail.Sequence != tm.Sequence || tail.Name != tm.StepName {
		return // already resolved; nothing to do
	}
	planner, ok := e.planners[st.WorkflowType]
	if !ok {
		e.log.Warn("timer fired for unregistered type", "workflow_id", tm.WorkflowID, "type", st.WorkflowType)
		return
	}
	fired := &history.HistoryEvent{Attributes: &history.HistoryEvent_TimerFired{
		TimerFired: &history.TimerFiredEvent{StepName: tm.StepName, Sequence: tm.Sequence},
	}}
	if err := e.appendLocked(tm.WorkflowID, fired, int64(len(events)+1)); err != nil {
		e.log.Error("timer fired: append", "workflow_id", tm.WorkflowID, "err", err)
		e.rearmCallback(e.timers, tm)
		return
	}
	e.metrics.timersFired.Inc()
	e.log.Info("timer fired", "workflow_id", tm.WorkflowID, "step", tm.StepName, "sequence", tm.Sequence)
	e.advanceOrRetry(tm, tm.WorkflowID, planner)
}

// advanceLocked runs one turn of the decision loop: replay history, Decide, and
// apply the result. Phase-1 workflows are sequential, so each call yields exactly
// one decision — schedule one step (then wait for a worker), or reach a terminal
// state. The caller must hold e.mu.
func (e *Engine) advanceLocked(workflowID string, planner Planner) error {
	events, err := e.store.ReadAll(workflowID)
	if err != nil {
		return fmt.Errorf("server: read history %s: %w", workflowID, err)
	}

	// A remote (polyglot) planner can't be consulted inline: deciding means a
	// network round-trip to an SDK worker, and e.mu is held here. Instead we
	// hand the history out as a workflow task and apply the command when it
	// comes back through CompleteWorkflowTask.
	if rp, ok := planner.(*remotePlanner); ok {
		return e.dispatchWorkflowTaskLocked(workflowID, rp.workflowType, events)
	}

	decision, err := planner.Decide(events)
	if err != nil {
		return fmt.Errorf("server: decide %s: %w", workflowID, err)
	}
	return e.applyDecisionLocked(workflowID, decision, int64(len(events)+1))
}

// applyDecisionLocked records the event a decision implies and performs its side
// effect (enqueue a task, arm a timer, ...). It is the single place a decision
// becomes durable, shared by in-process planners and remote SDK workers alike —
// so a TypeScript workflow and a Go workflow take exactly the same path through
// the engine. The caller must hold e.mu.
func (e *Engine) applyDecisionLocked(workflowID string, decision state.Decision, nextID int64) error {
	switch decision.Type {
	case state.DecideScheduleStep:
		scheduled := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
			StepScheduled: &history.StepScheduledEvent{
				StepName: decision.StepName, Sequence: decision.Sequence, Input: decision.Input,
			},
		}}
		if err := e.appendLocked(workflowID, scheduled, nextID); err != nil {
			return err
		}
		// Record-then-enqueue: the step is durable before any worker can see it,
		// so a crash can never dispatch a task the history doesn't know about.
		e.queue.enqueue(&Task{
			WorkflowId: workflowID, StepName: decision.StepName,
			Sequence: decision.Sequence, Input: decision.Input,
		})
		e.log.Info("step scheduled", "workflow_id", workflowID, "step", decision.StepName, "sequence", decision.Sequence)

	case state.DecideStartTimer:
		// The absolute deadline is computed here (not in pure Decide) and recorded,
		// so recovery can re-arm the same deadline after a restart.
		fireAt := time.Now().Add(decision.Delay)
		timerStarted := &history.HistoryEvent{Attributes: &history.HistoryEvent_TimerStarted{
			TimerStarted: &history.TimerStartedEvent{
				StepName: decision.StepName, Sequence: decision.Sequence,
				FireAt: fireAt.UnixNano(), Input: decision.Input,
			},
		}}
		if err := e.appendLocked(workflowID, timerStarted, nextID); err != nil {
			return err
		}
		e.timers.Add(timer.Timer{
			WorkflowID: workflowID, StepName: decision.StepName,
			Sequence: decision.Sequence, FireAt: fireAt,
		})
		e.log.Info("timer started", "workflow_id", workflowID, "step", decision.StepName,
			"sequence", decision.Sequence, "fire_at", fireAt)

	case state.DecideAwaitSignal:
		// A signal wait is a step with no worker task: record it as scheduled (so the
		// workflow is WAITING) but dispatch nothing. It completes when SignalWorkflow
		// delivers a matching signal — which may already be buffered, so try now.
		awaiting := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
			StepScheduled: &history.StepScheduledEvent{
				StepName: decision.StepName, Sequence: decision.Sequence, Input: decision.Input,
				AwaitsSignal: true, SignalName: decision.Signal,
			},
		}}
		if err := e.appendLocked(workflowID, awaiting, nextID); err != nil {
			return err
		}
		e.log.Info("awaiting signal", "workflow_id", workflowID, "step", decision.StepName,
			"sequence", decision.Sequence, "signal", decision.Signal)
		return e.tryDeliverSignalLocked(workflowID)

	case state.DecideCompleteWorkflow:
		done := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowCompleted{
			WorkflowCompleted: &history.WorkflowCompletedEvent{Output: decision.Output},
		}}
		if err := e.appendLocked(workflowID, done, nextID); err != nil {
			return err
		}
		e.metrics.workflowsFinished.With("completed").Inc()
		e.metrics.workflowsActive.Dec()
		e.log.Info("workflow completed", "workflow_id", workflowID)

	case state.DecideFailWorkflow:
		failedEv := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowFailed{
			WorkflowFailed: &history.WorkflowFailedEvent{Error: decision.Error},
		}}
		if err := e.appendLocked(workflowID, failedEv, nextID); err != nil {
			return err
		}
		e.metrics.workflowsFinished.With("failed").Inc()
		e.metrics.workflowsActive.Dec()
		e.log.Warn("workflow failed", "workflow_id", workflowID, "error", decision.Error)

	case state.DecideNoop:
		// A step is in flight (wait for the worker) or the workflow is terminal.
	}
	return nil
}

// appendLocked stamps the event id and wall-clock timestamp, then durably
// appends. The caller must hold e.mu so ids stay contiguous across workflows.
func (e *Engine) appendLocked(workflowID string, ev *history.HistoryEvent, eventID int64) error {
	ev.EventId = eventID
	ev.Timestamp = time.Now().UnixNano()
	if err := e.store.Append(workflowID, ev); err != nil {
		return fmt.Errorf("server: append %s: %w", workflowID, err)
	}
	return nil
}

// callbackRetryDelay is how long a failed timer callback waits before retrying.
// The timer has already been popped from its heap when the callback runs, so a
// transient I/O error (disk full, EIO) would otherwise strand the workflow until
// the next restart — re-arming turns that into a bounded retry loop that is loud
// in the logs and self-heals.
const callbackRetryDelay = 10 * time.Second

// rearmCallback re-arms a popped callback timer after a transient failure. The
// caller must hold e.mu.
func (e *Engine) rearmCallback(svc *timer.Service, tm timer.Timer) {
	svc.Add(timer.Timer{
		WorkflowID: tm.WorkflowID, StepName: tm.StepName,
		Sequence: tm.Sequence, FireAt: time.Now().Add(callbackRetryDelay),
	})
}

// advanceOrRetry runs the decision loop; on failure it schedules a retry instead
// of leaving the workflow stranded between "event recorded" and "next step
// dispatched". Advancing is idempotent — Decide derives everything from history —
// so a blind retry is safe. The caller must hold e.mu.
func (e *Engine) advanceOrRetry(tm timer.Timer, workflowID string, planner Planner) {
	if err := e.advanceLocked(workflowID, planner); err != nil {
		e.log.Error("advance failed; scheduling retry", "workflow_id", workflowID, "err", err)
		e.advanceRetries.Add(timer.Timer{WorkflowID: workflowID, FireAt: time.Now().Add(callbackRetryDelay)})
	}
}

// onAdvanceRetry re-runs a decision loop whose previous advance failed mid-flight.
// Guarded like every callback: only acts when the workflow is still RUNNING and
// its type is registered here.
func (e *Engine) onAdvanceRetry(tm timer.Timer) {
	e.mu.Lock()
	defer e.mu.Unlock()

	events, err := e.store.ReadAll(tm.WorkflowID)
	if err != nil {
		e.log.Error("advance retry: read history", "workflow_id", tm.WorkflowID, "err", err)
		e.advanceRetries.Add(timer.Timer{WorkflowID: tm.WorkflowID, FireAt: time.Now().Add(callbackRetryDelay)})
		return
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		// A corrupt history will not heal by retrying; recovery or an operator
		// must intervene. Say so loudly once per attempt-free cycle.
		e.log.Error("advance retry: history unreadable; giving up", "workflow_id", tm.WorkflowID, "err", err)
		return
	}
	if st.Status != state.StatusRunning {
		return // moved on (or terminal); nothing to decide
	}
	planner, ok := e.planners[st.WorkflowType]
	if !ok {
		e.log.Warn("advance retry for unregistered type", "workflow_id", tm.WorkflowID, "type", st.WorkflowType)
		return
	}
	if err := e.advanceLocked(tm.WorkflowID, planner); err != nil {
		e.log.Error("advance retry failed; will retry again", "workflow_id", tm.WorkflowID, "err", err)
		e.advanceRetries.Add(timer.Timer{WorkflowID: tm.WorkflowID, FireAt: time.Now().Add(callbackRetryDelay)})
	}
}

// EngineStats is a point-in-time snapshot of the engine's live gauges, for the
// health endpoint and operational tooling.
type EngineStats struct {
	ActiveWorkflows        int64
	TaskQueueDepth         int
	WorkflowTaskQueueDepth int
}

// Stats returns the current gauge values. Cheap: all three are atomic/channel-len reads.
func (e *Engine) Stats() EngineStats {
	return EngineStats{
		ActiveWorkflows:        e.metrics.workflowsActive.Value(),
		TaskQueueDepth:         e.queue.depth(),
		WorkflowTaskQueueDepth: e.workflowQueue.depth(),
	}
}
