package server

import (
	"container/list"
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"strings"
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

// statePlanner decides from replayed state the engine already holds, saving a
// replay of the whole history per decision (state.WorkflowDefinition).
type statePlanner interface {
	DecideState(st *state.State) (state.Decision, error)
}

// HistoryLog is where the engine records history mutations. Single-node mode
// writes straight to the HistoryStore; a cluster replicates each mutation
// through Raft and returns once it is applied locally. Reads always go to the
// store, which in both modes holds every recorded event.
type HistoryLog interface {
	Append(workflowID string, ev *history.HistoryEvent) error
	Delete(workflowID string) error
}

// Engine is the running heart of the workflow service: it owns the history
// store and the task queue, and turns external events (a client starting a
// workflow, a worker completing a step) into the same decision loop the Week-2
// state machine defines — read history, Decide, record the resulting event,
// dispatch the next task. It is the gRPC-driven sibling of cmd/workflowd's
// runDemo, with real external workers in place of the simulated inline one.
//
// Each workflow has its own lock and in-memory history (wfstate.go), so events
// for one workflow are recorded one at a time and their ids stay contiguous,
// while different workflows proceed in parallel. mu guards only the shared
// maps below and is never held across I/O. The task queues are separately
// concurrent, so polling never contends with recording.
type Engine struct {
	store         *history.HistoryStore
	hlog          HistoryLog
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
	cacheSize       int           // immutable after NewEngine

	wfMu sync.Mutex
	wfs  map[string]*wfState // workflows in use or cached
	idle *list.List          // cached workflows nobody holds, most recent first

	closeOnce sync.Once
	done      chan struct{} // closed by Close; ends long polls

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

// WithHistoryLog routes history writes through l instead of straight to the
// store. A cluster passes its Raft-backed log here.
func WithHistoryLog(l HistoryLog) Option {
	return func(e *Engine) { e.hlog = l }
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
		hlog:            store,
		done:            make(chan struct{}),
		queue:           newTaskQueue(),
		log:             logger,
		leaseDuration:   defaultLeaseDuration,
		decisionTimeout: defaultDecisionTimeout,
		cacheSize:       defaultStateCacheSize,
		workflowQueue:   newWorkflowTaskQueue(),
		wfs:             make(map[string]*wfState),
		idle:            list.New(),

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

// Close stops the engine's timer services and ends its long polls. The history
// store is owned by the caller and closed separately. Safe to call more than
// once.
func (e *Engine) Close() {
	e.closeOnce.Do(func() {
		close(e.done)
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
	planner, ok := e.planner(workflowType)
	if !ok {
		return "", fmt.Errorf("%w: %q is not registered on this node", ErrUnknownType, workflowType)
	}
	if workflowID == "" {
		// Timestamp alone can collide at coarse clock resolution or under rapid
		// calls; the random suffix makes a generated id effectively unique.
		workflowID = fmt.Sprintf("%s-%d-%08x", workflowType, time.Now().UnixNano(), rand.Uint32())
	}

	if err := validateID(workflowID); err != nil {
		return "", err
	}
	w := e.lockWorkflow(workflowID)
	defer e.unlockWorkflow(w)
	existing, _, err := e.historyLocked(w)
	if len(existing) > 0 {
		return "", fmt.Errorf("%w: %q already has history", ErrAlreadyExists, workflowID)
	}
	if err != nil {
		return "", err
	}

	started := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{
			WorkflowId: workflowID, WorkflowType: workflowType, Input: input,
		},
	}}
	if err := e.appendLocked(w, started, 1); err != nil {
		return "", err
	}
	e.metrics.workflowsStarted.Inc()
	e.metrics.workflowsActive.Inc()
	e.log.Info("workflow started", "workflow_id", workflowID, "type", workflowType)

	if err := e.advanceLocked(w, planner); err != nil {
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
	ids, err := e.store.ListWorkflows()
	if err != nil {
		return fmt.Errorf("server: list workflows for recovery: %w", err)
	}

	var c recoveryCounts
	for _, id := range ids {
		w := e.lockWorkflow(id)
		err := e.recoverLocked(w, &c)
		e.unlockWorkflow(w)
		if err != nil {
			return err
		}
	}
	e.log.Info("recovery complete", "workflows", len(ids),
		"requeued", c.requeued, "rearmed", c.rearmed, "advanced", c.advanced)
	return nil
}

type recoveryCounts struct{ requeued, rearmed, advanced int }

func (e *Engine) recoverLocked(w *wfState, c *recoveryCounts) error {
	id := w.id
	_, st, err := e.historyLocked(w)
	if err != nil {
		return fmt.Errorf("server: recover: %w", err)
	}
	if st.Status.IsTerminal() || st.Status == state.StatusNotStarted {
		return nil
	}
	// Recovered live workflows re-enter the active gauge (it is in-memory and
	// died with the old process); advancing below may immediately retire one.
	e.metrics.workflowsActive.Inc()

	// Resuming an in-flight step needs no planner, only deciding what comes
	// next does. SDK workers register after recovery, so a remote workflow's
	// activity, timer or signal must not wait for them.
	switch st.Status {
	case state.StatusWaiting:
		// The in-flight step's kind is reconstructed from history, so recovery
		// needs no definition to know how to resume it.
		tail := *st.Tail()
		switch tail.Kind {
		case state.KindTimer:
			// Re-arm against the durable deadline. If it already passed while we
			// were down, the timer service fires it immediately.
			e.timers.Add(timer.Timer{
				WorkflowID: id, StepName: tail.Name,
				Sequence: tail.Sequence, FireAt: time.Unix(0, tail.FireAt),
			})
			c.rearmed++
			e.log.Info("recovery: re-armed timer", "workflow_id", id,
				"step", tail.Name, "sequence", tail.Sequence, "fire_at", time.Unix(0, tail.FireAt))
		case state.KindSignal:
			// A signal may have been buffered before the crash but not yet
			// delivered; try now. Otherwise the wait stands until a new signal.
			if err := e.tryDeliverSignalLocked(w); err != nil {
				return fmt.Errorf("server: recover deliver signal %s: %w", id, err)
			}
		default: // activity
			e.queue.enqueue(&Task{
				WorkflowId: id, StepName: tail.Name,
				Sequence: tail.Sequence, Input: tail.Input,
			})
			c.requeued++
			e.log.Info("recovery: re-enqueued in-flight step", "workflow_id", id,
				"step", tail.Name, "sequence", tail.Sequence)
		}
	case state.StatusRunning:
		// A failed activity with retries left (the engine died during the backoff
		// window) must retry, not fail. The backoff timer was in memory, so re-arm
		// it — promptly, since the original window is unknown.
		if tail := st.Tail(); tail != nil && tail.Status == state.StepFailed && tail.Kind == state.KindActivity {
			if p := e.retryPolicy(tail); tail.Attempts < p.MaxAttempts {
				e.scheduleRetryLocked(id, tail, 0)
				c.rearmed++
				return nil
			}
		}
		planner, ok := e.planner(st.WorkflowType)
		if !ok {
			// Registering the type later resumes it (resumeType).
			e.log.Warn("recovery: workflow type not registered yet; waiting for a worker",
				"workflow_id", id, "type", st.WorkflowType)
			return nil
		}
		if err := e.advanceLocked(w, planner); err != nil {
			return fmt.Errorf("server: recover advance %s: %w", id, err)
		}
		c.advanced++
		e.log.Info("recovery: advanced mid-decision workflow", "workflow_id", id)
	}
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
	if err := validateID(workflowID); err != nil {
		return err
	}
	w := e.lockWorkflow(workflowID)
	defer e.unlockWorkflow(w)

	e.mu.Lock()
	err := e.checkLeaseLocked(workflowID, leaseToken)
	var claimedAt time.Time
	if err == nil {
		claimedAt = e.leases[workflowID].claimedAt
	}
	e.mu.Unlock()
	if err != nil {
		return err
	}

	events, st, err := e.historyLocked(w)
	if err != nil {
		return err
	}
	if st.Status != state.StatusWaiting {
		return fmt.Errorf("server: workflow %q has no step in flight (status %s)", workflowID, st.Status)
	}
	tail := *st.Tail()
	workflowType := st.WorkflowType
	if tail.Sequence != sequence || tail.Name != stepName {
		return fmt.Errorf("server: result %s/seq%d does not match in-flight step %s/seq%d",
			stepName, sequence, tail.Name, tail.Sequence)
	}
	if failed {
		failedEv := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
			StepFailed: &history.StepFailedEvent{
				StepName: stepName, Sequence: sequence, Error: stepErr, RetryCount: int32(tail.Attempts),
			},
		}}
		if err := e.appendLocked(w, failedEv, int64(len(events)+1)); err != nil {
			return err
		}
		// The attempt is over; its lease is spent (a retry gets a fresh one).
		e.dropLease(workflowID)
		e.metrics.taskResults.With("failed").Inc()
		e.metrics.stepDuration.Observe(time.Since(claimedAt).Seconds())
		e.log.Warn("step failed", "workflow_id", workflowID, "step", stepName,
			"sequence", sequence, "error", stepErr, "attempt", tail.Attempts)

		// Retry transparently if attempts remain (only activities retry); the
		// workflow sees the error only once the policy is exhausted.
		policy := e.retryPolicy(&tail)
		if tail.Kind == state.KindActivity && tail.Attempts < policy.MaxAttempts {
			e.scheduleRetryLocked(workflowID, &tail, policy.Delay(tail.Attempts))
			return nil
		}
		return e.advanceIfRegisteredLocked(w, workflowType)
	}

	completed := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
		StepCompleted: &history.StepCompletedEvent{StepName: stepName, Sequence: sequence, Output: output},
	}}
	if err := e.appendLocked(w, completed, int64(len(events)+1)); err != nil {
		return err
	}
	e.dropLease(workflowID)
	e.metrics.taskResults.With("completed").Inc()
	e.metrics.stepDuration.Observe(time.Since(claimedAt).Seconds())
	e.log.Info("step completed", "workflow_id", workflowID, "step", stepName, "sequence", sequence)
	return e.advanceIfRegisteredLocked(w, workflowType)
}

// PollTask blocks until a task is ready for a worker (or ctx is cancelled).
// A worker that names stepNames only receives tasks for those activities, so
// workers with different activity sets can share one engine; none means any.
// Dispatch grants the worker a lease on the task: the returned Task carries a
// token that CompleteTask and Heartbeat must present, and a deadline after
// which the engine assumes the worker died and reassigns the work.
func (e *Engine) PollTask(ctx context.Context, workerID string, stepNames ...string) (*Task, error) {
	ctx, cancel := e.pollContext(ctx)
	defer cancel()
	t, err := e.queue.poll(ctx, stepNames)
	if err != nil {
		return nil, e.pollError(err)
	}
	w := e.lockWorkflow(t.GetWorkflowId())
	if ctx.Err() != nil {
		// The poller gave up just as the task arrived (or while we waited for
		// the lock). Leasing it now would hand it to a worker that never sees
		// it, stranding it until the lease expires and costing an attempt; put
		// it back for the next poller.
		e.unlockWorkflow(w)
		e.queue.enqueue(t)
		return nil, e.pollError(ctx.Err())
	}
	le := e.claim(t, workerID)
	e.unlockWorkflow(w)
	e.log.Info("task dispatched", "worker_id", workerID, "workflow_id", t.GetWorkflowId(),
		"step", t.GetStepName(), "sequence", t.GetSequence(), "lease_deadline", le.deadline)
	return t, nil
}

// pollContext ends a long poll when either the caller gives up or the engine
// closes, so a worker parked on a deposed leader's queue is told at once.
func (e *Engine) pollContext(ctx context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		select {
		case <-e.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}

func (e *Engine) pollError(err error) error {
	select {
	case <-e.done:
		return ErrEngineClosed
	default:
		return err
	}
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
	if err := validateID(workflowID); err != nil {
		return err
	}
	w := e.lockWorkflow(workflowID)
	defer e.unlockWorkflow(w)

	events, st, err := e.historyLocked(w)
	if err != nil {
		return err
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
	if err := e.appendLocked(w, sig, int64(len(events)+1)); err != nil {
		return err
	}
	e.metrics.signalsReceived.Inc()
	e.log.Info("signal received", "workflow_id", workflowID, "signal", signalName)
	return e.tryDeliverSignalLocked(w)
}

// tryDeliverSignalLocked completes the in-flight step if it is a signal wait with a
// matching, not-yet-consumed buffered signal, then advances. It is a no-op when the
// workflow isn't waiting on a signal or no matching signal has arrived. The caller
// must hold w.mu. Called both when a signal arrives and when a wait is first reached
// (in case the signal was buffered earlier).
func (e *Engine) tryDeliverSignalLocked(w *wfState) error {
	events, st, err := e.historyLocked(w)
	if err != nil {
		return err
	}
	payload, ok := st.DeliverableSignal()
	if !ok {
		return nil // not waiting on a signal, or none has matched yet
	}
	tail := *st.Tail()
	completed := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
		StepCompleted: &history.StepCompletedEvent{StepName: tail.Name, Sequence: tail.Sequence, Output: payload},
	}}
	if err := e.appendLocked(w, completed, int64(len(events)+1)); err != nil {
		return err
	}
	e.log.Info("signal delivered", "workflow_id", w.id, "step", tail.Name, "sequence", tail.Sequence)
	return e.advanceIfRegisteredLocked(w, st.WorkflowType)
}

// onTimerFired is the timer service's callback: when a timer's deadline passes, the
// engine records TimerFired for the still-waiting timer step and advances. It runs
// on the timer goroutine, so it takes the workflow's lock like any other mutation.
// The status guard makes a stale fire (e.g. a recovery-rearmed timer that raced) a
// harmless no-op.
func (e *Engine) onTimerFired(tm timer.Timer) {
	w := e.lockWorkflow(tm.WorkflowID)
	defer e.unlockWorkflow(w)

	events, st, err := e.historyLocked(w)
	if err != nil {
		e.log.Error("timer fired", "workflow_id", tm.WorkflowID, "err", err)
		if st == nil && events == nil {
			e.rearmCallback(e.timers, tm) // read failed: transient, retry
		}
		return
	}
	tail := st.Tail()
	if st.Status != state.StatusWaiting || tail == nil || tail.Sequence != tm.Sequence || tail.Name != tm.StepName {
		return // already resolved; nothing to do
	}
	fired := &history.HistoryEvent{Attributes: &history.HistoryEvent_TimerFired{
		TimerFired: &history.TimerFiredEvent{StepName: tm.StepName, Sequence: tm.Sequence},
	}}
	if err := e.appendLocked(w, fired, int64(len(events)+1)); err != nil {
		e.log.Error("timer fired: append", "workflow_id", tm.WorkflowID, "err", err)
		e.rearmCallback(e.timers, tm)
		return
	}
	e.metrics.timersFired.Inc()
	e.log.Info("timer fired", "workflow_id", tm.WorkflowID, "step", tm.StepName, "sequence", tm.Sequence)
	e.advanceOrRetry(w, st.WorkflowType)
}

// advanceLocked runs one turn of the decision loop: replay history, Decide, and
// apply the result. Phase-1 workflows are sequential, so each call yields exactly
// one decision — schedule one step (then wait for a worker), or reach a terminal
// state. The caller must hold w.mu.
func (e *Engine) advanceLocked(w *wfState, planner Planner) error {
	events, st, err := e.historyLocked(w)
	if err != nil && events == nil {
		return err
	}
	workflowID := w.id

	// A remote (polyglot) planner can't be consulted inline: deciding means a
	// network round-trip to an SDK worker, and e.mu is held here. Instead we
	// hand the history out as a workflow task and apply the command when it
	// comes back through CompleteWorkflowTask.
	if rp, ok := planner.(*remotePlanner); ok {
		return e.dispatchWorkflowTaskLocked(w, rp.workflowType, events)
	}

	var decision state.Decision
	if sp, ok := planner.(statePlanner); ok && st != nil {
		decision, err = sp.DecideState(st)
	} else {
		decision, err = planner.Decide(events)
	}
	if err != nil {
		return fmt.Errorf("server: decide %s: %w", workflowID, err)
	}
	return e.applyDecisionLocked(w, decision, int64(len(events)+1))
}

// advanceIfRegisteredLocked advances the workflow if its type has a planner on
// this engine. If not, the workflow stays RUNNING with its latest result
// recorded, and registering the type resumes it. The caller must hold w.mu.
func (e *Engine) advanceIfRegisteredLocked(w *wfState, workflowType string) error {
	planner, ok := e.planner(workflowType)
	if !ok {
		e.log.Warn("workflow type not registered; waiting for a worker to register it",
			"workflow_id", w.id, "type", workflowType)
		return nil
	}
	return e.advanceLocked(w, planner)
}

// planner looks up the planner registered for a workflow type.
func (e *Engine) planner(workflowType string) (Planner, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	p, ok := e.planners[workflowType]
	return p, ok
}

// applyDecisionLocked records the event a decision implies and performs its side
// effect (enqueue a task, arm a timer, ...). It is the single place a decision
// becomes durable, shared by in-process planners and remote SDK workers alike —
// so a TypeScript workflow and a Go workflow take exactly the same path through
// the engine. The caller must hold w.mu.
func (e *Engine) applyDecisionLocked(w *wfState, decision state.Decision, nextID int64) error {
	workflowID := w.id
	switch decision.Type {
	case state.DecideScheduleStep:
		scheduled := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
			StepScheduled: &history.StepScheduledEvent{
				StepName: decision.StepName, Sequence: decision.Sequence, Input: decision.Input,
				RetryPolicy: decision.Retry.ToProto(),
			},
		}}
		if err := e.appendLocked(w, scheduled, nextID); err != nil {
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
		if err := e.appendLocked(w, timerStarted, nextID); err != nil {
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
		if err := e.appendLocked(w, awaiting, nextID); err != nil {
			return err
		}
		e.log.Info("awaiting signal", "workflow_id", workflowID, "step", decision.StepName,
			"sequence", decision.Sequence, "signal", decision.Signal)
		return e.tryDeliverSignalLocked(w)

	case state.DecideRecordMarker:
		// A marker completes as it is recorded, so the workflow is RUNNING again
		// and owes another decision straight away.
		marker := &history.HistoryEvent{Attributes: &history.HistoryEvent_MarkerRecorded{
			MarkerRecorded: &history.MarkerRecordedEvent{
				StepName: decision.StepName, Sequence: decision.Sequence,
				Marker: decision.Marker, Data: decision.Output,
			},
		}}
		if err := e.appendLocked(w, marker, nextID); err != nil {
			return err
		}
		e.log.Info("marker recorded", "workflow_id", workflowID, "step", decision.StepName,
			"sequence", decision.Sequence, "marker", decision.Marker)
		_, st, err := e.historyLocked(w)
		if err != nil {
			return err
		}
		return e.advanceIfRegisteredLocked(w, st.WorkflowType)

	case state.DecideCompleteWorkflow:
		done := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowCompleted{
			WorkflowCompleted: &history.WorkflowCompletedEvent{Output: decision.Output},
		}}
		if err := e.appendLocked(w, done, nextID); err != nil {
			return err
		}
		e.metrics.workflowsFinished.With("completed").Inc()
		e.metrics.workflowsActive.Dec()
		e.log.Info("workflow completed", "workflow_id", workflowID)

	case state.DecideFailWorkflow:
		failedEv := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowFailed{
			WorkflowFailed: &history.WorkflowFailedEvent{Error: decision.Error},
		}}
		if err := e.appendLocked(w, failedEv, nextID); err != nil {
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

// appendLocked stamps the event id and wall-clock timestamp, durably appends,
// and folds the event into the cached state. The caller must hold w.mu so the
// workflow's ids stay contiguous. Every terminal event passes through here, so
// this is where a finished workflow's append handle is released.
func (e *Engine) appendLocked(w *wfState, ev *history.HistoryEvent, eventID int64) error {
	workflowID := w.id
	ev.EventId = eventID
	ev.Timestamp = time.Now().UnixNano()
	if err := e.hlog.Append(workflowID, ev); err != nil {
		// The event may or may not have reached the log; read it back next time.
		w.forget()
		return fmt.Errorf("server: append %s: %w", workflowID, err)
	}
	w.recordLocked(ev)
	if isTerminalEvent(ev) {
		// The event is already durable; a failed close only costs a descriptor.
		if err := e.store.Release(workflowID); err != nil {
			e.log.Warn("release history handle", "workflow_id", workflowID, "err", err)
		}
	}
	return nil
}

func isTerminalEvent(ev *history.HistoryEvent) bool {
	switch ev.GetAttributes().(type) {
	case *history.HistoryEvent_WorkflowCompleted, *history.HistoryEvent_WorkflowFailed:
		return true
	}
	return false
}

// callbackRetryDelay is how long a failed timer callback waits before retrying.
// The timer has already been popped from its heap when the callback runs, so a
// transient I/O error (disk full, EIO) would otherwise strand the workflow until
// the next restart — re-arming turns that into a bounded retry loop that is loud
// in the logs and self-heals.
const callbackRetryDelay = 10 * time.Second

// rearmCallback re-arms a popped callback timer after a transient failure.
func (e *Engine) rearmCallback(svc *timer.Service, tm timer.Timer) {
	svc.Add(timer.Timer{
		WorkflowID: tm.WorkflowID, StepName: tm.StepName,
		Sequence: tm.Sequence, FireAt: time.Now().Add(callbackRetryDelay),
	})
}

// advanceOrRetry runs the decision loop; on failure it schedules a retry instead
// of leaving the workflow stranded between "event recorded" and "next step
// dispatched". Advancing is idempotent — Decide derives everything from history —
// so a blind retry is safe. The caller must hold w.mu.
func (e *Engine) advanceOrRetry(w *wfState, workflowType string) {
	if err := e.advanceIfRegisteredLocked(w, workflowType); err != nil {
		e.log.Error("advance failed; scheduling retry", "workflow_id", w.id, "err", err)
		e.advanceRetries.Add(timer.Timer{WorkflowID: w.id, FireAt: time.Now().Add(callbackRetryDelay)})
	}
}

// onAdvanceRetry re-runs a decision loop whose previous advance failed mid-flight.
// Guarded like every callback: only acts when the workflow is still RUNNING and
// its type is registered here.
func (e *Engine) onAdvanceRetry(tm timer.Timer) {
	w := e.lockWorkflow(tm.WorkflowID)
	defer e.unlockWorkflow(w)

	events, st, err := e.historyLocked(w)
	if err != nil && events == nil {
		e.log.Error("advance retry: read history", "workflow_id", tm.WorkflowID, "err", err)
		e.advanceRetries.Add(timer.Timer{WorkflowID: tm.WorkflowID, FireAt: time.Now().Add(callbackRetryDelay)})
		return
	}
	if err != nil {
		// A corrupt history will not heal by retrying; recovery or an operator
		// must intervene. Say so loudly once per attempt-free cycle.
		e.log.Error("advance retry: history unreadable; giving up", "workflow_id", tm.WorkflowID, "err", err)
		return
	}
	if st.Status != state.StatusRunning {
		return // moved on (or terminal); nothing to decide
	}
	planner, ok := e.planner(st.WorkflowType)
	if !ok {
		e.log.Warn("advance retry for unregistered type", "workflow_id", tm.WorkflowID, "type", st.WorkflowType)
		return
	}
	if err := e.advanceLocked(w, planner); err != nil {
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

// validateID turns a malformed workflow id into an invalid-argument error
// before it can name a lock or a file.
func validateID(workflowID string) error {
	if workflowID == "" || workflowID == "." || workflowID == ".." || strings.ContainsAny(workflowID, `/\`) {
		return fmt.Errorf("%w: invalid workflow id %q", ErrInvalidArgument, workflowID)
	}
	return nil
}
