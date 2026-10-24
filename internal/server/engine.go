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
	Planner
	DecideState(st *state.State) (state.Decision, error)
}

// batchPlanner returns every command the workflow code issued before it
// blocked, so several steps can be in flight at once (replay.Planner).
type batchPlanner interface {
	DecideAll(events []*history.HistoryEvent) ([]state.Decision, error)
}

// suggestingPlanner is a batchPlanner that is told where the history crossed
// the limits' warning share, so the code can continue as new in time.
type suggestingPlanner interface {
	DecideAllSuggesting(events []*history.HistoryEvent, suggestAt int64) ([]state.Decision, error)
}

// HistoryLog is where the engine records history mutations. Single-node mode
// writes straight to the HistoryStore; a cluster replicates each mutation
// through Raft and returns once it is applied locally. Reads always go to the
// store, which in both modes holds every recorded event.
type HistoryLog interface {
	Append(workflowID string, ev *history.HistoryEvent) error
	Delete(workflowID string) error
	// NewRun keeps a finished history under archiveID and starts the
	// workflow's next run with first (see HistoryStore.NewRun).
	NewRun(workflowID, archiveID string, first *history.HistoryEvent) error
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
	deadlines      *timer.Service // workflow execution and activity schedule-to-close timeouts
	log            *slog.Logger
	registry       *metrics.Registry
	metrics        *engineMetrics

	leaseDuration   time.Duration // immutable after NewEngine
	decisionTimeout time.Duration // immutable after NewEngine
	cacheSize       int           // immutable after NewEngine
	limits          Limits        // immutable after NewEngine

	wfMu sync.Mutex
	wfs  map[string]*wfState // workflows in use or cached
	idle *list.List          // cached workflows nobody holds, most recent first

	idx   visibilityIndex
	sched scheduler

	closeOnce sync.Once
	done      chan struct{} // closed by Close; ends long polls

	mu               sync.Mutex
	planners         map[string]Planner       // workflow type -> planner
	policies         map[string]RetryPolicy   // activity step name -> retry policy
	leases           map[leaseKey]*lease      // in-flight step -> its task lease
	leaseTokens      map[string]*lease        // lease token -> lease, for heartbeats
	tombstones       map[string]string        // lease token -> why it was taken away (cancellation)
	tombstoneOrder   []string                 // oldest first, to bound tombstones
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
		limits: Limits{
			MaxHistoryEvents: DefaultMaxHistoryEvents, MaxHistoryBytes: DefaultMaxHistoryBytes,
			MaxPayloadBytes: DefaultMaxPayloadBytes,
		},
		workflowQueue: newWorkflowTaskQueue(),
		wfs:           make(map[string]*wfState),
		idle:          list.New(),

		planners:         make(map[string]Planner),
		policies:         make(map[string]RetryPolicy),
		leases:           make(map[leaseKey]*lease),
		leaseTokens:      make(map[string]*lease),
		tombstones:       make(map[string]string),
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
	e.deadlines = timer.New(e.onDeadline)
	e.deadlines.Start()
	e.sched.retry = defaultScheduleRetry
	e.sched.timers = timer.New(e.onScheduleDue)
	e.sched.timers.Start()
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
		e.deadlines.Stop()
		e.sched.timers.Stop()
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

// StartOption customises StartWorkflow.
type StartOption func(*startOptions)

type startOptions struct {
	executionTimeout time.Duration
}

// WithExecutionTimeout ends the workflow as TIMED_OUT if it has not finished d
// after it starts. The limit is recorded with the start, so it holds across
// restarts and failovers.
func WithExecutionTimeout(d time.Duration) StartOption {
	return func(o *startOptions) { o.executionTimeout = d }
}

// StartWorkflow records WorkflowStarted for a new execution and schedules its
// first step, returning the workflow id. A blank workflowID is auto-generated;
// a supplied one must not already exist.
func (e *Engine) StartWorkflow(workflowType string, input []byte, workflowID string, opts ...StartOption) (string, error) {
	s, err := e.Start(workflowType, input, workflowID, opts...)
	return s.WorkflowID, err
}

// Start is StartWorkflow that also says which run it started.
func (e *Engine) Start(workflowType string, input []byte, workflowID string, opts ...StartOption) (Started, error) {
	var so startOptions
	for _, o := range opts {
		o(&so)
	}
	if so.executionTimeout < 0 {
		return Started{}, fmt.Errorf("%w: execution timeout must not be negative", ErrInvalidArgument)
	}
	if _, ok := e.planner(workflowType); !ok {
		return Started{}, fmt.Errorf("%w: %q is not registered on this node", ErrUnknownType, workflowType)
	}
	if err := e.checkPayload("workflow input", input); err != nil {
		return Started{}, err
	}
	if workflowID == "" {
		// Timestamp alone can collide at coarse clock resolution or under rapid
		// calls; the random suffix makes a generated id effectively unique.
		workflowID = fmt.Sprintf("%s-%d-%08x", workflowType, time.Now().UnixNano(), rand.Uint32())
	}
	if err := validateID(workflowID); err != nil {
		return Started{}, err
	}
	if len(workflowID) > maxWorkflowIDBytes {
		return Started{}, fmt.Errorf("%w: workflow id is %d bytes, over the %d-byte limit", ErrInvalidArgument, len(workflowID), maxWorkflowIDBytes)
	}
	w := e.lockWorkflow(workflowID)
	defer e.unlockWorkflow(w)
	existing, _, err := e.historyLocked(w)
	if len(existing) > 0 {
		return Started{}, fmt.Errorf("%w: %q already has history", ErrAlreadyExists, workflowID)
	}
	if err != nil {
		return Started{}, err
	}

	runID := newRunID()
	started := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{
			WorkflowId: workflowID, WorkflowType: workflowType, Input: input,
			ExecutionTimeoutMs: so.executionTimeout.Milliseconds(), RunId: runID,
		},
	}}
	if err := e.appendLocked(w, started, 1); err != nil {
		return Started{}, err
	}
	if err := e.startRunLocked(w); err != nil {
		return Started{}, err
	}
	return Started{WorkflowID: workflowID, RunID: runID}, nil
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
	if err := e.recoverWorkflows(); err != nil {
		return err
	}
	// After the index is built: a schedule that skips overlapping runs reads it.
	return e.recoverSchedules()
}

func (e *Engine) recoverWorkflows() (err error) {
	// The pass over every history also builds the visibility index.
	if first, _ := e.startIndex(); first {
		defer func() { e.finishIndex(err) }()
	}
	ids, err := e.store.ListWorkflows()
	if err != nil {
		return fmt.Errorf("server: list workflows for recovery: %w", err)
	}

	var c recoveryCounts
	format := history.FormatBase
	for _, id := range ids {
		if isReservedID(id) {
			continue
		}
		w := e.lockWorkflow(id)
		err := e.recoverLocked(w, &c)
		if w.loaded && w.fold != nil {
			format = max(format, w.fold.Format())
		}
		e.indexLocked(w)
		e.unlockWorkflow(w)
		if err != nil {
			return err
		}
	}
	// Directories written before formats were recorded learn theirs here.
	if err := e.store.RequireFormat(format); err != nil {
		return fmt.Errorf("server: record history format: %w", err)
	}
	e.log.Info("recovery complete", "workflows", len(ids), "history_format", e.store.Format(),
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
	if st.Status == state.StatusContinuedAsNew {
		// A crash came between ending this run and starting the next.
		e.log.Info("recovery: finishing continue-as-new", "workflow_id", id, "run_id", st.RunID)
		c.advanced++
		return e.startNextRunLocked(w)
	}
	if st.Status.IsTerminal() || st.Status == state.StatusNotStarted {
		return nil
	}
	// Recovered live workflows re-enter the active gauge (it is in-memory and
	// died with the old process); advancing below may immediately retire one.
	e.metrics.workflowsActive.Inc()
	if st.ExecutionTimeout > 0 {
		// A deadline that passed while we were down fires at once.
		e.armExecutionDeadline(id, st.RunID, time.Unix(0, st.StartedAt).Add(st.ExecutionTimeout))
	}

	// Resuming in-flight steps needs no planner, only deciding what comes next
	// does. SDK workers register after recovery, so a remote workflow's
	// activities, timers and signals must not wait for them. Each step's kind
	// is reconstructed from history, so no definition is needed either.
	steps := append([]state.Step(nil), st.Steps...)
	for i := range steps {
		step := &steps[i]
		if step.Kind == state.KindActivity && step.Open() {
			e.armStepDeadline(id, st.RunID, step)
		}
		switch {
		case step.Retrying:
			// The backoff timer was in memory: retry promptly, since the
			// original window is unknown.
			e.scheduleRetryLocked(id, st.RunID, step, 0)
			c.rearmed++
		case step.Status != state.StepScheduled:
		case step.Kind == state.KindTimer:
			// Re-arm against the durable deadline. If it already passed while we
			// were down, the timer service fires it immediately.
			e.timers.Add(timer.Timer{
				WorkflowID: id, RunID: st.RunID, StepName: step.Name,
				Sequence: step.Sequence, FireAt: time.Unix(0, step.FireAt),
			})
			c.rearmed++
			e.log.Info("recovery: re-armed timer", "workflow_id", id,
				"step", step.Name, "sequence", step.Sequence, "fire_at", time.Unix(0, step.FireAt))
		case step.Kind == state.KindActivity:
			e.queue.enqueue(activityTask(id, step))
			c.requeued++
			e.log.Info("recovery: re-enqueued in-flight step", "workflow_id", id,
				"step", step.Name, "sequence", step.Sequence)
		}
	}

	// A failure recorded before will_retry existed: re-arm the retry if the
	// policy allows one, rather than let the workflow see it.
	if tail := st.Tail(); st.Status == state.StatusRunning && tail != nil && tail.Status == state.StepFailed &&
		!tail.Retrying && tail.Kind == state.KindActivity {
		if _, retry := e.retryAfter(tail, time.Now()); retry {
			e.scheduleRetryLocked(id, st.RunID, tail, 0)
			c.rearmed++
			return nil
		}
	}

	// A signal may have been buffered before the crash but not yet delivered.
	delivered, err := e.deliverSignalsLocked(w)
	if err != nil {
		return fmt.Errorf("server: recover deliver signal %s: %w", id, err)
	}
	if !delivered && !st.OwesDecision() {
		return nil // waiting on steps in flight; their results will advance it
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
	return nil
}

// TaskFailure is a failed activity attempt as a worker reports it.
type TaskFailure struct {
	Message string
	// Type names the kind of failure, matched against the step's retry
	// policy's NonRetryableErrorTypes.
	Type string
	// NonRetryable fails the step now, whatever the policy.
	NonRetryable bool
}

// FailTask records a failed attempt of an activity in flight: retried if the
// policy allows it, else the step's result. See CompleteTask.
func (e *Engine) FailTask(workflowID, stepName string, sequence int64, leaseToken string, f TaskFailure) error {
	return e.completeTask(workflowID, stepName, sequence, leaseToken, nil, &f)
}

// CompleteTask records a worker's result for an activity in flight and advances
// the workflow. failed selects StepFailed (error) over StepCompleted (output).
// It rejects a result that does not match an activity in flight, which makes a
// duplicate or stale CompleteTask a clean error rather than a corrupt log.
// leaseToken must be the current lease on the task (from the dispatched Task):
// a worker whose lease expired — its task reassigned — is turned away here, so
// two workers can never both record a result for the same attempt.
func (e *Engine) CompleteTask(workflowID, stepName string, sequence int64, leaseToken string, output []byte, stepErr string, failed bool) error {
	if failed {
		return e.FailTask(workflowID, stepName, sequence, leaseToken, TaskFailure{Message: stepErr})
	}
	return e.completeTask(workflowID, stepName, sequence, leaseToken, output, nil)
}

func (e *Engine) completeTask(workflowID, stepName string, sequence int64, leaseToken string, output []byte, failure *TaskFailure) error {
	if err := validateID(workflowID); err != nil {
		return err
	}
	w := e.lockWorkflow(workflowID)
	defer e.unlockWorkflow(w)

	e.mu.Lock()
	le, err := e.checkLeaseLocked(workflowID, sequence, leaseToken)
	var claimedAt time.Time
	if err == nil {
		claimedAt = le.claimedAt
	}
	reason, tombstoned := e.tombstones[leaseToken]
	e.mu.Unlock()
	if tombstoned {
		return fmt.Errorf("%w: %s", ErrActivityCancelled, reason)
	}
	if err != nil {
		if _, st, herr := e.historyLocked(w); herr == nil && st.Status.IsTerminal() {
			return fmt.Errorf("%w: workflow %s (%w)", ErrActivityCancelled, st.Status, err)
		}
		return err
	}

	events, st, err := e.historyLocked(w)
	if err != nil {
		return err
	}
	step := st.Step(sequence)
	if st.Status.IsTerminal() || step == nil || step.Kind != state.KindActivity ||
		step.Status != state.StepScheduled || step.Name != stepName {
		return fmt.Errorf("server: result %s/seq%d does not match an activity in flight in workflow %q (status %s)",
			stepName, sequence, workflowID, st.Status)
	}
	attempt := *step
	workflowType, runID := st.WorkflowType, st.RunID
	if failure == nil {
		if err := e.checkPayload("activity output", output); err != nil {
			// Unrecordable as a result, so the step fails, for good: the same
			// output would come back from every retry.
			failure = &TaskFailure{Message: err.Error(), Type: ErrorTypePayloadTooLarge, NonRetryable: true}
		}
	}
	if failure != nil {
		failure.Message = e.clip(failure.Message)
		// Retry transparently if attempts remain: the workflow sees the error
		// only once the policy is exhausted, so the failure says which it is.
		attempt.ErrorType, attempt.NonRetryable = failure.Type, failure.NonRetryable
		delay, willRetry := e.retryAfter(&attempt, time.Now())
		stepErr := failure.Message
		failedEv := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
			StepFailed: &history.StepFailedEvent{
				StepName: stepName, Sequence: sequence, Error: stepErr,
				RetryCount: int32(attempt.Attempts), WillRetry: willRetry,
				ErrorType: failure.Type, NonRetryable: failure.NonRetryable,
			},
		}}
		if err := e.appendLocked(w, failedEv, int64(len(events)+1)); err != nil {
			return err
		}
		// The attempt is over; its lease is spent (a retry gets a fresh one).
		e.dropLease(workflowID, sequence)
		e.metrics.taskResults.With("failed").Inc()
		e.metrics.stepDuration.Observe(time.Since(claimedAt).Seconds())
		e.log.Warn("step failed", "workflow_id", workflowID, "step", stepName, "sequence", sequence,
			"error", stepErr, "error_type", failure.Type, "non_retryable", failure.NonRetryable,
			"attempt", attempt.Attempts, "will_retry", willRetry)
		if willRetry {
			e.scheduleRetryLocked(workflowID, runID, &attempt, delay)
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
	e.dropLease(workflowID, sequence)
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
	return e.SignalWorkflowOnce(workflowID, signalName, "", payload)
}

// SignalWorkflowOnce is SignalWorkflow with an idempotency key. The key is
// recorded with the signal, so a retry with the same key (after a lost reply,
// a restart or a leader change) succeeds without recording it again, even if
// the workflow has finished since. An empty key means no deduplication.
func (e *Engine) SignalWorkflowOnce(workflowID, signalName, requestID string, payload []byte) error {
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
	if name, seen := st.SignalRequests[requestID]; requestID != "" && seen {
		if name != signalName {
			return fmt.Errorf("%w: request id %q was already used for signal %q", ErrInvalidArgument, requestID, name)
		}
		e.metrics.signalsDeduplicated.Inc()
		e.log.Info("signal already recorded; not recording it again", "workflow_id", workflowID,
			"signal", signalName, "request_id", requestID)
		return nil
	}
	if st.Status.IsTerminal() {
		return fmt.Errorf("%w: %q is %s; cannot signal a terminal workflow", ErrAlreadyTerminal, workflowID, st.Status)
	}
	if err := e.checkPayload("signal payload", payload); err != nil {
		return err
	}

	sig := &history.HistoryEvent{Attributes: &history.HistoryEvent_SignalReceived{
		SignalReceived: &history.SignalReceivedEvent{SignalName: signalName, Payload: payload, RequestId: requestID},
	}}
	if err := e.appendLocked(w, sig, int64(len(events)+1)); err != nil {
		return err
	}
	e.metrics.signalsReceived.Inc()
	e.log.Info("signal received", "workflow_id", workflowID, "signal", signalName)
	return e.tryDeliverSignalLocked(w)
}

// tryDeliverSignalLocked delivers every buffered signal a waiting step can take,
// then advances if any was delivered. It is a no-op when no wait matches. The
// caller must hold w.mu. Called both when a signal arrives and when a wait is
// first reached (in case the signal was buffered earlier).
func (e *Engine) tryDeliverSignalLocked(w *wfState) error {
	delivered, err := e.deliverSignalsLocked(w)
	if err != nil || !delivered {
		return err
	}
	_, st, err := e.historyLocked(w)
	if err != nil {
		return err
	}
	return e.advanceIfRegisteredLocked(w, st.WorkflowType)
}

// deliverSignalsLocked completes each signal wait a buffered signal satisfies,
// without advancing. The caller must hold w.mu.
func (e *Engine) deliverSignalsLocked(w *wfState) (bool, error) {
	delivered := false
	for {
		events, st, err := e.historyLocked(w)
		if err != nil {
			return delivered, err
		}
		if st.Status.IsTerminal() {
			return delivered, nil
		}
		wait, payload, ok := st.NextSignalDelivery()
		if !ok {
			return delivered, nil
		}
		name, seq := wait.Name, wait.Sequence
		completed := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
			StepCompleted: &history.StepCompletedEvent{StepName: name, Sequence: seq, Output: payload},
		}}
		if err := e.appendLocked(w, completed, int64(len(events)+1)); err != nil {
			return delivered, err
		}
		delivered = true
		e.log.Info("signal delivered", "workflow_id", w.id, "step", name, "sequence", seq)
	}
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
		if events == nil {
			e.rearmCallback(e.timers, tm) // read failed: transient, retry
		}
		return
	}
	step := st.Step(tm.Sequence)
	if staleRun(tm, st) || st.Status.IsTerminal() || step == nil || step.Kind != state.KindTimer ||
		step.Status != state.StepScheduled || step.Name != tm.StepName {
		return // already resolved; nothing to do
	}
	workflowType := st.WorkflowType
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
	e.advanceOrRetry(w, workflowType)
}

// advanceLocked runs one turn of the decision loop: ask the planner what the
// workflow does next and apply it. It runs after anything that could unblock the
// workflow code (a start, a step result), even with other steps still in flight,
// since parallel code may continue as soon as one of them resolves. The caller
// must hold w.mu.
func (e *Engine) advanceLocked(w *wfState, planner Planner) error {
	events, st, err := e.historyLocked(w)
	if err != nil && events == nil {
		return err
	}
	if st != nil && st.Status.IsTerminal() {
		return nil
	}
	workflowID := w.id

	// A remote (polyglot) planner can't be consulted inline: deciding means a
	// network round-trip to an SDK worker, and the workflow's lock is held here.
	// Instead we hand the history out as a workflow task and apply the commands
	// when they come back through CompleteWorkflowTask.
	if rp, ok := planner.(*remotePlanner); ok {
		return e.dispatchWorkflowTaskLocked(w, rp.workflowType, events)
	}

	var decisions []state.Decision
	switch p := planner.(type) {
	case suggestingPlanner:
		decisions, err = p.DecideAllSuggesting(events, w.suggestAt)
	case batchPlanner:
		decisions, err = p.DecideAll(events)
	case statePlanner:
		if st == nil {
			_, err = p.Decide(events) // reports why the history is invalid
			break
		}
		var d state.Decision
		d, err = p.DecideState(st)
		decisions = []state.Decision{d}
	default:
		var d state.Decision
		d, err = planner.Decide(events)
		decisions = []state.Decision{d}
	}
	if err != nil {
		return fmt.Errorf("server: decide %s: %w", workflowID, err)
	}
	return e.applyDecisionsLocked(w, decisions)
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

// applyDecisionsLocked records the events a batch of decisions implies, in
// order, and performs their side effects (enqueue a task, arm a timer, ...). It
// is the single place decisions become durable, shared by in-process planners
// and remote SDK workers alike, so a TypeScript workflow and a Go workflow take
// exactly the same path through the engine. The caller must hold w.mu.
func (e *Engine) applyDecisionsLocked(w *wfState, decisions []state.Decision) error {
	workflowID := w.id
	recorded := false
	for _, decision := range decisions {
		nextID := int64(len(w.events) + 1)
		if err := e.checkDecisionPayloads(decision); err != nil {
			// The code will make the same decision on every replay: end the run.
			decision = state.Decision{Type: state.DecideFailWorkflow, Error: err.Error()}
		}
		switch decision.Type {
		case state.DecideScheduleStep:
			scheduled := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
				StepScheduled: &history.StepScheduledEvent{
					StepName: decision.StepName, Sequence: decision.Sequence, Input: decision.Input,
					RetryPolicy:              decision.Retry.ToProto(),
					StartToCloseTimeoutMs:    decision.StartToClose.Milliseconds(),
					ScheduleToCloseTimeoutMs: decision.ScheduleToClose.Milliseconds(),
				},
			}}
			if err := e.appendLocked(w, scheduled, nextID); err != nil {
				return err
			}
			step := &state.Step{
				Name: decision.StepName, Sequence: decision.Sequence, Input: decision.Input, Attempts: 1,
				StartToClose: decision.StartToClose, ScheduleToClose: decision.ScheduleToClose,
				ScheduledAt: scheduled.GetTimestamp(),
			}
			e.armStepDeadline(workflowID, w.runID(), step)
			// Record-then-enqueue: the step is durable before any worker can see it,
			// so a crash can never dispatch a task the history doesn't know about.
			e.queue.enqueue(activityTask(workflowID, step))
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
				WorkflowID: workflowID, RunID: w.runID(), StepName: decision.StepName,
				Sequence: decision.Sequence, FireAt: fireAt,
			})
			e.log.Info("timer started", "workflow_id", workflowID, "step", decision.StepName,
				"sequence", decision.Sequence, "fire_at", fireAt)

		case state.DecideAwaitSignal:
			// A signal wait is a step with no worker task: record it as scheduled (so the
			// workflow is WAITING) but dispatch nothing. It completes when SignalWorkflow
			// delivers a matching signal, which may already be buffered (see below).
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

		case state.DecideRecordMarker:
			// A marker completes as it is recorded.
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
			return nil

		case state.DecideContinueAsNew:
			return e.continueAsNewLocked(w, decision)

		case state.DecideFailWorkflow:
			failedEv := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowFailed{
				WorkflowFailed: &history.WorkflowFailedEvent{Error: e.clip(decision.Error)},
			}}
			if err := e.appendLocked(w, failedEv, nextID); err != nil {
				return err
			}
			e.metrics.workflowsFinished.With("failed").Inc()
			e.metrics.workflowsActive.Dec()
			e.log.Warn("workflow failed", "workflow_id", workflowID, "error", decision.Error)
			return nil

		case state.DecideCancelStep:
			if !e.cancelStepLocked(w, decision.Sequence, decision.Error) {
				continue // it already has a result: nothing to cancel
			}

		case state.DecideNoop:
			// Every step the code waits on is in flight.
			continue
		}
		recorded = true
	}
	if !recorded {
		return nil
	}
	// A new wait may match a signal that arrived earlier.
	delivered, err := e.deliverSignalsLocked(w)
	if err != nil {
		return err
	}
	_, st, err := e.historyLocked(w)
	if err != nil {
		return err
	}
	if delivered || st.Status == state.StatusRunning {
		// A wait was satisfied, or nothing is in flight (only markers were
		// recorded): the workflow code has more to do.
		return e.advanceIfRegisteredLocked(w, st.WorkflowType)
	}
	return nil
}

// appendLocked stamps the event id and wall-clock timestamp, durably appends,
// and folds the event into the cached state. The caller must hold w.mu so the
// workflow's ids stay contiguous. Every terminal event passes through here, so
// this is where a finished workflow's append handle is released.
func (e *Engine) appendLocked(w *wfState, ev *history.HistoryEvent, eventID int64) error {
	workflowID := w.id
	select {
	case <-e.done:
		// A closed engine (shut down, or a deposed leader's) must not write:
		// another engine may already own this history.
		return fmt.Errorf("server: append %s: %w", workflowID, ErrEngineClosed)
	default:
	}
	ev.EventId = eventID
	ev.Timestamp = time.Now().UnixNano()
	if !isTerminalEvent(ev) && w.loaded {
		if why := e.overLimit(w, eventSize(ev)); why != "" {
			return e.failAtLimitLocked(w, why)
		}
	}
	if w.fold != nil {
		// Stamped so a binary that predates what the event uses refuses it.
		if need := w.fold.FormatOf(ev); need > history.FormatBase {
			ev.MinReaderFormat = need
		}
	}
	if err := e.hlog.Append(workflowID, ev); err != nil {
		// The event may or may not have reached the log; read it back next time.
		w.forget()
		e.indexLocked(w)
		return fmt.Errorf("server: append %s: %w", workflowID, err)
	}
	warnedBefore := w.suggestAt != 0
	w.recordLocked(ev, e.limits)
	e.indexLocked(w)
	if !warnedBefore && w.suggestAt != 0 {
		e.metrics.historyLimit.With("warned").Inc()
		e.log.Warn("history is nearing its limit; the workflow should continue as new", "workflow_id", workflowID,
			"events", len(w.events), "bytes", w.bytes, "max_events", e.limits.MaxHistoryEvents, "max_bytes", e.limits.MaxHistoryBytes)
	}
	if isTerminalEvent(ev) {
		// The event is already durable; a failed close only costs a descriptor.
		if err := e.store.Release(workflowID); err != nil {
			e.log.Warn("release history handle", "workflow_id", workflowID, "err", err)
		}
		// Steps still in flight are abandoned: their workers are told to stop.
		e.dropLeases(workflowID, "workflow "+terminalStatus(ev))
		e.queue.dropWhere(func(t *Task) bool { return t.GetWorkflowId() == workflowID })
		e.dropDecision(workflowID)
		dropWorkflow(e.workflowQueue, workflowID)
	}
	return nil
}

// terminalStatus names how a terminal event ends its workflow.
func terminalStatus(ev *history.HistoryEvent) string {
	f := ev.GetWorkflowFailed()
	switch {
	case ev.GetWorkflowCompleted().GetContinuedAsNew() != nil:
		return "continued as new"
	case ev.GetWorkflowCompleted() != nil:
		return "completed"
	case f.GetCancelled():
		return "cancelled"
	case f.GetTimedOut():
		return "timed out"
	case f.GetTerminated():
		return "terminated"
	default:
		return "failed"
	}
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
		WorkflowID: tm.WorkflowID, RunID: tm.RunID, StepName: tm.StepName,
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
	// HistoryFormat is the format recorded for the data directory (0: none yet).
	HistoryFormat uint32
}

// Stats returns the current gauge values. Cheap: atomic, channel-length and
// in-memory reads.
func (e *Engine) Stats() EngineStats {
	return EngineStats{
		ActiveWorkflows:        e.metrics.workflowsActive.Value(),
		TaskQueueDepth:         e.queue.depth(),
		WorkflowTaskQueueDepth: e.workflowQueue.depth(),
		HistoryFormat:          e.store.Format(),
	}
}

// validateID turns a malformed workflow id into an invalid-argument error
// before it can name a lock or a file.
func validateID(workflowID string) error {
	if workflowID == "" || workflowID == "." || workflowID == ".." || strings.ContainsAny(workflowID, `/\`) {
		return fmt.Errorf("%w: invalid workflow id %q", ErrInvalidArgument, workflowID)
	}
	if isReservedID(workflowID) {
		return fmt.Errorf("%w: ids starting with %q are reserved", ErrInvalidArgument, reservedPrefix)
	}
	return nil
}
