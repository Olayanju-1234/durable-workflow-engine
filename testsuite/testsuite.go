// Package testsuite runs a workflow in a unit test, without a server: the
// activities are functions you supply (real or mocked), and time is virtual, so
// a workflow that sleeps for a week finishes instantly.
//
//	env := testsuite.NewEnv()
//	env.RegisterActivity("charge", func(ctx context.Context, in []byte) ([]byte, error) {
//		return nil, errors.New("card declined")
//	})
//	env.SignalAfter(time.Hour, "approve", []byte("yes"))
//	out, err := env.ExecuteWorkflow(Order, []byte("order-7"))
//
// The environment records history exactly as the engine would and replays the
// workflow against it for every decision, using the same replay engine as a
// worker. So a test also catches non-determinism: code that takes a different
// path on replay fails here the way it would in production.
//
// A retry policy set with workflow.WithRetryPolicy is honoured in virtual
// time, and so is a schedule-to-close timeout: no retry is planned that would
// start after it. Activities take no virtual time, so a start-to-close timeout
// never fires here. Engine-side policies (configured on the server by step
// name) are not simulated: without a policy on the call, a failed activity
// returns its error to the workflow at once.
package testsuite

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/internal/sdkctx"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// ErrWorkflowFailed wraps the error a workflow returned (or failed with).
var ErrWorkflowFailed = errors.New("testsuite: workflow failed")

// ErrBlocked means the workflow waits for a signal nobody will send.
var ErrBlocked = errors.New("testsuite: workflow blocked")

// maxDecisions stops a workflow that loops forever.
const maxDecisions = 100_000

// WorkflowID is the id every test execution runs under.
const WorkflowID = "test-workflow"

type scheduledSignal struct {
	at      time.Time
	name    string
	payload []byte
}

// Env is a single-use test environment: configure it, then call
// ExecuteWorkflow once.
type Env struct {
	activities map[string]worker.ActivityFunc
	start      time.Time
	now        time.Time
	signals    []scheduledSignal
	logger     *slog.Logger
	events     []*history.HistoryEvent
	executed   bool
	agenda     []happening
	order      int
	runs       int
	suggestAt  int // a run's history length at which ContinueAsNewSuggested turns true (0: never)
}

// NewEnv returns an environment whose virtual clock starts at
// 2026-01-01T00:00:00Z.
func NewEnv() *Env {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return &Env{
		activities: make(map[string]worker.ActivityFunc),
		start:      start,
		now:        start,
		logger:     slog.New(slog.DiscardHandler),
	}
}

// RegisterActivity supplies the function that runs for a step name.
func (e *Env) RegisterActivity(name string, fn worker.ActivityFunc) {
	e.activities[name] = fn
}

// SetStartTime moves the virtual clock's starting point.
func (e *Env) SetStartTime(t time.Time) {
	e.start, e.now = t, t
}

// SetLogger receives what the workflow logs through ctx.Logger().
func (e *Env) SetLogger(l *slog.Logger) {
	e.logger = l
}

// SignalAfter sends a signal d after the workflow starts, in virtual time.
func (e *Env) SignalAfter(d time.Duration, signal string, payload []byte) {
	e.signals = append(e.signals, scheduledSignal{at: e.start.Add(d), name: signal, payload: payload})
	slices.SortStableFunc(e.signals, func(a, b scheduledSignal) int { return a.at.Compare(b.at) })
}

// SuggestContinueAsNewAfter makes ctx.ContinueAsNewSuggested() true once a
// run's history has this many events, as the engine does near its limits.
func (e *Env) SuggestContinueAsNewAfter(events int) {
	e.suggestAt = events
}

// Runs is how many runs the workflow took: 1, plus one per continue-as-new.
func (e *Env) Runs() int { return e.runs }

// Now is the virtual time: where the clock stood when the workflow finished.
func (e *Env) Now() time.Time { return e.now }

// Elapsed is how much virtual time the workflow took.
func (e *Env) Elapsed() time.Duration { return e.now.Sub(e.start) }

// ExecuteWorkflow runs fn to completion and returns its output. A workflow that
// fails returns an error matching ErrWorkflowFailed. Other errors mean the test
// setup or the workflow code is broken: an unregistered activity, a signal that
// never comes (ErrBlocked), non-determinism, or a panic.
//
// Activities, timers, retries and signals happen in virtual time order, so
// parallel steps (the Async calls) interleave the way they would on a server:
// activities started together run at the same virtual instant, in the order
// they were started.
func (e *Env) ExecuteWorkflow(fn workflow.Func, input []byte) ([]byte, error) {
	if e.executed {
		return nil, errors.New("testsuite: an Env runs one workflow; create another")
	}
	e.executed = true
	e.startRun(input, "")
	for _, sig := range e.signals {
		e.push(happening{at: sig.at, signal: &sig})
	}

	decide := true
	for range maxDecisions {
		if decide {
			ds, err := replay.DecideAllWith(fn, e.events, e.logger, replay.Options{SuggestContinueAsNewAt: e.suggestion()})
			if err != nil {
				return nil, fmt.Errorf("testsuite: %w", err)
			}
			for _, d := range ds {
				switch d.Type {
				case state.DecideScheduleStep:
					if _, ok := e.activities[d.StepName]; !ok {
						return nil, fmt.Errorf("testsuite: activity %q is not registered; call RegisterActivity", d.StepName)
					}
					e.scheduleAttempt(d)
				case state.DecideStartTimer:
					fireAt := e.now.Add(d.Delay)
					e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_TimerStarted{
						TimerStarted: &history.TimerStartedEvent{StepName: d.StepName, Sequence: d.Sequence, FireAt: fireAt.UnixNano(), Input: d.Input},
					}})
					e.push(happening{at: fireAt, timer: &d})
				case state.DecideAwaitSignal:
					e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
						StepScheduled: &history.StepScheduledEvent{StepName: d.StepName, Sequence: d.Sequence, Input: d.Input, AwaitsSignal: true, SignalName: d.Signal},
					}})
				case state.DecideRecordMarker:
					e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_MarkerRecorded{
						MarkerRecorded: &history.MarkerRecordedEvent{StepName: d.StepName, Sequence: d.Sequence, Marker: d.Marker, Data: d.Output},
					}})
				case state.DecideCancelStep:
					if err := e.cancelStep(d); err != nil {
						return nil, err
					}
				case state.DecideCompleteWorkflow:
					return d.Output, nil
				case state.DecideFailWorkflow:
					return nil, fmt.Errorf("%w: %s", ErrWorkflowFailed, d.Error)
				case state.DecideContinueAsNew:
					if d.WorkflowType != "" {
						return nil, fmt.Errorf("testsuite: continue as new to type %q: an Env runs one workflow function", d.WorkflowType)
					}
					st, err := e.state()
					if err != nil {
						return nil, err
					}
					// What the old run planned dies with it; signals still to come
					// go to the new run, as they would on a server.
					e.agenda = slices.DeleteFunc(e.agenda, func(h happening) bool { return h.signal == nil })
					e.events = nil
					e.startRun(d.Input, st.RunID)
				}
			}
			decide = false
			st, err := e.state()
			if err != nil {
				return nil, err
			}
			// A new wait may take a signal sent earlier; with nothing in flight
			// (only markers recorded) the code has more to do at once.
			if e.deliverSignals(st) || (st.Status == state.StatusRunning && !isNoop(ds)) {
				decide = true
				continue
			}
		}

		h, ok := e.pop()
		if !ok {
			return nil, e.blocked()
		}
		if h.at.After(e.now) {
			e.now = h.at
		}
		switch {
		case h.attempt != nil:
			resolved, err := e.runAttempt(*h.attempt)
			if err != nil {
				return nil, err
			}
			decide = resolved
		case h.retry != nil:
			e.scheduleAttempt(*h.retry)
		case h.timer != nil:
			e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_TimerFired{
				TimerFired: &history.TimerFiredEvent{StepName: h.timer.StepName, Sequence: h.timer.Sequence},
			}})
			decide = true
		case h.signal != nil:
			e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_SignalReceived{
				SignalReceived: &history.SignalReceivedEvent{SignalName: h.signal.name, Payload: h.signal.payload},
			}})
			st, err := e.state()
			if err != nil {
				return nil, err
			}
			decide = e.deliverSignals(st)
		}
	}
	return nil, fmt.Errorf("testsuite: workflow made %d decisions without finishing", maxDecisions)
}

// startRun records the WorkflowStarted of the next run.
func (e *Env) startRun(input []byte, continuedFrom string) {
	e.runs++
	e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{
			WorkflowId: WorkflowID, WorkflowType: "test", Input: input,
			RunId: fmt.Sprintf("test-run-%d", e.runs), ContinuedFromRunId: continuedFrom,
		},
	}})
}

// suggestion is the event id at which the run reached SuggestContinueAsNewAfter.
func (e *Env) suggestion() int64 {
	if e.suggestAt <= 0 || len(e.events) < e.suggestAt {
		return 0
	}
	return int64(e.suggestAt)
}

func isNoop(ds []state.Decision) bool {
	return len(ds) == 0 || (len(ds) == 1 && ds[0].Type == state.DecideNoop)
}

// happening is something the environment does at a virtual time: run an
// activity attempt, schedule a retry, fire a timer or deliver a signal.
type happening struct {
	at      time.Time
	order   int // ties break in the order things were planned
	attempt *state.Decision
	retry   *state.Decision
	timer   *state.Decision
	signal  *scheduledSignal
}

func (e *Env) push(h happening) {
	e.order++
	h.order = e.order
	e.agenda = append(e.agenda, h)
}

func (e *Env) pop() (happening, bool) {
	if len(e.agenda) == 0 {
		return happening{}, false
	}
	best := 0
	for i, h := range e.agenda {
		if h.at.Before(e.agenda[best].at) || (h.at.Equal(e.agenda[best].at) && h.order < e.agenda[best].order) {
			best = i
		}
	}
	h := e.agenda[best]
	e.agenda = slices.Delete(e.agenda, best, best+1)
	return h, true
}

// scheduleAttempt records an attempt of an activity and plans to run it now.
func (e *Env) scheduleAttempt(d state.Decision) {
	e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
		StepScheduled: &history.StepScheduledEvent{
			StepName: d.StepName, Sequence: d.Sequence, Input: d.Input, RetryPolicy: d.Retry.ToProto(),
			StartToCloseTimeoutMs: d.StartToClose.Milliseconds(), ScheduleToCloseTimeoutMs: d.ScheduleToClose.Milliseconds(),
		},
	}})
	e.push(happening{at: e.now, attempt: &d})
}

// runAttempt runs one activity attempt and reports whether the step now has a
// result (rather than a retry planned).
func (e *Env) runAttempt(d state.Decision) (bool, error) {
	st, err := e.state()
	if err != nil {
		return false, err
	}
	step := st.Step(d.Sequence)
	attempt := step.Attempts
	ctx := context.WithValue(context.Background(), sdkctx.ActivityInfoKey{}, worker.ActivityInfo{
		WorkflowID: WorkflowID, StepName: d.StepName, Sequence: d.Sequence,
		ActivityID: fmt.Sprintf("%s/%d", WorkflowID, d.Sequence), Attempt: attempt,
	})
	out, err := e.activities[d.StepName](ctx, d.Input)
	if err == nil {
		e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
			StepCompleted: &history.StepCompletedEvent{StepName: d.StepName, Sequence: d.Sequence, Output: out},
		}})
		return true, nil
	}
	policy := state.RetryPolicy{MaxAttempts: 1}
	if d.Retry != nil {
		policy = *d.Retry
	}
	errType, nonRetryable := failureOf(err)
	willRetry := attempt < policy.MaxAttempts && !nonRetryable && policy.Retries(errType)
	if willRetry && step.ScheduleToClose > 0 {
		deadline := time.Unix(0, step.ScheduledAt).Add(step.ScheduleToClose)
		willRetry = e.now.Add(policy.Delay(attempt)).Before(deadline)
	}
	e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
		StepFailed: &history.StepFailedEvent{
			StepName: d.StepName, Sequence: d.Sequence, Error: err.Error(), RetryCount: int32(attempt), WillRetry: willRetry,
			ErrorType: errType, NonRetryable: nonRetryable,
		},
	}})
	if willRetry {
		e.push(happening{at: e.now.Add(policy.Delay(attempt)), retry: &d})
	}
	return !willRetry, nil
}

// cancelStep closes an open step the way the engine does, and forgets what
// was planned for it (an attempt, a retry, a timer).
func (e *Env) cancelStep(d state.Decision) error {
	st, err := e.state()
	if err != nil {
		return err
	}
	step := st.Step(d.Sequence)
	if step == nil || !step.Open() {
		return nil
	}
	e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
		StepFailed: &history.StepFailedEvent{StepName: step.Name, Sequence: step.Sequence, Error: "cancelled", Cancelled: true},
	}})
	e.agenda = slices.DeleteFunc(e.agenda, func(h happening) bool {
		for _, planned := range []*state.Decision{h.attempt, h.retry, h.timer} {
			if planned != nil && planned.Sequence == d.Sequence {
				return true
			}
		}
		return false
	})
	return nil
}

// failureOf reads an activity error the way a worker reports it.
func failureOf(err error) (string, bool) {
	var ae *worker.ActivityError
	if errors.As(err, &ae) {
		return ae.Type, ae.NonRetryable
	}
	return "", false
}

// deliverSignals completes every signal wait a buffered signal satisfies, the
// way the engine matches them, and reports whether it completed any.
func (e *Env) deliverSignals(st *state.State) bool {
	delivered := false
	for {
		wait, payload, ok := st.NextSignalDelivery()
		if !ok {
			return delivered
		}
		e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
			StepCompleted: &history.StepCompletedEvent{StepName: wait.Name, Sequence: wait.Sequence, Output: payload},
		}})
		delivered = true
		var err error
		if st, err = e.state(); err != nil {
			return delivered
		}
	}
}

// blocked explains why the workflow can make no progress.
func (e *Env) blocked() error {
	st, err := e.state()
	if err != nil {
		return err
	}
	for _, step := range st.Steps {
		if step.Kind == state.KindSignal && step.Status == state.StepScheduled {
			return fmt.Errorf("%w: waiting for signal %q at step %q, and none is scheduled; use SignalAfter",
				ErrBlocked, step.Signal, step.Name)
		}
	}
	return fmt.Errorf("%w: nothing left to happen, but the workflow has not finished", ErrBlocked)
}

func (e *Env) state() (*state.State, error) {
	st, err := state.Reconstruct(e.events)
	if err != nil {
		return nil, fmt.Errorf("testsuite: %w", err)
	}
	return st, nil
}

func (e *Env) append(ev *history.HistoryEvent) {
	ev.EventId = int64(len(e.events) + 1)
	ev.Timestamp = e.now.UnixNano()
	e.events = append(e.events, ev)
}
