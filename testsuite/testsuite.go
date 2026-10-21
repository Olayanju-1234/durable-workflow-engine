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
// Engine-side retry policies are not simulated: a failed activity returns its
// error to the workflow at once.
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

// Now is the virtual time: where the clock stood when the workflow finished.
func (e *Env) Now() time.Time { return e.now }

// Elapsed is how much virtual time the workflow took.
func (e *Env) Elapsed() time.Duration { return e.now.Sub(e.start) }

// ExecuteWorkflow runs fn to completion and returns its output. A workflow that
// fails returns an error matching ErrWorkflowFailed. Other errors mean the test
// setup or the workflow code is broken: an unregistered activity, a signal that
// never comes (ErrBlocked), non-determinism, or a panic.
func (e *Env) ExecuteWorkflow(fn workflow.Func, input []byte) ([]byte, error) {
	if e.executed {
		return nil, errors.New("testsuite: an Env runs one workflow; create another")
	}
	e.executed = true
	e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{WorkflowId: WorkflowID, WorkflowType: "test", Input: input},
	}})

	for range maxDecisions {
		e.deliverSignalsUntil(e.now)
		d, err := replay.Decide(fn, e.events, e.logger)
		if err != nil {
			return nil, fmt.Errorf("testsuite: %w", err)
		}
		switch d.Type {
		case state.DecideScheduleStep:
			if err := e.runActivity(d); err != nil {
				return nil, err
			}
		case state.DecideStartTimer:
			fireAt := e.now.Add(d.Delay)
			e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_TimerStarted{
				TimerStarted: &history.TimerStartedEvent{StepName: d.StepName, Sequence: d.Sequence, FireAt: fireAt.UnixNano(), Input: d.Input},
			}})
			e.deliverSignalsUntil(fireAt)
			e.now = fireAt
			e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_TimerFired{
				TimerFired: &history.TimerFiredEvent{StepName: d.StepName, Sequence: d.Sequence},
			}})
		case state.DecideAwaitSignal:
			e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
				StepScheduled: &history.StepScheduledEvent{StepName: d.StepName, Sequence: d.Sequence, Input: d.Input, AwaitsSignal: true, SignalName: d.Signal},
			}})
			if err := e.awaitSignal(d); err != nil {
				return nil, err
			}
		case state.DecideRecordMarker:
			e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_MarkerRecorded{
				MarkerRecorded: &history.MarkerRecordedEvent{StepName: d.StepName, Sequence: d.Sequence, Marker: d.Marker, Data: d.Output},
			}})
		case state.DecideCompleteWorkflow:
			return d.Output, nil
		case state.DecideFailWorkflow:
			return nil, fmt.Errorf("%w: %s", ErrWorkflowFailed, d.Error)
		default:
			return nil, fmt.Errorf("testsuite: unexpected decision %s", d.Type)
		}
	}
	return nil, fmt.Errorf("testsuite: workflow made %d decisions without finishing", maxDecisions)
}

func (e *Env) runActivity(d state.Decision) error {
	fn, ok := e.activities[d.StepName]
	if !ok {
		return fmt.Errorf("testsuite: activity %q is not registered; call RegisterActivity", d.StepName)
	}
	e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
		StepScheduled: &history.StepScheduledEvent{StepName: d.StepName, Sequence: d.Sequence, Input: d.Input},
	}})
	ctx := context.WithValue(context.Background(), sdkctx.ActivityInfoKey{}, worker.ActivityInfo{
		WorkflowID: WorkflowID, StepName: d.StepName, Sequence: d.Sequence,
	})
	out, err := fn(ctx, d.Input)
	if err != nil {
		e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
			StepFailed: &history.StepFailedEvent{StepName: d.StepName, Sequence: d.Sequence, Error: err.Error(), RetryCount: 1},
		}})
		return nil
	}
	e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
		StepCompleted: &history.StepCompletedEvent{StepName: d.StepName, Sequence: d.Sequence, Output: out},
	}})
	return nil
}

// awaitSignal advances virtual time to the next scheduled signal until one
// satisfies the wait, the way the engine matches buffered signals.
func (e *Env) awaitSignal(d state.Decision) error {
	for {
		st, err := state.Reconstruct(e.events)
		if err != nil {
			return fmt.Errorf("testsuite: %w", err)
		}
		if payload, ok := st.DeliverableSignal(); ok {
			e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
				StepCompleted: &history.StepCompletedEvent{StepName: d.StepName, Sequence: d.Sequence, Output: payload},
			}})
			return nil
		}
		if len(e.signals) == 0 {
			return fmt.Errorf("%w: waiting for signal %q at step %q, and none is scheduled; use SignalAfter",
				ErrBlocked, d.Signal, d.StepName)
		}
		e.now = e.signals[0].at
		e.deliverSignalsUntil(e.now)
	}
}

// deliverSignalsUntil records every scheduled signal due by t, in time order.
func (e *Env) deliverSignalsUntil(t time.Time) {
	for len(e.signals) > 0 && !e.signals[0].at.After(t) {
		sig := e.signals[0]
		e.signals = e.signals[1:]
		if sig.at.After(e.now) {
			e.now = sig.at
		}
		e.append(&history.HistoryEvent{Attributes: &history.HistoryEvent_SignalReceived{
			SignalReceived: &history.SignalReceivedEvent{SignalName: sig.name, Payload: sig.payload},
		}})
	}
}

func (e *Env) append(ev *history.HistoryEvent) {
	ev.EventId = int64(len(e.events) + 1)
	ev.Timestamp = e.now.UnixNano()
	e.events = append(e.events, ev)
}
