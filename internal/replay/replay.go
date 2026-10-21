// Package replay is the Go replay engine: it runs a workflow.Func against a
// workflow's history and returns the engine's next decision. The engine calls it
// in-process through the Planner seam; the public worker calls it for workflow
// tasks it polls over gRPC. Both paths share this one implementation.
package replay

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// ErrNondeterminism means the workflow code no longer issues the steps its
// history recorded, so replaying it would corrupt the execution.
var ErrNondeterminism = errors.New("replay: non-determinism")

// PanicError is a panic raised by workflow code (not a suspension).
type PanicError struct {
	Value any
	Stack []byte
}

func (e *PanicError) Error() string {
	return fmt.Sprintf("replay: workflow panicked: %v", e.Value)
}

// Planner adapts a workflow function to the engine's Planner interface.
type Planner struct {
	Fn     workflow.Func
	Logger *slog.Logger // for ctx.Logger(); nil discards
}

// Decide replays the function against events and returns the next decision.
func (p Planner) Decide(events []*history.HistoryEvent) (state.Decision, error) {
	return Decide(p.Fn, events, p.Logger)
}

// Decide replays fn against a workflow's history and returns the single next
// decision: schedule the first step history has no record of, wait (noop) while
// a step is in flight, or complete/fail once the function returns.
func Decide(fn workflow.Func, events []*history.HistoryEvent, logger *slog.Logger) (state.Decision, error) {
	st, err := state.Reconstruct(events)
	if err != nil {
		return state.Decision{}, fmt.Errorf("replay: %w", err)
	}
	// Terminal, or waiting on a step in flight: re-running the code would reach
	// the same open step and decide nothing.
	if st.Status != state.StatusRunning {
		return state.Decision{Type: state.DecideNoop}, nil
	}
	return newContext(st, events, logger).run(fn)
}

// replayContext walks the workflow's steps in call order. next is the 1-based
// sequence of the command being issued; it must line up with the steps history
// recorded, which is exactly the determinism check.
type replayContext struct {
	st         *state.State
	info       workflow.Info
	resolvedAt map[int64]int64 // step sequence -> unix nanos of the event that resolved it
	now        int64
	next       int64
	logger     *slog.Logger
	versions   map[string]workflow.Version // changeID -> version, fixed by the first GetVersion call
	retry      *state.RetryPolicy          // the current ExecuteActivity call's policy

	suspended bool
	decision  state.Decision
	err       error
}

func newContext(st *state.State, events []*history.HistoryEvent, logger *slog.Logger) *replayContext {
	c := &replayContext{
		st:         st,
		info:       workflow.Info{WorkflowID: events[0].GetWorkflowStarted().GetWorkflowId(), WorkflowType: st.WorkflowType},
		resolvedAt: make(map[int64]int64),
		now:        events[0].GetTimestamp(),
		versions:   make(map[string]workflow.Version),
	}
	for _, ev := range events {
		var seq int64
		switch a := ev.GetAttributes().(type) {
		case *history.HistoryEvent_StepCompleted:
			seq = a.StepCompleted.GetSequence()
		case *history.HistoryEvent_StepFailed:
			seq = a.StepFailed.GetSequence()
		case *history.HistoryEvent_TimerFired:
			seq = a.TimerFired.GetSequence()
		case *history.HistoryEvent_MarkerRecorded:
			seq = a.MarkerRecorded.GetSequence()
		default:
			continue
		}
		c.resolvedAt[seq] = ev.GetTimestamp() // a retried step keeps its last result's time
	}
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	c.logger = slog.New(&replayHandler{h: logger.Handler(), c: c}).With(
		"workflow_id", c.info.WorkflowID, "workflow_type", c.info.WorkflowType)
	return c
}

// run executes fn on its own goroutine so a suspension can end it with
// runtime.Goexit, which unlike a panic cannot be recovered by workflow code.
func (c *replayContext) run(fn workflow.Func) (state.Decision, error) {
	var (
		out      []byte
		fnErr    error
		panicked *PanicError
	)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				panicked = &PanicError{Value: r, Stack: debug.Stack()}
			}
		}()
		out, fnErr = fn(c, c.st.Input)
	}()
	<-done

	switch {
	case c.suspended:
		return c.decision, c.err
	case panicked != nil:
		return state.Decision{}, panicked
	case fnErr != nil:
		return state.Decision{Type: state.DecideFailWorkflow, Error: fnErr.Error()}, nil
	default:
		return state.Decision{Type: state.DecideCompleteWorkflow, Output: out}, nil
	}
}

// suspend stops the workflow goroutine. The first suspension wins: a durable
// call made while the goroutine unwinds (from a deferred function) only exits.
func (c *replayContext) suspend(d state.Decision, err error) {
	if !c.suspended {
		c.suspended, c.decision, c.err = true, d, err
	}
	runtime.Goexit()
}

func (c *replayContext) live() {
	if c.suspended {
		runtime.Goexit()
	}
}

func (c *replayContext) Input() []byte       { return c.st.Input }
func (c *replayContext) Info() workflow.Info { return c.info }
func (c *replayContext) Logger() *slog.Logger {
	return c.logger
}

func (c *replayContext) Now() time.Time {
	c.live()
	return time.Unix(0, c.now).UTC() // UTC: workers in other time zones must replay the same value
}

func (c *replayContext) ExecuteActivity(name string, input []byte, opts ...workflow.ActivityOption) ([]byte, error) {
	c.retry = retryPolicy(opts)
	return c.command(state.KindActivity, name, "", input, 0)
}

// retryPolicy is the engine form of the call's options, nil when unset.
func retryPolicy(opts []workflow.ActivityOption) *state.RetryPolicy {
	var o workflow.ActivityOptions
	for _, opt := range opts {
		opt(&o)
	}
	if o.RetryPolicy == nil {
		return nil
	}
	return &state.RetryPolicy{
		MaxAttempts: o.RetryPolicy.MaxAttempts,
		Backoff:     o.RetryPolicy.InitialBackoff,
		MaxBackoff:  o.RetryPolicy.MaxBackoff,
	}
}

func (c *replayContext) Sleep(name string, d time.Duration) {
	_, _ = c.command(state.KindTimer, name, "", nil, d)
}

func (c *replayContext) WaitForSignal(name, signal string) []byte {
	out, _ := c.command(state.KindSignal, name, signal, nil, 0)
	return out
}

func (c *replayContext) GetVersion(changeID string, minSupported, maxSupported workflow.Version) workflow.Version {
	c.live()
	v, ok := c.versions[changeID]
	if !ok {
		v = c.version(changeID, maxSupported)
		c.versions[changeID] = v
	}
	if v < minSupported || v > maxSupported {
		c.suspend(state.Decision{}, fmt.Errorf("%w: execution recorded version %d of %q, but the code supports %d..%d",
			ErrNondeterminism, v, changeID, minSupported, maxSupported))
	}
	return v
}

// version reads the version an execution chose for changeID, or records max.
// It consumes a sequence number only when a version marker is (or will be)
// there, so code that predates the GetVersion call still lines up.
func (c *replayContext) version(changeID string, max workflow.Version) workflow.Version {
	seq := c.next + 1
	if int(seq) > len(c.st.Steps) {
		c.next = seq
		c.suspend(state.Decision{
			Type: state.DecideRecordMarker, StepName: changeID, Sequence: seq,
			Marker: state.MarkerVersion, Output: []byte(strconv.Itoa(int(max))),
		}, nil)
	}
	rec := c.st.Steps[seq-1]
	if rec.Kind != state.KindMarker || rec.Marker != state.MarkerVersion || rec.Name != changeID {
		return workflow.DefaultVersion // this execution ran past here before the change
	}
	c.next = seq
	c.now = c.resolvedAt[seq]
	v, err := strconv.Atoi(string(rec.Output))
	if err != nil {
		c.suspend(state.Decision{}, fmt.Errorf("replay: version marker %q at step %d holds %q: %w", changeID, seq, rec.Output, err))
	}
	return workflow.Version(v)
}

func (c *replayContext) SideEffect(name string, fn func() []byte) []byte {
	c.live()
	seq := c.next + 1
	if int(seq) > len(c.st.Steps) {
		c.next = seq
		data := fn()
		c.suspend(state.Decision{
			Type: state.DecideRecordMarker, StepName: name, Sequence: seq,
			Marker: state.MarkerSideEffect, Output: data,
		}, nil)
	}
	out, _ := c.command(state.KindMarker, name, "", nil, 0)
	return out
}

// command is the heart of replay. For the next step in call order it returns
// the recorded result, waits (noop) if the step is still in flight, or suspends
// with the decision that creates the step if history has none yet.
func (c *replayContext) command(kind state.StepKind, name, signal string, input []byte, delay time.Duration) ([]byte, error) {
	c.live()
	c.next++
	seq := c.next

	if int(seq) <= len(c.st.Steps) {
		rec := c.st.Steps[seq-1]
		if rec.Kind != kind || rec.Name != name || (kind == state.KindMarker && rec.Marker != state.MarkerSideEffect) {
			c.suspend(state.Decision{}, fmt.Errorf("%w at step %d: workflow issued %s %q but history has %s %q",
				ErrNondeterminism, seq, kind, name, rec.Kind, rec.Name))
		}
		switch rec.Status {
		case state.StepCompleted:
			c.now = c.resolvedAt[seq]
			return rec.Output, nil
		case state.StepFailed:
			c.now = c.resolvedAt[seq]
			return nil, errors.New(rec.Error)
		default:
			c.suspend(state.Decision{Type: state.DecideNoop}, nil) // in flight: wait
		}
	}

	d := state.Decision{StepName: name, Sequence: seq, Input: input}
	switch kind {
	case state.KindActivity:
		d.Type = state.DecideScheduleStep
		d.Retry = c.retry
	case state.KindTimer:
		d.Type = state.DecideStartTimer
		d.Delay = delay
	case state.KindSignal:
		d.Type = state.DecideAwaitSignal
		d.Signal = signal
	}
	c.suspend(d, nil)
	return nil, nil // unreachable: suspend exits the goroutine
}

// replaying reports whether the code is still re-running steps history already
// resolved, i.e. whether anything it logs now was logged before.
func (c *replayContext) replaying() bool {
	return c.next < int64(len(c.st.Steps))
}

// replayHandler drops log records emitted while replaying.
type replayHandler struct {
	h slog.Handler
	c *replayContext
}

func (r *replayHandler) Enabled(ctx context.Context, l slog.Level) bool {
	return !r.c.replaying() && r.h.Enabled(ctx, l)
}

func (r *replayHandler) Handle(ctx context.Context, rec slog.Record) error {
	return r.h.Handle(ctx, rec)
}

func (r *replayHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &replayHandler{h: r.h.WithAttrs(attrs), c: r.c}
}

func (r *replayHandler) WithGroup(name string) slog.Handler {
	return &replayHandler{h: r.h.WithGroup(name), c: r.c}
}
