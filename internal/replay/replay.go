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
	"math"
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

// Decide replays the function against events and returns the first decision.
func (p Planner) Decide(events []*history.HistoryEvent) (state.Decision, error) {
	return Decide(p.Fn, events, p.Logger)
}

// DecideAll replays the function and returns every decision it made.
func (p Planner) DecideAll(events []*history.HistoryEvent) ([]state.Decision, error) {
	return DecideAll(p.Fn, events, p.Logger)
}

// DecideAllSuggesting is DecideAll that also tells the code where the history
// crossed the engine's warning threshold (ctx.ContinueAsNewSuggested).
func (p Planner) DecideAllSuggesting(events []*history.HistoryEvent, suggestAt int64) ([]state.Decision, error) {
	return DecideAllWith(p.Fn, events, p.Logger, Options{SuggestContinueAsNewAt: suggestAt})
}

// Options are what a replay knows beyond the history itself.
type Options struct {
	// SuggestContinueAsNewAt is the id of the event at which the history crossed
	// the engine's warning threshold (zero: it has not).
	SuggestContinueAsNewAt int64
}

// Decide is DecideAll for callers that take one decision at a time: the first
// command the code issued, or a noop.
func Decide(fn workflow.Func, events []*history.HistoryEvent, logger *slog.Logger) (state.Decision, error) {
	ds, err := DecideAll(fn, events, logger)
	if err != nil || len(ds) == 0 {
		return state.Decision{Type: state.DecideNoop}, err
	}
	return ds[0], nil
}

// DecideAll replays fn against a workflow's history and returns what happens
// next: every new step the code started before it blocked on one without a
// result (several, with the Async calls), then completion or failure if the
// function returned. A single noop means it is waiting on steps in flight.
func DecideAll(fn workflow.Func, events []*history.HistoryEvent, logger *slog.Logger) ([]state.Decision, error) {
	return DecideAllWith(fn, events, logger, Options{})
}

// DecideAllWith is DecideAll with what the engine knows beyond the history.
func DecideAllWith(fn workflow.Func, events []*history.HistoryEvent, logger *slog.Logger, opts Options) ([]state.Decision, error) {
	st, err := state.Reconstruct(events)
	if err != nil {
		return nil, fmt.Errorf("replay: %w", err)
	}
	// Terminal, or never started: there is nothing to run.
	if st.Status.IsTerminal() || st.Status == state.StatusNotStarted {
		return []state.Decision{{Type: state.DecideNoop}}, nil
	}
	c := newContext(st, events, logger)
	c.suggestAt = opts.SuggestContinueAsNewAt
	return c.run(fn)
}

// replayContext walks the workflow's steps in call order. next is the 1-based
// sequence of the latest command issued; it must line up with the steps
// history recorded, which is exactly the determinism check.
type replayContext struct {
	st         *state.State
	info       workflow.Info
	resolvedAt map[int64]int64 // step sequence -> unix nanos of the event that resolved it
	now        int64
	next       int64
	logger     *slog.Logger
	versions   map[string]workflow.Version // changeID -> version, fixed by the first GetVersion call
	pending    []state.Decision            // new steps issued during this replay

	latestResolved int64 // the newest step result in history (event id)
	observed       int64 // the newest step result the code has seen
	suggestAt      int64 // where the history crossed the limits' warning share

	// Steps this replay cancelled that history does not show closed yet, in
	// cancel order: they count as resolved, after every recorded result.
	cancelled map[int64]int64

	// Graceful cancellation (ADR-0014). cancelAt is the request's event id
	// (zero: none). The code is told at the first point it would wait, and the
	// decision that does so records a cancel marker holding the history length
	// it saw (cutoff) at sequence cutoffSeq. A replay shows the code no result
	// past the cutoff until it reaches the marker, so it waits, and is told, at
	// the same point every time.
	cancelAt, cancelTime int64
	cancelReason         string
	cutoff, cutoffSeq    int64
	historyLen           int64
	delivered            bool
	interrupted          map[int64]bool // steps the code was waiting on when told

	suspended bool
	decisions []state.Decision
	err       error
}

func newContext(st *state.State, events []*history.HistoryEvent, logger *slog.Logger) *replayContext {
	c := &replayContext{
		st: st,
		info: workflow.Info{
			WorkflowID: events[0].GetWorkflowStarted().GetWorkflowId(), WorkflowType: st.WorkflowType,
			RunID: st.RunID, ContinuedFromRunID: st.ContinuedFromRunID, TaskQueue: state.QueueName(st.TaskQueue),
		},
		resolvedAt: make(map[int64]int64),
		now:        events[0].GetTimestamp(),
		versions:   make(map[string]workflow.Version),
		cancelled:  make(map[int64]int64),
	}
	for _, ev := range events {
		var seq int64
		switch a := ev.GetAttributes().(type) {
		case *history.HistoryEvent_StepCompleted:
			seq = a.StepCompleted.GetSequence()
		case *history.HistoryEvent_StepFailed:
			if a.StepFailed.GetCancelled() {
				continue // the code cancelled it; it brings no news
			}
			seq = a.StepFailed.GetSequence()
		case *history.HistoryEvent_TimerFired:
			seq = a.TimerFired.GetSequence()
		default:
			// Markers too: the code recorded them itself, so on the first run
			// it never "observed" their time, and a replay must not either.
			continue
		}
		c.resolvedAt[seq] = ev.GetTimestamp() // a retried step keeps its last result's time
	}
	for i := range st.Steps {
		if st.Steps[i].Status != state.StepCancelled {
			c.latestResolved = max(c.latestResolved, st.Steps[i].ResolvedEventID)
		}
		if s := &st.Steps[i]; s.Kind == state.KindMarker && s.Marker == state.MarkerCancel && c.cutoffSeq == 0 {
			c.cutoffSeq = s.Sequence
			c.cutoff, _ = strconv.ParseInt(string(s.Output), 10, 64)
			if c.cutoff <= 0 {
				c.cutoff = -1 // unreadable: deliverCancel reports it
			}
		}
	}
	c.cancelAt, c.cancelTime, c.cancelReason = st.CancelRequestedEventID, st.CancelRequestedAt, st.CancelReason
	c.historyLen = int64(len(events))
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	c.logger = slog.New(&replayHandler{h: logger.Handler(), c: c}).With(
		"workflow_id", c.info.WorkflowID, "workflow_type", c.info.WorkflowType)
	return c
}

// run executes fn on its own goroutine so a suspension can end it with
// runtime.Goexit, which unlike a panic cannot be recovered by workflow code.
func (c *replayContext) run(fn workflow.Func) ([]state.Decision, error) {
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
		return c.decisions, c.err
	case panicked != nil:
		return nil, panicked
	case fnErr != nil && c.delivered && errors.Is(fnErr, workflow.ErrCancelled):
		// The code let the cancellation end it (after compensating, perhaps).
		return append(c.pending, state.Decision{Type: state.DecideFailWorkflow, Error: fnErr.Error(), Cancelled: true}), nil
	case fnErr != nil:
		var next *workflow.ContinueAsNewError
		if errors.As(fnErr, &next) {
			if c.next < int64(len(c.st.Steps)) {
				// The code stopped before steps history recorded: a different path.
				return nil, fmt.Errorf("%w: continued as new after %d steps, but history has %d",
					ErrNondeterminism, c.next, len(c.st.Steps))
			}
			return append(c.pending, state.Decision{
				Type: state.DecideContinueAsNew, Input: next.Input,
				WorkflowType: next.WorkflowType, ExecutionTimeout: max(next.ExecutionTimeout, 0),
				TaskQueue: next.TaskQueue,
			}), nil
		}
		return append(c.pending, state.Decision{Type: state.DecideFailWorkflow, Error: fnErr.Error()}), nil
	default:
		return append(c.pending, state.Decision{Type: state.DecideCompleteWorkflow, Output: out}), nil
	}
}

// suspend stops the workflow goroutine. The first suspension wins: a durable
// call made while the goroutine unwinds (from a deferred function) only exits.
func (c *replayContext) suspend(ds []state.Decision, err error) {
	if !c.suspended {
		c.suspended, c.decisions, c.err = true, ds, err
	}
	runtime.Goexit()
}

// block suspends because the code waits on a step with no result yet: what it
// started so far is the decision, or a noop if it started nothing new.
func (c *replayContext) block() {
	if len(c.pending) == 0 {
		c.suspend([]state.Decision{{Type: state.DecideNoop}}, nil)
	}
	c.suspend(c.pending, nil)
}

func (c *replayContext) nondeterministic(format string, args ...any) {
	c.suspend(nil, fmt.Errorf("%w"+format, append([]any{ErrNondeterminism}, args...)...))
}

func (c *replayContext) live() {
	if c.suspended {
		runtime.Goexit()
	}
}

func (c *replayContext) Input() []byte { return c.st.Input }

func (c *replayContext) CancelRequested() bool {
	c.live()
	return c.delivered
}

func (c *replayContext) ContinueAsNewSuggested() bool {
	c.live()
	return c.suggestAt > 0 && c.observed >= c.suggestAt
}
func (c *replayContext) Info() workflow.Info { return c.info }
func (c *replayContext) Logger() *slog.Logger {
	return c.logger
}

func (c *replayContext) Now() time.Time {
	c.live()
	return time.Unix(0, c.now).UTC() // UTC: workers in other time zones must replay the same value
}

func (c *replayContext) ExecuteActivity(name string, input []byte, opts ...workflow.ActivityOption) ([]byte, error) {
	return c.ExecuteActivityAsync(name, input, opts...).Get()
}

func (c *replayContext) ExecuteActivityAsync(name string, input []byte, opts ...workflow.ActivityOption) workflow.Future {
	var o workflow.ActivityOptions
	for _, opt := range opts {
		opt(&o)
	}
	return c.issue(state.Decision{
		Type: state.DecideScheduleStep, StepName: name, Input: input, Retry: retryPolicy(o.RetryPolicy),
		StartToClose: max(o.StartToCloseTimeout, 0), ScheduleToClose: max(o.ScheduleToCloseTimeout, 0),
		TaskQueue: o.TaskQueue,
	})
}

// retryPolicy is the engine form of the call's policy, nil when unset.
func retryPolicy(p *workflow.RetryPolicy) *state.RetryPolicy {
	if p == nil {
		return nil
	}
	return &state.RetryPolicy{
		MaxAttempts:            p.MaxAttempts,
		Backoff:                p.InitialBackoff,
		MaxBackoff:             p.MaxBackoff,
		NonRetryableErrorTypes: p.NonRetryableErrorTypes,
	}
}

func (c *replayContext) Sleep(name string, d time.Duration) {
	_, _ = c.NewTimer(name, d).Get()
}

func (c *replayContext) NewTimer(name string, d time.Duration) workflow.Future {
	return c.issue(state.Decision{Type: state.DecideStartTimer, StepName: name, Delay: d})
}

func (c *replayContext) WaitForSignal(name, signal string) []byte {
	out, _ := c.WaitForSignalAsync(name, signal).Get()
	return out
}

func (c *replayContext) WaitForSignalAsync(name, signal string) workflow.Future {
	return c.issue(state.Decision{Type: state.DecideAwaitSignal, StepName: name, Signal: signal})
}

// WaitAny returns the index of the future whose step got its result first in
// history. That order is fixed once recorded, so every replay picks the same
// one, whatever has resolved since.
func (c *replayContext) WaitAny(fs ...workflow.Future) int {
	c.live()
	pick := func() int {
		best, bestID := -1, int64(0)
		for i, f := range fs {
			ff := c.own(f, "WaitAny")
			id, ok := c.resolvedID(ff.seq)
			if ok && (best < 0 || id < bestID) {
				best, bestID = i, id
			}
		}
		return best
	}
	best := pick()
	if best < 0 && c.cancelPending() {
		c.deliverCancel() // the futures it waited on now count as resolved
		best = pick()
	}
	if best < 0 {
		c.block()
	}
	c.observe(fs[best].(*future).seq)
	return best
}

// resolvedID orders a step's result for WaitAny: its event id, or, for a step
// this replay cancelled, a place after every recorded result (the cancel will
// be recorded next, ahead of anything that happens later).
func (c *replayContext) resolvedID(seq int64) (int64, bool) {
	if rec := c.visible(seq); rec != nil {
		return rec.ResolvedEventID, true
	}
	if c.interrupted[seq] {
		return c.cancelAt, true
	}
	if n, ok := c.cancelled[seq]; ok {
		return math.MaxInt64/2 + n, true
	}
	return 0, false
}

// visible is the step's recorded result as the code may see it now, or nil.
// Before a replay reaches its cancel marker, results recorded after the
// cutoff stay hidden: the decision that received the cancellation had not
// seen them. Markers the code recorded itself are always there.
func (c *replayContext) visible(seq int64) *state.Step {
	rec := c.st.Step(seq)
	if rec == nil || rec.Open() {
		return nil
	}
	if c.cutoff != 0 && !c.delivered && rec.Kind != state.KindMarker && rec.ResolvedEventID > c.cutoff {
		return nil
	}
	return rec
}

// cancelPending: a graceful cancel was requested and the code not told yet.
func (c *replayContext) cancelPending() bool {
	return c.cancelAt > 0 && !c.delivered
}

// deliverCancel tells the code it is being cancelled, where it would first
// wait: every step it started that has no result it can see is interrupted
// (waiting on it is ErrWorkflowCancelled) and, if still open, cancelled; the
// clock moves to the request. The first decision to do so records a cancel
// marker; a replay checks it reached the same point.
func (c *replayContext) deliverCancel() {
	seq := c.next + 1
	switch {
	case c.cutoffSeq == 0:
		c.pending = append(c.pending, state.Decision{
			Type: state.DecideRecordMarker, StepName: "cancellation", Sequence: seq,
			Marker: state.MarkerCancel, Output: []byte(strconv.FormatInt(c.historyLen, 10)),
		})
	case c.cutoff < 0:
		c.suspend(nil, fmt.Errorf("replay: cancel marker at step %d is unreadable", c.cutoffSeq))
	case seq != c.cutoffSeq:
		c.nondeterministic(": the cancellation reached the code at step %d, but history has it at step %d", seq, c.cutoffSeq)
	}
	c.next = seq
	c.interrupted = make(map[int64]bool)
	for s := int64(1); s < seq; s++ {
		name, kind := c.stepAt(s)
		if kind == state.KindMarker || c.visible(s) != nil {
			continue
		}
		if _, byCode := c.cancelled[s]; byCode {
			continue
		}
		c.interrupted[s] = true
		if rec := c.st.Step(s); rec == nil || rec.Open() {
			c.pending = append(c.pending, state.Decision{
				Type: state.DecideCancelStep, StepName: name, Sequence: s, Error: "the workflow is being cancelled",
			})
		}
	}
	// Only now: which steps were waited on is judged by what the code could see.
	c.delivered = true
	c.now = max(c.now, c.cancelTime)
}

// stepAt names the step at seq, recorded or started in this replay.
func (c *replayContext) stepAt(seq int64) (string, state.StepKind) {
	if rec := c.st.Step(seq); rec != nil {
		return rec.Name, rec.Kind
	}
	for _, d := range c.pending {
		if d.Sequence == seq && d.Type != state.DecideCancelStep {
			return d.StepName, kindOf(d.Type)
		}
	}
	return "", state.KindMarker
}

func (c *replayContext) own(f workflow.Future, op string) *future {
	ff, ok := f.(*future)
	if !ok || ff.c != c {
		panic("workflow: " + op + " takes futures from this workflow's Context")
	}
	return ff
}

func (c *replayContext) CancelStep(f workflow.Future) {
	c.live()
	ff := c.own(f, "CancelStep")
	if _, done := c.resolvedID(ff.seq); done {
		return // it has a result (or is already cancelled): nothing to do
	}
	name, _ := c.stepAt(ff.seq)
	c.cancelled[ff.seq] = int64(len(c.cancelled) + 1)
	if rec := c.st.Step(ff.seq); rec == nil || rec.Open() {
		// Recorded already when a replay hides the step's cancel past its cutoff.
		c.pending = append(c.pending, state.Decision{Type: state.DecideCancelStep, StepName: name, Sequence: ff.seq})
	}
}

func (c *replayContext) GetVersion(changeID string, minSupported, maxSupported workflow.Version) workflow.Version {
	c.live()
	v, ok := c.versions[changeID]
	if !ok {
		v = c.version(changeID, maxSupported)
		c.versions[changeID] = v
	}
	if v < minSupported || v > maxSupported {
		c.nondeterministic(": execution recorded version %d of %q, but the code supports %d..%d",
			v, changeID, minSupported, maxSupported)
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
		c.pending = append(c.pending, state.Decision{
			Type: state.DecideRecordMarker, StepName: changeID, Sequence: seq,
			Marker: state.MarkerVersion, Output: []byte(strconv.Itoa(int(max))),
		})
		return max
	}
	rec := c.st.Steps[seq-1]
	if rec.Kind != state.KindMarker || rec.Marker != state.MarkerVersion || rec.Name != changeID {
		return workflow.DefaultVersion // this execution ran past here before the change
	}
	c.next = seq
	c.observe(seq)
	v, err := strconv.Atoi(string(rec.Output))
	if err != nil {
		c.suspend(nil, fmt.Errorf("replay: version marker %q at step %d holds %q: %w", changeID, seq, rec.Output, err))
	}
	return workflow.Version(v)
}

func (c *replayContext) SideEffect(name string, fn func() []byte) []byte {
	c.live()
	if seq := c.next + 1; int(seq) > len(c.st.Steps) {
		c.next = seq
		data := fn()
		c.pending = append(c.pending, state.Decision{
			Type: state.DecideRecordMarker, StepName: name, Sequence: seq,
			Marker: state.MarkerSideEffect, Output: data,
		})
		return data
	}
	out, _ := c.issue(state.Decision{Type: state.DecideRecordMarker, StepName: name, Marker: state.MarkerSideEffect}).Get()
	return out
}

// issue is the heart of replay. It gives the next durable call its sequence
// and checks it against the step history recorded there (the determinism
// check); a call history has no step for yet becomes a new decision. Either
// way the caller gets a future for the step's result.
func (c *replayContext) issue(d state.Decision) *future {
	c.live()
	c.next++
	seq := c.next
	kind := kindOf(d.Type)
	if int(seq) <= len(c.st.Steps) {
		rec := c.st.Steps[seq-1]
		if rec.Kind != kind || rec.Name != d.StepName || (kind == state.KindMarker && rec.Marker != state.MarkerSideEffect) {
			c.nondeterministic(" at step %d: workflow issued %s %q but history has %s %q",
				seq, kind, d.StepName, rec.Kind, rec.Name)
		}
		return &future{c: c, seq: seq}
	}
	d.Sequence = seq
	c.pending = append(c.pending, d)
	return &future{c: c, seq: seq}
}

func kindOf(t state.DecisionType) state.StepKind {
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

// observe moves the workflow clock to a step's result as the code sees it.
func (c *replayContext) observe(seq int64) {
	c.now = max(c.now, c.resolvedAt[seq])
	if rec := c.st.Step(seq); rec != nil && rec.Status != state.StepCancelled {
		c.observed = max(c.observed, rec.ResolvedEventID)
	}
}

// future is a durable call's eventual result.
type future struct {
	c   *replayContext
	seq int64
}

// Get returns the step's recorded result, or suspends the workflow until it
// has one. A step the engine is still retrying has no result yet.
func (f *future) Get() ([]byte, error) {
	c := f.c
	c.live()
	if c.interrupted[f.seq] {
		return nil, c.cancelError()
	}
	rec := c.visible(f.seq)
	if _, ok := c.cancelled[f.seq]; ok && rec == nil {
		return nil, workflow.ErrCancelled
	}
	if rec == nil && c.cancelPending() {
		c.deliverCancel()
		if c.interrupted[f.seq] {
			return nil, c.cancelError()
		}
	}
	if rec == nil {
		c.block()
	}
	if rec.Status == state.StepCancelled {
		return nil, fmt.Errorf("%w: %s", workflow.ErrCancelled, rec.Error)
	}
	c.observe(f.seq)
	if rec.Status == state.StepFailed {
		if rec.Kind != state.KindActivity {
			return nil, errors.New(rec.Error)
		}
		return nil, &workflow.ActivityError{StepName: rec.Name, Message: rec.Error, Type: rec.ErrorType, NonRetryable: rec.NonRetryable}
	}
	return rec.Output, nil
}

func (c *replayContext) cancelError() error {
	if c.cancelReason == "" {
		return workflow.ErrWorkflowCancelled
	}
	return fmt.Errorf("%w (%s)", workflow.ErrWorkflowCancelled, c.cancelReason)
}

// replaying reports whether the code is still re-running steps history already
// resolved, i.e. whether anything it logs now was logged before: it has not
// yet reached the last recorded step, or not yet seen the newest result.
func (c *replayContext) replaying() bool {
	return c.next < int64(len(c.st.Steps)) || c.observed < c.latestResolved
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
