package server

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// Reset (ADR-0021) starts a finished run again from a point in its history.
// The new run, under the same workflow id, begins with the events before that
// point copied unchanged (ids and times included), so the workflow code
// replays them exactly as before and decides afresh from there: by default,
// from the step whose failure ended the run. Signals recorded after the point
// are recorded again on the new run; the finished run is kept as an earlier
// run, like one continue-as-new or id reuse left behind.

// ErrNotFinished: the operation needs a finished run, and this one is live.
var ErrNotFinished = errors.New("server: workflow run not finished")

// ResetOptions says where to reset and why.
type ResetOptions struct {
	// EventID is the first event of the finished run to leave out; zero
	// picks the default point (DefaultResetPoint).
	EventID int64
	Reason  string
}

// ResetResult is what a reset did.
type ResetResult struct {
	RunID            string
	PreviousRunID    string
	ResetEventID     int64
	ReappliedSignals int
}

// ResetPoint is where a reset with no event id starts a finished run over.
type ResetPoint struct {
	// EventID is the first event left out.
	EventID int64
	// The step it opens, if it opens one: the most recent step that did not
	// succeed (it failed for good, or was still open when the run ended).
	StepName string
	Sequence int64
}

// DefaultResetPoint is where a finished run is reset when no event is named:
// the opening event of the most recent step that failed or was still open
// when the run ended, so that step runs again; with no such step, the run's
// last event, so its code decides again how to end. Never after a graceful
// cancel request, which the new run would otherwise inherit. A live run, or
// one that continued as new, has none.
func DefaultResetPoint(events []*history.HistoryEvent, st *state.State) (ResetPoint, bool) {
	if len(events) < 2 || !st.Status.IsTerminal() || st.Status == state.StatusContinuedAsNew {
		return ResetPoint{}, false
	}
	var pick *state.Step
	for i := range st.Steps {
		if s := &st.Steps[i]; s.Status == state.StepFailed || s.Open() {
			pick = s
		}
	}
	p := ResetPoint{EventID: events[len(events)-1].GetEventId()}
	if pick != nil {
		if id := openingEvent(events, pick.Sequence); id > 0 {
			p = ResetPoint{EventID: id, StepName: pick.Name, Sequence: pick.Sequence}
		}
	}
	if c := st.CancelRequestedEventID; c > 0 && c < p.EventID {
		p = ResetPoint{EventID: c}
	}
	return p, p.EventID >= 2
}

// openingEvent is the id of the event that opened step seq (its first
// attempt), or zero.
func openingEvent(events []*history.HistoryEvent, seq int64) int64 {
	for _, ev := range events {
		switch a := ev.GetAttributes().(type) {
		case *history.HistoryEvent_StepScheduled:
			if a.StepScheduled.GetSequence() == seq {
				return ev.GetEventId()
			}
		case *history.HistoryEvent_TimerStarted:
			if a.TimerStarted.GetSequence() == seq {
				return ev.GetEventId()
			}
		case *history.HistoryEvent_MarkerRecorded:
			if a.MarkerRecorded.GetSequence() == seq {
				return ev.GetEventId()
			}
		}
	}
	return 0
}

// ResetWorkflow starts a finished run of a workflow (by store key) again from
// opts.EventID (see the package comment above). It refuses a live run:
// cancel or terminate it first.
func (e *Engine) ResetWorkflow(workflowID string, opts ResetOptions) (ResetResult, error) {
	if err := validateID(workflowID); err != nil {
		return ResetResult{}, fmt.Errorf("%w: %q", ErrNotFound, workflowID)
	}
	w := e.lockWorkflow(workflowID)
	defer e.unlockWorkflow(w)

	events, st, err := e.historyLocked(w)
	if err != nil {
		return ResetResult{}, err
	}
	if len(events) == 0 {
		return ResetResult{}, fmt.Errorf("%w: %q", ErrNotFound, localID(workflowID))
	}
	if !st.Status.IsTerminal() || st.Status == state.StatusContinuedAsNew {
		return ResetResult{}, fmt.Errorf("%w: %q is %s; only a finished run can be reset (cancel or terminate it first)",
			ErrNotFinished, localID(workflowID), st.Status)
	}
	if len(localID(workflowID)) > maxWorkflowIDBytes {
		return ResetResult{}, fmt.Errorf("%w: workflow id is %d bytes, over the %d an earlier run's name allows",
			ErrInvalidArgument, len(localID(workflowID)), maxWorkflowIDBytes)
	}
	cut := opts.EventID
	if cut == 0 {
		p, ok := DefaultResetPoint(events, st)
		if !ok {
			return ResetResult{}, fmt.Errorf("%w: %q has no point to reset to", ErrInvalidArgument, localID(workflowID))
		}
		cut = p.EventID
	}
	if cut < 2 || cut > int64(len(events)) {
		return ResetResult{}, fmt.Errorf("%w: reset event id must be between 2 and %d (the run's last event), got %d",
			ErrInvalidArgument, len(events), cut)
	}

	now := time.Now()
	run, reapplied, err := resetRun(events, cut, newRunID(), st.RunID, e.clip(opts.Reason), now)
	if err != nil {
		return ResetResult{}, fmt.Errorf("server: reset %s: %w", workflowID, err)
	}
	select {
	case <-e.done:
		return ResetResult{}, fmt.Errorf("server: reset %s: %w", workflowID, ErrEngineClosed)
	default:
	}
	archive := archiveID(w.id, st.RunID, events[len(events)-1].GetTimestamp())
	sealed := make([]*history.HistoryEvent, len(run))
	for i, ev := range run {
		if sealed[i], err = e.store.Seal(ev); err != nil {
			return ResetResult{}, fmt.Errorf("server: reset %s: %w", workflowID, err)
		}
	}
	if err := e.hlog.NewRun(w.id, archive, sealed[0], sealed[1:]...); err != nil {
		w.forget()
		e.indexLocked(w)
		return ResetResult{}, fmt.Errorf("server: reset %s: %w", workflowID, err)
	}
	w.reset(e.limits, run...)
	e.indexLocked(w)
	res := ResetResult{
		RunID: run[0].GetWorkflowStarted().GetRunId(), PreviousRunID: st.RunID,
		ResetEventID: cut, ReappliedSignals: reapplied,
	}
	e.metrics.workflowsReset.Inc()
	e.log.Info("workflow reset", "workflow_id", workflowID, "previous_run_id", res.PreviousRunID, "run_id", res.RunID,
		"reset_event_id", cut, "kept_events", cut-1, "reapplied_signals", reapplied, "reason", opts.Reason)
	// The reset is durable now; a failure from here on is the new run's to retry.
	var c recoveryCounts
	if err := e.resumeRunLocked(w, &c); err != nil {
		return res, err
	}
	return res, nil
}

// resetRun builds a reset run's history from a finished run's: the events
// before cut, unchanged but for the first (a new run id, where it came from,
// and the execution timeout counted again from now), then each signal
// recorded from cut on, recorded again.
func resetRun(events []*history.HistoryEvent, cut int64, runID, fromRunID, reason string, now time.Time) ([]*history.HistoryEvent, int, error) {
	fold := state.NewFolder()
	out := make([]*history.HistoryEvent, 0, cut)
	for _, ev := range events[:cut-1] {
		c := proto.Clone(ev).(*history.HistoryEvent)
		c.RaftIndex = 0
		if ws := c.GetWorkflowStarted(); ws != nil {
			ws.RunId, ws.ResetFromRunId, ws.ResetEventId, ws.ResetReason = runID, fromRunID, cut, reason
			ws.Trace = childTrace(ws.GetTrace())
			if ws.ExecutionTimeoutMs > 0 {
				// The deadline is derived from the start, which replay needs unchanged.
				ws.ExecutionTimeoutMs += now.Sub(time.Unix(0, c.GetTimestamp())).Milliseconds()
			}
		}
		if err := fold.Apply(c); err != nil {
			return nil, 0, err
		}
		out = append(out, c)
	}
	if out[0].GetWorkflowStarted() == nil {
		return nil, 0, fmt.Errorf("history does not begin with WorkflowStarted")
	}
	reapplied := 0
	for _, ev := range events[cut-1:] {
		sig := ev.GetSignalReceived()
		if sig == nil || sig.GetCancelRequest() {
			continue
		}
		c := &history.HistoryEvent{
			EventId: int64(len(out) + 1), Timestamp: now.UnixNano(),
			Attributes: &history.HistoryEvent_SignalReceived{SignalReceived: &history.SignalReceivedEvent{
				SignalName: sig.GetSignalName(), Payload: sig.GetPayload(), RequestId: sig.GetRequestId(),
			}},
		}
		if need := fold.FormatOf(c); need > history.FormatBase {
			c.MinReaderFormat = need
		}
		if err := fold.Apply(c); err != nil {
			return nil, 0, err
		}
		out = append(out, c)
		reapplied++
	}
	return out, reapplied, nil
}

// ResetWorkflow resets a finished run; see Engine.ResetWorkflow.
func (s *Scope) ResetWorkflow(workflowID string, opts ResetOptions) (ResetResult, error) {
	k, err := s.lookupKey(workflowID)
	if err != nil {
		return ResetResult{}, err
	}
	return s.e.ResetWorkflow(k, opts)
}
