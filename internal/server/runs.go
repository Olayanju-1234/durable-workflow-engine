package server

import (
	"fmt"
	"math/rand/v2"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// Runs (ADR-0012). A workflow id has one current run at a time, in
// <id>.history. Continue-as-new ends it (a WorkflowCompleted with
// continued_as_new set) and starts the next with an empty history; the
// finished run is kept under an archive name until retention removes it:
//
//	__workflowd-run~<closed at, hex unix nanos>~<run id>~<workflow id>
//
// The prefix is reserved, so recovery, listing and the index skip archived
// runs, and binaries before 0.3.0 ignore them too. The name alone tells
// retention when the run closed and ListWorkflowRuns which workflow it belongs to.

const archivePrefix = reservedPrefix + "-run~"

// maxWorkflowIDBytes keeps an archived run's file name under the 255 bytes
// file systems allow. Ids of workflows started before 0.3.0 are not checked.
const maxWorkflowIDBytes = 190

func newRunID() string { return fmt.Sprintf("%016x", rand.Uint64()) }

// archiveID is the store key an archived run of a workflow (by store key)
// is kept under, in the workflow's namespace.
func archiveID(workflowKey, runID string, closedAt int64) string {
	ns, id := history.SplitKey(workflowKey)
	return history.Key(ns, fmt.Sprintf("%s%016x~%s~%s", archivePrefix, closedAt, runID, id))
}

// parseArchiveID splits an archived run's store key; workflowKey is the store
// key of the workflow it belongs to, and ok is false for any other key.
func parseArchiveID(key string) (workflowKey, runID string, closedAt int64, ok bool) {
	ns, id := history.SplitKey(key)
	rest, found := strings.CutPrefix(id, archivePrefix)
	if !found {
		return "", "", 0, false
	}
	parts := strings.SplitN(rest, "~", 3)
	if len(parts) != 3 {
		return "", "", 0, false
	}
	at, err := strconv.ParseUint(parts[0], 16, 64)
	if err != nil {
		return "", "", 0, false
	}
	return history.Key(ns, parts[2]), parts[1], int64(at), true
}

// Started is what a start did.
type Started struct {
	WorkflowID string
	RunID      string
	// Existing: the id was taken and IdConflictPolicy USE_EXISTING returned
	// the run that holds it instead of starting one.
	Existing bool
}

// newRunLocked replaces the workflow's finished history with first, keeping
// the finished one as an archived run, and loads the new run. The caller holds
// w.mu, has checked that the current run is finished, and has built first.
func (e *Engine) newRunLocked(w *wfState, first *history.HistoryEvent) error {
	select {
	case <-e.done:
		return fmt.Errorf("server: new run of %s: %w", w.id, ErrEngineClosed)
	default:
	}
	events, st, err := e.historyLocked(w)
	if err != nil {
		return err
	}
	if !st.Status.IsTerminal() {
		return fmt.Errorf("server: new run of %s: its current run %s is still %s", w.id, st.RunID, st.Status)
	}
	archive := archiveID(w.id, st.RunID, events[len(events)-1].GetTimestamp())
	first.EventId, first.Timestamp = 1, time.Now().UnixNano()
	rec, err := e.store.Seal(first)
	if err != nil {
		return fmt.Errorf("server: new run of %s: %w", w.id, err)
	}
	if err := e.hlog.NewRun(w.id, archive, rec); err != nil {
		w.forget()
		e.indexLocked(w)
		return fmt.Errorf("server: new run of %s: %w", w.id, err)
	}
	w.reset(e.limits, first)
	e.indexLocked(w)
	return nil
}

// startRunLocked begins a run whose WorkflowStarted is already recorded:
// arms its execution timeout, counts it and asks for its first decision.
func (e *Engine) startRunLocked(w *wfState) error {
	_, st, err := e.historyLocked(w)
	if err != nil {
		return err
	}
	if st.ExecutionTimeout > 0 {
		e.armExecutionDeadline(w.id, st.RunID, time.Unix(0, st.StartedAt).Add(st.ExecutionTimeout))
	}
	e.metrics.started(nsOf(w.id))
	e.metrics.workflowsActive.Inc()
	e.log.Info("workflow started", "workflow_id", w.id, "run_id", st.RunID, "type", st.WorkflowType,
		"continued_from", st.ContinuedFromRunID)
	return e.advanceIfRegisteredLocked(w, st.WorkflowType)
}

// continueAsNewLocked ends the current run as continued-as-new and starts the
// next. The caller holds w.mu.
func (e *Engine) continueAsNewLocked(w *wfState, d state.Decision) error {
	events, st, err := e.historyLocked(w)
	if err != nil {
		return err
	}
	if len(w.id) > maxWorkflowIDBytes {
		return e.failWorkflowLocked(w, fmt.Sprintf(
			"cannot continue as new: workflow id is %d bytes, over the %d an archived run's name allows", len(w.id), maxWorkflowIDBytes))
	}
	next := &history.ContinuedAsNew{
		RunId: newRunID(), WorkflowType: d.WorkflowType, Input: d.Input,
		ExecutionTimeoutMs: d.ExecutionTimeout.Milliseconds(),
	}
	if next.WorkflowType == "" {
		next.WorkflowType = st.WorkflowType
	}
	if d.ExecutionTimeout <= 0 {
		next.ExecutionTimeoutMs = st.ExecutionTimeout.Milliseconds()
	}
	next.TaskQueue = recordedQueue(d.TaskQueue)
	if d.TaskQueue == "" {
		next.TaskQueue = st.TaskQueue
	}
	ended := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowCompleted{
		WorkflowCompleted: &history.WorkflowCompletedEvent{ContinuedAsNew: next},
	}}
	if err := e.appendLocked(w, ended, int64(len(events)+1)); err != nil {
		return err
	}
	e.metrics.finished(nsOf(w.id), "continued_as_new")
	e.metrics.workflowsActive.Dec()
	e.log.Info("workflow continued as new", "workflow_id", w.id, "run_id", st.RunID, "next_run_id", next.RunId,
		"events", len(events)+1)
	return e.startNextRunLocked(w)
}

// startNextRunLocked starts the run a continued-as-new history names. It is
// also how recovery finishes a continue-as-new a crash interrupted: the
// finished run is still current, and its last event says what comes next.
func (e *Engine) startNextRunLocked(w *wfState) error {
	_, st, err := e.historyLocked(w)
	if err != nil {
		return err
	}
	next := st.ContinuedAsNew
	first := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{
			WorkflowId: localID(w.id), WorkflowType: next.GetWorkflowType(), Input: next.GetInput(),
			ExecutionTimeoutMs: next.GetExecutionTimeoutMs(),
			RunId:              next.GetRunId(), ContinuedFromRunId: st.RunID,
			TaskQueue: next.GetTaskQueue(),
		},
	}}
	if err := e.newRunLocked(w, first); err != nil {
		return err
	}
	return e.startRunLocked(w)
}

// failWorkflowLocked ends the current run as FAILED with msg.
func (e *Engine) failWorkflowLocked(w *wfState, msg string) error {
	return e.applyDecisionsLocked(w, []state.Decision{{Type: state.DecideFailWorkflow, Error: msg}})
}

// archivedRuns lists the archived runs of a workflow id, newest first.
func (e *Engine) archivedRuns(workflowID string) ([]archivedRun, error) {
	ids, err := e.store.ListWorkflows()
	if err != nil {
		return nil, fmt.Errorf("server: list runs of %s: %w", workflowID, err)
	}
	var runs []archivedRun
	for _, id := range ids {
		if wf, run, at, ok := parseArchiveID(id); ok && wf == workflowID {
			runs = append(runs, archivedRun{file: id, runID: run, closedAt: at})
		}
	}
	sort.Slice(runs, func(i, j int) bool { return runs[i].closedAt > runs[j].closedAt })
	return runs, nil
}

type archivedRun struct {
	file     string
	runID    string
	closedAt int64
}

// ListWorkflowRuns returns the runs kept for a workflow id: the current one,
// then earlier ones (continued as new, or finished before the id was reused),
// newest first. Unknown ids return ErrNotFound.
func (e *Engine) ListWorkflowRuns(workflowID string) ([]*RunInfo, error) {
	if err := validateID(workflowID); err != nil {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, workflowID)
	}
	var out []*RunInfo
	w := e.lockWorkflow(workflowID)
	events, st, err := e.historyLocked(w)
	if err == nil && len(events) > 0 {
		out = append(out, runInfo(events, st, true))
	}
	e.unlockWorkflow(w)
	if err != nil {
		return nil, err
	}
	runs, err := e.archivedRuns(workflowID)
	if err != nil {
		return nil, err
	}
	for _, r := range runs {
		evs, err := e.store.ReadAll(r.file)
		if err != nil || len(evs) == 0 {
			continue // purged since it was listed
		}
		rst, err := state.Reconstruct(evs)
		if err != nil {
			out = append(out, &RunInfo{RunId: r.runID, ClosedAt: r.closedAt})
			continue
		}
		out = append(out, runInfo(evs, rst, false))
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("%w: %q", ErrNotFound, workflowID)
	}
	return out, nil
}

func runInfo(events []*history.HistoryEvent, st *state.State, current bool) *RunInfo {
	ri := &RunInfo{RunId: st.RunID, Status: protoStatus(st), StartedAt: st.StartedAt, Current: current}
	if st.Status.IsTerminal() {
		ri.ClosedAt = events[len(events)-1].GetTimestamp()
	}
	return ri
}

// archivedRunHistory reads an earlier run of a workflow id.
func (e *Engine) archivedRunHistory(workflowID, runID string) ([]*history.HistoryEvent, error) {
	runs, err := e.archivedRuns(workflowID)
	if err != nil {
		return nil, err
	}
	for _, r := range runs {
		if r.runID == runID {
			evs, err := e.store.ReadAll(r.file)
			if err != nil {
				return nil, fmt.Errorf("server: read run %s of %s: %w", runID, workflowID, err)
			}
			if len(evs) > 0 {
				return evs, nil
			}
		}
	}
	return nil, fmt.Errorf("%w: %q has no run %q", ErrNotFound, workflowID, runID)
}

// purgeArchivedRuns deletes archived runs that closed before their
// namespace's cutoff.
func (e *Engine) purgeArchivedRuns(cutoffOf cutoffFunc) (int, error) {
	ids, err := e.store.ListWorkflows()
	if err != nil {
		return 0, err
	}
	purged := 0
	for _, id := range ids {
		wf, run, closedAt, ok := parseArchiveID(id)
		if !ok {
			continue
		}
		if cutoff, keep := cutoffOf(nsOf(id)); !keep || closedAt > cutoff.UnixNano() {
			continue
		}
		if err := e.archiveRun(id, wf, run, closedAt); err != nil {
			e.log.Warn("retention: kept archived run", "workflow_id", wf, "run_id", run, "err", err)
			continue
		}
		if err := e.hlog.Delete(id); err != nil {
			e.log.Warn("retention: skipped archived run", "workflow_id", wf, "run_id", run, "err", err)
			continue
		}
		purged++
		e.metrics.workflowsPurged.Inc()
		e.log.Info("retention: purged archived run", "workflow_id", wf, "run_id", run)
	}
	return purged, nil
}
