package server

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/archive"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// Archiving (ADR-0019). With an archive store, retention exports a finished
// run's history there before it deletes it, and deletes it only once the
// store has it: a run that cannot be archived stays on disk, retried every
// pass, and is never lost. Archived runs are read back by namespace.

// ErrArchiveOff: the engine has no archive store (--archive).
var ErrArchiveOff = errors.New("server: archiving is not configured on this engine (--archive)")

// archiveTimeout bounds one upload.
const archiveTimeout = 2 * time.Minute

// WithArchive exports finished histories to st before retention deletes them.
func WithArchive(st archive.Store) Option {
	return func(e *Engine) { e.archive = st }
}

// Archive is where the engine archives histories (nil: nowhere).
func (e *Engine) Archive() archive.Store { return e.archive }

// archiveRun exports one run's history, kept under fileKey, before it is
// deleted. The caller holds the workflow's lock (for a current run).
func (e *Engine) archiveRun(fileKey, workflowKey, runID string, closedAt int64) error {
	if e.archive == nil {
		return nil
	}
	data, err := e.store.ReadRaw(fileKey)
	if err != nil {
		return err
	}
	if len(data) == 0 {
		return fmt.Errorf("server: archive %s: its history is empty", fileKey)
	}
	run := archive.Run{Namespace: nsOf(workflowKey), WorkflowID: localID(workflowKey), RunID: runID, ClosedAt: closedAt}
	ctx, cancel := context.WithTimeout(context.Background(), archiveTimeout)
	defer cancel()
	if err := e.archive.Put(ctx, run.Key(), data); err != nil {
		e.metrics.archiveFailures.Inc()
		return fmt.Errorf("server: archive %s run %s (kept until it can be archived): %w", workflowKey, runID, err)
	}
	e.metrics.runsArchived.Inc()
	e.log.Info("retention: archived run", "workflow_id", workflowKey, "run_id", runID, "bytes", len(data), "archive", e.archive.String())
	return nil
}

// ArchivedRuns lists the namespace's archived runs whose workflow ids start
// with prefix, newest first per workflow, at most limit (0: all).
func (s *Scope) ArchivedRuns(ctx context.Context, prefix string, limit int) ([]archive.Run, error) {
	if s.e.archive == nil {
		return nil, ErrArchiveOff
	}
	if err := checkNamespace(s.ns); err != nil {
		return nil, err
	}
	if strings.ContainsAny(prefix, `/\`) {
		return nil, fmt.Errorf("%w: invalid prefix %q", ErrInvalidArgument, prefix)
	}
	return s.archivedRuns(ctx, prefix, limit)
}

// archivedRuns lists archived runs whose keys, within the namespace, start
// with keyPrefix.
func (s *Scope) archivedRuns(ctx context.Context, keyPrefix string, limit int) ([]archive.Run, error) {
	keys, err := s.e.archive.List(ctx, s.ns+"/"+keyPrefix)
	if err != nil {
		return nil, err
	}
	var runs []archive.Run
	for _, k := range keys {
		if r, ok := archive.ParseKey(k); ok && r.Namespace == s.ns {
			runs = append(runs, r)
		}
	}
	sort.SliceStable(runs, func(i, j int) bool {
		if runs[i].WorkflowID != runs[j].WorkflowID {
			return runs[i].WorkflowID < runs[j].WorkflowID
		}
		return runs[i].ClosedAt > runs[j].ClosedAt
	})
	if limit > 0 && len(runs) > limit {
		runs = runs[:limit]
	}
	return runs, nil
}

// ArchivedHistory reads an archived run of a workflow: runID's, or the one
// that closed last when runID is empty.
func (s *Scope) ArchivedHistory(ctx context.Context, workflowID, runID string) (archive.Run, []*history.HistoryEvent, error) {
	if err := validateLocalID(workflowID); err != nil {
		return archive.Run{}, nil, fmt.Errorf("%w: %q", ErrNotFound, workflowID)
	}
	if s.e.archive == nil {
		return archive.Run{}, nil, ErrArchiveOff
	}
	runs, err := s.archivedRuns(ctx, workflowID+"/", 0)
	if err != nil {
		return archive.Run{}, nil, err
	}
	var run *archive.Run
	for i := range runs {
		if runs[i].WorkflowID == workflowID && (runID == "" || runs[i].RunID == runID) {
			run = &runs[i]
			break
		}
	}
	if run == nil {
		what := fmt.Sprintf("no archived run of %q", workflowID)
		if runID != "" {
			what = fmt.Sprintf("%q has no archived run %q", workflowID, runID)
		}
		return archive.Run{}, nil, fmt.Errorf("%w: %s", ErrNotFound, what)
	}
	data, err := s.e.archive.Get(ctx, run.Key())
	if errors.Is(err, archive.ErrNotFound) {
		return archive.Run{}, nil, fmt.Errorf("%w: %v", ErrNotFound, err)
	}
	if err != nil {
		return archive.Run{}, nil, err
	}
	events, err := history.DecodeRaw(data)
	if err == nil {
		events, err = s.e.store.OpenAll(events)
	}
	if err != nil {
		return archive.Run{}, nil, fmt.Errorf("server: archived run %s of %s: %w", run.RunID, workflowID, err)
	}
	return *run, events, nil
}

// DescribeArchived describes an archived run like GetWorkflowRun.
func (s *Scope) DescribeArchived(ctx context.Context, workflowID, runID string) (*GetWorkflowResponse, error) {
	_, events, err := s.ArchivedHistory(ctx, workflowID, runID)
	if err != nil {
		return nil, err
	}
	st, err := state.Reconstruct(events)
	if err != nil {
		return nil, fmt.Errorf("server: reconstruct archived %s: %w", workflowID, err)
	}
	var bytes int64
	for _, ev := range events {
		bytes += eventSize(ev)
	}
	return describeRun(workflowID, events, st, bytes)
}

func archiveName(st archive.Store) string {
	if st == nil {
		return ""
	}
	return st.String()
}
