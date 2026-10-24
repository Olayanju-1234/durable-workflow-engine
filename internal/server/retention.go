package server

import (
	"context"
	"fmt"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// PurgeTerminated deletes the history of every finished workflow (completed,
// failed or terminated) whose last event is older than cutoff, in every
// namespace, and returns how many it deleted. Running and waiting workflows
// are never touched, however old. Each workflow is checked and deleted under
// its own lock, so a purge never stalls live traffic.
func (e *Engine) PurgeTerminated(cutoff time.Time) (int, error) {
	return e.purge(func(string) (time.Time, bool) { return cutoff, true })
}

// cutoffFunc gives a namespace's retention cutoff, or false to keep its
// finished workflows forever.
type cutoffFunc func(ns string) (time.Time, bool)

// retentionCutoffs is the cutoff of each namespace at now: its own retention
// if it has one configured, else keep (zero: forever).
func (e *Engine) retentionCutoffs(now time.Time, keep time.Duration) cutoffFunc {
	return func(ns string) (time.Time, bool) {
		d := keep
		if cfg := e.namespaces.Get(ns); cfg != nil && cfg.Retention != nil {
			d = *cfg.Retention
		}
		return now.Add(-d), d > 0
	}
}

func (e *Engine) purge(cutoffOf cutoffFunc) (int, error) {
	due, err := e.indexed(func(s *WorkflowSummary) bool {
		cutoff, ok := cutoffOf(nsOf(s.GetWorkflowId()))
		return ok && isTerminalStatus(s.GetStatus()) && s.GetUpdatedAt() <= cutoff.UnixNano()
	})
	if err != nil {
		return 0, fmt.Errorf("server: purge: %w", err)
	}
	purged, err := e.purgeArchivedRuns(cutoffOf)
	if err != nil {
		return 0, fmt.Errorf("server: purge archived runs: %w", err)
	}
	for _, s := range due {
		id := s.GetWorkflowId()
		cutoff, _ := cutoffOf(nsOf(id))
		ok, err := e.purgeIfExpired(id, cutoff)
		if err != nil {
			// One unreadable log must not block retention for the rest.
			e.log.Warn("retention: skipped workflow", "workflow_id", id, "err", err)
			continue
		}
		if ok {
			purged++
		}
	}
	return purged, nil
}

func (e *Engine) purgeIfExpired(workflowID string, cutoff time.Time) (bool, error) {
	w := e.lockWorkflow(workflowID)
	defer e.unlockWorkflow(w)

	events, st, err := e.historyLocked(w)
	if err != nil {
		return false, err
	}
	if len(events) == 0 {
		return false, nil
	}
	// A current run that continued as new is mid-way to its next run (a crash
	// came in between; recovery finishes it): never purge it.
	if !st.Status.IsTerminal() || st.Status == state.StatusContinuedAsNew ||
		time.Unix(0, events[len(events)-1].GetTimestamp()).After(cutoff) {
		return false, nil
	}
	w.forget()
	if err := e.hlog.Delete(workflowID); err != nil {
		e.indexLocked(w) // the delete may or may not have happened
		return false, err
	}
	e.indexDelete(workflowID)
	e.metrics.workflowsPurged.Inc()
	e.log.Info("retention: purged workflow", "workflow_id", workflowID, "status", st.Status.String())
	return true, nil
}

// RunRetention purges finished workflows older than keep (or than their
// namespace's own retention), once at start and then every interval, until
// ctx ends.
func (e *Engine) RunRetention(ctx context.Context, keep, interval time.Duration) {
	for {
		n, err := e.purge(e.retentionCutoffs(time.Now(), keep))
		if err != nil {
			e.log.Error("retention pass failed", "err", err)
		} else if n > 0 {
			e.log.Info("retention pass complete", "purged", n, "keep", keep)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func isTerminalStatus(s WorkflowStatus) bool {
	switch s {
	case WorkflowStatus_WORKFLOW_STATUS_COMPLETED, WorkflowStatus_WORKFLOW_STATUS_FAILED,
		WorkflowStatus_WORKFLOW_STATUS_TERMINATED, WorkflowStatus_WORKFLOW_STATUS_TIMED_OUT,
		WorkflowStatus_WORKFLOW_STATUS_CANCELLED, WorkflowStatus_WORKFLOW_STATUS_CONTINUED_AS_NEW:
		return true
	}
	return false
}
