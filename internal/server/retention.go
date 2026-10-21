package server

import (
	"context"
	"fmt"
	"time"
)

// PurgeTerminated deletes the history of every finished workflow (completed,
// failed or terminated) whose last event is older than cutoff, and returns how
// many it deleted. Running and waiting workflows are never touched, however
// old. Each workflow is checked and deleted under its own lock, so a purge
// never stalls live traffic.
func (e *Engine) PurgeTerminated(cutoff time.Time) (int, error) {
	ids, err := e.store.ListWorkflows()
	if err != nil {
		return 0, fmt.Errorf("server: purge: %w", err)
	}
	purged := 0
	for _, id := range ids {
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
	if !st.Status.IsTerminal() || time.Unix(0, events[len(events)-1].GetTimestamp()).After(cutoff) {
		return false, nil
	}
	w.forget()
	if err := e.hlog.Delete(workflowID); err != nil {
		return false, err
	}
	e.metrics.workflowsPurged.Inc()
	e.log.Info("retention: purged workflow", "workflow_id", workflowID, "status", st.Status.String())
	return true, nil
}

// RunRetention purges finished workflows older than keep, once at start and
// then every interval, until ctx ends.
func (e *Engine) RunRetention(ctx context.Context, keep, interval time.Duration) {
	for {
		n, err := e.PurgeTerminated(time.Now().Add(-keep))
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
