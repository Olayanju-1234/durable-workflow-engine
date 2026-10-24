package server

import (
	"errors"
	"fmt"
	"hash/fnv"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/timer"
)

// Schedule controls (ADR-0020): pause and resume, an overlap policy, jitter,
// backfill over a past range, and a preview of the next fires. They are all
// fields of the schedule's spec, recorded by the same upsert event as before,
// so a cluster replicates them and recovery restores them; a spec that uses
// one an older binary would ignore (and so fire wrongly) is stamped format 6.

const (
	// bufferCheck marks a scheduler timer that checks whether a held
	// (BUFFER_ONE) fire can start; its Sequence is that fire's time.
	bufferCheck = "buffer"
	// bufferPoll is how often a held fire looks for the running run's end.
	bufferPoll = 2 * time.Second
	// maxJitter bounds Schedule.jitter_ms.
	maxJitter = 24 * time.Hour
	// maxBackfill and maxPreview bound one backfill or preview.
	maxBackfill = 1000
	maxPreview  = 1000
	maxNote     = 1024
)

// overlapOf is a spec's effective overlap policy: skip_if_running is SKIP,
// and the default is ALLOW_ALL, as before 0.4.0.
func overlapOf(spec *Schedule) OverlapPolicy {
	switch {
	case spec.GetOverlapPolicy() != OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED:
		return spec.GetOverlapPolicy()
	case spec.GetSkipIfRunning():
		return OverlapPolicy_OVERLAP_POLICY_SKIP
	default:
		return OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL
	}
}

// normalizeControls checks a spec's controls and records SKIP as
// skip_if_running too, so a binary that predates overlap policies skips.
func normalizeControls(spec *Schedule) error {
	if spec.GetSkipIfRunning() && spec.GetOverlapPolicy() != OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED &&
		spec.GetOverlapPolicy() != OverlapPolicy_OVERLAP_POLICY_SKIP {
		return fmt.Errorf("skip_if_running contradicts overlap policy %s", spec.GetOverlapPolicy())
	}
	if _, ok := OverlapPolicy_name[int32(spec.GetOverlapPolicy())]; !ok {
		return fmt.Errorf("unknown overlap policy %d", spec.GetOverlapPolicy())
	}
	if spec.GetOverlapPolicy() == OverlapPolicy_OVERLAP_POLICY_SKIP {
		spec.SkipIfRunning = true
	}
	if j := time.Duration(spec.GetJitterMs()) * time.Millisecond; j < 0 || j > maxJitter {
		return fmt.Errorf("jitter must be between 0 and %s", maxJitter)
	}
	if len(spec.GetNote()) > maxNote {
		return fmt.Errorf("the note is over %d bytes", maxNote)
	}
	return nil
}

// scheduleFormat is the history format a spec needs: an older binary would
// fire a paused schedule, ignore jitter, and start a held fire at once.
func scheduleFormat(spec *Schedule) uint32 {
	if spec.GetPaused() || spec.GetJitterMs() > 0 || spec.GetOverlapPolicy() == OverlapPolicy_OVERLAP_POLICY_BUFFER_ONE {
		return history.FormatNamespaces
	}
	return history.FormatBase
}

// jitterOf is how long after its fire time a run starts: an offset in
// [0, jitter) fixed by the schedule and the fire time, so a restart (or the
// preview) picks the same one.
func jitterOf(key string, spec *Schedule, fire time.Time) time.Duration {
	j := spec.GetJitterMs()
	if j <= 0 {
		return 0
	}
	h := fnv.New64a()
	fmt.Fprintf(h, "%s@%d", key, fire.UnixNano())
	return time.Duration(h.Sum64()%uint64(j)) * time.Millisecond
}

// fireRunID is the workflow id a fire starts.
func fireRunID(scheduleID string, fire time.Time) string {
	return fmt.Sprintf("%s-%s", scheduleID, fire.UTC().Format(runIDTimeFormat))
}

// runningRun names a run of the schedule (other than runID) that is still
// running, or "" if none is.
func (e *Engine) runningRun(ns, scheduleID, runID string) string {
	live, err := e.indexed(func(s *WorkflowSummary) bool {
		sns, id := history.SplitKey(s.GetWorkflowId())
		return sns == ns && strings.HasPrefix(id, scheduleID+"-") && id != runID &&
			!isTerminalStatus(s.GetStatus()) && s.GetStatus() != WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED
	})
	if err != nil || len(live) == 0 {
		return ""
	}
	return localID(live[0].GetWorkflowId())
}

func (e *Engine) armBufferLocked(ent *scheduleEntry) {
	e.sched.timers.Add(timer.Timer{WorkflowID: ent.key(), StepName: bufferCheck,
		Sequence: ent.buffered.UnixNano(), FireAt: time.Now().Add(bufferPoll)})
}

// onBufferCheckLocked starts a held fire once no run of its schedule is
// running. The caller holds e.sched.mu.
func (e *Engine) onBufferCheckLocked(tm timer.Timer) {
	ent, ok := e.sched.entries[tm.WorkflowID]
	fire := time.Unix(0, tm.Sequence)
	if !ok || !ent.buffered.Equal(fire) || ent.spec.GetPaused() {
		return // deleted, paused, or already started
	}
	if e.runningRun(ent.ns, ent.spec.GetScheduleId(), fireRunID(ent.spec.GetScheduleId(), fire)) != "" ||
		e.startFireLocked(ent, fire) {
		e.armBufferLocked(ent) // still running, or the start must be retried
		return
	}
	ent.buffered = time.Time{}
}

// PauseSchedule stops a default-namespace schedule firing; see Scope.PauseSchedule.
func (e *Engine) PauseSchedule(scheduleID, note string) error {
	return e.In("").PauseSchedule(scheduleID, note)
}

// ResumeSchedule resumes a paused default-namespace schedule.
func (e *Engine) ResumeSchedule(scheduleID string) error {
	return e.In("").ResumeSchedule(scheduleID)
}

// setPaused records a schedule's paused state (with a note when pausing).
func (e *Engine) setPaused(ns, scheduleID string, paused bool, note string) error {
	e.sched.mu.Lock()
	defer e.sched.mu.Unlock()
	if err := e.loadSchedulesLocked(); err != nil {
		return err
	}
	ent, ok := e.sched.entries[history.Key(ns, scheduleID)]
	if !ok {
		return fmt.Errorf("%w: schedule %q", ErrNotFound, scheduleID)
	}
	spec := proto.Clone(ent.spec).(*Schedule)
	spec.Paused, spec.Note = paused, note
	what := "paused"
	if !paused {
		what, spec.Note = "resumed", ""
	}
	return e.putScheduleLocked(ns, spec, what)
}

// PauseSchedule stops a schedule firing until ResumeSchedule. Fires that come
// due while it is paused are not started later, and a fire held by
// BUFFER_ONE is dropped. note says why (who paused it, until when).
func (s *Scope) PauseSchedule(scheduleID, note string) error {
	if checkNamespace(s.ns) != nil || validateLocalID(scheduleID) != nil {
		return fmt.Errorf("%w: schedule %q", ErrNotFound, scheduleID)
	}
	return s.e.setPaused(s.ns, scheduleID, true, note)
}

// ResumeSchedule makes a paused schedule fire again, from its next fire time.
func (s *Scope) ResumeSchedule(scheduleID string) error {
	if checkNamespace(s.ns) != nil || validateLocalID(scheduleID) != nil {
		return fmt.Errorf("%w: schedule %q", ErrNotFound, scheduleID)
	}
	return s.e.setPaused(s.ns, scheduleID, false, "")
}

// BackfillSchedule backfills a default-namespace schedule; see
// Scope.BackfillSchedule.
func (e *Engine) BackfillSchedule(scheduleID string, from, to time.Time, policy OverlapPolicy) (BackfillResult, error) {
	return e.In("").BackfillSchedule(scheduleID, from, to, policy)
}

// BackfillResult is what a backfill did.
type BackfillResult struct {
	Started, Existing, Skipped int
}

// BackfillSchedule starts the runs a schedule would have started for its fire
// times after from and at or before to, both in the past, under the ids a
// fire uses, so a fire that already ran (or a backfill repeated) is left
// alone. ALLOW_ALL (the default) starts every fire; SKIP starts one only if
// no run of the schedule is running then. It works on a paused schedule too.
func (s *Scope) BackfillSchedule(scheduleID string, from, to time.Time, policy OverlapPolicy) (BackfillResult, error) {
	var res BackfillResult
	if checkNamespace(s.ns) != nil || validateLocalID(scheduleID) != nil {
		return res, fmt.Errorf("%w: schedule %q", ErrNotFound, scheduleID)
	}
	switch policy {
	case OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED, OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL, OverlapPolicy_OVERLAP_POLICY_SKIP:
	default:
		return res, fmt.Errorf("%w: a backfill takes ALLOW_ALL or SKIP, not %s", ErrInvalidArgument, policy)
	}
	if !from.Before(to) || to.After(time.Now()) {
		return res, fmt.Errorf("%w: a backfill needs a start before its end, and an end that is not in the future", ErrInvalidArgument)
	}
	e := s.e
	e.sched.mu.Lock()
	if err := e.loadSchedulesLocked(); err != nil {
		e.sched.mu.Unlock()
		return res, err
	}
	ent, ok := e.sched.entries[history.Key(s.ns, scheduleID)]
	var spec *Schedule
	var fires []time.Time
	if ok {
		spec = proto.Clone(ent.spec).(*Schedule)
		for t := ent.cron.Next(from); !t.IsZero() && !t.After(to); t = ent.cron.Next(t) {
			if fires = append(fires, t); len(fires) > maxBackfill {
				break
			}
		}
	}
	e.sched.mu.Unlock()
	switch {
	case !ok:
		return res, fmt.Errorf("%w: schedule %q", ErrNotFound, scheduleID)
	case len(fires) > maxBackfill:
		return res, fmt.Errorf("%w: the range holds more than %d fires; backfill it in parts", ErrInvalidArgument, maxBackfill)
	}
	for _, fire := range fires {
		runID := fireRunID(scheduleID, fire)
		if policy == OverlapPolicy_OVERLAP_POLICY_SKIP && e.runningRun(s.ns, scheduleID, runID) != "" {
			res.Skipped++
			continue
		}
		_, err := e.StartWorkflow(spec.GetWorkflowType(), spec.GetInput(), runID, inNamespace(s.ns), WithTaskQueue(spec.GetTaskQueue()))
		switch {
		case err == nil:
			res.Started++
		case errors.Is(err, ErrAlreadyExists):
			res.Existing++
		default:
			return res, fmt.Errorf("backfill stopped after starting %d runs: %w", res.Started, err)
		}
	}
	e.log.Info("schedule backfilled", "namespace", s.ns, "schedule_id", scheduleID, "from", from, "to", to,
		"started", res.Started, "existing", res.Existing, "skipped", res.Skipped)
	return res, nil
}

// SchedulePreview is when a schedule fires next.
type SchedulePreview struct {
	Fires, Starts []time.Time // fire times, and when each run starts (jitter)
	Paused        bool
}

// PreviewSchedule says when an existing schedule (spec nil) or a spec not yet
// created fires next, count times (default 10).
func (s *Scope) PreviewSchedule(scheduleID string, spec *Schedule, count int) (SchedulePreview, error) {
	var p SchedulePreview
	if count <= 0 {
		count = 10
	}
	if count > maxPreview {
		return p, fmt.Errorf("%w: preview at most %d fires", ErrInvalidArgument, maxPreview)
	}
	if err := checkNamespace(s.ns); err != nil {
		return p, err
	}
	if spec == nil {
		e := s.e
		e.sched.mu.Lock()
		err := e.loadSchedulesLocked()
		ent, ok := e.sched.entries[history.Key(s.ns, scheduleID)]
		if ok {
			spec = proto.Clone(ent.spec).(*Schedule)
		}
		e.sched.mu.Unlock()
		if err != nil {
			return p, err
		}
		if !ok || validateLocalID(scheduleID) != nil {
			return p, fmt.Errorf("%w: schedule %q", ErrNotFound, scheduleID)
		}
	} else {
		spec = proto.Clone(spec).(*Schedule)
		if spec.GetScheduleId() == "" {
			spec.ScheduleId = "preview"
		}
	}
	ent, err := newScheduleEntry(s.ns, spec, time.Now())
	if err != nil {
		return p, err
	}
	p.Paused = spec.GetPaused()
	for t := ent.cron.Next(time.Now()); !t.IsZero() && len(p.Fires) < count; t = ent.cron.Next(t) {
		p.Fires = append(p.Fires, t)
		p.Starts = append(p.Starts, t.Add(jitterOf(ent.key(), spec, t)))
	}
	return p, nil
}
