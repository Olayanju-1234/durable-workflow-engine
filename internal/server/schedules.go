package server

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/cron"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/timer"
)

// Schedules start a workflow each time a cron expression fires.
//
// They are stored in a reserved history that is shaped like a workflow that
// never advances: a WorkflowStarted, then one SignalReceived per change (an
// upsert carrying the Schedule, or a delete). Using only existing event types
// keeps every binary able to read the data directory, and going through the
// engine's HistoryLog replicates schedules in a cluster for free. Only the
// engine fires them, and in a cluster only the leader runs an engine.
//
// A fire starts the workflow under "<schedule id>-<fire time>", so firing is
// idempotent: a fire repeated after a restart or a leader change finds the run
// already there. Recover catches up the latest fire missed within
// scheduleCatchUp while no engine was running.
const (
	scheduleLogID   = reservedPrefix + "-schedules"
	scheduleLogType = reservedPrefix + "/schedules"
	signalUpsert    = "schedule.upsert"
	signalDelete    = "schedule.delete"
	scheduleCatchUp = 24 * time.Hour
	runIDTimeFormat = "20060102T150405Z"
)

type scheduleEntry struct {
	spec      *Schedule
	createdAt time.Time
	cron      cron.Schedule
	next      time.Time // the fire time armed, zero when none
}

type scheduler struct {
	mu      sync.Mutex
	loaded  bool
	entries map[string]*scheduleEntry
	timers  *timer.Service
	retry   time.Duration // wait before retrying a fire that could not start
}

const defaultScheduleRetry = 10 * time.Second

// CreateSchedule records a schedule (replacing one with the same id) and arms
// its next fire. The workflow type need not be registered yet: a fire waits
// for it.
func (e *Engine) CreateSchedule(spec *Schedule) error {
	ent, err := newScheduleEntry(proto.Clone(spec).(*Schedule), time.Now())
	if err != nil {
		return err
	}
	e.sched.mu.Lock()
	defer e.sched.mu.Unlock()
	if err := e.loadSchedulesLocked(); err != nil {
		return err
	}
	if cur, ok := e.sched.entries[spec.GetScheduleId()]; ok && proto.Equal(cur.spec, ent.spec) {
		return nil // the same schedule again: a retried create
	}
	data, err := proto.Marshal(ent.spec)
	if err != nil {
		return fmt.Errorf("server: encode schedule: %w", err)
	}
	at, err := e.appendScheduleChange(signalUpsert, data)
	if err != nil {
		return err
	}
	ent.createdAt = at
	e.sched.entries[ent.spec.GetScheduleId()] = ent
	e.armScheduleLocked(ent, ent.cron.Next(time.Now()))
	e.log.Info("schedule created", "schedule_id", ent.spec.GetScheduleId(), "cron", ent.spec.GetCron(),
		"workflow_type", ent.spec.GetWorkflowType(), "next_fire_at", ent.next)
	return nil
}

// DeleteSchedule stops a schedule. Runs it already started are unaffected.
func (e *Engine) DeleteSchedule(scheduleID string) error {
	e.sched.mu.Lock()
	defer e.sched.mu.Unlock()
	if err := e.loadSchedulesLocked(); err != nil {
		return err
	}
	if _, ok := e.sched.entries[scheduleID]; !ok {
		return fmt.Errorf("%w: schedule %q", ErrNotFound, scheduleID)
	}
	if _, err := e.appendScheduleChange(signalDelete, []byte(scheduleID)); err != nil {
		return err
	}
	delete(e.sched.entries, scheduleID)
	e.log.Info("schedule deleted", "schedule_id", scheduleID)
	return nil
}

// ListSchedules returns every schedule, sorted by id.
func (e *Engine) ListSchedules() ([]*ScheduleInfo, error) {
	e.sched.mu.Lock()
	defer e.sched.mu.Unlock()
	if err := e.loadSchedulesLocked(); err != nil {
		return nil, err
	}
	out := make([]*ScheduleInfo, 0, len(e.sched.entries))
	for _, ent := range e.sched.entries {
		info := &ScheduleInfo{Schedule: proto.Clone(ent.spec).(*Schedule), CreatedAt: ent.createdAt.UnixNano()}
		if !ent.next.IsZero() {
			info.NextFireAt = ent.next.UnixNano()
		}
		out = append(out, info)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].GetSchedule().GetScheduleId() < out[j].GetSchedule().GetScheduleId()
	})
	return out, nil
}

func newScheduleEntry(spec *Schedule, now time.Time) (*scheduleEntry, error) {
	id := spec.GetScheduleId()
	if err := validateID(id); err != nil || strings.ContainsAny(id, " \t\n") {
		return nil, fmt.Errorf("%w: invalid schedule id %q", ErrInvalidArgument, id)
	}
	if spec.GetWorkflowType() == "" {
		return nil, fmt.Errorf("%w: schedule %q needs a workflow type", ErrInvalidArgument, id)
	}
	loc, err := time.LoadLocation(spec.GetTimezone()) // "" is UTC
	if err != nil {
		return nil, fmt.Errorf("%w: schedule %q: time zone: %v", ErrInvalidArgument, id, err)
	}
	c, err := cron.Parse(spec.GetCron(), loc)
	if err != nil {
		return nil, fmt.Errorf("%w: schedule %q: %v", ErrInvalidArgument, id, err)
	}
	if c.Next(now).IsZero() {
		return nil, fmt.Errorf("%w: schedule %q: %q never fires", ErrInvalidArgument, id, spec.GetCron())
	}
	return &scheduleEntry{spec: spec, cron: c}, nil
}

// appendScheduleChange records one change in the schedule history and returns
// its timestamp. The caller holds e.sched.mu.
func (e *Engine) appendScheduleChange(signal string, payload []byte) (time.Time, error) {
	w := e.lockWorkflow(scheduleLogID)
	defer e.unlockWorkflow(w)
	events, _, err := e.historyLocked(w)
	if err != nil {
		return time.Time{}, err
	}
	if len(events) == 0 {
		started := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
			WorkflowStarted: &history.WorkflowStartedEvent{WorkflowId: scheduleLogID, WorkflowType: scheduleLogType},
		}}
		if err := e.appendLocked(w, started, 1); err != nil {
			return time.Time{}, err
		}
	}
	ev := &history.HistoryEvent{Attributes: &history.HistoryEvent_SignalReceived{
		SignalReceived: &history.SignalReceivedEvent{SignalName: signal, Payload: payload},
	}}
	if err := e.appendLocked(w, ev, int64(len(w.events)+1)); err != nil {
		return time.Time{}, err
	}
	return time.Unix(0, ev.GetTimestamp()), nil
}

// loadSchedulesLocked reads the schedule history once. The caller holds
// e.sched.mu.
func (e *Engine) loadSchedulesLocked() error {
	if e.sched.loaded {
		return nil
	}
	w := e.lockWorkflow(scheduleLogID)
	events, _, err := e.historyLocked(w)
	e.unlockWorkflow(w)
	if err != nil {
		return fmt.Errorf("server: load schedules: %w", err)
	}
	entries := make(map[string]*scheduleEntry)
	for _, ev := range events {
		sig := ev.GetSignalReceived()
		switch sig.GetSignalName() {
		case signalUpsert:
			spec := &Schedule{}
			if err := proto.Unmarshal(sig.GetPayload(), spec); err != nil {
				return fmt.Errorf("server: load schedules: event %d: %w", ev.GetEventId(), err)
			}
			ent, err := newScheduleEntry(spec, time.Now())
			if err != nil {
				// A schedule that was valid when created but no longer parses
				// (say, a time zone this machine lacks) must not hide the rest.
				e.log.Error("schedule cannot be loaded; it will not fire", "schedule_id", spec.GetScheduleId(), "err", err)
				delete(entries, spec.GetScheduleId())
				continue
			}
			ent.createdAt = time.Unix(0, ev.GetTimestamp())
			entries[spec.GetScheduleId()] = ent
		case signalDelete:
			delete(entries, string(sig.GetPayload()))
		}
	}
	e.sched.entries, e.sched.loaded = entries, true
	return nil
}

// recoverSchedules runs at startup: it starts the latest run each schedule
// missed while no engine was up (if it is not already there), then arms the
// next fire.
func (e *Engine) recoverSchedules() error {
	e.sched.mu.Lock()
	defer e.sched.mu.Unlock()
	if err := e.loadSchedulesLocked(); err != nil {
		return err
	}
	now := time.Now()
	ids := make([]string, 0, len(e.sched.entries))
	for id := range e.sched.entries {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		ent := e.sched.entries[id]
		from := ent.createdAt
		if cutoff := now.Add(-scheduleCatchUp); from.Before(cutoff) {
			from = cutoff
		}
		var missed time.Time
		for t := ent.cron.Next(from); !t.IsZero() && !t.After(now); t = ent.cron.Next(t) {
			missed = t
		}
		if !missed.IsZero() && e.fireLocked(ent, missed) {
			// Not startable yet (say, its SDK has not registered): keep trying
			// this fire until it starts or the next one is due.
			e.armRetryLocked(ent, missed)
			continue
		}
		e.armScheduleLocked(ent, ent.cron.Next(now))
	}
	if len(ids) > 0 {
		e.log.Info("schedules recovered", "schedules", len(ids))
	}
	return nil
}

// armScheduleLocked sets the entry's next fire. Timer.Sequence carries the
// fire time, so a stale or superseded timer is recognised and ignored.
func (e *Engine) armScheduleLocked(ent *scheduleEntry, at time.Time) {
	ent.next = at
	if at.IsZero() {
		return
	}
	e.sched.timers.Add(timer.Timer{WorkflowID: ent.spec.GetScheduleId(), Sequence: at.UnixNano(), FireAt: at})
}

func (e *Engine) armRetryLocked(ent *scheduleEntry, fire time.Time) {
	ent.next = fire
	e.sched.timers.Add(timer.Timer{WorkflowID: ent.spec.GetScheduleId(), Sequence: fire.UnixNano(), FireAt: time.Now().Add(e.sched.retry)})
}

func (e *Engine) onScheduleDue(tm timer.Timer) {
	e.sched.mu.Lock()
	defer e.sched.mu.Unlock()
	ent, ok := e.sched.entries[tm.WorkflowID]
	fire := time.Unix(0, tm.Sequence)
	if !ok || !ent.next.Equal(fire) {
		return // deleted, replaced, or already handled
	}
	next := ent.cron.Next(fire)
	if e.fireLocked(ent, fire) && (next.IsZero() || time.Now().Before(next)) {
		e.armRetryLocked(ent, fire)
		return
	}
	// Fires missed while this one was retried are skipped, not replayed.
	if now := time.Now(); !next.IsZero() && next.Before(now) {
		next = ent.cron.Next(now)
	}
	e.armScheduleLocked(ent, next)
}

// fireLocked starts the run for one fire time and reports whether it should be
// retried. The caller holds e.sched.mu.
func (e *Engine) fireLocked(ent *scheduleEntry, at time.Time) (retry bool) {
	spec := ent.spec
	runID := fmt.Sprintf("%s-%s", spec.GetScheduleId(), at.UTC().Format(runIDTimeFormat))
	if spec.GetSkipIfRunning() {
		live, err := e.indexed(func(s *WorkflowSummary) bool {
			return strings.HasPrefix(s.GetWorkflowId(), spec.GetScheduleId()+"-") && s.GetWorkflowId() != runID &&
				!isTerminalStatus(s.GetStatus()) && s.GetStatus() != WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED
		})
		if err == nil && len(live) > 0 {
			e.metrics.schedulesSkipped.Inc()
			e.log.Info("schedule fire skipped: an earlier run is still running", "schedule_id", spec.GetScheduleId(),
				"fire_at", at, "running", live[0].GetWorkflowId())
			return false
		}
	}
	_, err := e.StartWorkflow(spec.GetWorkflowType(), spec.GetInput(), runID)
	switch {
	case err == nil:
		e.metrics.schedulesFired.Inc()
		e.log.Info("schedule fired", "schedule_id", spec.GetScheduleId(), "workflow_id", runID, "fire_at", at)
		return false
	case errors.Is(err, ErrAlreadyExists):
		return false // this fire already ran (before a restart or a leader change)
	case errors.Is(err, ErrNotLeader):
		return false // this engine is closing; the next leader's recovery fires it
	default:
		e.log.Warn("schedule fire failed; will retry", "schedule_id", spec.GetScheduleId(),
			"workflow_id", runID, "err", err)
		return true
	}
}
