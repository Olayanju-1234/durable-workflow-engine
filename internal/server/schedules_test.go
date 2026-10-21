package server

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

func everySecond(id string) *Schedule {
	return &Schedule{ScheduleId: id, Cron: "@every 1s", WorkflowType: "Parked", Input: []byte("tick")}
}

// runsOf lists the workflows a schedule started.
func runsOf(t *testing.T, e *Engine, scheduleID string) []string {
	t.Helper()
	all, err := e.ListWorkflows(0)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, s := range all {
		if strings.HasPrefix(s.GetWorkflowId(), scheduleID+"-") {
			ids = append(ids, s.GetWorkflowId())
		}
	}
	return ids
}

func waitForRuns(t *testing.T, e *Engine, scheduleID string, n int, d time.Duration) []string {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		runs := runsOf(t, e, scheduleID)
		if len(runs) >= n {
			return runs
		}
		if time.Now().After(deadline) {
			t.Fatalf("schedule %s started %d runs, want %d", scheduleID, len(runs), n)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func reopen(t *testing.T, dir string, opts ...Option) *Engine {
	t.Helper()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e := NewEngine(store, discardLogger(), opts...)
	t.Cleanup(e.Close)
	e.Register(parked)
	return e
}

func TestScheduleFires(t *testing.T) {
	e, _ := newLockingEngine(t)
	if err := e.CreateSchedule(everySecond("tick")); err != nil {
		t.Fatal(err)
	}
	runs := waitForRuns(t, e, "tick", 2, 4*time.Second)
	for _, id := range runs {
		at, err := time.Parse(runIDTimeFormat, strings.TrimPrefix(id, "tick-"))
		if err != nil || at.Nanosecond() != 0 {
			t.Fatalf("run id %q does not name a whole-second fire time: %v", id, err)
		}
	}
	scheds, err := e.ListSchedules()
	if err != nil || len(scheds) != 1 || scheds[0].GetNextFireAt() == 0 {
		t.Fatalf("ListSchedules = %v, %v", scheds, err)
	}
	if got := listedIDs(t, e); strings.Contains(got, scheduleLogID) {
		t.Fatal("the schedule history is listed as a workflow")
	}
	if _, err := e.GetWorkflow(scheduleLogID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetWorkflow(schedule history) = %v, want ErrNotFound", err)
	}
}

// listedIDs joins every listed id, for substring checks.
func listedIDs(t *testing.T, e *Engine) string {
	t.Helper()
	all, err := e.ListWorkflows(0)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, s := range all {
		ids = append(ids, s.GetWorkflowId())
	}
	return strings.Join(ids, ",")
}

// TestScheduleCatchesUpAfterDowntime: a fire that fell while no engine ran is
// started by the next engine's recovery, exactly once, even when the engine
// that missed it had already started an earlier one.
func TestScheduleCatchesUpAfterDowntime(t *testing.T) {
	dir := t.TempDir()
	e1 := reopen(t, dir)
	if err := e1.CreateSchedule(everySecond("job")); err != nil {
		t.Fatal(err)
	}
	waitForRuns(t, e1, "job", 1, 3*time.Second)
	e1.Close()
	time.Sleep(2100 * time.Millisecond) // at least two fires pass with no engine

	e2 := reopen(t, dir)
	before := time.Now()
	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}
	runs := runsOf(t, e2, "job")
	caughtUp := false
	for _, id := range runs {
		if at := fireTime(t, id); !at.Before(before.Truncate(time.Second)) {
			caughtUp = true
		}
	}
	if !caughtUp {
		t.Fatalf("recovery at %s did not start the fire it missed; runs: %v", before, runs)
	}

	// Another restart catches up the same fire: it must not start it again.
	e2.Close()
	e3 := reopen(t, dir)
	if err := e3.Recover(); err != nil {
		t.Fatal(err)
	}
	seen := map[time.Time]string{}
	for _, id := range runsOf(t, e3, "job") {
		at := fireTime(t, id)
		if prev, dup := seen[at]; dup {
			t.Fatalf("fire %s started twice: %s and %s", at, prev, id)
		}
		seen[at] = id
	}
}

// fireTime reads the fire time out of a schedule run's workflow id.
func fireTime(t *testing.T, runID string) time.Time {
	t.Helper()
	_, stamp, _ := strings.Cut(runID, "-")
	at, err := time.Parse(runIDTimeFormat, stamp[:len(runIDTimeFormat)])
	if err != nil {
		t.Fatalf("run id %q: %v", runID, err)
	}
	return at
}

func TestDeleteSchedulePersists(t *testing.T) {
	dir := t.TempDir()
	e1 := reopen(t, dir)
	if err := e1.CreateSchedule(&Schedule{ScheduleId: "gone", Cron: "0 0 1 1 *", WorkflowType: "Parked"}); err != nil {
		t.Fatal(err)
	}
	if err := e1.CreateSchedule(&Schedule{ScheduleId: "kept", Cron: "@daily", WorkflowType: "Parked"}); err != nil {
		t.Fatal(err)
	}
	if err := e1.DeleteSchedule("gone"); err != nil {
		t.Fatal(err)
	}
	if err := e1.DeleteSchedule("gone"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second delete = %v, want ErrNotFound", err)
	}
	e1.Close()
	e2 := reopen(t, dir)
	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}
	scheds, err := e2.ListSchedules()
	if err != nil || len(scheds) != 1 || scheds[0].GetSchedule().GetScheduleId() != "kept" {
		t.Fatalf("after restart: %v, %v; want only kept", scheds, err)
	}
	// An older binary reads the schedule history as an ordinary workflow that
	// never advances, so it can still start on this data directory.
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	events, err := store.ReadAll(scheduleLogID)
	if err != nil {
		t.Fatal(err)
	}
	st, err := state.Reconstruct(events)
	if err != nil || st.Status != state.StatusRunning {
		t.Fatalf("schedule history replays as %v, %v; want a RUNNING workflow", st, err)
	}
}

func TestScheduleWaitsForItsWorkflowType(t *testing.T) {
	e, _ := newLockingEngine(t)
	e.sched.retry = 100 * time.Millisecond
	if err := e.CreateSchedule(&Schedule{ScheduleId: "late", Cron: "@every 1s", WorkflowType: "Remote"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond) // a fire comes and fails: no such type yet
	if runs := runsOf(t, e, "late"); len(runs) != 0 {
		t.Fatalf("started %v before the type existed", runs)
	}
	if err := e.RegisterWorkflowTypes([]string{"Remote"}, "w", "test"); err != nil {
		t.Fatal(err)
	}
	waitForRuns(t, e, "late", 1, 3*time.Second)
}

func TestScheduleSkipIfRunning(t *testing.T) {
	e, _ := newLockingEngine(t)
	s := everySecond("solo")
	s.SkipIfRunning = true
	if err := e.CreateSchedule(s); err != nil {
		t.Fatal(err)
	}
	waitForRuns(t, e, "solo", 1, 3*time.Second)
	time.Sleep(2200 * time.Millisecond) // two more fires while the first run waits forever
	if runs := runsOf(t, e, "solo"); len(runs) != 1 {
		t.Fatalf("started %v; overlapping runs should be skipped", runs)
	}
}

func TestCreateScheduleValidates(t *testing.T) {
	e, _ := newLockingEngine(t)
	for _, s := range []*Schedule{
		{ScheduleId: "", Cron: "@daily", WorkflowType: "Parked"},
		{ScheduleId: "a/b", Cron: "@daily", WorkflowType: "Parked"},
		{ScheduleId: reservedPrefix + "x", Cron: "@daily", WorkflowType: "Parked"},
		{ScheduleId: "x", Cron: "61 * * * *", WorkflowType: "Parked"},
		{ScheduleId: "x", Cron: "@daily", WorkflowType: ""},
		{ScheduleId: "x", Cron: "@daily", Timezone: "Mars/Olympus", WorkflowType: "Parked"},
		{ScheduleId: "x", Cron: "0 0 30 2 *", WorkflowType: "Parked"},
	} {
		if err := e.CreateSchedule(s); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("CreateSchedule(%v) = %v, want ErrInvalidArgument", s, err)
		}
	}
	ok := &Schedule{ScheduleId: "x", Cron: "@daily", Timezone: "Europe/London", WorkflowType: "Parked"}
	for range 2 { // the same schedule twice is one schedule (a retried create)
		if err := e.CreateSchedule(ok); err != nil {
			t.Fatal(err)
		}
	}
	w := e.lockWorkflow(scheduleLogID)
	n := len(w.events)
	e.unlockWorkflow(w)
	if n != 2 {
		t.Fatalf("schedule history has %d events, want 2 (started + one upsert)", n)
	}
}
