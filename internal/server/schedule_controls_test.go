package server

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

// A paused schedule does not fire, catches nothing up, and stays paused
// across a restart; resumed, it fires again.
func TestPauseAndResume(t *testing.T) {
	dir := t.TempDir()
	e := reopen(t, dir)
	if err := e.CreateSchedule(everySecond("p")); err != nil {
		t.Fatal(err)
	}
	waitForRuns(t, e, "p", 1, 3*time.Second)
	if err := e.PauseSchedule("p", "maintenance until 18:00"); err != nil {
		t.Fatal(err)
	}
	before := len(runsOf(t, e, "p"))
	time.Sleep(2200 * time.Millisecond)
	if got := len(runsOf(t, e, "p")); got != before {
		t.Fatalf("a paused schedule started %d runs", got-before)
	}
	list, _ := e.ListSchedules()
	if !list[0].GetSchedule().GetPaused() || list[0].GetSchedule().GetNote() != "maintenance until 18:00" || list[0].GetNextFireAt() != 0 {
		t.Fatalf("listed as %v", list[0])
	}
	e.Close()

	e = reopen(t, dir)
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	if got := len(runsOf(t, e, "p")); got != before {
		t.Fatalf("a paused schedule fired after a restart: %d runs, want %d", got, before)
	}
	if err := e.ResumeSchedule("p"); err != nil {
		t.Fatal(err)
	}
	waitForRuns(t, e, "p", before+1, 3*time.Second)
	if list, _ := e.ListSchedules(); list[0].GetSchedule().GetPaused() || list[0].GetSchedule().GetNote() != "" {
		t.Fatalf("resumed schedule listed as %v", list[0])
	}
	if err := e.PauseSchedule("missing", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pause a missing schedule: %v", err)
	}
}

// BUFFER_ONE holds one fire while a run is going and starts it when that run
// finishes; the fires after it are skipped.
func TestOverlapBufferOne(t *testing.T) {
	e := reopen(t, t.TempDir())
	s := everySecond("b")
	s.OverlapPolicy = OverlapPolicy_OVERLAP_POLICY_BUFFER_ONE
	if err := e.CreateSchedule(s); err != nil {
		t.Fatal(err)
	}
	first := waitForRuns(t, e, "b", 1, 3*time.Second)
	time.Sleep(2500 * time.Millisecond) // two or more fires while the first run parks
	if runs := runsOf(t, e, "b"); len(runs) != 1 {
		t.Fatalf("runs %v while the first was going; want it alone", runs)
	}
	list, _ := e.ListSchedules()
	held := list[0].GetBufferedFireAt()
	if held == 0 {
		t.Fatal("no fire was held")
	}
	if err := e.TerminateWorkflow(first[0], "done"); err != nil {
		t.Fatal(err)
	}
	runs := waitForRuns(t, e, "b", 2, 5*time.Second)
	want := fireRunID("b", time.Unix(0, held))
	if !slices.Contains(runs, want) {
		t.Fatalf("runs %v; the held fire %s should have started", runs, want)
	}
}

// ALLOW_ALL starts every fire, overlapping; SKIP is stored as skip_if_running
// too, so a binary that predates overlap policies skips as well.
func TestOverlapAllowAllAndSkip(t *testing.T) {
	e := reopen(t, t.TempDir())
	all := everySecond("all")
	all.OverlapPolicy = OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL
	if err := e.CreateSchedule(all); err != nil {
		t.Fatal(err)
	}
	waitForRuns(t, e, "all", 2, 4*time.Second)
	skip := everySecond("skip")
	skip.OverlapPolicy = OverlapPolicy_OVERLAP_POLICY_SKIP
	if err := e.CreateSchedule(skip); err != nil {
		t.Fatal(err)
	}
	list, _ := e.ListSchedules()
	for _, s := range list {
		if s.GetSchedule().GetScheduleId() == "skip" && !s.GetSchedule().GetSkipIfRunning() {
			t.Fatal("SKIP was not recorded as skip_if_running")
		}
	}
	bad := everySecond("bad")
	bad.SkipIfRunning, bad.OverlapPolicy = true, OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL
	if err := e.CreateSchedule(bad); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("contradictory overlap settings: %v", err)
	}
}

// Jitter delays each run by an offset fixed per fire, inside the jitter,
// and the preview shows the same offsets.
func TestJitter(t *testing.T) {
	e := reopen(t, t.TempDir())
	s := everySecond("j")
	s.JitterMs = 800
	p, err := e.In("").PreviewSchedule("", s, 50)
	if err != nil {
		t.Fatal(err)
	}
	spread := false
	for i, fire := range p.Fires {
		off := p.Starts[i].Sub(fire)
		if off < 0 || off >= 800*time.Millisecond {
			t.Fatalf("fire %v starts %v after, outside [0, 800ms)", fire, off)
		}
		spread = spread || off > 0
	}
	if !spread {
		t.Fatal("no fire was jittered")
	}
	if err := e.CreateSchedule(s); err != nil {
		t.Fatal(err)
	}
	again, _ := e.In("").PreviewSchedule("j", nil, 5)
	for i := range again.Fires {
		if want := again.Fires[i].Add(jitterOf(history.Key("", "j"), s, again.Fires[i])); !again.Starts[i].Equal(want) {
			t.Fatal("the preview and the scheduler disagree on a fire's jitter")
		}
	}
	runs := waitForRuns(t, e, "j", 2, 5*time.Second)
	for _, id := range runs {
		w, _ := e.GetWorkflow(id)
		fire := fireTime(t, id)
		started := time.Unix(0, w.GetSummary().GetStartedAt())
		if d := started.Sub(fire); d < jitterOf("j", s, fire)-time.Millisecond || d > jitterOf("j", s, fire)+500*time.Millisecond {
			t.Fatalf("run %s started %v after its fire, want about %v", id, d, jitterOf("j", s, fire))
		}
	}
	if err := e.CreateSchedule(&Schedule{ScheduleId: "x", Cron: "@hourly", WorkflowType: "Parked", JitterMs: -1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative jitter: %v", err)
	}
}

// Backfill starts the missed fires of a past range under the fire ids, once.
func TestBackfill(t *testing.T) {
	e := reopen(t, t.TempDir())
	if err := e.CreateSchedule(&Schedule{ScheduleId: "hourly", Cron: "0 * * * *", WorkflowType: "Parked"}); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Hour)
	from, to := now.Add(-5*time.Hour), now.Add(-time.Hour)
	res, err := e.BackfillSchedule("hourly", from, to, OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED)
	if err != nil || res != (BackfillResult{Started: 4}) {
		t.Fatalf("backfill = %+v, %v; want 4 started (after the start, up to the end)", res, err)
	}
	for h := 4; h >= 1; h-- {
		if _, err := e.GetWorkflow(fireRunID("hourly", now.Add(-time.Duration(h)*time.Hour))); err != nil {
			t.Fatalf("run for %dh ago: %v", h, err)
		}
	}
	if res, err := e.BackfillSchedule("hourly", from, to, OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL); err != nil || res != (BackfillResult{Existing: 4}) {
		t.Fatalf("repeated backfill = %+v, %v; want 4 existing", res, err)
	}
	// SKIP starts a fire only while no run of the schedule is going: the
	// first one parks, so the rest are skipped.
	if err := e.CreateSchedule(&Schedule{ScheduleId: "solo", Cron: "0 * * * *", WorkflowType: "Parked"}); err != nil {
		t.Fatal(err)
	}
	if res, err := e.BackfillSchedule("solo", from, to, OverlapPolicy_OVERLAP_POLICY_SKIP); err != nil || res != (BackfillResult{Started: 1, Skipped: 3}) {
		t.Fatalf("SKIP backfill = %+v, %v", res, err)
	}
	for _, c := range []struct {
		name     string
		from, to time.Time
		policy   OverlapPolicy
	}{
		{"future end", from, time.Now().Add(time.Hour), 0},
		{"reversed", to, from, 0},
		{"too many fires", now.Add(-2000 * time.Hour), to, 0},
		{"buffer one", from, to, OverlapPolicy_OVERLAP_POLICY_BUFFER_ONE},
	} {
		if _, err := e.BackfillSchedule("hourly", c.from, c.to, c.policy); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("%s: %v, want ErrInvalidArgument", c.name, err)
		}
	}
	if _, err := e.BackfillSchedule("missing", from, to, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing schedule: %v", err)
	}
}

func TestPreview(t *testing.T) {
	e := reopen(t, t.TempDir())
	if err := e.CreateSchedule(&Schedule{ScheduleId: "daily", Cron: "30 9 * * *", Timezone: "Europe/London", WorkflowType: "Parked"}); err != nil {
		t.Fatal(err)
	}
	p, err := e.In("").PreviewSchedule("daily", nil, 3)
	if err != nil || len(p.Fires) != 3 {
		t.Fatalf("preview = %v, %v", p, err)
	}
	london, _ := time.LoadLocation("Europe/London")
	for i, f := range p.Fires {
		if l := f.In(london); l.Hour() != 9 || l.Minute() != 30 || (i > 0 && !f.After(p.Fires[i-1])) {
			t.Fatalf("fire %d at %v", i, l)
		}
	}
	if _, err := e.In("").PreviewSchedule("", &Schedule{Cron: "not cron", WorkflowType: "X"}, 3); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a bad spec: %v", err)
	}
	if _, err := e.In("").PreviewSchedule("nope", nil, 3); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a missing schedule: %v", err)
	}
}

// A control an older binary would ignore (and so fire wrongly) needs format
// 6; the ones it reads the same way do not.
func TestScheduleControlFormat(t *testing.T) {
	for _, c := range []struct {
		name string
		edit func(*Schedule)
		want uint32
	}{
		{"plain", func(*Schedule) {}, history.FormatBase},
		{"skip", func(s *Schedule) { s.OverlapPolicy = OverlapPolicy_OVERLAP_POLICY_SKIP }, history.FormatBase},
		{"allow all", func(s *Schedule) { s.OverlapPolicy = OverlapPolicy_OVERLAP_POLICY_ALLOW_ALL }, history.FormatBase},
		{"buffer one", func(s *Schedule) { s.OverlapPolicy = OverlapPolicy_OVERLAP_POLICY_BUFFER_ONE }, history.FormatNamespaces},
		{"jitter", func(s *Schedule) { s.JitterMs = 1000 }, history.FormatNamespaces},
		{"paused", func(s *Schedule) { s.Paused = true }, history.FormatNamespaces},
	} {
		e := reopen(t, t.TempDir())
		s := &Schedule{ScheduleId: "s", Cron: "@hourly", WorkflowType: "Parked"}
		c.edit(s)
		if err := e.CreateSchedule(s); err != nil {
			t.Fatal(err)
		}
		evs, _ := e.store.ReadAll(scheduleLogID)
		if got := max(evs[len(evs)-1].GetMinReaderFormat(), history.FormatBase); got != c.want {
			t.Errorf("%s: stamped %d, want %d", c.name, got, c.want)
		}
		if f := max(e.store.Format(), history.FormatBase); f != c.want {
			t.Errorf("%s: directory format %d, want %d", c.name, f, c.want)
		}
	}
}

// When a fire comes and no run is going, a held fire starts first, and the new
// fire is the one held: newer fires never starve an older held one.
func TestHeldFireGoesFirst(t *testing.T) {
	e := reopen(t, t.TempDir())
	if err := e.CreateSchedule(&Schedule{ScheduleId: "h", Cron: "0 0 1 1 *", WorkflowType: "Parked",
		OverlapPolicy: OverlapPolicy_OVERLAP_POLICY_BUFFER_ONE}); err != nil {
		t.Fatal(err)
	}
	held := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	next := held.Add(time.Hour)
	e.sched.mu.Lock()
	ent := e.sched.entries["h"]
	ent.buffered = held
	retry := e.fireLocked(ent, next)
	buffered := ent.buffered
	e.sched.mu.Unlock()
	if retry {
		t.Fatal("the fire asked to be retried")
	}
	if _, err := e.GetWorkflow(fireRunID("h", held)); err != nil {
		t.Fatalf("the held fire did not start: %v", err)
	}
	if _, err := e.GetWorkflow(fireRunID("h", next)); !errors.Is(err, ErrNotFound) {
		t.Fatalf("the newer fire started past the held one: %v", err)
	}
	if !buffered.Equal(next) {
		t.Fatalf("held %v after the fire, want the newer fire %v", buffered, next)
	}
}
