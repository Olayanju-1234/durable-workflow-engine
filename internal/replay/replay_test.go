package replay

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

var t0 = time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)

// at stamps ids and timestamps one second apart, starting at t0.
func at(evs ...*history.HistoryEvent) []*history.HistoryEvent {
	for i, ev := range evs {
		ev.EventId = int64(i + 1)
		ev.Timestamp = t0.Add(time.Duration(i) * time.Second).UnixNano()
	}
	return evs
}

func started(input string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{WorkflowId: "wf-1", WorkflowType: "T", Input: []byte(input)},
	}}
}

func scheduled(name string, seq int64) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
		StepScheduled: &history.StepScheduledEvent{StepName: name, Sequence: seq},
	}}
}

func completed(name string, seq int64, out string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
		StepCompleted: &history.StepCompletedEvent{StepName: name, Sequence: seq, Output: []byte(out)},
	}}
}

func failed(name string, seq int64, msg string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
		StepFailed: &history.StepFailedEvent{StepName: name, Sequence: seq, Error: msg},
	}}
}

// Workflow code that recovers panics must not see past a suspension: with
// panic-based yields, recover() swallowed it and the code ran on as if the
// activity had returned.
func TestSuspensionCannotBeRecovered(t *testing.T) {
	ranOn := false
	greedy := func(ctx workflow.Context, in []byte) ([]byte, error) {
		func() {
			defer func() { _ = recover() }()
			ctx.ExecuteActivity("charge", in)
		}()
		ranOn = true
		return []byte("swallowed"), nil
	}
	d, err := Decide(greedy, at(started("in")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.Type != state.DecideScheduleStep || d.StepName != "charge" {
		t.Fatalf("got %+v, want schedule charge", d)
	}
	if ranOn {
		t.Fatal("workflow code ran past a suspended call")
	}
}

// A durable call from a deferred function runs while the suspended goroutine
// unwinds. It must not replace the decision already made.
func TestDeferredCallDoesNotChangeDecision(t *testing.T) {
	cleanup := func(ctx workflow.Context, in []byte) ([]byte, error) {
		defer ctx.ExecuteActivity("cleanup", nil)
		return ctx.ExecuteActivity("work", in)
	}
	d, err := Decide(cleanup, at(started("in")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.Type != state.DecideScheduleStep || d.StepName != "work" || d.Sequence != 1 {
		t.Fatalf("got %+v, want schedule work/1", d)
	}
}

func TestPanicBecomesError(t *testing.T) {
	boom := func(ctx workflow.Context, in []byte) ([]byte, error) {
		var m map[string]int
		m["x"] = 1 // nil map write
		return nil, nil
	}
	_, err := Decide(boom, at(started("in")), nil)
	var pe *PanicError
	if !errors.As(err, &pe) || !strings.Contains(err.Error(), "panicked") || len(pe.Stack) == 0 {
		t.Fatalf("got %v, want a PanicError with a stack", err)
	}
}

func TestNondeterminismIsAnError(t *testing.T) {
	drifted := func(ctx workflow.Context, in []byte) ([]byte, error) {
		return ctx.ExecuteActivity("renamed", in)
	}
	_, err := Decide(drifted, at(started("in"), scheduled("first", 1), completed("first", 1, "a")), nil)
	if !errors.Is(err, ErrNondeterminism) {
		t.Fatalf("got %v, want ErrNondeterminism", err)
	}
}

// Now is the time of the latest event the code has replayed past, so it reads
// the same whether the decision is being made or replayed later.
func TestNowIsDeterministic(t *testing.T) {
	var seen []time.Time
	wf := func(ctx workflow.Context, in []byte) ([]byte, error) {
		seen = append(seen, ctx.Now())
		if _, err := ctx.ExecuteActivity("a", in); err != nil {
			seen = append(seen, ctx.Now())
			return ctx.ExecuteActivity("b", in)
		}
		return nil, nil
	}
	evs := at(started("in"), scheduled("a", 1), failed("a", 1, "x"))
	if _, err := Decide(wf, evs, nil); err != nil {
		t.Fatal(err)
	}
	// Event 3 (the failure) is stamped t0+2s.
	want := []time.Time{t0, t0.Add(2 * time.Second)}
	if len(seen) != 2 || !seen[0].Equal(want[0]) || !seen[1].Equal(want[1]) {
		t.Fatalf("Now() = %v, want %v", seen, want)
	}

	// A later replay over a longer history sees the same values at the same points.
	seen = nil
	evs = at(started("in"), scheduled("a", 1), failed("a", 1, "x"), scheduled("b", 2), completed("b", 2, "ok"))
	if _, err := Decide(wf, evs, nil); err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || !seen[0].Equal(want[0]) || !seen[1].Equal(want[1]) {
		t.Fatalf("on replay Now() = %v, want %v", seen, want)
	}
}

func TestLoggerSkipsReplayedSteps(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	wf := func(ctx workflow.Context, in []byte) ([]byte, error) {
		ctx.Logger().Info("before a")
		a, _ := ctx.ExecuteActivity("a", in)
		ctx.Logger().Info("after a")
		return ctx.ExecuteActivity("b", a)
	}
	if _, err := Decide(wf, at(started("in"), scheduled("a", 1), completed("a", 1, "A")), logger); err != nil {
		t.Fatal(err)
	}
	out := buf.String()
	if strings.Contains(out, "before a") || !strings.Contains(out, "after a") || !strings.Contains(out, "workflow_id=wf-1") {
		t.Fatalf("log output %q: want only the new line, tagged with the workflow id", out)
	}
}

func TestInfo(t *testing.T) {
	var info workflow.Info
	wf := func(ctx workflow.Context, in []byte) ([]byte, error) {
		info = ctx.Info()
		return nil, nil
	}
	if _, err := Decide(wf, at(started("in")), nil); err != nil {
		t.Fatal(err)
	}
	if info.WorkflowID != "wf-1" || info.WorkflowType != "T" {
		t.Fatalf("info = %+v", info)
	}
}
