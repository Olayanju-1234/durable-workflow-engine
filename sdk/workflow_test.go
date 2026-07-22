package sdk

import (
	"strings"
	"testing"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// --- tiny history builders --------------------------------------------------

func started(input string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{WorkflowId: "wf", WorkflowType: "T", Input: []byte(input)},
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

func withIDs(evs ...*history.HistoryEvent) []*history.HistoryEvent {
	for i, ev := range evs {
		ev.EventId = int64(i + 1)
	}
	return evs
}

// twoActivities is a workflow whose data flow it controls itself: the second
// activity's input is the first's output.
func twoActivities(ctx Context, in []byte) ([]byte, error) {
	a, err := ctx.ExecuteActivity("first", in)
	if err != nil {
		return nil, err
	}
	return ctx.ExecuteActivity("second", a)
}

// TestReplayDecidesInOrder walks a two-activity workflow and asserts replay emits
// the right decision at each step: schedule first, then second (chaining the first's
// output), then complete with the last output.
func TestReplayDecidesInOrder(t *testing.T) {
	w := Workflow{Type: "T", Fn: twoActivities}

	d, err := w.Decide(withIDs(started("in")))
	if err != nil {
		t.Fatalf("decide 1: %v", err)
	}
	if d.Type != state.DecideScheduleStep || d.StepName != "first" || d.Sequence != 1 || string(d.Input) != "in" {
		t.Fatalf("decide 1: got %+v, want schedule first/1/in", d)
	}

	d, err = w.Decide(withIDs(started("in"), scheduled("first", 1), completed("first", 1, "AAA")))
	if err != nil {
		t.Fatalf("decide 2: %v", err)
	}
	if d.Type != state.DecideScheduleStep || d.StepName != "second" || d.Sequence != 2 || string(d.Input) != "AAA" {
		t.Fatalf("decide 2: got %+v, want schedule second/2/AAA", d)
	}

	d, err = w.Decide(withIDs(
		started("in"),
		scheduled("first", 1), completed("first", 1, "AAA"),
		scheduled("second", 2), completed("second", 2, "BBB"),
	))
	if err != nil {
		t.Fatalf("decide 3: %v", err)
	}
	if d.Type != state.DecideCompleteWorkflow || string(d.Output) != "BBB" {
		t.Fatalf("decide 3: got %+v, want complete/BBB", d)
	}
}

// TestReplayInFlightIsNoop: while a step is dispatched but not yet done, replay has
// nothing to decide.
func TestReplayInFlightIsNoop(t *testing.T) {
	w := Workflow{Type: "T", Fn: twoActivities}
	d, err := w.Decide(withIDs(started("in"), scheduled("first", 1)))
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	if d.Type != state.DecideNoop {
		t.Fatalf("in-flight decision: got %v, want NOOP", d.Type)
	}
}

// TestReplayPropagatesActivityError: a workflow that returns an activity's error
// fails the workflow.
func TestReplayPropagatesActivityError(t *testing.T) {
	w := Workflow{Type: "T", Fn: twoActivities}
	failed := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
		StepFailed: &history.StepFailedEvent{StepName: "first", Sequence: 1, Error: "boom"},
	}}
	d, err := w.Decide(withIDs(started("in"), scheduled("first", 1), failed))
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	if d.Type != state.DecideFailWorkflow || d.Error != "boom" {
		t.Fatalf("got %+v, want fail/boom", d)
	}
}

// TestReplayDetectsNonDeterminism: if the workflow code issues a different step than
// history recorded at that position, replay refuses rather than corrupting state.
func TestReplayDetectsNonDeterminism(t *testing.T) {
	// History recorded "first" at step 1; this workflow now issues "renamed".
	drifted := Workflow{Type: "T", Fn: func(ctx Context, in []byte) ([]byte, error) {
		return ctx.ExecuteActivity("renamed", in)
	}}
	_, err := drifted.Decide(withIDs(started("in"), scheduled("first", 1), completed("first", 1, "a")))
	if err == nil || !strings.Contains(err.Error(), "non-determinism") {
		t.Fatalf("expected non-determinism error, got %v", err)
	}
}

// TestReplayIsPure: the same history yields the same decision every time — the
// property that makes re-running after a crash safe.
func TestReplayIsPure(t *testing.T) {
	w := Workflow{Type: "T", Fn: twoActivities}
	events := withIDs(started("in"), scheduled("first", 1), completed("first", 1, "AAA"))
	d1, _ := w.Decide(events)
	d2, _ := w.Decide(events)
	if d1.Type != d2.Type || d1.StepName != d2.StepName || string(d1.Input) != string(d2.Input) {
		t.Fatalf("replay not pure: %+v vs %+v", d1, d2)
	}
}
