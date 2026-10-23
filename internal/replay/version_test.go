package replay

import (
	"errors"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

func marker(name string, seq int64, kind, data string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_MarkerRecorded{
		MarkerRecorded: &history.MarkerRecordedEvent{StepName: name, Sequence: seq, Marker: kind, Data: []byte(data)},
	}}
}

// v1 charges then ships.
func v1(ctx workflow.Context, in []byte) ([]byte, error) {
	if _, err := ctx.ExecuteActivity("charge", in); err != nil {
		return nil, err
	}
	return ctx.ExecuteActivity("ship", in)
}

// v2 adds a fraud check before charging, guarded by GetVersion.
func v2(ctx workflow.Context, in []byte) ([]byte, error) {
	if ctx.GetVersion("fraud-check", workflow.DefaultVersion, 1) == 1 {
		if _, err := ctx.ExecuteActivity("fraud-check", in); err != nil {
			return nil, err
		}
	}
	return v1(ctx, in)
}

// v2Unguarded makes the same change without GetVersion.
func v2Unguarded(ctx workflow.Context, in []byte) ([]byte, error) {
	if _, err := ctx.ExecuteActivity("fraud-check", in); err != nil {
		return nil, err
	}
	return v1(ctx, in)
}

// An execution that charged under v1 is replayed by v2 after a deploy.
var chargedUnderV1 = func() []*history.HistoryEvent {
	return at(started("o"), scheduled("charge", 1), completed("charge", 1, "ok"))
}

func TestOldExecutionReplaysOldBranch(t *testing.T) {
	d, err := Decide(v2, chargedUnderV1(), nil)
	if err != nil {
		t.Fatalf("v2 on a v1 history: %v", err)
	}
	if d.Type != state.DecideScheduleStep || d.StepName != "ship" || d.Sequence != 2 {
		t.Fatalf("got %+v, want schedule ship/2 (the old branch)", d)
	}

	// The problem GetVersion solves: the same change, unguarded, breaks it.
	if _, err := Decide(v2Unguarded, chargedUnderV1(), nil); !errors.Is(err, ErrNondeterminism) {
		t.Fatalf("unguarded change on a v1 history: got %v, want non-determinism", err)
	}
}

func TestNewExecutionRecordsAndFollowsNewBranch(t *testing.T) {
	d, err := Decide(v2, at(started("o")), nil)
	if err != nil {
		t.Fatal(err)
	}
	want := state.Decision{Type: state.DecideRecordMarker, StepName: "fraud-check", Sequence: 1, Marker: state.MarkerVersion, Output: []byte("1")}
	if d.Type != want.Type || d.StepName != want.StepName || d.Sequence != 1 || d.Marker != want.Marker || string(d.Output) != "1" {
		t.Fatalf("first decision %+v, want %+v", d, want)
	}

	d, err = Decide(v2, at(started("o"), marker("fraud-check", 1, state.MarkerVersion, "1")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.Type != state.DecideScheduleStep || d.StepName != "fraud-check" || d.Sequence != 2 {
		t.Fatalf("after the marker: %+v, want schedule fraud-check/2", d)
	}
}

// An execution started before the change, still before the changed point when
// v2 is deployed, has never run the old branch there, so it takes the new one.
func TestOldExecutionAtTheChangeTakesNewBranch(t *testing.T) {
	early := func(ctx workflow.Context, in []byte) ([]byte, error) {
		ctx.ExecuteActivity("reserve", in)
		return v2(ctx, in)
	}
	d, err := Decide(early, at(started("o"), scheduled("reserve", 1), completed("reserve", 1, "r")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if d.Type != state.DecideRecordMarker || d.Sequence != 2 {
		t.Fatalf("got %+v, want a version marker at step 2", d)
	}
}

func TestRetiredVersionFails(t *testing.T) {
	v3 := func(ctx workflow.Context, in []byte) ([]byte, error) {
		ctx.GetVersion("fraud-check", 1, 1) // the old branch is gone
		ctx.ExecuteActivity("fraud-check", in)
		return v1(ctx, in)
	}
	if _, err := Decide(v3, chargedUnderV1(), nil); !errors.Is(err, ErrNondeterminism) {
		t.Fatalf("v1 execution under code that dropped its branch: got %v, want non-determinism", err)
	}
}

func TestVersionIsStablePerChangeID(t *testing.T) {
	var got []workflow.Version
	wf := func(ctx workflow.Context, in []byte) ([]byte, error) {
		got = append(got, ctx.GetVersion("c", workflow.DefaultVersion, 2))
		ctx.ExecuteActivity("a", in)
		got = append(got, ctx.GetVersion("c", workflow.DefaultVersion, 3)) // max moved; recorded value wins
		return nil, nil
	}
	evs := at(started("o"), marker("c", 1, state.MarkerVersion, "2"), scheduled("a", 2), completed("a", 2, ""))
	if _, err := Decide(wf, evs, nil); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0] != 2 || got[1] != 2 {
		t.Fatalf("versions %v, want [2 2]", got)
	}
}

func TestSideEffectRecordedOnce(t *testing.T) {
	calls := 0
	wf := func(ctx workflow.Context, in []byte) ([]byte, error) {
		id := ctx.SideEffect("order-id", func() []byte { calls++; return []byte("fresh") })
		return ctx.ExecuteActivity("use", id)
	}
	d, err := Decide(wf, at(started("o")), nil)
	if err != nil || d.Type != state.DecideRecordMarker || d.Marker != state.MarkerSideEffect || string(d.Output) != "fresh" {
		t.Fatalf("frontier: %+v %v", d, err)
	}
	d, err = Decide(wf, at(started("o"), marker("order-id", 1, state.MarkerSideEffect, "recorded")), nil)
	if err != nil || d.Type != state.DecideScheduleStep || string(d.Input) != "recorded" {
		t.Fatalf("replay: %+v %v, want the recorded value", d, err)
	}
	if calls != 1 {
		t.Fatalf("fn ran %d times, want once", calls)
	}
}

// TestNowIgnoresMarkers: a marker is recorded by the code itself, so the first
// run never observes its time. A replay must not either, or ctx.Now() after a
// GetVersion or SideEffect would read differently on replay.
func TestNowIgnoresMarkers(t *testing.T) {
	fn := func(ctx workflow.Context, in []byte) ([]byte, error) {
		ctx.GetVersion("change", workflow.DefaultVersion, 1)
		ctx.SideEffect("id", func() []byte { return []byte("x") })
		before := ctx.Now()
		if _, err := ctx.ExecuteActivity("work", nil); err != nil {
			return nil, err
		}
		return []byte(before.Format(time.RFC3339)), nil
	}
	marker := func(name string, seq int64, kind, data string) *history.HistoryEvent {
		return &history.HistoryEvent{Attributes: &history.HistoryEvent_MarkerRecorded{MarkerRecorded: &history.MarkerRecordedEvent{
			StepName: name, Sequence: seq, Marker: kind, Data: []byte(data)}}}
	}
	evs := at(started(""), marker("change", 1, "version", "1"), marker("id", 2, "side_effect", "x"),
		scheduled("work", 3), completed("work", 3, "done"))
	ds, err := DecideAll(fn, evs, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 1 || ds[0].Type != state.DecideCompleteWorkflow || string(ds[0].Output) != t0.Format(time.RFC3339) {
		t.Fatalf("decisions %+v, want completion with the start time %s", ds, t0.Format(time.RFC3339))
	}
}
