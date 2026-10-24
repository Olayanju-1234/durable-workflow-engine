package replay

import (
	"errors"
	"testing"

	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// ContinueAsNew becomes the last command, after the steps the code started.
func TestContinueAsNewDecision(t *testing.T) {
	fn := func(ctx workflow.Context, in []byte) ([]byte, error) {
		ctx.ExecuteActivityAsync("fire-and-forget", in)
		return nil, workflow.ContinueAsNew([]byte("next"), workflow.WithNewWorkflowType("Other"))
	}
	ds, err := DecideAll(fn, at(started("x")), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 2 || ds[0].Type != state.DecideScheduleStep || ds[1].Type != state.DecideContinueAsNew ||
		string(ds[1].Input) != "next" || ds[1].WorkflowType != "Other" {
		t.Fatalf("decisions %+v", ds)
	}
}

// Continuing as new before the steps history recorded would drop them:
// that is a different execution, so it is reported as non-determinism.
func TestContinueAsNewBeforeRecordedStepsIsNondeterministic(t *testing.T) {
	early := func(ctx workflow.Context, in []byte) ([]byte, error) {
		return nil, workflow.ContinueAsNew(in)
	}
	_, err := DecideAll(early, at(started("x"), scheduled("a", 1), completed("a", 1, "ok")), nil)
	if !errors.Is(err, ErrNondeterminism) {
		t.Fatalf("err = %v, want ErrNondeterminism", err)
	}
}

// ContinueAsNewSuggested is a function of the history the code has observed,
// not of when a replay happens: the same point of the code always answers the
// same, however far the history has grown since.
func TestContinueAsNewSuggestedIsDeterministic(t *testing.T) {
	var answers []bool
	fn := func(ctx workflow.Context, in []byte) ([]byte, error) {
		answers = answers[:0]
		for i := 0; i < 3; i++ {
			answers = append(answers, ctx.ContinueAsNewSuggested())
			if _, err := ctx.ExecuteActivity("tick", nil); err != nil {
				return nil, err
			}
		}
		return nil, nil
	}
	// Results land at events 3, 5 and 7; the threshold was crossed at event 4.
	evs := at(started("x"), scheduled("tick", 1), completed("tick", 1, ""), scheduled("tick", 2),
		completed("tick", 2, ""), scheduled("tick", 3), completed("tick", 3, ""))
	if _, err := DecideAllWith(fn, evs, nil, Options{SuggestContinueAsNewAt: 4}); err != nil {
		t.Fatal(err)
	}
	if want := []bool{false, false, true}; !equalBools(answers, want) {
		t.Fatalf("answers %v, want %v", answers, want)
	}
	// The same code over a shorter prefix of the same history agrees on the
	// points both replays reach.
	if _, err := DecideAllWith(fn, evs[:5], nil, Options{SuggestContinueAsNewAt: 4}); err != nil {
		t.Fatal(err)
	}
	if want := []bool{false, false, true}; !equalBools(answers, want) {
		t.Fatalf("on a prefix: answers %v, want %v", answers, want)
	}
}

func equalBools(a, b []bool) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
