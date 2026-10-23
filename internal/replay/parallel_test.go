package replay

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

func timerStarted(name string, seq int64) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_TimerStarted{
		TimerStarted: &history.TimerStartedEvent{StepName: name, Sequence: seq, FireAt: t0.Add(time.Hour).UnixNano()},
	}}
}

func timerFired(name string, seq int64) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_TimerFired{TimerFired: &history.TimerFiredEvent{StepName: name, Sequence: seq}}}
}

func decideAll(t *testing.T, fn workflow.Func, evs []*history.HistoryEvent) []state.Decision {
	t.Helper()
	ds, err := DecideAll(fn, evs, nil)
	if err != nil {
		t.Fatal(err)
	}
	return ds
}

// fanOut starts three activities at once and joins them.
func fanOut(ctx workflow.Context, in []byte) ([]byte, error) {
	fs := []workflow.Future{
		ctx.ExecuteActivityAsync("a", in),
		ctx.ExecuteActivityAsync("b", in),
		ctx.ExecuteActivityAsync("c", in),
	}
	var out []byte
	for _, f := range fs {
		r, err := f.Get()
		if err != nil {
			return nil, err
		}
		out = append(out, r...)
	}
	return out, nil
}

func TestParallelActivitiesAreOneDecision(t *testing.T) {
	ds := decideAll(t, fanOut, at(started("x")))
	if len(ds) != 3 {
		t.Fatalf("got %d decisions, want all three activities at once: %+v", len(ds), ds)
	}
	for i, d := range ds {
		if d.Type != state.DecideScheduleStep || d.Sequence != int64(i+1) {
			t.Fatalf("decision %d = %+v", i, d)
		}
	}
	// b finished first; a and c are still running: nothing new to do.
	ds = decideAll(t, fanOut, at(started("x"), scheduled("a", 1), scheduled("b", 2), scheduled("c", 3), completed("b", 2, "B")))
	if len(ds) != 1 || ds[0].Type != state.DecideNoop {
		t.Fatalf("got %+v, want a noop while a and c run", ds)
	}
	ds = decideAll(t, fanOut, at(started("x"), scheduled("a", 1), scheduled("b", 2), scheduled("c", 3),
		completed("c", 3, "C"), completed("a", 1, "A"), completed("b", 2, "B")))
	if len(ds) != 1 || ds[0].Type != state.DecideCompleteWorkflow || string(ds[0].Output) != "ABC" {
		t.Fatalf("got %+v, want ABC (futures joined in code order)", ds)
	}
}

// race waits for whichever of a reply and a deadline comes first.
func race(ctx workflow.Context, _ []byte) ([]byte, error) {
	reply := ctx.ExecuteActivityAsync("reply", nil)
	deadline := ctx.NewTimer("deadline", time.Minute)
	if ctx.WaitAny(reply, deadline) == 1 {
		return []byte("timed out"), nil
	}
	out, _ := reply.Get()
	return out, nil
}

// TestWaitAnyIsDecidedByHistoryOrder: WaitAny picks the step whose result was
// recorded first, so a replay that sees both results makes the same choice as
// the decision that saw only the first.
func TestWaitAnyIsDecidedByHistoryOrder(t *testing.T) {
	head := []*history.HistoryEvent{started(""), scheduled("reply", 1), timerStarted("deadline", 2)}
	timerFirst := at(append(head, timerFired("deadline", 2))...)
	if ds := decideAll(t, race, timerFirst); ds[0].Type != state.DecideCompleteWorkflow || string(ds[0].Output) != "timed out" {
		t.Fatalf("timer resolved first: %+v", ds)
	}
	// Later the reply arrives too. The code listed reply first, but history
	// says the timer won, and replay must agree.
	both := at(append(append([]*history.HistoryEvent{}, head...), timerFired("deadline", 2), completed("reply", 1, "yes"))...)
	if ds := decideAll(t, race, both); string(ds[0].Output) != "timed out" {
		t.Fatalf("with both results, replay chose %q; the decision recorded \"timed out\"", ds[0].Output)
	}
	replyFirst := at(append(append([]*history.HistoryEvent{}, head...), completed("reply", 1, "yes"), timerFired("deadline", 2))...)
	if ds := decideAll(t, race, replyFirst); string(ds[0].Output) != "yes" {
		t.Fatalf("reply resolved first: %+v", ds)
	}
}

// TestRetryingStepIsNotAResult: a failure the engine will retry must not reach
// workflow code, even when another step's result wakes it up.
func TestRetryingStepIsNotAResult(t *testing.T) {
	retrying := failed("a", 1, "flaky")
	retrying.GetStepFailed().WillRetry = true
	ds := decideAll(t, fanOut, at(started("x"), scheduled("a", 1), scheduled("b", 2), scheduled("c", 3),
		retrying, completed("b", 2, "B"), completed("c", 3, "C")))
	if len(ds) != 1 || ds[0].Type != state.DecideNoop {
		t.Fatalf("got %+v; a is being retried, so the workflow must keep waiting", ds)
	}
}

func TestMarkersJoinTheBatch(t *testing.T) {
	fn := func(ctx workflow.Context, in []byte) ([]byte, error) {
		v := ctx.GetVersion("v", workflow.DefaultVersion, 2)
		id := ctx.SideEffect("id", func() []byte { return []byte("42") })
		return ctx.ExecuteActivity("go", append([]byte{byte('0' + v)}, id...))
	}
	ds := decideAll(t, fn, at(started("")))
	if len(ds) != 3 || ds[0].Type != state.DecideRecordMarker || ds[1].Type != state.DecideRecordMarker ||
		ds[2].Type != state.DecideScheduleStep || ds[2].Sequence != 3 || string(ds[2].Input) != "242" {
		t.Fatalf("got %+v; want version marker, side effect and the activity in one decision", ds)
	}
}

func TestCompletingLeavesStepsBehind(t *testing.T) {
	fn := func(ctx workflow.Context, in []byte) ([]byte, error) {
		ctx.NewTimer("never-waited-for", time.Hour)
		return []byte("done"), nil
	}
	ds := decideAll(t, fn, at(started("")))
	if len(ds) != 2 || ds[0].Type != state.DecideStartTimer || ds[1].Type != state.DecideCompleteWorkflow {
		t.Fatalf("got %+v", ds)
	}
}

// TestCancelStepIsPartOfTheBatch: a cancel takes effect in the replay that
// issues it, so the cancelled future's Get returns ErrCancelled at once and
// the code's next step goes out with the cancel, not one round trip later.
// Once history shows the cancel, replay issues nothing for it again.
func TestCancelStepIsPartOfTheBatch(t *testing.T) {
	fn := func(ctx workflow.Context, in []byte) ([]byte, error) {
		reply := ctx.WaitForSignalAsync("reply", "approved")
		deadline := ctx.NewTimer("deadline", time.Hour)
		if ctx.WaitAny(reply, deadline) == 1 {
			ctx.CancelStep(reply)
			if _, err := reply.Get(); !errors.Is(err, workflow.ErrCancelled) {
				return nil, fmt.Errorf("cancelled reply: %v", err)
			}
		}
		return ctx.ExecuteActivity("next", nil)
	}
	waitSig := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{StepScheduled: &history.StepScheduledEvent{
		StepName: "reply", Sequence: 1, AwaitsSignal: true, SignalName: "approved"}}}
	base := []*history.HistoryEvent{started("x"), waitSig, timerStarted("deadline", 2), timerFired("deadline", 2)}
	ds := decideAll(t, fn, at(base...))
	if len(ds) != 2 || ds[0].Type != state.DecideCancelStep || ds[0].Sequence != 1 || ds[0].StepName != "reply" ||
		ds[1].Type != state.DecideScheduleStep || ds[1].StepName != "next" || ds[1].Sequence != 3 {
		t.Fatalf("decisions %+v, want [cancel reply#1, schedule next#3]", ds)
	}
	cancelled := &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{StepFailed: &history.StepFailedEvent{
		StepName: "reply", Sequence: 1, Error: "cancelled", Cancelled: true}}}
	ds = decideAll(t, fn, at(append(base, cancelled, scheduled("next", 3))...))
	if len(ds) != 1 || ds[0].Type != state.DecideNoop {
		t.Fatalf("after the cancel was recorded: %+v, want a noop", ds)
	}
}
