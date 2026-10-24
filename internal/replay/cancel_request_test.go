package replay

import (
	"errors"
	"testing"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

func cancelRequest(reason string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_SignalReceived{SignalReceived: &history.SignalReceivedEvent{
		SignalName: state.CancelSignal, Payload: []byte(reason), CancelRequest: true,
	}}}
}

func cancelMarker(seq int64, seen string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_MarkerRecorded{MarkerRecorded: &history.MarkerRecordedEvent{
		StepName: "cancellation", Sequence: seq, Marker: state.MarkerCancel, Data: []byte(seen),
	}}}
}

func stepCancelled(name string, seq int64) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{StepFailed: &history.StepFailedEvent{
		StepName: name, Sequence: seq, Error: "cancelled", Cancelled: true,
	}}}
}

// reserveAndShip waits on two activities; told it is being cancelled, it
// releases what it reserved and lets the cancellation end it.
func reserveAndShip(ctx workflow.Context, in []byte) ([]byte, error) {
	reserve := ctx.ExecuteActivityAsync("reserve", in)
	ship := ctx.ExecuteActivityAsync("ship", in)
	if _, err := reserve.Get(); err != nil {
		return nil, err
	}
	if _, err := ship.Get(); err != nil {
		if errors.Is(err, workflow.ErrWorkflowCancelled) {
			ctx.ExecuteActivity("release", in)
		}
		return nil, err
	}
	return []byte("shipped"), nil
}

func decisionTypes(ds []state.Decision) []string {
	var out []string
	for _, d := range ds {
		s := d.Type.String()
		if d.StepName != "" {
			s += " " + d.StepName
		}
		out = append(out, s)
	}
	return out
}

func sameStrings(a, b []string) bool {
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

// The first decision after the request tells the code where it first waits:
// a result recorded after the request but before the decision is seen (the
// decision saw it), and the decision records where the code was told.
func TestCancelRequestDeliveredWhereTheCodeWaits(t *testing.T) {
	h := at(started("x"), scheduled("reserve", 1), scheduled("ship", 2), cancelRequest("customer left"), completed("reserve", 1, "r"))
	ds, err := DecideAll(reserveAndShip, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"RECORD_MARKER cancellation", "CANCEL_STEP ship", "SCHEDULE_STEP release"}
	if got := decisionTypes(ds); !sameStrings(got, want) {
		t.Fatalf("decisions %v, want %v", got, want)
	}
	if ds[0].Marker != state.MarkerCancel || string(ds[0].Output) != "5" || ds[0].Sequence != 3 || ds[2].Sequence != 4 {
		t.Fatalf("marker %+v, release %+v", ds[0], ds[2])
	}
}

// Replaying the same execution later gives the same path: the delivery
// decision's cutoff hides the step's later cancel, so the code waits, and is
// told, at the same point, and the cancellation ends the workflow CANCELLED.
func TestCancelRequestReplaysTheSamePath(t *testing.T) {
	h := at(started("x"), scheduled("reserve", 1), scheduled("ship", 2), cancelRequest("customer left"), completed("reserve", 1, "r"),
		cancelMarker(3, "5"), stepCancelled("ship", 2), scheduled("release", 4), completed("release", 4, "released"))
	ds, err := DecideAll(reserveAndShip, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 1 || ds[0].Type != state.DecideFailWorkflow || !ds[0].Cancelled || ds[0].Error != "workflow: step cancelled: the workflow is being cancelled (customer left)" {
		t.Fatalf("decisions %+v, want one cancelled failure", ds)
	}
}

// Told while it waits on nothing it can see, before any result arrived: the
// steps it started are cancelled; a later replay agrees though their
// cancellations are in history by then.
func TestCancelRequestBeforeAnyResult(t *testing.T) {
	first := at(started("x"), scheduled("reserve", 1), scheduled("ship", 2), cancelRequest(""))
	ds, err := DecideAll(reserveAndShip, first, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := decisionTypes(ds), []string{"RECORD_MARKER cancellation", "CANCEL_STEP reserve", "CANCEL_STEP ship", "FAIL_WORKFLOW"}; !sameStrings(got, want) || !ds[3].Cancelled {
		t.Fatalf("decisions %v, want %v, cancelled (it never got past reserve: no release)", got, want)
	}
	later := at(started("x"), scheduled("reserve", 1), scheduled("ship", 2), cancelRequest(""),
		cancelMarker(3, "4"), stepCancelled("reserve", 1), stepCancelled("ship", 2))
	ds, err = DecideAll(reserveAndShip, later, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 1 || ds[0].Type != state.DecideFailWorkflow || !ds[0].Cancelled {
		t.Fatalf("replay decisions %+v, want one cancelled failure", ds)
	}
}

// Code that catches the cancellation and returns normally completes; code
// that cancels its own step and returns that error has failed, not been
// cancelled (nobody asked).
func TestCancelRequestOutcomes(t *testing.T) {
	shrug := func(ctx workflow.Context, in []byte) ([]byte, error) {
		if _, err := ctx.ExecuteActivity("wait", in); err != nil {
			return []byte("gave up: " + err.Error()), nil
		}
		return []byte("done"), nil
	}
	ds, err := DecideAll(shrug, at(started("x"), scheduled("wait", 1), cancelRequest("")), nil)
	if err != nil || len(ds) != 3 || ds[2].Type != state.DecideCompleteWorkflow {
		t.Fatalf("decisions %+v %v, want marker, cancel, complete", ds, err)
	}
	selfCancel := func(ctx workflow.Context, in []byte) ([]byte, error) {
		f := ctx.ExecuteActivityAsync("wait", in)
		ctx.CancelStep(f)
		_, err := f.Get()
		return nil, err
	}
	ds, err = DecideAll(selfCancel, at(started("x"), scheduled("wait", 1)), nil)
	if err != nil || ds[len(ds)-1].Type != state.DecideFailWorkflow || ds[len(ds)-1].Cancelled {
		t.Fatalf("decisions %+v %v, want a plain failure", ds, err)
	}
}

// The clock moves to the request when the code is told.
func TestCancelRequestMovesTheClock(t *testing.T) {
	var told int64
	fn := func(ctx workflow.Context, in []byte) ([]byte, error) {
		_, err := ctx.ExecuteActivity("wait", in)
		told = ctx.Now().UnixNano()
		return nil, err
	}
	h := at(started("x"), scheduled("wait", 1), cancelRequest(""))
	if _, err := DecideAll(fn, h, nil); err != nil {
		t.Fatal(err)
	}
	if told != h[2].GetTimestamp() {
		t.Fatalf("Now() after the cancellation = %d, want the request's time %d", told, h[2].GetTimestamp())
	}
}

// Sleep has no error to return: CancelRequested says the code was told.
func TestCancelRequestedAfterSleep(t *testing.T) {
	var seen []bool
	fn := func(ctx workflow.Context, in []byte) ([]byte, error) {
		seen = append(seen[:0], ctx.CancelRequested())
		ctx.Sleep("settle", 1)
		seen = append(seen, ctx.CancelRequested())
		if ctx.CancelRequested() {
			return nil, workflow.ErrWorkflowCancelled
		}
		return nil, nil
	}
	h := at(started("x"), &history.HistoryEvent{Attributes: &history.HistoryEvent_TimerStarted{
		TimerStarted: &history.TimerStartedEvent{StepName: "settle", Sequence: 1, FireAt: 1},
	}}, cancelRequest(""))
	ds, err := DecideAll(fn, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[0] || !seen[1] || !ds[len(ds)-1].Cancelled {
		t.Fatalf("CancelRequested before/after the sleep: %v; decisions %v", seen, decisionTypes(ds))
	}
}

// Code changed so that it waits, and is told, at another step than history
// recorded: non-determinism, not a quietly different cancellation.
func TestCancelRequestAtAnotherStepIsNondeterministic(t *testing.T) {
	h := at(started("x"), scheduled("reserve", 1), scheduled("ship", 2), cancelRequest(""),
		cancelMarker(3, "4"), stepCancelled("reserve", 1), stepCancelled("ship", 2))
	waitsOnReserveFirst := func(ctx workflow.Context, in []byte) ([]byte, error) {
		if _, err := ctx.ExecuteActivityAsync("reserve", in).Get(); err != nil {
			return nil, err
		}
		return ctx.ExecuteActivity("ship", in)
	}
	if _, err := DecideAll(waitsOnReserveFirst, h, nil); !errors.Is(err, ErrNondeterminism) {
		t.Fatalf("err = %v, want ErrNondeterminism", err)
	}
}

// A step that had its result when the code was told keeps it: only what the
// code was still waiting on is interrupted.
func TestCancelRequestKeepsResultsAlreadyIn(t *testing.T) {
	fn := func(ctx workflow.Context, in []byte) ([]byte, error) {
		quote := ctx.ExecuteActivityAsync("quote", in)
		ship := ctx.ExecuteActivityAsync("ship", in)
		if _, err := ship.Get(); err != nil {
			price, qerr := quote.Get()
			return append([]byte("refund "), price...), qerr
		}
		return nil, nil
	}
	h := at(started("x"), scheduled("quote", 1), scheduled("ship", 2), completed("quote", 1, "42"), cancelRequest(""))
	ds, err := DecideAll(fn, h, nil)
	if err != nil {
		t.Fatal(err)
	}
	last := ds[len(ds)-1]
	if last.Type != state.DecideCompleteWorkflow || string(last.Output) != "refund 42" {
		t.Fatalf("decisions %v, last %+v: the quote's result was lost", decisionTypes(ds), last)
	}
}
