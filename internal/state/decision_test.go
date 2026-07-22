package state

import (
	"fmt"
	"testing"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

var orderDef = WorkflowDefinition{Type: "Order", Steps: Activities("reserve", "charge", "ship")}

// TestDecide is the Week-2 deliverable: a hand-built history goes in, and we
// assert the single decision the engine makes. This is the whole engine in
// miniature — a pure function from log to next action.
func TestDecide(t *testing.T) {
	tests := []struct {
		name     string
		events   []*history.HistoryEvent
		wantType DecisionType
		wantName string // for SCHEDULE_STEP
		wantSeq  int64  // for SCHEDULE_STEP
		wantData string // step input for SCHEDULE_STEP, or output for COMPLETE
	}{
		{
			name:     "empty history is a no-op (caller records WorkflowStarted first)",
			events:   nil,
			wantType: DecideNoop,
		},
		{
			name:     "started → schedule first step with the workflow input",
			events:   withIDs(started("Order", "in")),
			wantType: DecideScheduleStep, wantName: "reserve", wantSeq: 1, wantData: "in",
		},
		{
			name:     "step in flight → no-op (wait for the worker)",
			events:   withIDs(started("Order", "in"), scheduled("reserve", 1, "in")),
			wantType: DecideNoop,
		},
		{
			name:     "first step done → schedule second, chaining its output as input",
			events:   withIDs(started("Order", "in"), scheduled("reserve", 1, "in"), completed("reserve", 1, "reserved")),
			wantType: DecideScheduleStep, wantName: "charge", wantSeq: 2, wantData: "reserved",
		},
		{
			name: "all steps done → complete with the last step's output",
			events: withIDs(
				started("Order", "in"),
				scheduled("reserve", 1, "in"), completed("reserve", 1, "reserved"),
				scheduled("charge", 2, "reserved"), completed("charge", 2, "paid"),
				scheduled("ship", 3, "paid"), completed("ship", 3, "tracking-123"),
			),
			wantType: DecideCompleteWorkflow, wantData: "tracking-123",
		},
		{
			name:     "step failed → fail the workflow (no retry policy in Phase 1)",
			events:   withIDs(started("Order", "in"), scheduled("reserve", 1, "in"), failed("reserve", 1, "out of stock")),
			wantType: DecideFailWorkflow, wantData: "out of stock",
		},
		{
			name: "already completed → no-op",
			events: withIDs(
				started("Order", "in"),
				scheduled("reserve", 1, "in"), completed("reserve", 1, "reserved"),
				scheduled("charge", 2, "reserved"), completed("charge", 2, "paid"),
				scheduled("ship", 3, "paid"), completed("ship", 3, "tracking-123"),
				wfCompleted("tracking-123"),
			),
			wantType: DecideNoop,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Decide(orderDef, tt.events)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Type != tt.wantType {
				t.Fatalf("decision type: got %v, want %v", got.Type, tt.wantType)
			}
			switch tt.wantType {
			case DecideScheduleStep:
				if got.StepName != tt.wantName || got.Sequence != tt.wantSeq || string(got.Input) != tt.wantData {
					t.Errorf("schedule: got {%q seq=%d in=%q}, want {%q seq=%d in=%q}",
						got.StepName, got.Sequence, got.Input, tt.wantName, tt.wantSeq, tt.wantData)
				}
			case DecideCompleteWorkflow:
				if string(got.Output) != tt.wantData {
					t.Errorf("complete output: got %q, want %q", got.Output, tt.wantData)
				}
			case DecideFailWorkflow:
				if got.Error != tt.wantData {
					t.Errorf("fail error: got %q, want %q", got.Error, tt.wantData)
				}
			}
		})
	}
}

// TestDecideEmptyDefinitionCompletesImmediately covers a started workflow whose
// plan has no steps: there is nothing to schedule, so it completes at once.
func TestDecideEmptyDefinitionCompletesImmediately(t *testing.T) {
	got, err := Decide(WorkflowDefinition{Type: "Noop"}, withIDs(started("Noop", "in")))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Type != DecideCompleteWorkflow {
		t.Fatalf("type: got %v, want COMPLETE_WORKFLOW", got.Type)
	}
}

// TestDriveToCompletion runs the full decision loop the way the engine will:
// record WorkflowStarted, then repeatedly Decide and apply the result —
// simulating a worker that succeeds — until the workflow reaches a terminal
// state. It proves the pure decision function composes into a complete run.
func TestDriveToCompletion(t *testing.T) {
	var hist []*history.HistoryEvent
	apply := func(ev *history.HistoryEvent) {
		ev.EventId = int64(len(hist) + 1)
		hist = append(hist, ev)
	}

	apply(started(orderDef.Type, "order-7"))

	for range 100 { // bound the loop so a logic bug can't hang the test
		d, err := Decide(orderDef, hist)
		if err != nil {
			t.Fatalf("decide: %v", err)
		}
		if d.Type == DecideNoop {
			t.Fatalf("unexpected NOOP before terminal; history len=%d", len(hist))
		}
		if d.Type == DecideScheduleStep {
			apply(scheduled(d.StepName, d.Sequence, string(d.Input)))
			// A worker runs the step and reports success: output echoes the step.
			apply(completed(d.StepName, d.Sequence, "done:"+d.StepName))
			continue
		}
		if d.Type == DecideCompleteWorkflow {
			apply(wfCompleted(string(d.Output)))
			break
		}
		t.Fatalf("unexpected decision %v", d.Type)
	}

	st, err := Reconstruct(hist)
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if st.Status != StatusCompleted {
		t.Fatalf("final status: got %v, want COMPLETED", st.Status)
	}
	if string(st.Output) != "done:ship" {
		t.Errorf("final output: got %q, want done:ship", st.Output)
	}
	// started + 3×(scheduled+completed) + completed = 8 events.
	if len(hist) != 8 {
		t.Errorf("history length: got %d, want 8", len(hist))
	}
}

// TestDriveToFailure runs the same loop but the first step fails; the engine
// must fail the workflow rather than proceed.
func TestDriveToFailure(t *testing.T) {
	hist := withIDs(
		started(orderDef.Type, "order-7"),
		scheduled("reserve", 1, "order-7"),
		failed("reserve", 1, "out of stock"),
	)

	d, err := Decide(orderDef, hist)
	if err != nil {
		t.Fatalf("decide: %v", err)
	}
	if d.Type != DecideFailWorkflow || d.Error != "out of stock" {
		t.Fatalf("got %v err=%q, want FAIL_WORKFLOW err=%q", d.Type, d.Error, "out of stock")
	}

	hist = append(hist, wfFailed(d.Error))
	hist[len(hist)-1].EventId = int64(len(hist))

	// After recording the failure, the workflow is terminal and Decide is a no-op.
	d, err = Decide(orderDef, hist)
	if err != nil {
		t.Fatalf("decide after fail: %v", err)
	}
	if d.Type != DecideNoop {
		t.Fatalf("post-terminal decision: got %v, want NOOP", d.Type)
	}

	st, _ := Reconstruct(hist)
	if st.Status != StatusFailed || st.Error != "out of stock" {
		t.Errorf("final state: got %v err=%q, want FAILED err=%q", st.Status, st.Error, "out of stock")
	}
}

// Example shows the decision loop's shape for docs; it is not a strict test.
func ExampleDecide() {
	def := WorkflowDefinition{Type: "Order", Steps: Activities("reserve", "charge")}
	d, _ := Decide(def, []*history.HistoryEvent{started("Order", "in")})
	fmt.Printf("%v %s\n", d.Type, d.StepName)
	// Output: SCHEDULE_STEP reserve
}
