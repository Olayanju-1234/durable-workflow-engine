package server

import (
	"reflect"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// A Go SDK worker sends CommandFromDecision; the engine applies
// decisionFromCommand. The pair must round-trip every decision type.
func TestCommandRoundTrip(t *testing.T) {
	for _, d := range []state.Decision{
		{Type: state.DecideScheduleStep, StepName: "charge", Sequence: 2, Input: []byte("in")},
		{Type: state.DecideStartTimer, StepName: "wait", Sequence: 3, Input: []byte("in"), Delay: 1500 * time.Millisecond},
		{Type: state.DecideAwaitSignal, StepName: "gate", Sequence: 4, Signal: "approved"},
		{Type: state.DecideCompleteWorkflow, Output: []byte("out")},
		{Type: state.DecideFailWorkflow, Error: "boom"},
		{Type: state.DecideRecordMarker, StepName: "v1-fix", Sequence: 5, Marker: state.MarkerVersion, Output: []byte("2")},
		{Type: state.DecideNoop},
	} {
		got, err := decisionFromCommand(CommandFromDecision(d))
		if err != nil {
			t.Fatalf("%v: %v", d.Type, err)
		}
		if !reflect.DeepEqual(got, d) {
			t.Errorf("round trip changed the decision:\n got %+v\nwant %+v", got, d)
		}
	}
}
