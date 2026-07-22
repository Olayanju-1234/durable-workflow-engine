package state

import (
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

// --- tiny builders to keep hand-built histories readable -------------------

func started(typ string, input string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{WorkflowId: "wf", WorkflowType: typ, Input: []byte(input)},
	}}
}

func scheduled(name string, seq int64, input string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
		StepScheduled: &history.StepScheduledEvent{StepName: name, Sequence: seq, Input: []byte(input)},
	}}
}

func completed(name string, seq int64, output string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
		StepCompleted: &history.StepCompletedEvent{StepName: name, Sequence: seq, Output: []byte(output)},
	}}
}

func failed(name string, seq int64, errMsg string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
		StepFailed: &history.StepFailedEvent{StepName: name, Sequence: seq, Error: errMsg},
	}}
}

func wfCompleted(output string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowCompleted{
		WorkflowCompleted: &history.WorkflowCompletedEvent{Output: []byte(output)},
	}}
}

func wfFailed(errMsg string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowFailed{
		WorkflowFailed: &history.WorkflowFailedEvent{Error: errMsg},
	}}
}

func timerStarted(name string, seq int64, fireAt int64, input string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_TimerStarted{
		TimerStarted: &history.TimerStartedEvent{StepName: name, Sequence: seq, FireAt: fireAt, Input: []byte(input)},
	}}
}

func timerFired(name string, seq int64) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_TimerFired{
		TimerFired: &history.TimerFiredEvent{StepName: name, Sequence: seq},
	}}
}

func signalReceived(name, payload string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_SignalReceived{
		SignalReceived: &history.SignalReceivedEvent{SignalName: name, Payload: []byte(payload)},
	}}
}

// withIDs stamps monotonic event ids so LastEventID assertions are meaningful.
func withIDs(evs ...*history.HistoryEvent) []*history.HistoryEvent {
	for i, ev := range evs {
		ev.EventId = int64(i + 1)
	}
	return evs
}

// --- reconstruction: status transitions ------------------------------------

func TestReconstructStatus(t *testing.T) {
	tests := []struct {
		name       string
		events     []*history.HistoryEvent
		wantStatus Status
		wantSteps  int
	}{
		{"empty", nil, StatusNotStarted, 0},
		{"just started", withIDs(started("T", "in")), StatusRunning, 0},
		{"step in flight", withIDs(started("T", "in"), scheduled("a", 1, "in")), StatusWaiting, 1},
		{"step completed, awaiting next decision", withIDs(started("T", "in"), scheduled("a", 1, "in"), completed("a", 1, "out")), StatusRunning, 1},
		{"step failed, awaiting next decision", withIDs(started("T", "in"), scheduled("a", 1, "in"), failed("a", 1, "boom")), StatusRunning, 1},
		{"workflow completed", withIDs(started("T", "in"), scheduled("a", 1, "in"), completed("a", 1, "out"), wfCompleted("final")), StatusCompleted, 1},
		{"workflow failed", withIDs(started("T", "in"), scheduled("a", 1, "in"), failed("a", 1, "boom"), wfFailed("boom")), StatusFailed, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st, err := Reconstruct(tt.events)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if st.Status != tt.wantStatus {
				t.Errorf("status: got %v, want %v", st.Status, tt.wantStatus)
			}
			if len(st.Steps) != tt.wantSteps {
				t.Errorf("step count: got %d, want %d", len(st.Steps), tt.wantSteps)
			}
		})
	}
}

// TestReconstructMultiStep proves the fold captures per-step results and
// terminal output across a realistic three-step run.
func TestReconstructMultiStep(t *testing.T) {
	events := withIDs(
		started("Order", `{"id":7}`),
		scheduled("reserve", 1, `{"id":7}`),
		completed("reserve", 1, "reserved"),
		scheduled("charge", 2, "reserved"),
		completed("charge", 2, "paid"),
		scheduled("ship", 3, "paid"),
		completed("ship", 3, "tracking-123"),
		wfCompleted("tracking-123"),
	)

	st, err := Reconstruct(events)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.Status != StatusCompleted {
		t.Fatalf("status: got %v, want COMPLETED", st.Status)
	}
	if st.WorkflowType != "Order" {
		t.Errorf("type: got %q, want Order", st.WorkflowType)
	}
	if string(st.Output) != "tracking-123" {
		t.Errorf("output: got %q, want tracking-123", st.Output)
	}
	if st.LastEventID != 8 {
		t.Errorf("last event id: got %d, want 8", st.LastEventID)
	}
	if len(st.Steps) != 3 {
		t.Fatalf("steps: got %d, want 3", len(st.Steps))
	}
	for i, want := range []struct {
		name   string
		output string
	}{{"reserve", "reserved"}, {"charge", "paid"}, {"ship", "tracking-123"}} {
		s := st.Steps[i]
		if s.Name != want.name || s.Status != StepCompleted || string(s.Output) != want.output {
			t.Errorf("step %d: got {%q %v %q}, want {%q COMPLETED %q}", i, s.Name, s.Status, s.Output, want.name, want.output)
		}
		if s.Sequence != int64(i+1) {
			t.Errorf("step %d sequence: got %d, want %d", i, s.Sequence, i+1)
		}
	}
}

// --- reconstruction: timers and signals ------------------------------------

// TestReconstructTimerStep proves a timer step is WAITING while pending (with its
// durable FireAt reconstructed) and, once fired, completes with its input passed
// through as output so the pipeline continues.
func TestReconstructTimerStep(t *testing.T) {
	const fireAt = int64(1_700_000_000_000_000_000)

	// Pending timer → WAITING, FireAt available for recovery re-arm.
	pending := withIDs(started("T", "in"), timerStarted("wait", 1, fireAt, "carry"))
	st, err := Reconstruct(pending)
	if err != nil {
		t.Fatalf("reconstruct pending timer: %v", err)
	}
	if st.Status != StatusWaiting {
		t.Fatalf("pending timer status: got %v, want WAITING", st.Status)
	}
	if tail := st.Tail(); tail.FireAt != fireAt {
		t.Errorf("timer FireAt: got %d, want %d", tail.FireAt, fireAt)
	}

	// Fired timer → RUNNING again, output == input (a sleep produces no new data).
	fired := withIDs(started("T", "in"), timerStarted("wait", 1, fireAt, "carry"), timerFired("wait", 1))
	st, err = Reconstruct(fired)
	if err != nil {
		t.Fatalf("reconstruct fired timer: %v", err)
	}
	if st.Status != StatusRunning {
		t.Fatalf("fired timer status: got %v, want RUNNING", st.Status)
	}
	tail := st.Tail()
	if tail.Status != StepCompleted || string(tail.Output) != "carry" {
		t.Errorf("fired timer step: got {%v %q}, want {COMPLETED %q}", tail.Status, tail.Output, "carry")
	}
}

// TestReconstructBuffersSignals proves signals are folded into State.Signals in
// arrival order and do not disturb step tracking (a signal can arrive mid-step).
func TestReconstructBuffersSignals(t *testing.T) {
	events := withIDs(
		started("T", "in"),
		signalReceived("approve", "yes"),
		scheduled("a", 1, "in"),
		signalReceived("cancel", "no"),
	)
	st, err := Reconstruct(events)
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if st.Status != StatusWaiting {
		t.Errorf("status: got %v, want WAITING (step a in flight)", st.Status)
	}
	if len(st.Signals) != 2 {
		t.Fatalf("signals: got %d, want 2", len(st.Signals))
	}
	if st.Signals[0].Name != "approve" || string(st.Signals[0].Payload) != "yes" ||
		st.Signals[1].Name != "cancel" || string(st.Signals[1].Payload) != "no" {
		t.Errorf("signal buffer order/content wrong: %+v", st.Signals)
	}
}

// TestDecideTimerAndSignal walks a mixed pipeline (activity → sleep → await signal
// → activity) and asserts Decide emits the right decision at each boundary, with
// input chaining flowing across the timer and the signal.
func TestDecideTimerAndSignal(t *testing.T) {
	def := WorkflowDefinition{Type: "Mix", Steps: []StepDef{
		Activity("reserve"),
		Sleep("wait", 2*time.Hour),
		AwaitSignal("approval", "approve"),
		Activity("ship"),
	}}

	// reserve done → start the timer, carrying reserve's output as the timer input.
	h := withIDs(started("Mix", "in"), scheduled("reserve", 1, "in"), completed("reserve", 1, "reserved"))
	d, err := Decide(def, h)
	if err != nil {
		t.Fatalf("decide after reserve: %v", err)
	}
	if d.Type != DecideStartTimer || d.StepName != "wait" || d.Sequence != 2 || d.Delay != 2*time.Hour || string(d.Input) != "reserved" {
		t.Fatalf("start timer: got %+v", d)
	}

	// timer fired → await the signal, still carrying "reserved".
	h = withIDs(append(h, timerStarted("wait", 2, 123, "reserved"), timerFired("wait", 2))...)
	d, err = Decide(def, h)
	if err != nil {
		t.Fatalf("decide after timer: %v", err)
	}
	if d.Type != DecideAwaitSignal || d.StepName != "approval" || d.Sequence != 3 || d.Signal != "approve" || string(d.Input) != "reserved" {
		t.Fatalf("await signal: got %+v", d)
	}

	// signal delivered (recorded by the engine as StepScheduled+StepCompleted with
	// the payload) → schedule the final activity, chaining the signal payload.
	h = withIDs(append(h, scheduled("approval", 3, "reserved"), completed("approval", 3, "approved!"))...)
	d, err = Decide(def, h)
	if err != nil {
		t.Fatalf("decide after signal: %v", err)
	}
	if d.Type != DecideScheduleStep || d.StepName != "ship" || d.Sequence != 4 || string(d.Input) != "approved!" {
		t.Fatalf("schedule ship: got %+v", d)
	}
}

// TestReconstructRetry proves a re-scheduled sequence after a failure re-opens the
// same step (another attempt) instead of creating a new one, so a retried activity
// stays a single logical step whose Attempts count grows.
func TestReconstructRetry(t *testing.T) {
	// charge fails, is retried (same sequence), then succeeds.
	events := withIDs(
		started("Order", "in"),
		scheduled("charge", 1, "in"),
		failed("charge", 1, "timeout"),
		scheduled("charge", 1, "in"), // retry: same sequence
		completed("charge", 1, "paid"),
	)
	st, err := Reconstruct(events)
	if err != nil {
		t.Fatalf("reconstruct: %v", err)
	}
	if len(st.Steps) != 1 {
		t.Fatalf("steps: got %d, want 1 (retry re-opens, not appends)", len(st.Steps))
	}
	step := st.Steps[0]
	if step.Status != StepCompleted || string(step.Output) != "paid" {
		t.Errorf("step: got {%s %q}, want {COMPLETED paid}", step.Status, step.Output)
	}
	if step.Attempts != 2 {
		t.Errorf("attempts: got %d, want 2", step.Attempts)
	}
	if st.Status != StatusRunning { // one step done, awaiting next decision
		t.Errorf("status: got %s, want RUNNING", st.Status)
	}
}

// --- reconstruction: a corrupt/inconsistent log is rejected ----------------

func TestReconstructRejectsBadHistory(t *testing.T) {
	tests := []struct {
		name    string
		events  []*history.HistoryEvent
		wantSub string
	}{
		{"started not first", withIDs(scheduled("a", 1, "x"), started("T", "in")), "before WorkflowStarted"},
		{"completion with nothing in flight", withIDs(started("T", "in"), completed("a", 1, "out")), "no step in flight"},
		{"double schedule without result", withIDs(started("T", "in"), scheduled("a", 1, "x"), scheduled("b", 2, "y")), "still in flight"},
		{"sequence gap", withIDs(started("T", "in"), scheduled("a", 1, "x"), completed("a", 1, "o"), scheduled("b", 3, "y")), "want 2"},
		{"result sequence mismatch", withIDs(started("T", "in"), scheduled("a", 1, "x"), completed("a", 2, "o")), "in-flight step has 1"},
		{"result name mismatch", withIDs(started("T", "in"), scheduled("a", 1, "x"), completed("b", 1, "o")), "in-flight step is \"a\""},
		{"event after terminal", withIDs(started("T", "in"), wfFailed("boom"), scheduled("a", 1, "x")), "follows a terminal event"},
		{"complete with step in flight", withIDs(started("T", "in"), scheduled("a", 1, "x"), wfCompleted("out")), "still in flight"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Reconstruct(tt.events)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantSub)
			}
			if !strings.Contains(err.Error(), tt.wantSub) {
				t.Errorf("error %q does not contain %q", err.Error(), tt.wantSub)
			}
		})
	}
}
