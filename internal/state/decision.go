package state

import (
	"fmt"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

// StepKind is what a workflow step does when the engine reaches it.
//
//	KindActivity → dispatch to a worker and wait for its result (the Phase-1 default).
//	KindTimer    → pause durably until a deadline (a Sleep); no worker involved.
//	KindSignal   → pause until a matching external signal arrives (a WaitForEvent).
//	KindMarker   → a recorded value (version choice, side effect); complete on arrival.
type StepKind int

const (
	KindActivity StepKind = iota
	KindTimer
	KindSignal
	KindMarker
)

func (k StepKind) String() string {
	switch k {
	case KindActivity:
		return "ACTIVITY"
	case KindTimer:
		return "TIMER"
	case KindSignal:
		return "SIGNAL"
	case KindMarker:
		return "MARKER"
	default:
		return fmt.Sprintf("StepKind(%d)", int(k))
	}
}

// Marker kinds a KindMarker step can record.
const (
	MarkerVersion    = "version"     // workflow.GetVersion: the version an execution runs
	MarkerSideEffect = "side_effect" // workflow.SideEffect: a recorded result
)

// RetryPolicy controls automatic re-execution of a failed activity: the
// workflow sees the error only once attempts are exhausted. MaxAttempts counts
// the first try (1 = no retry); Backoff is the first delay, doubled after each
// further failure and capped at MaxBackoff (zero = no cap).
type RetryPolicy struct {
	MaxAttempts int
	Backoff     time.Duration
	MaxBackoff  time.Duration
}

// Delay is how long to wait before retrying after the given attempt failed
// (attempt 1 waits Backoff).
func (p RetryPolicy) Delay(attempt int) time.Duration {
	d := p.Backoff
	for i := 1; i < attempt; i++ {
		d *= 2
		if p.MaxBackoff > 0 && d >= p.MaxBackoff {
			return p.MaxBackoff
		}
	}
	return d
}

// StepDef is one planned step in a WorkflowDefinition. The zero value is an
// activity, so a plain list of names still describes a linear activity pipeline.
type StepDef struct {
	Name   string
	Kind   StepKind
	Delay  time.Duration // KindTimer: how long to sleep from when the step is reached
	Signal string        // KindSignal: the signal name this step waits for
}

// Activity, Sleep, and AwaitSignal are the constructors for the three step kinds;
// Activities builds a linear pipeline of activity steps from bare names (used by
// the many workflows that are just "run these steps in order").
func Activity(name string) StepDef { return StepDef{Name: name, Kind: KindActivity} }
func Sleep(name string, d time.Duration) StepDef {
	return StepDef{Name: name, Kind: KindTimer, Delay: d}
}
func AwaitSignal(name, signal string) StepDef {
	return StepDef{Name: name, Kind: KindSignal, Signal: signal}
}

func Activities(names ...string) []StepDef {
	steps := make([]StepDef, len(names))
	for i, n := range names {
		steps[i] = Activity(n)
	}
	return steps
}

// WorkflowDefinition is the plan the engine executes: an ordered list of steps.
// It is the Phase-1/2 stand-in for "workflow code" — enough to drive a linear
// pipeline of activities, sleeps, and signal waits. In Phase 2's replay engine the
// steps come from an actual Go function's execution; the state reconstruction and
// decision logic here stay unchanged.
type WorkflowDefinition struct {
	Type  string
	Steps []StepDef
}

// Decide makes a WorkflowDefinition a "planner": the engine can ask any planner
// (this data-driven one, or Phase-2's replay engine over real Go code) for the next
// action without knowing which it is. It simply delegates to the package Decide.
func (d WorkflowDefinition) Decide(events []*history.HistoryEvent) (Decision, error) {
	return Decide(d, events)
}

// DecisionType is the engine's next action after replaying history.
type DecisionType int

const (
	DecideNoop             DecisionType = iota // nothing to do: terminal, or a step is in flight
	DecideScheduleStep                         // dispatch the next activity to a worker
	DecideStartTimer                           // begin a durable sleep for a timer step
	DecideAwaitSignal                          // park the workflow until a signal arrives
	DecideCompleteWorkflow                     // all steps done: record success
	DecideFailWorkflow                         // a step failed: record failure
	DecideRecordMarker                         // record a marker step, then decide again
)

func (d DecisionType) String() string {
	switch d {
	case DecideNoop:
		return "NOOP"
	case DecideScheduleStep:
		return "SCHEDULE_STEP"
	case DecideStartTimer:
		return "START_TIMER"
	case DecideAwaitSignal:
		return "AWAIT_SIGNAL"
	case DecideCompleteWorkflow:
		return "COMPLETE_WORKFLOW"
	case DecideFailWorkflow:
		return "FAIL_WORKFLOW"
	case DecideRecordMarker:
		return "RECORD_MARKER"
	default:
		return fmt.Sprintf("DecisionType(%d)", int(d))
	}
}

// Decision is what the engine should do next. It carries only the data the caller
// needs to record the corresponding event; it performs no I/O and reads no clock.
// The caller (the gRPC server) turns a Decision into a HistoryEvent — assigning the
// event id, timestamp, and (for a timer) the absolute fire time — and acts on it.
type Decision struct {
	Type DecisionType

	// Set when Type is DecideScheduleStep, DecideStartTimer, DecideAwaitSignal or
	// DecideRecordMarker.
	StepName string
	Sequence int64
	Input    []byte

	// Set when Type == DecideStartTimer: the delay from now until the timer fires.
	// The caller adds it to the current time to get the absolute deadline, keeping
	// Decide pure (no clock reads inside replay).
	Delay time.Duration

	// Set when Type == DecideAwaitSignal: the signal name the step waits for.
	Signal string

	// Optionally set when Type == DecideScheduleStep: the retry policy the
	// workflow code chose, recorded on the step. Nil means the engine's.
	Retry *RetryPolicy

	// Set when Type == DecideRecordMarker: the marker kind ("version" or
	// "side_effect"); its data rides in Output.
	Marker string

	// Set when Type == DecideCompleteWorkflow or DecideRecordMarker.
	Output []byte

	// Set when Type == DecideFailWorkflow.
	Error string
}

// Decide replays history and returns the single next action for the workflow. It
// is pure: same definition + same history → same decision, every time. That
// determinism is what lets the engine re-run Decide after a crash and resume
// without duplicating work — and it is why Decide never reads the clock (timer
// deadlines are computed by the caller from the returned Delay).
//
// The caller is expected to have already recorded WorkflowStarted before the first
// Decide call; on an empty history Decide is a no-op.
//
// Phase-1/2 semantics: steps run strictly in order, one at a time. A step that
// fails fails the whole workflow — retry policies arrive in Week 7. Each step's
// input is the previous step's output (a timer/signal step passes its input
// through), forming a simple pipeline.
func Decide(def WorkflowDefinition, events []*history.HistoryEvent) (Decision, error) {
	st, err := Reconstruct(events)
	if err != nil {
		return Decision{}, err
	}
	return def.DecideState(st)
}

// DecideState is Decide for a caller that already holds the replayed state,
// such as the engine, which keeps it in memory.
func (def WorkflowDefinition) DecideState(st *State) (Decision, error) {

	// Terminal, in-flight, or not-yet-started: the engine has nothing to decide.
	if st.Status != StatusRunning {
		return Decision{Type: DecideNoop}, nil
	}

	// Guard against a definition that drifted from the recorded history (steps
	// renamed, reordered, or removed across a deploy): replaying old history against
	// a new plan would silently execute a different workflow. Phase 1/2 is strictly
	// sequential, so a step recorded at sequence N must be def.Steps[N-1]. Real
	// versioning (workflow.Version) arrives in Phase 5.
	for i := range st.Steps {
		seq := st.Steps[i].Sequence
		if seq < 1 || int(seq) > len(def.Steps) {
			return Decision{}, fmt.Errorf("state: step %q at sequence %d is outside definition %q (%d steps)",
				st.Steps[i].Name, seq, def.Type, len(def.Steps))
		}
		if want := def.Steps[seq-1].Name; st.Steps[i].Name != want {
			return Decision{}, fmt.Errorf("state: definition drift: history step %d is %q but definition %q has %q there",
				seq, st.Steps[i].Name, def.Type, want)
		}
	}

	tail := st.Tail()
	if tail != nil && tail.Status == StepFailed {
		return Decision{Type: DecideFailWorkflow, Error: tail.Error}, nil
	}

	// In RUNNING state with no in-flight or failed step, every recorded step has
	// completed, so the tail's sequence is the count of finished steps and also the
	// 0-based index of the next step to schedule.
	var done int64
	if tail != nil {
		done = tail.Sequence
	}
	if done >= int64(len(def.Steps)) {
		out := st.Input
		if tail != nil {
			out = tail.Output
		}
		return Decision{Type: DecideCompleteWorkflow, Output: out}, nil
	}

	input := st.Input
	if tail != nil {
		input = tail.Output // chain the previous step's output into the next
	}
	next := def.Steps[done]
	base := Decision{StepName: next.Name, Sequence: done + 1, Input: input}
	switch next.Kind {
	case KindTimer:
		base.Type = DecideStartTimer
		base.Delay = next.Delay
	case KindSignal:
		base.Type = DecideAwaitSignal
		base.Signal = next.Signal
	default:
		base.Type = DecideScheduleStep
	}
	return base, nil
}
