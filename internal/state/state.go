package state

import (
	"fmt"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

// Status is the lifecycle state of a workflow, reconstructed from its history.
//
//	NotStarted → no history yet.
//	Running    → started, no step in flight; the engine owes a decision.
//	Waiting    → a step is scheduled and awaiting a worker result.
//	Completed  → terminal: WorkflowCompleted was recorded.
//	Failed     → terminal: WorkflowFailed was recorded.
//	TimedOut   → terminal; reserved for Phase 2 timers (no event produces it yet).
type Status int

const (
	StatusNotStarted Status = iota
	StatusRunning
	StatusWaiting
	StatusCompleted
	StatusFailed
	StatusTimedOut
)

func (s Status) String() string {
	switch s {
	case StatusNotStarted:
		return "NOT_STARTED"
	case StatusRunning:
		return "RUNNING"
	case StatusWaiting:
		return "WAITING"
	case StatusCompleted:
		return "COMPLETED"
	case StatusFailed:
		return "FAILED"
	case StatusTimedOut:
		return "TIMED_OUT"
	default:
		return fmt.Sprintf("Status(%d)", int(s))
	}
}

// IsTerminal reports whether the workflow has reached an end state.
func (s Status) IsTerminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusTimedOut
}

// StepStatus is the lifecycle of a single step within a workflow.
type StepStatus int

const (
	StepScheduled StepStatus = iota // dispatched to a worker, result not yet recorded
	StepCompleted                   // worker reported success
	StepFailed                      // worker reported failure
)

func (s StepStatus) String() string {
	switch s {
	case StepScheduled:
		return "SCHEDULED"
	case StepCompleted:
		return "COMPLETED"
	case StepFailed:
		return "FAILED"
	default:
		return fmt.Sprintf("StepStatus(%d)", int(s))
	}
}

// Step is the reconstructed view of one workflow step. Sequence is 1-based and
// contiguous: the engine schedules steps strictly in order (Phase 1 is
// sequential; parallel fan-out arrives with the replay engine in Phase 2).
type Step struct {
	Name     string
	Sequence int64
	Kind     StepKind // reconstructed from the opening event, so runtime needs no definition
	Status   StepStatus
	Input    []byte
	Output   []byte       // set once Status == StepCompleted
	Error    string       // set once Status == StepFailed
	FireAt   int64        // timer steps only: unix-nanos deadline (from TimerStarted), for recovery re-arm
	Signal   string       // signal steps only: the awaited signal name
	Marker   string       // marker steps only: "version" or "side_effect"
	Attempts int          // activity steps: how many times scheduled (>1 means it was retried)
	Retry    *RetryPolicy // activity steps: the policy recorded with the step, if any
}

// ReceivedSignal is one external signal delivered to a workflow, in arrival order.
// Signals are buffered here so a wait can be matched to a signal that arrived
// earlier (see Reconstruct); the engine decides which wait consumes which signal.
type ReceivedSignal struct {
	Name    string
	Payload []byte
}

// State is a workflow's current position, folded from its entire history. It is
// the output of replay: a pure function of the event log and nothing else.
type State struct {
	Status       Status
	WorkflowType string
	Input        []byte
	Steps        []Step           // in schedule order; Steps[i].Sequence == i+1
	Signals      []ReceivedSignal // external signals, in arrival order (buffered)
	Output       []byte           // workflow output, set when Status == StatusCompleted
	Error        string           // workflow error, set when Status == StatusFailed
	Terminated   bool             // the failure was an operator termination, not workflow code
	LastEventID  int64            // EventId of the final event consumed
}

// Tail returns the most recently scheduled step, or nil if none exist yet.
func (s *State) Tail() *Step {
	if len(s.Steps) == 0 {
		return nil
	}
	return &s.Steps[len(s.Steps)-1]
}

// Reconstruct folds a workflow's full history into its current State. It is the
// heart of durable execution: because state is derived purely from the log,
// crash recovery is just calling Reconstruct again on the same events.
//
// It also validates structural invariants and returns an error on an
// inconsistent log (e.g. a completion for a step that was never scheduled, or
// events after a terminal one). A durable engine must refuse to trust a corrupt
// history rather than reconstruct a plausible-but-wrong state.
func Reconstruct(events []*history.HistoryEvent) (*State, error) {
	st := &State{Status: StatusNotStarted}
	openIdx := -1       // index in st.Steps of the in-flight step, or -1
	lastSeq := int64(0) // highest sequence scheduled so far

	for i, ev := range events {
		if st.Status.IsTerminal() {
			return nil, fmt.Errorf("state: event %d (id %d) follows a terminal event", i, ev.GetEventId())
		}
		st.LastEventID = ev.GetEventId()

		switch attr := ev.GetAttributes().(type) {
		case *history.HistoryEvent_WorkflowStarted:
			if i != 0 {
				return nil, fmt.Errorf("state: WorkflowStarted at position %d, must be first event", i)
			}
			ws := attr.WorkflowStarted
			st.Status = StatusRunning
			st.WorkflowType = ws.GetWorkflowType()
			st.Input = ws.GetInput()

		case *history.HistoryEvent_StepScheduled:
			ss := attr.StepScheduled
			if st.Status == StatusNotStarted {
				return nil, fmt.Errorf("state: StepScheduled at position %d before WorkflowStarted", i)
			}
			if openIdx != -1 {
				return nil, fmt.Errorf("state: StepScheduled %q at position %d while step %q is still in flight",
					ss.GetStepName(), i, st.Steps[openIdx].Name)
			}
			seq := ss.GetSequence()
			// Retry: re-scheduling the same sequence after a failure re-opens that step
			// (another attempt) rather than starting a new one. Attempts tracks the count.
			if n := len(st.Steps); n > 0 {
				last := &st.Steps[n-1]
				if seq == last.Sequence && last.Status == StepFailed && last.Kind == KindActivity && last.Name == ss.GetStepName() {
					last.Status = StepScheduled
					last.Error = ""
					last.Input = ss.GetInput()
					last.Attempts++
					if p := RetryPolicyFromProto(ss.GetRetryPolicy()); p != nil {
						last.Retry = p
					}
					openIdx = n - 1
					break
				}
			}
			if seq != lastSeq+1 {
				return nil, fmt.Errorf("state: StepScheduled at position %d has sequence %d, want %d", i, seq, lastSeq+1)
			}
			// A StepScheduled opens either an activity (has a worker task) or a signal
			// wait (no task); the awaits_signal flag distinguishes them from history.
			kind := KindActivity
			if ss.GetAwaitsSignal() {
				kind = KindSignal
			}
			st.Steps = append(st.Steps, Step{
				Name:     ss.GetStepName(),
				Sequence: seq,
				Kind:     kind,
				Status:   StepScheduled,
				Input:    ss.GetInput(),
				Signal:   ss.GetSignalName(),
				Attempts: 1,
				Retry:    RetryPolicyFromProto(ss.GetRetryPolicy()),
			})
			openIdx = len(st.Steps) - 1
			lastSeq = seq

		case *history.HistoryEvent_TimerStarted:
			ts := attr.TimerStarted
			if st.Status == StatusNotStarted {
				return nil, fmt.Errorf("state: TimerStarted at position %d before WorkflowStarted", i)
			}
			if openIdx != -1 {
				return nil, fmt.Errorf("state: TimerStarted %q at position %d while step %q is still in flight",
					ts.GetStepName(), i, st.Steps[openIdx].Name)
			}
			seq := ts.GetSequence()
			if seq != lastSeq+1 {
				return nil, fmt.Errorf("state: TimerStarted at position %d has sequence %d, want %d", i, seq, lastSeq+1)
			}
			st.Steps = append(st.Steps, Step{
				Name:     ts.GetStepName(),
				Sequence: seq,
				Kind:     KindTimer,
				Status:   StepScheduled,
				Input:    ts.GetInput(),
				FireAt:   ts.GetFireAt(),
			})
			openIdx = len(st.Steps) - 1
			lastSeq = seq

		case *history.HistoryEvent_MarkerRecorded:
			mr := attr.MarkerRecorded
			if st.Status == StatusNotStarted {
				return nil, fmt.Errorf("state: MarkerRecorded at position %d before WorkflowStarted", i)
			}
			if openIdx != -1 {
				return nil, fmt.Errorf("state: MarkerRecorded %q at position %d while step %q is still in flight",
					mr.GetStepName(), i, st.Steps[openIdx].Name)
			}
			if seq := mr.GetSequence(); seq != lastSeq+1 {
				return nil, fmt.Errorf("state: MarkerRecorded at position %d has sequence %d, want %d", i, seq, lastSeq+1)
			}
			st.Steps = append(st.Steps, Step{
				Name:     mr.GetStepName(),
				Sequence: mr.GetSequence(),
				Kind:     KindMarker,
				Status:   StepCompleted,
				Output:   mr.GetData(),
				Marker:   mr.GetMarker(),
			})
			lastSeq = mr.GetSequence()

		case *history.HistoryEvent_StepCompleted:
			if err := checkOpenStep(st, openIdx, i, attr.StepCompleted.GetStepName(), attr.StepCompleted.GetSequence()); err != nil {
				return nil, err
			}
			st.Steps[openIdx].Status = StepCompleted
			st.Steps[openIdx].Output = attr.StepCompleted.GetOutput()
			openIdx = -1

		case *history.HistoryEvent_TimerFired:
			tf := attr.TimerFired
			if err := checkOpenStep(st, openIdx, i, tf.GetStepName(), tf.GetSequence()); err != nil {
				return nil, err
			}
			// A sleep produces no new data: its output is its input, so the pipeline
			// continues past the timer with the value that flowed into it.
			st.Steps[openIdx].Status = StepCompleted
			st.Steps[openIdx].Output = st.Steps[openIdx].Input
			openIdx = -1

		case *history.HistoryEvent_SignalReceived:
			if st.Status == StatusNotStarted {
				return nil, fmt.Errorf("state: SignalReceived at position %d before WorkflowStarted", i)
			}
			sr := attr.SignalReceived
			st.Signals = append(st.Signals, ReceivedSignal{Name: sr.GetSignalName(), Payload: sr.GetPayload()})

		case *history.HistoryEvent_StepFailed:
			if err := checkOpenStep(st, openIdx, i, attr.StepFailed.GetStepName(), attr.StepFailed.GetSequence()); err != nil {
				return nil, err
			}
			st.Steps[openIdx].Status = StepFailed
			st.Steps[openIdx].Error = attr.StepFailed.GetError()
			openIdx = -1

		case *history.HistoryEvent_WorkflowCompleted:
			if openIdx != -1 {
				return nil, fmt.Errorf("state: WorkflowCompleted at position %d while step %q is still in flight", i, st.Steps[openIdx].Name)
			}
			st.Status = StatusCompleted
			st.Output = attr.WorkflowCompleted.GetOutput()

		case *history.HistoryEvent_WorkflowFailed:
			// Deliberately asymmetric with WorkflowCompleted above: a failure is
			// allowed while a step is still in flight, because timeouts and (Phase 3)
			// cancellation fail a workflow whose step never reported a result. The
			// open step simply stays SCHEDULED.
			st.Status = StatusFailed
			st.Error = attr.WorkflowFailed.GetError()
			st.Terminated = attr.WorkflowFailed.GetTerminated()

		default:
			return nil, fmt.Errorf("state: event %d has unknown or empty attributes", i)
		}
	}

	// No terminal event: the status is decided by whether a step is in flight.
	if !st.Status.IsTerminal() && st.Status != StatusNotStarted {
		if openIdx != -1 {
			st.Status = StatusWaiting
		} else {
			st.Status = StatusRunning
		}
	}
	return st, nil
}

// checkOpenStep validates that a step result (completion/failure) refers to the
// step currently in flight, by both sequence and name.
func checkOpenStep(st *State, openIdx, pos int, name string, seq int64) error {
	if openIdx == -1 {
		return fmt.Errorf("state: step result %q at position %d with no step in flight", name, pos)
	}
	open := st.Steps[openIdx]
	if open.Sequence != seq {
		return fmt.Errorf("state: step result at position %d has sequence %d, in-flight step has %d", pos, seq, open.Sequence)
	}
	if open.Name != name {
		return fmt.Errorf("state: step result at position %d names %q, in-flight step is %q", pos, name, open.Name)
	}
	return nil
}

// DeliverableSignal reports the payload that satisfies the in-flight step, if
// that step is a signal wait and a matching buffered signal has arrived.
// Matching is by arrival order: the k-th wait for signal S consumes the k-th
// received S, so a signal that arrived before the wait is delivered correctly.
func (s *State) DeliverableSignal() ([]byte, bool) {
	tail := s.Tail()
	if tail == nil || tail.Status != StepScheduled || tail.Kind != KindSignal {
		return nil, false
	}
	consumed := 0
	for i := 0; i < len(s.Steps)-1; i++ {
		if st := s.Steps[i]; st.Kind == KindSignal && st.Signal == tail.Signal && st.Status == StepCompleted {
			consumed++
		}
	}
	seen := 0
	for _, sig := range s.Signals {
		if sig.Name == tail.Signal {
			if seen == consumed {
				return sig.Payload, true
			}
			seen++
		}
	}
	return nil, false
}

// RetryPolicyFromProto converts a recorded policy; nil or unset gives nil.
func RetryPolicyFromProto(p *history.RetryPolicy) *RetryPolicy {
	if p == nil || p.GetMaxAttempts() == 0 {
		return nil
	}
	return &RetryPolicy{
		MaxAttempts: int(p.GetMaxAttempts()),
		Backoff:     time.Duration(p.GetInitialBackoffMs()) * time.Millisecond,
		MaxBackoff:  time.Duration(p.GetMaxBackoffMs()) * time.Millisecond,
	}
}

// ToProto is the recorded form of p; nil stays nil.
func (p *RetryPolicy) ToProto() *history.RetryPolicy {
	if p == nil {
		return nil
	}
	return &history.RetryPolicy{
		MaxAttempts:      int32(p.MaxAttempts),
		InitialBackoffMs: p.Backoff.Milliseconds(),
		MaxBackoffMs:     p.MaxBackoff.Milliseconds(),
	}
}
