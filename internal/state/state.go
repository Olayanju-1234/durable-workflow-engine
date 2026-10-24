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
//	TimedOut   → terminal: the engine ended it when its execution timeout passed.
//	Cancelled  → terminal: a client cancelled it.
//	ContinuedAsNew → terminal for this run: the workflow id goes on as a new run.
type Status int

const (
	StatusNotStarted Status = iota
	StatusRunning
	StatusWaiting
	StatusCompleted
	StatusFailed
	StatusTimedOut
	StatusCancelled
	StatusContinuedAsNew
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
	case StatusCancelled:
		return "CANCELLED"
	case StatusContinuedAsNew:
		return "CONTINUED_AS_NEW"
	default:
		return fmt.Sprintf("Status(%d)", int(s))
	}
}

// IsTerminal reports whether the workflow has reached an end state.
func (s Status) IsTerminal() bool {
	return s == StatusCompleted || s == StatusFailed || s == StatusTimedOut || s == StatusCancelled ||
		s == StatusContinuedAsNew
}

// StepStatus is the lifecycle of a single step within a workflow.
type StepStatus int

const (
	StepScheduled StepStatus = iota // dispatched to a worker, result not yet recorded
	StepCompleted                   // worker reported success
	StepFailed                      // worker reported failure
	StepCancelled                   // workflow code cancelled it before it had a result
)

func (s StepStatus) String() string {
	switch s {
	case StepScheduled:
		return "SCHEDULED"
	case StepCompleted:
		return "COMPLETED"
	case StepFailed:
		return "FAILED"
	case StepCancelled:
		return "CANCELLED"
	default:
		return fmt.Sprintf("StepStatus(%d)", int(s))
	}
}

// Step is the reconstructed view of one workflow step. Sequence is 1-based and
// contiguous: each new step takes the next sequence. Several steps may be in
// flight at once (parallel activities and timers).
type Step struct {
	Name     string
	Sequence int64
	Kind     StepKind // reconstructed from the opening event, so runtime needs no definition
	Status   StepStatus
	Input    []byte
	Output   []byte // set once Status == StepCompleted
	Error    string // set once Status == StepFailed (the reason, once StepCancelled)
	// The latest failure's kind and whether the worker said not to retry it.
	ErrorType    string
	NonRetryable bool
	FireAt       int64        // timer steps only: unix-nanos deadline (from TimerStarted), for recovery re-arm
	Signal       string       // signal steps only: the awaited signal name
	Marker       string       // marker steps only: "version" or "side_effect"
	Attempts     int          // activity steps: how many times scheduled (>1 means it was retried)
	Retry        *RetryPolicy // activity steps: the policy recorded with the step, if any
	// Activity steps: the timeouts recorded with the step (zero = none) and
	// when it was first scheduled (unix nanos), the start of schedule-to-close.
	StartToClose    time.Duration
	ScheduleToClose time.Duration
	ScheduledAt     int64
	// Retrying: the latest attempt failed and the engine will run another, so
	// the failure is not the step's result; workflow code still waits on it.
	Retrying bool
	// ResolvedEventID is the id of the event that gave the step its result
	// (zero while it has none). It orders results for WaitAny.
	ResolvedEventID int64
	// ResolvedAt is that event's timestamp (unix nanos): the workflow clock
	// moves to it when the code observes the result.
	ResolvedAt int64
}

// Open reports whether the step has no result yet: in flight, or retrying.
func (s *Step) Open() bool {
	return s.Status == StepScheduled || s.Retrying
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
	StartedAt    int64            // unix nanos of WorkflowStarted
	// RunID is this run's id: recorded on WorkflowStarted, or for a history
	// from before run ids, derived from its start time (LegacyRunID).
	RunID string
	// ContinuedFromRunID is the run this one continued from, if any.
	ContinuedFromRunID string
	// ContinuedAsNew describes the next run, once Status is StatusContinuedAsNew.
	ContinuedAsNew *history.ContinuedAsNew
	// ExecutionTimeout is how long after StartedAt the workflow times out
	// (zero = never).
	ExecutionTimeout time.Duration
	LastEventID      int64 // EventId of the final event consumed
	// LastEventResolves: the latest event gave a step its result (or started
	// the workflow), so the workflow code may have more to do.
	LastEventResolves bool
	// SignalRequests maps each signal idempotency key recorded to its signal
	// name (nil until one is).
	SignalRequests map[string]string
}

// Step returns the step with the given sequence, or nil.
func (s *State) Step(seq int64) *Step {
	if seq < 1 || seq > int64(len(s.Steps)) {
		return nil
	}
	return &s.Steps[seq-1]
}

// OwesDecision reports whether the workflow code must be asked what to do
// next: nothing is in flight, or a step just got its result.
func (s *State) OwesDecision() bool {
	return s.Status == StatusRunning || (s.Status == StatusWaiting && s.LastEventResolves)
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
	f := NewFolder()
	for _, ev := range events {
		if err := f.Apply(ev); err != nil {
			return nil, err
		}
	}
	return f.State(), nil
}

// Folder is Reconstruct one event at a time, so a caller that keeps a
// workflow's state in memory folds each new event in instead of replaying the
// whole history. After an error it is poisoned: the history is invalid.
type Folder struct {
	st       State
	open     int    // steps in flight (StepScheduled)
	retrying int    // steps whose failed attempt will be retried
	lastSeq  int64  // highest sequence scheduled so far
	n        int    // events applied
	format   uint32 // the newest history format any applied event needs
	err      error
}

// NewFolder starts from an empty history.
func NewFolder() *Folder {
	return &Folder{st: State{Status: StatusNotStarted}}
}

// State is the state after every event applied so far. It is updated in
// place by later Applies.
func (f *Folder) State() *State { return &f.st }

// Err is the error that poisoned the folder, if any.
func (f *Folder) Err() error { return f.err }

// Format is the oldest history format that reads every event applied so far.
func (f *Folder) Format() uint32 { return max(f.format, history.FormatBase) }

// FormatOf is the oldest history format that reads ev correctly as the next
// event: its stamp, or for events written before stamps existed, the format
// of the feature it uses. Pure: it does not apply ev.
func (f *Folder) FormatOf(ev *history.HistoryEvent) uint32 {
	need := max(ev.GetMinReaderFormat(), history.FormatBase)
	switch attr := ev.GetAttributes().(type) {
	case *history.HistoryEvent_MarkerRecorded:
		need = max(need, history.FormatMarkers)
	case *history.HistoryEvent_StepScheduled:
		if attr.StepScheduled.GetSequence() > f.lastSeq && f.open+f.retrying > 0 {
			need = max(need, history.FormatParallelSteps)
		}
	case *history.HistoryEvent_TimerStarted:
		if f.open+f.retrying > 0 {
			need = max(need, history.FormatParallelSteps)
		}
	case *history.HistoryEvent_StepFailed:
		sf := attr.StepFailed
		if s := f.st.Step(sf.GetSequence()); sf.GetCancelled() || (s != nil && s.Retrying && !sf.GetWillRetry()) {
			need = max(need, history.FormatClosedSteps)
		}
	case *history.HistoryEvent_WorkflowCompleted:
		if attr.WorkflowCompleted.GetContinuedAsNew() != nil {
			need = max(need, history.FormatRuns)
		}
	}
	return need
}

// LegacyRunID is the run id of a history recorded before run ids existed:
// its start time in hex, which is fixed once recorded.
func LegacyRunID(startedAt int64) string {
	return fmt.Sprintf("%016x", startedAt)
}

// Apply folds one more event into the state.
func (f *Folder) Apply(ev *history.HistoryEvent) error {
	if f.err != nil {
		return f.err
	}
	need := f.FormatOf(ev)
	if need > history.FormatVersion {
		f.err = history.TooNewError(need, fmt.Sprintf("event %d", ev.GetEventId()))
		return f.err
	}
	if ev.GetAttributes() == nil && len(ev.ProtoReflect().GetUnknown()) > 0 {
		// A kind of event this binary has no field for: a newer binary wrote it.
		f.err = fmt.Errorf("%w: event %d is of a kind this binary does not know (written by a newer workflowd); "+
			"this binary reads up to history format %d (docs/UPGRADING.md)",
			history.ErrFormatTooNew, ev.GetEventId(), history.FormatVersion)
		return f.err
	}
	if err := f.apply(ev); err != nil {
		f.err = err
		return err
	}
	f.format = max(f.format, need)
	f.n++
	return nil
}

func (f *Folder) apply(ev *history.HistoryEvent) error {
	st, i := &f.st, f.n
	if st.Status.IsTerminal() {
		return fmt.Errorf("state: event %d (id %d) follows a terminal event", i, ev.GetEventId())
	}
	if _, started := ev.GetAttributes().(*history.HistoryEvent_WorkflowStarted); !started && st.Status == StatusNotStarted {
		return fmt.Errorf("state: %s at position %d before WorkflowStarted", eventName(ev), i)
	}
	st.LastEventID = ev.GetEventId()
	resolves := false

	switch attr := ev.GetAttributes().(type) {
	case *history.HistoryEvent_WorkflowStarted:
		if i != 0 {
			return fmt.Errorf("state: WorkflowStarted at position %d, must be first event", i)
		}
		ws := attr.WorkflowStarted
		st.Status = StatusRunning
		st.WorkflowType = ws.GetWorkflowType()
		st.Input = ws.GetInput()
		st.StartedAt = ev.GetTimestamp()
		st.ExecutionTimeout = time.Duration(ws.GetExecutionTimeoutMs()) * time.Millisecond
		st.RunID, st.ContinuedFromRunID = ws.GetRunId(), ws.GetContinuedFromRunId()
		if st.RunID == "" {
			st.RunID = LegacyRunID(ev.GetTimestamp())
		}
		resolves = true

	case *history.HistoryEvent_StepScheduled:
		ss := attr.StepScheduled
		seq := ss.GetSequence()
		if seq >= 1 && seq <= f.lastSeq {
			// A retry re-opens a failed activity at its own sequence: another
			// attempt of the same step, not a new one.
			s := &st.Steps[seq-1]
			if s.Kind != KindActivity || s.Status != StepFailed || s.Name != ss.GetStepName() || ss.GetAwaitsSignal() {
				return fmt.Errorf("state: StepScheduled %q at position %d reuses sequence %d, which is not a failed activity %q",
					ss.GetStepName(), i, seq, s.Name)
			}
			if s.Retrying {
				f.retrying--
			}
			s.Status, s.Error, s.Retrying, s.ResolvedEventID, s.ResolvedAt = StepScheduled, "", false, 0, 0
			s.ErrorType, s.NonRetryable = "", false
			s.Input = ss.GetInput()
			s.Attempts++
			if p := RetryPolicyFromProto(ss.GetRetryPolicy()); p != nil {
				s.Retry = p
			}
			if d := msDuration(ss.GetStartToCloseTimeoutMs()); d > 0 {
				s.StartToClose = d
			}
			if d := msDuration(ss.GetScheduleToCloseTimeoutMs()); d > 0 {
				s.ScheduleToClose = d
			}
			f.open++
			break
		}
		if seq != f.lastSeq+1 {
			return fmt.Errorf("state: StepScheduled at position %d has sequence %d, want %d", i, seq, f.lastSeq+1)
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

			StartToClose:    msDuration(ss.GetStartToCloseTimeoutMs()),
			ScheduleToClose: msDuration(ss.GetScheduleToCloseTimeoutMs()),
			ScheduledAt:     ev.GetTimestamp(),
		})
		f.open++
		f.lastSeq = seq

	case *history.HistoryEvent_TimerStarted:
		ts := attr.TimerStarted
		if seq := ts.GetSequence(); seq != f.lastSeq+1 {
			return fmt.Errorf("state: TimerStarted at position %d has sequence %d, want %d", i, seq, f.lastSeq+1)
		}
		st.Steps = append(st.Steps, Step{
			Name:     ts.GetStepName(),
			Sequence: ts.GetSequence(),
			Kind:     KindTimer,
			Status:   StepScheduled,
			Input:    ts.GetInput(),
			FireAt:   ts.GetFireAt(),
		})
		f.open++
		f.lastSeq = ts.GetSequence()

	case *history.HistoryEvent_MarkerRecorded:
		mr := attr.MarkerRecorded
		if seq := mr.GetSequence(); seq != f.lastSeq+1 {
			return fmt.Errorf("state: MarkerRecorded at position %d has sequence %d, want %d", i, seq, f.lastSeq+1)
		}
		st.Steps = append(st.Steps, Step{
			Name:            mr.GetStepName(),
			Sequence:        mr.GetSequence(),
			Kind:            KindMarker,
			Status:          StepCompleted,
			Output:          mr.GetData(),
			Marker:          mr.GetMarker(),
			ResolvedEventID: ev.GetEventId(),
			ResolvedAt:      ev.GetTimestamp(),
		})
		f.lastSeq = mr.GetSequence()
		resolves = true

	case *history.HistoryEvent_StepCompleted:
		s, err := f.openStep(i, attr.StepCompleted.GetStepName(), attr.StepCompleted.GetSequence())
		if err != nil {
			return err
		}
		s.Status, s.Output, s.ResolvedEventID, s.ResolvedAt = StepCompleted, attr.StepCompleted.GetOutput(), ev.GetEventId(), ev.GetTimestamp()
		f.open--
		resolves = true

	case *history.HistoryEvent_TimerFired:
		s, err := f.openStep(i, attr.TimerFired.GetStepName(), attr.TimerFired.GetSequence())
		if err != nil {
			return err
		}
		// A sleep produces no new data: its output is its input, so the pipeline
		// continues past the timer with the value that flowed into it.
		s.Status, s.Output, s.ResolvedEventID, s.ResolvedAt = StepCompleted, s.Input, ev.GetEventId(), ev.GetTimestamp()
		f.open--
		resolves = true

	case *history.HistoryEvent_SignalReceived:
		sr := attr.SignalReceived
		st.Signals = append(st.Signals, ReceivedSignal{Name: sr.GetSignalName(), Payload: sr.GetPayload()})
		if id := sr.GetRequestId(); id != "" {
			if st.SignalRequests == nil {
				st.SignalRequests = make(map[string]string)
			}
			st.SignalRequests[id] = sr.GetSignalName()
		}

	case *history.HistoryEvent_StepFailed:
		sf := attr.StepFailed
		if s := st.Step(sf.GetSequence()); s != nil && s.Retrying && !sf.GetWillRetry() && s.Name == sf.GetStepName() {
			// A final failure closes a step between attempts (its deadline
			// passed, or it was cancelled): the retry will not happen.
			s.Error, s.Retrying, s.ResolvedEventID, s.ResolvedAt = sf.GetError(), false, ev.GetEventId(), ev.GetTimestamp()
			s.ErrorType, s.NonRetryable = sf.GetErrorType(), sf.GetNonRetryable()
			if sf.GetCancelled() {
				s.Status = StepCancelled
			}
			f.retrying--
			resolves = true
			break
		}
		s, err := f.openStep(i, sf.GetStepName(), sf.GetSequence())
		if err != nil {
			return err
		}
		s.Status, s.Error = StepFailed, sf.GetError()
		s.ErrorType, s.NonRetryable = sf.GetErrorType(), sf.GetNonRetryable()
		f.open--
		if sf.GetCancelled() {
			s.Status, s.ResolvedEventID, s.ResolvedAt = StepCancelled, ev.GetEventId(), ev.GetTimestamp()
			resolves = true
			break
		}
		if sf.GetWillRetry() {
			s.Retrying = true
			f.retrying++
		} else {
			s.ResolvedEventID, s.ResolvedAt = ev.GetEventId(), ev.GetTimestamp()
			resolves = true
		}

	case *history.HistoryEvent_WorkflowCompleted:
		// Steps still in flight are abandoned: workflow code may return without
		// waiting for every timer or activity it started.
		st.Status = StatusCompleted
		st.Output = attr.WorkflowCompleted.GetOutput()
		if can := attr.WorkflowCompleted.GetContinuedAsNew(); can != nil {
			st.Status, st.ContinuedAsNew = StatusContinuedAsNew, can
		}

	case *history.HistoryEvent_WorkflowFailed:
		st.Status = StatusFailed
		if attr.WorkflowFailed.GetTimedOut() {
			st.Status = StatusTimedOut
		}
		if attr.WorkflowFailed.GetCancelled() {
			st.Status = StatusCancelled
		}
		st.Error = attr.WorkflowFailed.GetError()
		st.Terminated = attr.WorkflowFailed.GetTerminated()

	default:
		return fmt.Errorf("state: event %d has unknown or empty attributes", i)
	}
	st.LastEventResolves = resolves

	// No terminal event: the status is decided by whether a step is in flight.
	if !st.Status.IsTerminal() {
		if f.open > 0 || f.retrying > 0 {
			st.Status = StatusWaiting
		} else {
			st.Status = StatusRunning
		}
	}
	return nil
}

// openStep finds the in-flight step a result refers to, by sequence and name.
func (f *Folder) openStep(pos int, name string, seq int64) (*Step, error) {
	s := f.st.Step(seq)
	if s == nil || s.Status != StepScheduled {
		return nil, fmt.Errorf("state: step result %q at position %d: no step in flight at sequence %d", name, pos, seq)
	}
	if s.Name != name {
		return nil, fmt.Errorf("state: step result at position %d names %q, in-flight step is %q", pos, name, s.Name)
	}
	return s, nil
}

func eventName(ev *history.HistoryEvent) string {
	switch ev.GetAttributes().(type) {
	case *history.HistoryEvent_StepScheduled:
		return "StepScheduled"
	case *history.HistoryEvent_TimerStarted:
		return "TimerStarted"
	case *history.HistoryEvent_MarkerRecorded:
		return "MarkerRecorded"
	case *history.HistoryEvent_SignalReceived:
		return "SignalReceived"
	case nil:
		return "an empty event"
	default:
		return "a step result"
	}
}

// NextSignalDelivery finds a signal wait that a buffered signal can satisfy:
// the waiting step and the payload it receives. Matching is by arrival order:
// waits for signal S are served in sequence order, the k-th wait for S getting
// the k-th S received, so a signal sent before its wait is still delivered.
func (s *State) NextSignalDelivery() (*Step, []byte, bool) {
	consumed := map[string]int{}
	for i := range s.Steps {
		if st := &s.Steps[i]; st.Kind == KindSignal && st.Status == StepCompleted {
			consumed[st.Signal]++
		}
	}
	for i := range s.Steps {
		w := &s.Steps[i]
		if w.Kind != KindSignal || w.Status != StepScheduled {
			continue
		}
		seen, want := 0, consumed[w.Signal]
		for _, sig := range s.Signals {
			if sig.Name != w.Signal {
				continue
			}
			if seen == want {
				return w, sig.Payload, true
			}
			seen++
		}
		consumed[w.Signal]++ // a later wait for the same signal needs the next one
	}
	return nil, nil, false
}

// DeliverableSignal is NextSignalDelivery for the payload alone.
func (s *State) DeliverableSignal() ([]byte, bool) {
	_, payload, ok := s.NextSignalDelivery()
	return payload, ok
}

func msDuration(ms int64) time.Duration { return time.Duration(ms) * time.Millisecond }

// RetryPolicyFromProto converts a recorded policy; nil or unset gives nil.
func RetryPolicyFromProto(p *history.RetryPolicy) *RetryPolicy {
	if p == nil || p.GetMaxAttempts() == 0 {
		return nil
	}
	return &RetryPolicy{
		MaxAttempts:            int(p.GetMaxAttempts()),
		Backoff:                time.Duration(p.GetInitialBackoffMs()) * time.Millisecond,
		MaxBackoff:             time.Duration(p.GetMaxBackoffMs()) * time.Millisecond,
		NonRetryableErrorTypes: p.GetNonRetryableErrorTypes(),
	}
}

// ToProto is the recorded form of p; nil stays nil.
func (p *RetryPolicy) ToProto() *history.RetryPolicy {
	if p == nil {
		return nil
	}
	return &history.RetryPolicy{
		MaxAttempts:            int32(p.MaxAttempts),
		InitialBackoffMs:       p.Backoff.Milliseconds(),
		MaxBackoffMs:           p.MaxBackoff.Milliseconds(),
		NonRetryableErrorTypes: p.NonRetryableErrorTypes,
	}
}
