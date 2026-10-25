package server

import (
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// pipeline runs a, b and c in order; any failure fails the workflow.
func pipeline(ctx workflow.Context, in []byte) ([]byte, error) {
	out := in
	for _, step := range []string{"a", "b", "c"} {
		var err error
		if out, err = ctx.ExecuteActivity(step, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// waitThenFinish runs a, waits for the "go" signal, then runs b with it.
func waitThenFinish(ctx workflow.Context, in []byte) ([]byte, error) {
	if _, err := ctx.ExecuteActivity("a", in); err != nil {
		return nil, err
	}
	sig := ctx.WaitForSignal("wait-go", "go")
	return ctx.ExecuteActivity("b", sig)
}

// fanOutBoth runs a and b at once and needs both.
func fanOutBoth(ctx workflow.Context, in []byte) ([]byte, error) {
	fa := ctx.ExecuteActivityAsync("a", in)
	fb := ctx.ExecuteActivityAsync("b", in)
	ra, err := fa.Get()
	if err != nil {
		return nil, err
	}
	rb, err := fb.Get()
	if err != nil {
		return nil, err
	}
	return append(append(ra, '+'), rb...), nil
}

// stepWorker runs activities by name, counting each run; fail decides
// whether an attempt fails for good.
type stepWorker struct {
	mu   sync.Mutex
	runs map[string]int
	fail func(step string) bool
}

func (w *stepWorker) count(step string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.runs[step]
}

// runOne handles exactly one task and returns its step name.
func (w *stepWorker) runOne(t *testing.T, e *Engine) string {
	t.Helper()
	task := mustPoll(t, e, "w", 3*time.Second)
	step := task.GetStepName()
	w.mu.Lock()
	w.runs[step]++
	failing := w.fail != nil && w.fail(step)
	w.mu.Unlock()
	var err error
	if failing {
		err = e.FailTask(task.GetWorkflowId(), step, task.GetSequence(), task.GetLeaseToken(),
			TaskFailure{Message: step + " broke", NonRetryable: true})
	} else {
		err = e.CompleteTask(task.GetWorkflowId(), step, task.GetSequence(), task.GetLeaseToken(), append(task.GetInput(), []byte(">"+step)...), "", false)
	}
	if err != nil {
		t.Fatalf("report %s: %v", step, err)
	}
	return step
}

func newStepWorker(fail func(string) bool) *stepWorker {
	return &stepWorker{runs: map[string]int{}, fail: fail}
}

// The point of reset: the step that failed runs again once what broke it is
// fixed, and the steps before it are not run twice. The new run starts with
// the old run's events before that step, byte for byte, and the old run is
// kept as an earlier run.
func TestResetRerunsTheFailedStepOnly(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"Pipeline": pipeline})
	broken := true
	w := newStepWorker(func(step string) bool { return step == "b" && broken })
	if _, err := e.Start("Pipeline", []byte("in"), "pipe-1"); err != nil {
		t.Fatal(err)
	}
	w.runOne(t, e) // a
	w.runOne(t, e) // b fails for good
	failed := waitFinished(t, e, "pipe-1", 3*time.Second)
	if failed.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_FAILED {
		t.Fatalf("status %v, want FAILED", failed.GetSummary().GetStatus())
	}
	if failed.GetResetStepName() != "b" || failed.GetResetStepSequence() != 2 || failed.GetResetEventId() != 4 {
		t.Fatalf("default reset point %d %q #%d, want event 4 opening step b #2",
			failed.GetResetEventId(), failed.GetResetStepName(), failed.GetResetStepSequence())
	}
	old, _ := e.store.ReadAll("pipe-1")

	broken = false
	res, err := e.ResetWorkflow("pipe-1", ResetOptions{Reason: "b is fixed"})
	if err != nil {
		t.Fatal(err)
	}
	if res.ResetEventID != 4 || res.PreviousRunID != failed.GetSummary().GetRunId() || res.RunID == res.PreviousRunID {
		t.Fatalf("reset %+v", res)
	}
	if got := w.runOne(t, e); got != "b" {
		t.Fatalf("after the reset the engine dispatched %q, want b again (a already succeeded)", got)
	}
	w.runOne(t, e) // c
	done := waitFinished(t, e, "pipe-1", 3*time.Second)
	if done.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_COMPLETED || string(done.GetOutput()) != "in>a>b>c" {
		t.Fatalf("after reset: %v %q", done.GetSummary().GetStatus(), done.GetOutput())
	}
	if w.count("a") != 1 || w.count("b") != 2 || w.count("c") != 1 {
		t.Fatalf("runs a=%d b=%d c=%d, want 1, 2, 1", w.count("a"), w.count("b"), w.count("c"))
	}

	now, _ := e.store.ReadAll("pipe-1")
	ws := now[0].GetWorkflowStarted()
	if ws.GetRunId() != res.RunID || ws.GetResetFromRunId() != res.PreviousRunID || ws.GetResetEventId() != 4 ||
		ws.GetResetReason() != "b is fixed" || now[0].GetTimestamp() != old[0].GetTimestamp() {
		t.Fatalf("new run's start %v (ts %d), old start ts %d", ws, now[0].GetTimestamp(), old[0].GetTimestamp())
	}
	for i := 1; i < 3; i++ {
		if !proto.Equal(now[i], old[i]) {
			t.Fatalf("kept event %d changed:\n old %v\n new %v", i+1, old[i], now[i])
		}
	}
	if done.GetResetFromRunId() != res.PreviousRunID {
		t.Fatalf("describe says reset from %q", done.GetResetFromRunId())
	}
	runs, err := e.ListWorkflowRuns("pipe-1")
	if err != nil || len(runs) != 2 || runs[1].GetRunId() != res.PreviousRunID || runs[1].GetStatus() != WorkflowStatus_WORKFLOW_STATUS_FAILED {
		t.Fatalf("runs %v %v, want the new run then the failed one", runs, err)
	}
	if v := e.metrics.workflowsReset.Value(); v != 1 {
		t.Fatalf("workflows_reset_total = %v", v)
	}
}

// A signal that arrived after the point a run is reset to is not lost: it is
// recorded again on the new run, idempotency key and all. A cancel request
// is not: resetting a cancelled workflow is how it is un-cancelled.
func TestResetReappliesLaterSignals(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"Waiter": waitThenFinish})
	broken := true
	w := newStepWorker(func(step string) bool { return step == "a" && broken })
	if _, err := e.Start("Waiter", []byte("in"), "wait-1"); err != nil {
		t.Fatal(err)
	}
	// The signal lands while a is in flight, then a fails the workflow.
	task := mustPoll(t, e, "w", 3*time.Second)
	if err := e.SignalWorkflowOnce("wait-1", "go", "req-1", []byte("green")); err != nil {
		t.Fatal(err)
	}
	if err := e.FailTask("wait-1", "a", task.GetSequence(), task.GetLeaseToken(), TaskFailure{Message: "broke", NonRetryable: true}); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, e, "wait-1", 3*time.Second)

	broken = false
	res, err := e.ResetWorkflow("wait-1", ResetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.ResetEventID != 2 || res.ReappliedSignals != 1 {
		t.Fatalf("reset %+v, want event 2 and one signal reapplied", res)
	}
	evs, _ := e.store.ReadAll("wait-1")
	if sig := evs[1].GetSignalReceived(); sig == nil || sig.GetRequestId() != "req-1" || string(sig.GetPayload()) != "green" {
		t.Fatalf("event 2 of the new run: %v", evs[1])
	}
	// The same request again is still recognised: recorded once.
	if err := e.SignalWorkflowOnce("wait-1", "go", "req-1", []byte("green")); err != nil {
		t.Fatal(err)
	}
	w.runOne(t, e) // a
	w.runOne(t, e) // b, with the reapplied signal
	done := waitFinished(t, e, "wait-1", 3*time.Second)
	if string(done.GetOutput()) != "green>b" {
		t.Fatalf("output %q, want the reapplied signal's payload through b", done.GetOutput())
	}
}

func TestResetDoesNotReapplyACancelRequest(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"Pipeline": pipeline})
	if _, err := e.Start("Pipeline", nil, "cxl-1"); err != nil {
		t.Fatal(err)
	}
	mustPoll(t, e, "w", 3*time.Second)
	// The code is told at its wait and lets the cancellation end it.
	if err := e.RequestCancelWorkflow("cxl-1", "stop"); err != nil {
		t.Fatal(err)
	}
	if done := waitFinished(t, e, "cxl-1", 3*time.Second); done.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_CANCELLED {
		t.Fatalf("status %v, want CANCELLED", done.GetSummary().GetStatus())
	}
	resp, _ := e.GetWorkflow("cxl-1")
	if resp.GetResetEventId() != 3 {
		t.Fatalf("default point %d, want 3 (the cancel request itself)", resp.GetResetEventId())
	}
	res, err := e.ResetWorkflow("cxl-1", ResetOptions{})
	if err != nil || res.ReappliedSignals != 0 {
		t.Fatalf("reset %+v %v", res, err)
	}
	evs, _ := e.store.ReadAll("cxl-1")
	for _, ev := range evs {
		if ev.GetSignalReceived().GetCancelRequest() {
			t.Fatalf("the new run inherited the cancel request: %v", evs)
		}
	}
	if st := mustState(t, e, "cxl-1"); st.Status != state.StatusWaiting || st.CancelRequestedEventID != 0 {
		t.Fatalf("new run %s, cancel requested at %d", st.Status, st.CancelRequestedEventID)
	}
}

func mustState(t *testing.T, e *Engine, id string) *state.State {
	t.Helper()
	evs, err := e.store.ReadAll(id)
	if err != nil {
		t.Fatal(err)
	}
	st, err := state.Reconstruct(evs)
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func TestResetRefusals(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"Pipeline": pipeline})
	if _, err := e.Start("Pipeline", nil, "live-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.ResetWorkflow("live-1", ResetOptions{}); !errors.Is(err, ErrNotFinished) {
		t.Fatalf("reset of a live run: %v, want ErrNotFinished", err)
	}
	if _, err := e.ResetWorkflow("nobody", ResetOptions{}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("reset of an unknown id: %v, want ErrNotFound", err)
	}
	if err := e.TerminateWorkflow("live-1", "x"); err != nil {
		t.Fatal(err)
	}
	evs, _ := e.store.ReadAll("live-1")
	for _, bad := range []int64{1, -1, int64(len(evs)) + 1} {
		if _, err := e.ResetWorkflow("live-1", ResetOptions{EventID: bad}); !errors.Is(err, ErrInvalidArgument) {
			t.Fatalf("reset to event %d: %v, want ErrInvalidArgument", bad, err)
		}
	}
	// Resetting to the last event undoes only the ending.
	if _, err := e.ResetWorkflow("live-1", ResetOptions{EventID: int64(len(evs))}); err != nil {
		t.Fatal(err)
	}
	if st := mustState(t, e, "live-1"); st.Status != state.StatusWaiting || len(st.Steps) != 1 {
		t.Fatalf("after undoing the termination: %s with %d steps", st.Status, len(st.Steps))
	}
}

// A reset run is an ordinary run on disk: a restart recovers it mid-flight.
func TestResetRunSurvivesARestart(t *testing.T) {
	dir := t.TempDir()
	e := timeoutEngine(t, newStore(t, dir), map[string]workflow.Func{"Pipeline": pipeline})
	broken := true
	w := newStepWorker(func(step string) bool { return step == "b" && broken })
	if _, err := e.Start("Pipeline", []byte("in"), "pipe-2"); err != nil {
		t.Fatal(err)
	}
	w.runOne(t, e)
	w.runOne(t, e)
	waitFinished(t, e, "pipe-2", 3*time.Second)
	if _, err := e.ResetWorkflow("pipe-2", ResetOptions{}); err != nil {
		t.Fatal(err)
	}
	e.Close()

	e2 := timeoutEngine(t, newStore(t, dir), map[string]workflow.Func{"Pipeline": pipeline})
	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}
	broken = false
	if got := w.runOne(t, e2); got != "b" {
		t.Fatalf("recovered reset run dispatched %q, want b", got)
	}
	w.runOne(t, e2)
	if done := waitFinished(t, e2, "pipe-2", 3*time.Second); string(done.GetOutput()) != "in>a>b>c" {
		t.Fatalf("output %q", done.GetOutput())
	}
}

// A run that timed out can be reset: its execution timeout counts again from
// the reset (the recorded start, which replay needs, does not move).
func TestResetGivesTheExecutionTimeoutAgain(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"Slow": waitsForever})
	if _, err := e.Start("Slow", nil, "slow-1", WithExecutionTimeout(300*time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, e, "slow-1", 3*time.Second)
	time.Sleep(200 * time.Millisecond) // well past the first deadline
	resetAt := time.Now()
	if _, err := e.ResetWorkflow("slow-1", ResetOptions{}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if st := mustState(t, e, "slow-1"); st.Status != state.StatusWaiting {
		t.Fatalf("100ms after the reset: %s, want WAITING (a fresh 300ms)", st.Status)
	}
	done := waitFinished(t, e, "slow-1", 3*time.Second)
	if done.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_TIMED_OUT {
		t.Fatalf("status %v", done.GetSummary().GetStatus())
	}
	if took := time.Since(resetAt); took < 280*time.Millisecond {
		t.Fatalf("timed out %s after the reset, want about 300ms", took)
	}
}

// Steps in flight at the reset point carry on: a parallel step whose result
// came after the point runs again, and the code re-issues the failed one.
func TestResetWithParallelSteps(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"FanOut": fanOutBoth})
	if _, err := e.Start("FanOut", []byte("x"), "fan-1"); err != nil {
		t.Fatal(err)
	}
	// Both are scheduled at once; a succeeds, then b fails for good.
	byStep := map[string]*Task{}
	for range 2 {
		task := mustPoll(t, e, "w", 3*time.Second)
		byStep[task.GetStepName()] = task
	}
	a, b := byStep["a"], byStep["b"]
	if err := e.CompleteTask("fan-1", "a", a.GetSequence(), a.GetLeaseToken(), []byte("A"), "", false); err != nil {
		t.Fatal(err)
	}
	if err := e.FailTask("fan-1", "b", b.GetSequence(), b.GetLeaseToken(), TaskFailure{Message: "no", NonRetryable: true}); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, e, "fan-1", 3*time.Second)

	w := newStepWorker(nil)
	res, err := e.ResetWorkflow("fan-1", ResetOptions{})
	if err != nil || res.ResetEventID != 3 {
		t.Fatalf("reset %+v %v, want event 3 (b's opening)", res, err)
	}
	ran := map[string]bool{w.runOne(t, e): true, w.runOne(t, e): true}
	if !ran["a"] || !ran["b"] {
		t.Fatalf("after the reset ran %v, want a again (its result came after the point) and b", ran)
	}
	if done := waitFinished(t, e, "fan-1", 3*time.Second); string(done.GetOutput()) != "x>a+x>b" {
		t.Fatalf("output %q", done.GetOutput())
	}
}

func TestDefaultResetPoint(t *testing.T) {
	ev := func(id int64, attr any) *history.HistoryEvent {
		e := &history.HistoryEvent{EventId: id, Timestamp: id}
		switch a := attr.(type) {
		case *history.WorkflowStartedEvent:
			e.Attributes = &history.HistoryEvent_WorkflowStarted{WorkflowStarted: a}
		case *history.StepScheduledEvent:
			e.Attributes = &history.HistoryEvent_StepScheduled{StepScheduled: a}
		case *history.StepCompletedEvent:
			e.Attributes = &history.HistoryEvent_StepCompleted{StepCompleted: a}
		case *history.StepFailedEvent:
			e.Attributes = &history.HistoryEvent_StepFailed{StepFailed: a}
		case *history.TimerStartedEvent:
			e.Attributes = &history.HistoryEvent_TimerStarted{TimerStarted: a}
		case *history.WorkflowFailedEvent:
			e.Attributes = &history.HistoryEvent_WorkflowFailed{WorkflowFailed: a}
		case *history.WorkflowCompletedEvent:
			e.Attributes = &history.HistoryEvent_WorkflowCompleted{WorkflowCompleted: a}
		}
		return e
	}
	start := &history.WorkflowStartedEvent{WorkflowType: "T"}
	cases := []struct {
		name   string
		events []*history.HistoryEvent
		want   ResetPoint
		ok     bool
	}{
		{"a step failed for good, after a retry", []*history.HistoryEvent{
			ev(1, start), ev(2, &history.StepScheduledEvent{StepName: "a", Sequence: 1}),
			ev(3, &history.StepCompletedEvent{StepName: "a", Sequence: 1}),
			ev(4, &history.StepScheduledEvent{StepName: "b", Sequence: 2}),
			ev(5, &history.StepFailedEvent{StepName: "b", Sequence: 2, WillRetry: true}),
			ev(6, &history.StepScheduledEvent{StepName: "b", Sequence: 2}),
			ev(7, &history.StepFailedEvent{StepName: "b", Sequence: 2}),
			ev(8, &history.WorkflowFailedEvent{Error: "b"}),
		}, ResetPoint{EventID: 4, StepName: "b", Sequence: 2}, true},
		{"the code caught one failure, then another ended it", []*history.HistoryEvent{
			ev(1, start), ev(2, &history.StepScheduledEvent{StepName: "a", Sequence: 1}),
			ev(3, &history.StepFailedEvent{StepName: "a", Sequence: 1}),
			ev(4, &history.StepScheduledEvent{StepName: "b", Sequence: 2}),
			ev(5, &history.StepFailedEvent{StepName: "b", Sequence: 2}),
			ev(6, &history.WorkflowFailedEvent{Error: "b"}),
		}, ResetPoint{EventID: 4, StepName: "b", Sequence: 2}, true},
		{"timed out on a timer", []*history.HistoryEvent{
			ev(1, start), ev(2, &history.TimerStartedEvent{StepName: "nap", Sequence: 1, FireAt: 99}),
			ev(3, &history.WorkflowFailedEvent{TimedOut: true}),
		}, ResetPoint{EventID: 2, StepName: "nap", Sequence: 1}, true},
		{"the code failed with every step done", []*history.HistoryEvent{
			ev(1, start), ev(2, &history.StepScheduledEvent{StepName: "a", Sequence: 1}),
			ev(3, &history.StepCompletedEvent{StepName: "a", Sequence: 1}),
			ev(4, &history.WorkflowFailedEvent{Error: "bug"}),
		}, ResetPoint{EventID: 4}, true},
		{"completed: the ending can be undone", []*history.HistoryEvent{
			ev(1, start), ev(2, &history.WorkflowCompletedEvent{}),
		}, ResetPoint{EventID: 2}, true},
		{"live", []*history.HistoryEvent{
			ev(1, start), ev(2, &history.StepScheduledEvent{StepName: "a", Sequence: 1}),
		}, ResetPoint{}, false},
		{"continued as new", []*history.HistoryEvent{
			ev(1, start), ev(2, &history.WorkflowCompletedEvent{ContinuedAsNew: &history.ContinuedAsNew{RunId: "n"}}),
		}, ResetPoint{}, false},
	}
	for _, c := range cases {
		st, err := state.Reconstruct(c.events)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got, ok := DefaultResetPoint(c.events, st)
		if ok != c.ok || got != c.want {
			t.Errorf("%s: %+v %v, want %+v %v", c.name, got, ok, c.want, c.ok)
		}
	}
}

// The store writes a reset run whole: the old history is kept under its
// archive name, and the new one holds every event given.
func TestStoreNewRunWithSeveralEvents(t *testing.T) {
	dir := t.TempDir()
	s := newStore(t, dir)
	mk := func(id int64, attr *history.HistoryEvent) *history.HistoryEvent { attr.EventId = id; return attr }
	started := func(run string) *history.HistoryEvent {
		return &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{WorkflowStarted: &history.WorkflowStartedEvent{RunId: run}}}
	}
	for i, ev := range []*history.HistoryEvent{started("r1"), {Attributes: &history.HistoryEvent_WorkflowCompleted{WorkflowCompleted: &history.WorkflowCompletedEvent{}}}} {
		if err := s.Append("wf", mk(int64(i+1), ev)); err != nil {
			t.Fatal(err)
		}
	}
	sig := &history.HistoryEvent{Attributes: &history.HistoryEvent_SignalReceived{SignalReceived: &history.SignalReceivedEvent{SignalName: "s"}}}
	if err := s.NewRun("wf", archiveID("wf", "r1", 7), mk(1, started("r2")), mk(2, sig)); err != nil {
		t.Fatal(err)
	}
	evs, _ := s.ReadAll("wf")
	if len(evs) != 2 || evs[0].GetWorkflowStarted().GetRunId() != "r2" || evs[1].GetSignalReceived().GetSignalName() != "s" {
		t.Fatalf("new run %v", evs)
	}
	old, _ := s.ReadAll(archiveID("wf", "r1", 7))
	if len(old) != 2 || old[0].GetWorkflowStarted().GetRunId() != "r1" {
		t.Fatalf("archived run %v", old)
	}
}
