package server

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// customStep is liftoff's liftoff.custom-step pattern: wait for the company's
// callback or a deadline, whichever comes first. With cancelLoser it closes
// the race's loser, as SDK code should; without, it is the code liftoff runs.
func customStep(deadline time.Duration, cancelLoser bool) workflow.Func {
	return func(ctx workflow.Context, in []byte) ([]byte, error) {
		callback := ctx.WaitForSignalAsync("await-callback", "callback")
		timer := ctx.NewTimer("deadline", deadline)
		race := []workflow.Future{callback, timer}
		i := ctx.WaitAny(race...)
		if cancelLoser {
			ctx.CancelStep(race[1-i])
		}
		if i == 0 {
			return []byte("called back"), nil
		}
		if !cancelLoser {
			return []byte("deadline passed"), nil
		}
		// A callback arriving after the deadline belongs to the next wait, not
		// to the abandoned one.
		return ctx.WaitForSignal("late-callback", "callback"), nil
	}
}

func stepStatuses(t *testing.T, e *Engine, id string) string {
	t.Helper()
	resp, err := e.GetWorkflow(id)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, s := range resp.GetSteps() {
		out = append(out, s.GetStepName()+"="+s.GetStatus())
	}
	return strings.Join(out, " ")
}

// TestLosingTimerIsNotShownAsScheduled reproduces the liftoff report: a
// workflow completes on the callback, and its losing timer used to read
// SCHEDULED forever. A step still open when its workflow finished is
// presented as CANCELLED, and the timer firing later records nothing.
func TestLosingTimerIsNotShownAsScheduled(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"CustomStep": customStep(200*time.Millisecond, false)})
	id, err := e.StartWorkflow("CustomStep", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SignalWorkflow(id, "callback", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if st := waitCompleted(t, e, id, 3*time.Second); string(st.Output) != "called back" {
		t.Fatalf("output %q", st.Output)
	}
	if got, want := stepStatuses(t, e, id), "await-callback=COMPLETED deadline=CANCELLED"; got != want {
		t.Fatalf("steps %q, want %q", got, want)
	}
	before, _ := e.store.ReadAll(id)
	time.Sleep(300 * time.Millisecond) // past the deadline
	after, _ := e.store.ReadAll(id)
	if len(after) != len(before) {
		t.Fatalf("history grew from %d to %d events after the workflow finished", len(before), len(after))
	}
}

// TestCancelledSignalWaitStopsConsumingSignals: the deadline wins, the code
// cancels the losing signal wait, and a late callback goes to the wait that
// still wants it. Left open, the abandoned wait (an earlier sequence) would
// have swallowed it and the workflow would hang.
func TestCancelledSignalWaitStopsConsumingSignals(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"CustomStep": customStep(50*time.Millisecond, true)})
	id, err := e.StartWorkflow("CustomStep", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, e, id, func(st *state.State) bool { return len(st.Steps) == 3 }, 3*time.Second)
	if err := e.SignalWorkflow(id, "callback", []byte("late but welcome")); err != nil {
		t.Fatal(err)
	}
	st := waitCompleted(t, e, id, 3*time.Second)
	if string(st.Output) != "late but welcome" {
		t.Fatalf("output %q, want the late callback delivered to the second wait", st.Output)
	}
	if got, want := stepStatuses(t, e, id), "await-callback=CANCELLED deadline=COMPLETED late-callback=COMPLETED"; got != want {
		t.Fatalf("steps %q, want %q", got, want)
	}
	evs, _ := e.store.ReadAll(id)
	var cancel bool
	for _, ev := range evs {
		cancel = cancel || ev.GetStepFailed().GetCancelled()
	}
	if !cancel {
		t.Fatal("no StepFailed{cancelled} in history")
	}
}

// TestCancelledTimerNeverFires: the callback wins and the code cancels the
// deadline; it is closed at once and its later fire records nothing.
func TestCancelledTimerNeverFires(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Twice": func(ctx workflow.Context, in []byte) ([]byte, error) {
			callback := ctx.WaitForSignalAsync("first", "callback")
			timer := ctx.NewTimer("deadline", 150*time.Millisecond)
			if ctx.WaitAny(callback, timer) == 0 {
				ctx.CancelStep(timer)
			}
			if _, err := timer.Get(); !errors.Is(err, workflow.ErrCancelled) {
				return nil, errors.New("the cancelled timer's Get did not report ErrCancelled")
			}
			return ctx.WaitForSignal("second", "done"), nil
		},
	})
	id, err := e.StartWorkflow("Twice", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SignalWorkflow(id, "callback", nil); err != nil {
		t.Fatal(err)
	}
	waitForState(t, e, id, func(st *state.State) bool { return len(st.Steps) == 3 }, 3*time.Second)
	if got, want := stepStatuses(t, e, id), "first=COMPLETED deadline=CANCELLED second=SCHEDULED"; got != want {
		t.Fatalf("steps %q, want %q", got, want)
	}
	time.Sleep(250 * time.Millisecond)
	evs, _ := e.store.ReadAll(id)
	for _, ev := range evs {
		if ev.GetTimerFired() != nil {
			t.Fatal("a cancelled timer fired")
		}
	}
	if err := e.SignalWorkflow(id, "done", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	if st := waitCompleted(t, e, id, 3*time.Second); st.Status != state.StatusCompleted {
		t.Fatalf("status %s error %q", st.Status, st.Error)
	}
}

// TestCancelledActivityTellsItsWorker: an activity that lost a race is
// cancelled while its worker runs it. The worker's heartbeat and completion
// say so (ErrActivityCancelled, not a lost lease), even though the workflow
// is still running. A queued attempt nobody claimed is dropped.
func TestCancelledActivityTellsItsWorker(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Hedge": func(ctx workflow.Context, in []byte) ([]byte, error) {
			running := ctx.ExecuteActivityAsync("slow", in)
			queued := ctx.ExecuteActivityAsync("never-claimed", in)
			timer := ctx.NewTimer("budget", 100*time.Millisecond)
			if ctx.WaitAny(running, queued, timer) == 2 {
				ctx.CancelStep(running)
				ctx.CancelStep(queued)
			}
			return ctx.WaitForSignal("hold", "release"), nil
		},
	})
	id, err := e.StartWorkflow("Hedge", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	task, err := e.PollTask(ctx, "w", "slow")
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, e, id, func(st *state.State) bool { return len(st.Steps) == 4 }, 3*time.Second)
	if err := e.Heartbeat(id, task.GetLeaseToken()); !errors.Is(err, ErrActivityCancelled) {
		t.Fatalf("heartbeat: %v, want ErrActivityCancelled", err)
	}
	err = e.CompleteTask(id, "slow", task.GetSequence(), task.GetLeaseToken(), []byte("too late"), "", false)
	if !errors.Is(err, ErrActivityCancelled) {
		t.Fatalf("complete: %v, want ErrActivityCancelled", err)
	}
	if e.queue.depth() != 0 {
		t.Fatalf("%d tasks still queued for cancelled steps", e.queue.depth())
	}
	if got, want := stepStatuses(t, e, id), "slow=CANCELLED never-claimed=CANCELLED budget=COMPLETED hold=SCHEDULED"; got != want {
		t.Fatalf("steps %q, want %q", got, want)
	}
}

// TestCancelRetryingActivity: a step between attempts is closed by the cancel
// (a final failure on a retrying step), and its retry never runs.
func TestCancelRetryingActivity(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Retrying": func(ctx workflow.Context, in []byte) ([]byte, error) {
			flaky := ctx.ExecuteActivityAsync("flaky", in,
				workflow.WithRetryPolicy(workflow.RetryPolicy{MaxAttempts: 3, InitialBackoff: 300 * time.Millisecond}))
			timer := ctx.NewTimer("budget", 100*time.Millisecond)
			if ctx.WaitAny(flaky, timer) == 1 {
				ctx.CancelStep(flaky)
			}
			_, err := flaky.Get()
			return []byte(err.Error()), nil
		},
	})
	id, err := e.StartWorkflow("Retrying", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	task := mustPoll(t, e, "w", time.Second)
	if err := e.CompleteTask(id, "flaky", 1, task.GetLeaseToken(), nil, "503", true); err != nil {
		t.Fatal(err)
	}
	st := waitCompleted(t, e, id, 3*time.Second)
	if !strings.Contains(string(st.Output), "step cancelled") || st.Steps[0].Status != state.StepCancelled || st.Steps[0].Attempts != 1 {
		t.Fatalf("output %q step %+v, want the retrying step cancelled after 1 attempt", st.Output, st.Steps[0])
	}
	time.Sleep(350 * time.Millisecond) // past the backoff
	if e.queue.depth() != 0 {
		t.Fatal("the cancelled step's retry was dispatched")
	}
}

// TestCancelWorkflow: CANCELLED for clients, a cancelled answer for the
// worker still running its activity, and only once.
func TestCancelWorkflow(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"Slow": waitsForever})
	id, err := e.StartWorkflow("Slow", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	task := mustPoll(t, e, "w", time.Second)
	if err := e.CancelWorkflow(id, "customer changed their mind"); err != nil {
		t.Fatal(err)
	}
	st := waitCompleted(t, e, id, time.Second)
	if st.Status != state.StatusCancelled || st.Error != "cancelled: customer changed their mind" {
		t.Fatalf("status %s error %q", st.Status, st.Error)
	}
	if err := e.Heartbeat(id, task.GetLeaseToken()); !errors.Is(err, ErrActivityCancelled) || !strings.Contains(err.Error(), "workflow cancelled") {
		t.Fatalf("heartbeat: %v, want ErrActivityCancelled naming the workflow's cancellation", err)
	}
	if err := e.CancelWorkflow(id, ""); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("second cancel: %v, want ErrAlreadyTerminal", err)
	}
	if err := e.CancelWorkflow("no-such-workflow", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("unknown workflow: %v, want ErrNotFound", err)
	}
	resp, _ := e.GetWorkflow(id)
	if resp.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_CANCELLED || resp.GetSteps()[0].GetStatus() != "CANCELLED" {
		t.Fatalf("GetWorkflow: %v", resp)
	}
	listed, _ := e.ListWorkflows(WorkflowStatus_WORKFLOW_STATUS_CANCELLED)
	if len(listed) != 1 || listed[0].GetWorkflowId() != id {
		t.Fatalf("ListWorkflows(CANCELLED) = %v", listed)
	}
}

// TestCancelStepCommandValidation: a cancel must name an existing, non-marker
// step by its recorded name, possibly one opened earlier in the same batch.
// Cancelling a step that already has a result records nothing.
func TestCancelStepCommandValidation(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), nil)
	if err := e.RegisterWorkflowTypes([]string{"Remote"}, "w", "test"); err != nil {
		t.Fatal(err)
	}
	id, err := e.StartWorkflow("Remote", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	poll := func() *WorkflowTask {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		wt, err := e.PollWorkflowTask(ctx, "w", []string{"Remote"})
		if err != nil {
			t.Fatal(err)
		}
		return wt
	}
	timerCmd := &WorkflowCommand{Command: &WorkflowCommand_StartTimer{StartTimer: &StartTimerCommand{StepName: "t", Sequence: 1, DelayMs: 60000}}}
	cancelCmd := func(name string, seq int64) *WorkflowCommand {
		return &WorkflowCommand{Command: &WorkflowCommand_CancelStep{CancelStep: &CancelStepCommand{StepName: name, Sequence: seq, Reason: "why not"}}}
	}
	wt := poll()
	for _, bad := range [][]*WorkflowCommand{
		{cancelCmd("t", 1)},           // nothing at sequence 1 yet
		{timerCmd, cancelCmd("x", 1)}, // wrong name
		{timerCmd, cancelCmd("t", 2)}, // a step that doesn't exist
	} {
		if err := e.CompleteWorkflowTask(id, wt.GetTaskToken(), bad, ""); !errors.Is(err, ErrInvalidCommand) {
			t.Fatalf("%v: %v, want ErrInvalidCommand", bad, err)
		}
	}
	// Opened and cancelled in one batch: recorded, then closed.
	if err := e.CompleteWorkflowTask(id, wt.GetTaskToken(), []*WorkflowCommand{timerCmd, cancelCmd("t", 1)}, ""); err != nil {
		t.Fatal(err)
	}
	evs, _ := e.store.ReadAll(id)
	if len(evs) != 3 || !evs[2].GetStepFailed().GetCancelled() || evs[2].GetStepFailed().GetError() != "cancelled: why not" {
		t.Fatalf("history %v", evs)
	}
	// Cancelling it again is a no-op.
	wt = poll()
	if err := e.CompleteWorkflowTask(id, wt.GetTaskToken(), []*WorkflowCommand{cancelCmd("t", 1)}, ""); err != nil {
		t.Fatal(err)
	}
	if again, _ := e.store.ReadAll(id); len(again) != 3 {
		t.Fatalf("a second cancel recorded %d events", len(again)-3)
	}
}
