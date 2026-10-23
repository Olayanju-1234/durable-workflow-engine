package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

func timeoutEngine(t *testing.T, store *history.HistoryStore, fns map[string]workflow.Func, opts ...Option) *Engine {
	t.Helper()
	e := NewEngine(store, discardLogger(), opts...)
	t.Cleanup(e.Close)
	for name, fn := range fns {
		e.RegisterWorkflow(name, replay.Planner{Fn: fn})
	}
	return e
}

func newStore(t *testing.T, dir string) *history.HistoryStore {
	t.Helper()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// waitsForever runs an activity no worker will pick up.
func waitsForever(ctx workflow.Context, in []byte) ([]byte, error) {
	return ctx.ExecuteActivity("nobody-runs-this", in)
}

// TestExecutionTimeoutEndsWorkflow: a workflow still waiting at its deadline
// ends as TIMED_OUT, durably, and its in-flight activity is abandoned.
func TestExecutionTimeoutEndsWorkflow(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"Slow": waitsForever})
	id, err := e.StartWorkflow("Slow", []byte("in"), "", WithExecutionTimeout(150*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	task := mustPoll(t, e, "w", time.Second)
	st := waitCompleted(t, e, id, 3*time.Second)
	if st.Status != state.StatusTimedOut || !strings.Contains(st.Error, "execution timeout of 150ms exceeded") {
		t.Fatalf("status %s error %q, want TIMED_OUT after 150ms", st.Status, st.Error)
	}
	evs, _ := e.store.ReadAll(id)
	if last := evs[len(evs)-1].GetWorkflowFailed(); last == nil || !last.GetTimedOut() {
		t.Fatalf("last event %v, want WorkflowFailed{timed_out}", evs[len(evs)-1])
	}
	if err := e.CompleteTask(id, task.GetStepName(), task.GetSequence(), task.GetLeaseToken(), nil, "", false); err == nil {
		t.Fatal("a worker reported a result for a timed-out workflow")
	}
	resp, err := e.GetWorkflow(id)
	if err != nil || resp.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_TIMED_OUT {
		t.Fatalf("GetWorkflow status %v (%v), want TIMED_OUT", resp.GetSummary().GetStatus(), err)
	}
}

// TestExecutionTimeoutSurvivesRestart: the deadline is a function of history,
// so an engine that was down when it passed times the workflow out on recovery.
func TestExecutionTimeoutSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	store := newStore(t, dir)
	fns := map[string]workflow.Func{"Slow": waitsForever}
	first := NewEngine(store, discardLogger())
	first.RegisterWorkflow("Slow", replay.Planner{Fn: waitsForever})
	id, err := first.StartWorkflow("Slow", nil, "", WithExecutionTimeout(200*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	first.Close() // "crash" before the deadline
	time.Sleep(300 * time.Millisecond)

	e := timeoutEngine(t, store, fns)
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	if st := waitCompleted(t, e, id, 3*time.Second); st.Status != state.StatusTimedOut {
		t.Fatalf("status %s, want TIMED_OUT after recovery", st.Status)
	}
}

// TestExecutionTimeoutLeavesFinishedWorkflowAlone: a workflow that finishes in
// time keeps its result when the deadline fires later.
func TestExecutionTimeoutLeavesFinishedWorkflowAlone(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Quick": func(ctx workflow.Context, in []byte) ([]byte, error) { return in, nil },
	})
	id, err := e.StartWorkflow("Quick", []byte("done"), "", WithExecutionTimeout(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)
	evs, _ := e.store.ReadAll(id)
	st, err := state.Reconstruct(evs)
	if err != nil || st.Status != state.StatusCompleted || len(evs) != 2 {
		t.Fatalf("status %v after the deadline (%d events, %v), want COMPLETED untouched", st.Status, len(evs), err)
	}
	if _, err := e.StartWorkflow("Quick", nil, "", WithExecutionTimeout(-time.Second)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("negative timeout: %v, want ErrInvalidArgument", err)
	}
}

// TestStartToCloseFailsAttemptDespiteHeartbeats: heartbeats keep a lease
// alive but cannot stretch an attempt past its start-to-close timeout. The
// attempt fails like a dead worker's, the retry runs, and the first worker's
// late result is refused.
func TestStartToCloseFailsAttemptDespiteHeartbeats(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Hung": func(ctx workflow.Context, in []byte) ([]byte, error) {
			return ctx.ExecuteActivity("hang", in,
				workflow.WithStartToCloseTimeout(150*time.Millisecond),
				workflow.WithRetryPolicy(workflow.RetryPolicy{MaxAttempts: 2}))
		},
	}, WithLeaseDuration(time.Second))
	id, err := e.StartWorkflow("Hung", []byte("x"), "")
	if err != nil {
		t.Fatal(err)
	}
	stuck := mustPoll(t, e, "stuck", time.Second)
	if got := stuck.GetStartToCloseTimeoutMs(); got != 150 {
		t.Fatalf("task start_to_close_timeout_ms = %d, want 150", got)
	}
	stop := make(chan struct{})
	go func() { // a worker that is alive but never finishes
		for {
			select {
			case <-stop:
				return
			case <-time.After(30 * time.Millisecond):
				_ = e.Heartbeat(id, stuck.GetLeaseToken())
			}
		}
	}()
	retry := mustPoll(t, e, "healthy", 2*time.Second)
	close(stop)
	if err := e.CompleteTask(id, "hang", 1, stuck.GetLeaseToken(), []byte("late"), "", false); err == nil {
		t.Fatal("the timed-out attempt's result was accepted")
	}
	if err := e.CompleteTask(id, "hang", 1, retry.GetLeaseToken(), []byte("ok"), "", false); err != nil {
		t.Fatal(err)
	}
	st := waitCompleted(t, e, id, 3*time.Second)
	if st.Status != state.StatusCompleted || string(st.Output) != "ok" || st.Steps[0].Attempts != 2 {
		t.Fatalf("status %s output %q attempts %d, want COMPLETED ok after 2 attempts", st.Status, st.Output, st.Steps[0].Attempts)
	}
	evs, _ := e.store.ReadAll(id)
	var timeoutErr string
	for _, ev := range evs {
		if f := ev.GetStepFailed(); f != nil {
			timeoutErr = f.GetError()
		}
	}
	if !strings.Contains(timeoutErr, "start-to-close timeout of 150ms exceeded") {
		t.Fatalf("first attempt failed with %q, want a start-to-close timeout", timeoutErr)
	}
}

// TestScheduleToCloseStopsRetries: no retry is planned that would start after
// the step's deadline, so a policy allowing ten attempts stops at three.
func TestScheduleToCloseStopsRetries(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Flaky": func(ctx workflow.Context, in []byte) ([]byte, error) {
			return ctx.ExecuteActivity("flaky", in,
				workflow.WithScheduleToCloseTimeout(300*time.Millisecond),
				workflow.WithRetryPolicy(workflow.RetryPolicy{MaxAttempts: 10, InitialBackoff: 50 * time.Millisecond}))
		},
	})
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runEngineWorker(ctx, e, func(string, []byte) ([]byte, error) {
		calls.Add(1)
		return nil, errors.New("upstream 503")
	})
	id, err := e.StartWorkflow("Flaky", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	st := waitCompleted(t, e, id, 3*time.Second)
	// Attempts at ~0, ~50 and ~150 ms; the next would start at ~350 ms.
	if st.Status != state.StatusFailed || st.Steps[0].Attempts != 3 || calls.Load() != 3 {
		t.Fatalf("status %s, %d attempts, %d calls; want FAILED after 3 (backoffs 50, 100, then 200 would pass 300ms)",
			st.Status, st.Steps[0].Attempts, calls.Load())
	}
	if !strings.Contains(st.Error, "upstream 503") {
		t.Fatalf("workflow error %q, want the last attempt's error", st.Error)
	}
}

// TestScheduleToCloseFailsStepNobodyRuns: an activity that never gets a
// worker fails at its deadline, and the workflow can handle the error.
func TestScheduleToCloseFailsStepNobodyRuns(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Fallback": func(ctx workflow.Context, in []byte) ([]byte, error) {
			if _, err := ctx.ExecuteActivity("nobody-runs-this", in, workflow.WithScheduleToCloseTimeout(150*time.Millisecond)); err != nil {
				return []byte("fallback: " + err.Error()), nil
			}
			return nil, errors.New("the activity cannot have run")
		},
	})
	id, err := e.StartWorkflow("Fallback", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	st := waitCompleted(t, e, id, 3*time.Second)
	if st.Status != state.StatusCompleted || !strings.Contains(string(st.Output), "schedule-to-close timeout of 150ms exceeded") {
		t.Fatalf("status %s output %q, want COMPLETED via the fallback", st.Status, st.Output)
	}
	if e.queue.depth() != 0 {
		t.Fatalf("the timed-out step is still queued (%d tasks)", e.queue.depth())
	}
}

// TestScheduleToCloseAfterRestartDuringBackoff: the engine was down while the
// step waited for a retry and past its deadline. Recovery records the final
// failure on the retrying step instead of running another attempt.
func TestScheduleToCloseAfterRestartDuringBackoff(t *testing.T) {
	dir := t.TempDir()
	store := newStore(t, dir)
	fn := func(ctx workflow.Context, in []byte) ([]byte, error) {
		_, err := ctx.ExecuteActivity("flaky", in,
			workflow.WithScheduleToCloseTimeout(400*time.Millisecond),
			workflow.WithRetryPolicy(workflow.RetryPolicy{MaxAttempts: 5, InitialBackoff: 200 * time.Millisecond}))
		if err != nil {
			return []byte("gave up: " + err.Error()), nil
		}
		return []byte("ran"), nil
	}
	first := NewEngine(store, discardLogger())
	first.RegisterWorkflow("Flaky", replay.Planner{Fn: fn})
	id, err := first.StartWorkflow("Flaky", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	task := mustPoll(t, first, "w", time.Second)
	if err := first.CompleteTask(id, "flaky", 1, task.GetLeaseToken(), nil, "boom", true); err != nil {
		t.Fatal(err)
	}
	first.Close() // down during the 200 ms backoff, and past the 400 ms deadline
	time.Sleep(500 * time.Millisecond)

	e := timeoutEngine(t, store, map[string]workflow.Func{"Flaky": fn})
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	st := waitCompleted(t, e, id, 3*time.Second)
	if st.Status != state.StatusCompleted || !strings.Contains(string(st.Output), "schedule-to-close timeout of 400ms exceeded") {
		t.Fatalf("status %s output %q, want the timeout handled by the workflow", st.Status, st.Output)
	}
	if st.Steps[0].Attempts != 1 {
		t.Fatalf("%d attempts, want 1: no attempt may start after the deadline", st.Steps[0].Attempts)
	}
}

// TestTimeoutCommandsOverTheWire: an SDK's schedule_activity carries its
// timeouts into history and onto the task; a negative one is refused.
func TestTimeoutCommandsOverTheWire(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), nil)
	if err := e.RegisterWorkflowTypes([]string{"Remote"}, "w", "test"); err != nil {
		t.Fatal(err)
	}
	id, err := e.StartWorkflow("Remote", nil, "", WithExecutionTimeout(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wt, err := e.PollWorkflowTask(ctx, "w", []string{"Remote"})
	if err != nil {
		t.Fatal(err)
	}
	cmd := func(startToClose, scheduleToClose int64) []*WorkflowCommand {
		return []*WorkflowCommand{{Command: &WorkflowCommand_ScheduleActivity{ScheduleActivity: &ScheduleActivityCommand{
			StepName: "a", Sequence: 1, StartToCloseTimeoutMs: startToClose, ScheduleToCloseTimeoutMs: scheduleToClose,
		}}}}
	}
	if err := e.CompleteWorkflowTask(id, wt.GetTaskToken(), cmd(-1, 0), ""); !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("negative timeout: %v, want ErrInvalidCommand", err)
	}
	if err := e.CompleteWorkflowTask(id, wt.GetTaskToken(), cmd(2000, 60000), ""); err != nil {
		t.Fatal(err)
	}
	task := mustPoll(t, e, "w", time.Second)
	if task.GetStartToCloseTimeoutMs() != 2000 {
		t.Fatalf("task start_to_close_timeout_ms = %d, want 2000", task.GetStartToCloseTimeoutMs())
	}
	evs, _ := e.store.ReadAll(id)
	ss := evs[1].GetStepScheduled()
	if evs[0].GetWorkflowStarted().GetExecutionTimeoutMs() != 3600000 ||
		ss.GetStartToCloseTimeoutMs() != 2000 || ss.GetScheduleToCloseTimeoutMs() != 60000 {
		t.Fatalf("recorded timeouts: started %v, scheduled %v", evs[0], ss)
	}
	st, _ := state.Reconstruct(evs)
	want := fmt.Sprint(time.Hour, 2*time.Second, time.Minute)
	if got := fmt.Sprint(st.ExecutionTimeout, st.Steps[0].StartToClose, st.Steps[0].ScheduleToClose); got != want {
		t.Fatalf("replayed timeouts %s, want %s", got, want)
	}
}
