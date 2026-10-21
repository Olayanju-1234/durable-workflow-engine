package server

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// fanOut runs three activities at once and joins them in code order.
func fanOut(opts ...workflow.ActivityOption) workflow.Func {
	return func(ctx workflow.Context, in []byte) ([]byte, error) {
		fs := []workflow.Future{
			ctx.ExecuteActivityAsync("a", in, opts...),
			ctx.ExecuteActivityAsync("b", in),
			ctx.ExecuteActivityAsync("c", in),
		}
		var out []byte
		for _, f := range fs {
			r, err := f.Get()
			if err != nil {
				return nil, err
			}
			out = append(out, r...)
		}
		return out, nil
	}
}

func parallelEngine(t *testing.T, dir string, fn workflow.Func) *Engine {
	t.Helper()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e := NewEngine(store, discardLogger())
	t.Cleanup(e.Close)
	e.RegisterWorkflow("Fan", replay.Planner{Fn: fn})
	return e
}

// TestParallelActivitiesRunConcurrently: all three activities are dispatched
// at once, each under its own lease, and none can finish until all three are
// running. With one lease per workflow, a second claim would invalidate the
// first and its result would be refused.
func TestParallelActivitiesRunConcurrently(t *testing.T) {
	e := parallelEngine(t, t.TempDir(), fanOut())
	var running sync.WaitGroup
	running.Add(3)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for range 3 {
		go runEngineWorker(ctx, e, func(step string, _ []byte) ([]byte, error) {
			running.Done()
			running.Wait() // every activity is in flight before any reports
			return []byte(strings.ToUpper(step)), nil
		})
	}
	id, err := e.StartWorkflow("Fan", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	final := waitCompleted(t, e, id, 5*time.Second)
	if final.Status != state.StatusCompleted || string(final.Output) != "ABC" {
		t.Fatalf("status %s output %q error %q, want COMPLETED ABC", final.Status, final.Output, final.Error)
	}
	evs, _ := e.store.ReadAll(id)
	if evs[1].GetStepScheduled() == nil || evs[2].GetStepScheduled() == nil || evs[3].GetStepScheduled() == nil {
		t.Fatal("the three activities were not scheduled together")
	}
}

// TestParallelStepsSurviveRestart: after a crash, every step still in flight
// is dispatched again, not just the latest one.
func TestParallelStepsSurviveRestart(t *testing.T) {
	dir := t.TempDir()
	e1 := parallelEngine(t, dir, fanOut())
	ctx1, cancel1 := context.WithCancel(context.Background())
	for range 3 {
		go runEngineWorker(ctx1, e1, func(step string, _ []byte) ([]byte, error) {
			if step != "b" {
				<-ctx1.Done() // a and c hang until the "crash"
				return nil, ctx1.Err()
			}
			return []byte("B"), nil
		})
	}
	id, err := e1.StartWorkflow("Fan", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, e1, id, func(st *state.State) bool { return st.Step(2) != nil && st.Step(2).Status == state.StepCompleted }, 3*time.Second)
	e1.Close() // the "crash": a and c never report to it
	cancel1()

	e2 := parallelEngine(t, dir, fanOut())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	var reran sync.Map
	go runEngineWorker(ctx2, e2, func(step string, _ []byte) ([]byte, error) {
		reran.Store(step, true)
		return []byte(strings.ToUpper(step)), nil
	})
	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}
	final := waitCompleted(t, e2, id, 5*time.Second)
	if string(final.Output) != "ABC" {
		t.Fatalf("output %q, want ABC", final.Output)
	}
	if _, again := reran.Load("b"); again {
		t.Fatal("b had completed before the restart but ran again")
	}
}

// TestRetriedParallelStepIsHiddenFromWorkflow: while one activity is between
// retries, results from the others wake the workflow; it must keep waiting for
// the retried one instead of seeing its failure.
func TestRetriedParallelStepIsHiddenFromWorkflow(t *testing.T) {
	e := parallelEngine(t, t.TempDir(), fanOut(workflow.WithRetryPolicy(workflow.RetryPolicy{MaxAttempts: 3, InitialBackoff: 50 * time.Millisecond})))
	var aCalls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for range 3 {
		go runEngineWorker(ctx, e, func(step string, _ []byte) ([]byte, error) {
			if step == "a" && aCalls.Add(1) == 1 {
				return nil, errors.New("flaky")
			}
			if step != "a" {
				// Report only once a's failure is recorded, so these results wake
				// the workflow while a is between attempts.
				for !aFailedOnce(e) {
					time.Sleep(time.Millisecond)
				}
			}
			return []byte(strings.ToUpper(step)), nil
		})
	}
	id, err := e.StartWorkflow("Fan", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	final := waitCompleted(t, e, id, 5*time.Second)
	if final.Status != state.StatusCompleted || string(final.Output) != "ABC" {
		t.Fatalf("status %s output %q error %q; the retried failure leaked to the workflow", final.Status, final.Output, final.Error)
	}
	if final.Step(1).Attempts != 2 {
		t.Fatalf("a ran %d times, want 2", final.Step(1).Attempts)
	}
}

func raceWorkflow(ctx workflow.Context, _ []byte) ([]byte, error) {
	reply := ctx.WaitForSignalAsync("reply", "approve")
	deadline := ctx.NewTimer("deadline", 100*time.Millisecond)
	if ctx.WaitAny(reply, deadline) == 1 {
		return []byte("timed out"), nil
	}
	return reply.Get()
}

func TestSignalRacesATimer(t *testing.T) {
	e := parallelEngine(t, t.TempDir(), raceWorkflow)
	slow, err := e.StartWorkflow("Fan", nil, "slow")
	if err != nil {
		t.Fatal(err)
	}
	fast, err := e.StartWorkflow("Fan", nil, "fast")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SignalWorkflow(fast, "approve", []byte("yes")); err != nil {
		t.Fatal(err)
	}
	if got := waitCompleted(t, e, fast, 3*time.Second); string(got.Output) != "yes" {
		t.Fatalf("signalled in time: %q", got.Output)
	}
	if got := waitCompleted(t, e, slow, 3*time.Second); string(got.Output) != "timed out" {
		t.Fatalf("no signal: %q", got.Output)
	}
	// The timer of the finished race fires later, harmlessly.
	time.Sleep(150 * time.Millisecond)
	if st := waitCompleted(t, e, fast, time.Second); st.Status != state.StatusCompleted {
		t.Fatalf("fast is %s after its abandoned timer fired", st.Status)
	}
}

func TestValidateDecisionBatches(t *testing.T) {
	st, err := state.Reconstruct([]*history.HistoryEvent{{EventId: 1, Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{WorkflowId: "w", WorkflowType: "T"},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	act := func(seq int64) state.Decision {
		return state.Decision{Type: state.DecideScheduleStep, StepName: "a", Sequence: seq}
	}
	done := state.Decision{Type: state.DecideCompleteWorkflow}
	for name, ds := range map[string][]state.Decision{
		"gap":                 {act(1), act(3)},
		"repeat":              {act(1), act(1)},
		"terminal not last":   {done, act(1)},
		"noop inside batch":   {act(1), {Type: state.DecideNoop}},
		"starts at the wrong": {act(2)},
	} {
		if err := validateDecisions(st, ds); !errors.Is(err, ErrInvalidCommand) {
			t.Errorf("%s: %v, want ErrInvalidCommand", name, err)
		}
	}
	if err := validateDecisions(st, []state.Decision{act(1), act(2), done}); err != nil {
		t.Errorf("valid batch: %v", err)
	}
}

func aFailedOnce(e *Engine) bool {
	ids, _ := e.store.ListWorkflows()
	for _, id := range ids {
		evs, _ := e.store.ReadAll(id)
		for _, ev := range evs {
			if f := ev.GetStepFailed(); f != nil && f.GetStepName() == "a" {
				return true
			}
		}
	}
	return false
}

// TestRecoveryDecidesAfterAResultLandedBeforeTheCrash: the engine recorded a
// step's result and died before asking the workflow what to do next. The other
// step it waits on (a signal) may never come, so recovery itself must decide.
func TestRecoveryDecidesAfterAResultLandedBeforeTheCrash(t *testing.T) {
	dir := t.TempDir()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	for i, ev := range []*history.HistoryEvent{
		{Attributes: &history.HistoryEvent_WorkflowStarted{WorkflowStarted: &history.WorkflowStartedEvent{WorkflowId: "w", WorkflowType: "Fan"}}},
		{Attributes: &history.HistoryEvent_StepScheduled{StepScheduled: &history.StepScheduledEvent{StepName: "reply", Sequence: 1, AwaitsSignal: true, SignalName: "never"}}},
		{Attributes: &history.HistoryEvent_StepScheduled{StepScheduled: &history.StepScheduledEvent{StepName: "a", Sequence: 2}}},
		{Attributes: &history.HistoryEvent_StepCompleted{StepCompleted: &history.StepCompletedEvent{StepName: "a", Sequence: 2, Output: []byte("A")}}},
	} {
		ev.EventId, ev.Timestamp = int64(i+1), time.Now().UnixNano()
		if err := store.Append("w", ev); err != nil {
			t.Fatal(err)
		}
	}
	store.Close()

	e := parallelEngine(t, dir, func(ctx workflow.Context, _ []byte) ([]byte, error) {
		reply := ctx.WaitForSignalAsync("reply", "never")
		a := ctx.ExecuteActivityAsync("a", nil)
		if ctx.WaitAny(reply, a) == 1 {
			return a.Get()
		}
		return reply.Get()
	})
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	if final := waitCompleted(t, e, "w", 3*time.Second); string(final.Output) != "A" {
		t.Fatalf("output %q, status %s; want A", final.Output, final.Status)
	}
}
