package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

func pollQueue(t *testing.T, e *Engine, queue string, d time.Duration, steps ...string) (*Task, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	return e.In("").PollTask(ctx, "worker-"+queue, queue, steps...)
}

// Two apps implement the same activity name on different queues: each worker
// gets only its own queue's tasks, and a worker naming no queue gets neither.
func TestTaskQueuesRouteActivities(t *testing.T) {
	e := nsEngine(t, t.TempDir())
	for _, q := range []string{"payments", "shipping", ""} {
		if _, err := e.Start("Flow", []byte("for "+q), "wf-"+q, WithTaskQueue(q)); err != nil {
			t.Fatalf("start on %q: %v", q, err)
		}
	}
	for _, q := range []string{"payments", "shipping", ""} {
		task, err := pollQueue(t, e, q, time.Second, "work")
		if err != nil || string(task.GetInput()) != "for "+q || task.GetTaskQueue() != state.QueueName(q) {
			t.Fatalf("queue %q polled %v, %v", q, task, err)
		}
		if extra, err := pollQueue(t, e, q, 100*time.Millisecond); err == nil {
			t.Fatalf("queue %q polled a second task, %q", q, extra.GetInput())
		}
	}
	// "default" names the default queue.
	if _, err := e.Start("Flow", []byte("named default"), "wf-d", WithTaskQueue("default")); err != nil {
		t.Fatal(err)
	}
	if task, err := pollQueue(t, e, "", time.Second); err != nil || string(task.GetInput()) != "named default" {
		t.Fatalf("default poll = %v, %v", task, err)
	}
	if resp, _ := e.GetWorkflow("wf-payments"); resp.GetSummary().GetTaskQueue() != "payments" ||
		resp.GetSteps()[0].GetTaskQueue() != "payments" {
		t.Fatalf("describe = %v", resp)
	}
}

// An activity runs on its workflow's queue unless the code names another; a
// retry, a recovery and a continue-as-new keep the queue.
func TestTaskQueueFollowsTheWorkflow(t *testing.T) {
	dir := t.TempDir()
	setup := func() *Engine {
		e := nsEngine(t, dir)
		e.RegisterWorkflow("Router", replay.Planner{Fn: func(ctx workflow.Context, in []byte) ([]byte, error) {
			if string(in) == "next" {
				return ctx.ExecuteActivity("own", nil)
			}
			if _, err := ctx.ExecuteActivity("own", nil, workflow.WithRetryPolicy(workflow.RetryPolicy{MaxAttempts: 2})); err != nil {
				return nil, err
			}
			if _, err := ctx.ExecuteActivity("elsewhere", nil, workflow.WithTaskQueue("other")); err != nil {
				return nil, err
			}
			return nil, workflow.ContinueAsNew([]byte("next"))
		}})
		return e
	}
	e := setup()
	if _, err := e.Start("Router", nil, "r1", WithTaskQueue("wf-q")); err != nil {
		t.Fatal(err)
	}
	first, err := pollQueue(t, e, "wf-q", time.Second, "own")
	if err != nil {
		t.Fatalf("own activity on the workflow's queue: %v", err)
	}
	if err := e.FailTask("r1", "own", first.GetSequence(), first.GetLeaseToken(), TaskFailure{Message: "try again"}); err != nil {
		t.Fatal(err)
	}
	if task, err := pollQueue(t, e, "", 300*time.Millisecond); err == nil {
		t.Fatalf("the retry went to the default queue: %v", task)
	}
	retry, err := pollQueue(t, e, "wf-q", time.Second, "own")
	if err != nil || retry.GetAttempt() != 2 {
		t.Fatalf("retry on the workflow's queue = %v, %v", retry, err)
	}
	if err := e.CompleteTask("r1", "own", retry.GetSequence(), retry.GetLeaseToken(), nil, "", false); err != nil {
		t.Fatal(err)
	}
	if _, err := pollQueue(t, e, "other", time.Second, "elsewhere"); err != nil {
		t.Fatalf("named activity on its queue: %v", err)
	}
	// A restart: the activity in flight comes back on its own queue.
	e.Close()
	e = setup()
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	if task, err := pollQueue(t, e, "wf-q", 200*time.Millisecond); err == nil {
		t.Fatalf("the recovered activity went to the workflow's queue: %v", task)
	}
	recovered, err := pollQueue(t, e, "other", time.Second, "elsewhere")
	if err != nil {
		t.Fatalf("recovered activity on its queue: %v", err)
	}
	if err := e.CompleteTask("r1", "elsewhere", recovered.GetSequence(), recovered.GetLeaseToken(), nil, "", false); err != nil {
		t.Fatal(err)
	}
	// The next run is on the same queue.
	next, err := pollQueue(t, e, "wf-q", time.Second, "own")
	if err != nil {
		t.Fatalf("the next run's activity: %v", err)
	}
	if resp, _ := e.GetWorkflow("r1"); resp.GetSummary().GetTaskQueue() != "wf-q" || resp.GetSummary().GetRunId() == "" {
		t.Fatalf("next run = %v", resp.GetSummary())
	}
	_ = next
}

// Workflow tasks for SDK-run types go only to workers polling the run's queue.
func TestTaskQueuesRouteWorkflowTasks(t *testing.T) {
	e := nsEngine(t, t.TempDir())
	if err := e.RegisterWorkflowTypes([]string{"Remote"}, "sdk", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Start("Remote", nil, "r", WithTaskQueue("sdk-a")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if task, err := e.In("").PollWorkflowTask(ctx, "w", "", []string{"Remote"}); err == nil {
		t.Fatalf("a default-queue SDK worker received %s", task.GetWorkflowId())
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	task, err := e.In("").PollWorkflowTask(ctx2, "w", "sdk-a", nil)
	if err != nil || task.GetWorkflowId() != "r" || task.GetTaskQueue() != "sdk-a" {
		t.Fatalf("sdk-a poll = %v, %v", task, err)
	}
	// Its activity, scheduled with no queue, goes to the workflow's.
	cmd := CommandFromDecision(state.Decision{Type: state.DecideScheduleStep, StepName: "act", Sequence: 1})
	if err := e.CompleteWorkflowTask("r", task.GetTaskToken(), []*WorkflowCommand{cmd}, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := pollQueue(t, e, "sdk-a", time.Second, "act"); err != nil {
		t.Fatalf("activity on the workflow's queue: %v", err)
	}
}

// A named queue needs format 6, recorded on the event and the directory; the
// default queue changes nothing.
func TestTaskQueueFormat(t *testing.T) {
	dir := t.TempDir()
	e := nsEngine(t, dir)
	if _, err := e.Start("Flow", nil, "plain", WithTaskQueue("default")); err != nil {
		t.Fatal(err)
	}
	if f := e.store.Format(); f >= history.FormatNamespaces {
		t.Fatalf("format %d after a default-queue start", f)
	}
	evs, _ := e.store.ReadAll("plain")
	if evs[0].GetWorkflowStarted().GetTaskQueue() != "" || evs[1].GetStepScheduled().GetTaskQueue() != "" {
		t.Fatal("the default queue was recorded by name")
	}
	if _, err := e.Start("Flow", nil, "named", WithTaskQueue("q")); err != nil {
		t.Fatal(err)
	}
	if f := e.store.Format(); f != history.FormatNamespaces {
		t.Fatalf("format %d after a named-queue start, want %d", f, history.FormatNamespaces)
	}
	evs, _ = e.store.ReadAll("named")
	for _, ev := range evs {
		if ev.GetMinReaderFormat() != history.FormatNamespaces {
			t.Fatalf("event %d is stamped %d, want %d", ev.GetEventId(), ev.GetMinReaderFormat(), history.FormatNamespaces)
		}
	}
}

func TestTaskQueueNamesAreChecked(t *testing.T) {
	e := nsEngine(t, t.TempDir())
	for _, bad := range []string{"has space", "-lead", "a/b", string(make([]byte, 129))} {
		if _, err := e.Start("Flow", nil, "", WithTaskQueue(bad)); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("start on %q: %v", bad, err)
		}
		if _, err := pollQueue(t, e, bad, 10*time.Millisecond); !errors.Is(err, ErrInvalidArgument) {
			t.Errorf("poll %q: %v", bad, err)
		}
	}
	if err := e.CreateSchedule(&Schedule{ScheduleId: "s", Cron: "@hourly", WorkflowType: "Flow", TaskQueue: "a b"}); !errors.Is(err, ErrInvalidArgument) {
		t.Errorf("schedule on a bad queue: %v", err)
	}
	if _, err := decisionFromCommand(&WorkflowCommand{Command: &WorkflowCommand_ScheduleActivity{
		ScheduleActivity: &ScheduleActivityCommand{StepName: "x", Sequence: 1, TaskQueue: "a b"}}}); !errors.Is(err, ErrInvalidCommand) {
		t.Errorf("command on a bad queue: %v", err)
	}
}

// A schedule's runs start on its queue.
func TestScheduleStartsOnItsQueue(t *testing.T) {
	e := nsEngine(t, t.TempDir())
	if err := e.CreateSchedule(&Schedule{ScheduleId: "tick", Cron: "@every 1s", WorkflowType: "Flow", TaskQueue: "cron-q"}); err != nil {
		t.Fatal(err)
	}
	task, err := pollQueue(t, e, "cron-q", 3*time.Second)
	if err != nil {
		t.Fatalf("the scheduled run's activity: %v", err)
	}
	if resp, _ := e.GetWorkflow(task.GetWorkflowId()); resp.GetSummary().GetTaskQueue() != "cron-q" {
		t.Fatalf("run = %v", resp.GetSummary())
	}
}
