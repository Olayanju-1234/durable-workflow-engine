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

// stubborn ignores a cancellation and waits again.
func stubborn(ctx workflow.Context, in []byte) ([]byte, error) {
	if _, err := ctx.NewTimer("first", time.Hour).Get(); err != nil {
		ctx.Sleep("again", time.Hour)
	}
	return nil, nil
}

// orderWithCompensation reserves, waits a long time to ship, and when told it
// is being cancelled releases the reservation and lets the cancellation end it.
func orderWithCompensation(ctx workflow.Context, in []byte) ([]byte, error) {
	reserved, err := ctx.ExecuteActivity("reserve", in)
	if err != nil {
		return nil, err
	}
	if _, err := ctx.NewTimer("settle", time.Hour).Get(); err != nil {
		if errors.Is(err, workflow.ErrWorkflowCancelled) {
			if _, cerr := ctx.ExecuteActivity("release", reserved); cerr != nil {
				return nil, cerr
			}
		}
		return nil, err
	}
	return ctx.ExecuteActivity("ship", reserved)
}

func waitForStep(t *testing.T, e *Engine, id, step string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := e.GetWorkflow(id); err == nil {
			for _, s := range resp.GetSteps() {
				if s.GetStepName() == step {
					return
				}
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s never reached step %q", id, step)
}

// A graceful cancel reaches the code at its wait; the compensation it starts
// runs normally, and the workflow ends CANCELLED.
func TestGracefulCancelRunsCompensation(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"Order": orderWithCompensation})
	runWorker(t, e)
	id, err := e.StartWorkflow("Order", []byte("o-1"), "")
	if err != nil {
		t.Fatal(err)
	}
	waitForStep(t, e, id, "settle")
	if err := e.RequestCancelWorkflow(id, "customer left"); err != nil {
		t.Fatal(err)
	}
	resp := waitFinished(t, e, id, 3*time.Second)
	if resp.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_CANCELLED || !resp.GetCancelRequested() ||
		resp.GetCancelReason() != "customer left" || !strings.Contains(resp.GetSummary().GetError(), "the workflow is being cancelled (customer left)") {
		t.Fatalf("workflow %v cancel_requested=%v reason=%q", resp.GetSummary(), resp.GetCancelRequested(), resp.GetCancelReason())
	}
	if got, want := stepStatuses(t, e, id), "reserve=COMPLETED settle=CANCELLED cancellation=COMPLETED release=COMPLETED"; got != want {
		t.Fatalf("steps %q, want %q", got, want)
	}
	if got := e.metrics.workflowsFinished.With("cancelled").Value(); got != 1 {
		t.Fatalf("workflows_finished_total{cancelled} = %v", got)
	}
}

// Code that catches the cancellation and returns normally completes.
func TestGracefulCancelCaughtCompletes(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Shrug": func(ctx workflow.Context, in []byte) ([]byte, error) {
			if _, err := ctx.NewTimer("wait", time.Hour).Get(); err != nil {
				return []byte("stopped early: " + err.Error()), nil
			}
			return []byte("waited"), nil
		},
	})
	id, err := e.StartWorkflow("Shrug", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.RequestCancelWorkflow(id, ""); err != nil {
		t.Fatal(err)
	}
	resp := waitFinished(t, e, id, 2*time.Second)
	if resp.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_COMPLETED || !strings.HasPrefix(string(resp.GetOutput()), "stopped early") {
		t.Fatalf("workflow %v output %q", resp.GetSummary(), resp.GetOutput())
	}
}

// A data-driven definition has no compensation: a graceful cancel ends it at
// once, and the worker running its activity is told.
func TestGracefulCancelDataDriven(t *testing.T) {
	e := newTestEngine(t, t.TempDir(), state.WorkflowDefinition{Type: "Pipe", Steps: state.Activities("a", "b")})
	id, err := e.StartWorkflow("Pipe", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	task := mustPoll(t, e, "w", time.Second)
	if err := e.RequestCancelWorkflow(id, "no longer needed"); err != nil {
		t.Fatal(err)
	}
	resp, err := e.GetWorkflow(id)
	if err != nil || resp.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_CANCELLED {
		t.Fatalf("workflow %v %v", resp.GetSummary(), err)
	}
	if err := e.Heartbeat(id, task.GetLeaseToken()); !errors.Is(err, ErrActivityCancelled) {
		t.Fatalf("heartbeat after the cancel: %v, want ErrActivityCancelled", err)
	}
}

func TestGracefulCancelRequestRules(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"Slow": waitsForever, "Stubborn": stubborn})
	id, err := e.StartWorkflow("Slow", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if err := e.SignalWorkflow(id, state.CancelSignal, nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a signal under the reserved name: %v, want ErrInvalidArgument", err)
	}
	// Slow's activity never runs, so the code is told and the run ends.
	if err := e.RequestCancelWorkflow(id, ""); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, e, id, 2*time.Second)
	if err := e.RequestCancelWorkflow(id, ""); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("cancel of a finished workflow: %v", err)
	}
	if err := e.RequestCancelWorkflow("nope", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cancel of an unknown workflow: %v", err)
	}

	// Asking twice records one request (this code ignores the first).
	id2, _ := e.StartWorkflow("Stubborn", nil, "")
	if err := e.RequestCancelWorkflow(id2, "first"); err != nil {
		t.Fatal(err)
	}
	waitForStep(t, e, id2, "again")
	if err := e.RequestCancelWorkflow(id2, "second"); err != nil {
		t.Fatal(err)
	}
	events, _ := e.store.ReadAll(id2)
	requests := 0
	for _, ev := range events {
		if ev.GetSignalReceived().GetCancelRequest() {
			requests++
		}
	}
	if requests != 1 {
		t.Fatalf("%d cancel requests recorded, want 1", requests)
	}
}

// A cancel requested while no SDK worker runs the type is delivered when one
// registers, though the workflow was waiting on a step.
func TestGracefulCancelReachesALateSDK(t *testing.T) {
	dir := t.TempDir()
	e := newTestEngine(t, dir)
	if err := e.RegisterWorkflowTypes([]string{"Remote"}, "sdk", "test"); err != nil {
		t.Fatal(err)
	}
	id, err := e.StartWorkflow("Remote", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	task, err := e.PollWorkflowTask(ctx, "sdk", []string{"Remote"})
	if err != nil {
		t.Fatal(err)
	}
	cmd := CommandFromDecision(state.Decision{Type: state.DecideStartTimer, StepName: "wait", Sequence: 1, Delay: time.Hour})
	if err := e.CompleteWorkflowTask(id, task.GetTaskToken(), []*WorkflowCommand{cmd}, ""); err != nil {
		t.Fatal(err)
	}
	e.Close()

	restarted := newTestEngine(t, dir) // registrations are gone
	if err := restarted.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := restarted.RequestCancelWorkflow(id, "bye"); err != nil {
		t.Fatal(err)
	}
	if err := restarted.RegisterWorkflowTypes([]string{"Remote"}, "sdk", "test"); err != nil {
		t.Fatal(err)
	}
	task, err = restarted.PollWorkflowTask(ctx, "sdk", []string{"Remote"})
	if err != nil {
		t.Fatalf("no workflow task after registering: %v", err)
	}
	last := task.GetHistory()[len(task.GetHistory())-1]
	if !last.GetSignalReceived().GetCancelRequest() {
		t.Fatalf("task's last event %v, want the cancel request", last)
	}
}
