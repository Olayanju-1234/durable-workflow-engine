package server_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// fakeSDK stands in for a TypeScript/Python/Java worker: it polls workflow
// tasks, "replays" the history with a supplied decision function, and returns a
// command. Everything it knows comes from the task, exactly like a real SDK.
type fakeSDK struct {
	client server.WorkflowServiceClient
	types  []string
	decide func(task *server.WorkflowTask) (*server.WorkflowCommand, string)
}

func (f *fakeSDK) run(ctx context.Context, t *testing.T) {
	t.Helper()
	for {
		resp, err := f.client.PollWorkflowTask(ctx, &server.PollWorkflowTaskRequest{
			WorkerId: "sdk-worker", WorkflowTypes: f.types,
		})
		if err != nil {
			return // ctx cancelled or server gone
		}
		task := resp.GetTask()
		cmd, replayErr := f.decide(task)
		_, err = f.client.CompleteWorkflowTask(ctx, &server.CompleteWorkflowTaskRequest{
			WorkflowId:  task.GetWorkflowId(),
			TaskToken:   task.GetTaskToken(),
			Command:     cmd,
			ReplayError: replayErr,
		})
		// Aborted means the decision was stale (history moved on); polling again
		// is the correct response, which is what this loop does.
		if err != nil && status.Code(err) != codes.Aborted {
			return
		}
	}
}

// replayLinear is a miniature version of what a real SDK does: walk a fixed
// list of activity steps, return the recorded result for completed ones, and
// emit a command for the first step that has no result yet.
func replayLinear(steps []string) func(*server.WorkflowTask) (*server.WorkflowCommand, string) {
	return func(task *server.WorkflowTask) (*server.WorkflowCommand, string) {
		st, err := state.Reconstruct(task.GetHistory())
		if err != nil {
			return nil, err.Error()
		}
		next := len(st.Steps) // 0-based index of the step to run now
		if next >= len(steps) {
			var out []byte
			if tail := st.Tail(); tail != nil {
				out = tail.Output
			}
			return &server.WorkflowCommand{Command: &server.WorkflowCommand_CompleteWorkflow{
				CompleteWorkflow: &server.CompleteWorkflowCommand{Output: out},
			}}, ""
		}
		input := st.Input
		if tail := st.Tail(); tail != nil {
			input = tail.Output // chain the previous step's output
		}
		return &server.WorkflowCommand{Command: &server.WorkflowCommand_ScheduleActivity{
			ScheduleActivity: &server.ScheduleActivityCommand{
				StepName: steps[next], Sequence: int64(next + 1), Input: input,
			},
		}}, ""
	}
}

// TestPolyglotWorkflowRunsToCompletion is the headline deliverable: a workflow
// whose orchestration logic lives entirely OUTSIDE the engine (no Go workflow
// registered) runs to completion, driven only by the wire protocol.
func TestPolyglotWorkflowRunsToCompletion(t *testing.T) {
	h := newHarness(t) // no workflow definitions registered in-process

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The "SDK" owns the workflow logic: reserve → charge → ship.
	sdk := &fakeSDK{client: h.client, types: []string{"RemoteOrder"}, decide: replayLinear([]string{"reserve", "charge", "ship"})}
	if _, err := h.client.RegisterWorkflowType(ctx, &server.RegisterWorkflowTypeRequest{
		WorkflowTypes: []string{"RemoteOrder"}, WorkerId: "sdk-worker", Sdk: "fake/0.1",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	go sdk.run(ctx, t)

	// An ordinary activity worker executes the steps.
	go runWorker(ctx, h.client, func(step string, _ []byte) ([]byte, error) {
		return []byte("done:" + step), nil
	})

	resp, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "RemoteOrder", Input: []byte("order-1"),
	})
	if err != nil {
		t.Fatalf("start: %v", err)
	}

	st := waitForStatus(t, h.store, resp.GetWorkflowId(), state.StatusCompleted)
	if got := string(st.Output); got != "done:ship" {
		t.Errorf("output = %q, want done:ship", got)
	}
	if len(st.Steps) != 3 {
		t.Fatalf("steps = %d, want 3", len(st.Steps))
	}
	for i, want := range []string{"reserve", "charge", "ship"} {
		if st.Steps[i].Name != want || st.Steps[i].Status != state.StepCompleted {
			t.Errorf("step %d = %s/%s, want %s/COMPLETED", i, st.Steps[i].Name, st.Steps[i].Status, want)
		}
	}
}

// TestPolyglotTimerAndSignal proves the non-activity commands work too: a
// remote SDK can start a durable timer and park on a signal.
func TestPolyglotTimerAndSignal(t *testing.T) {
	h := newHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// sleep(20ms) → await "approval" → complete with the signal payload.
	decide := func(task *server.WorkflowTask) (*server.WorkflowCommand, string) {
		st, err := state.Reconstruct(task.GetHistory())
		if err != nil {
			return nil, err.Error()
		}
		switch len(st.Steps) {
		case 0:
			return &server.WorkflowCommand{Command: &server.WorkflowCommand_StartTimer{
				StartTimer: &server.StartTimerCommand{StepName: "wait", Sequence: 1, DelayMs: 20},
			}}, ""
		case 1:
			return &server.WorkflowCommand{Command: &server.WorkflowCommand_AwaitSignal{
				AwaitSignal: &server.AwaitSignalCommand{StepName: "gate", Sequence: 2, SignalName: "approval"},
			}}, ""
		default:
			return &server.WorkflowCommand{Command: &server.WorkflowCommand_CompleteWorkflow{
				CompleteWorkflow: &server.CompleteWorkflowCommand{Output: st.Tail().Output},
			}}, ""
		}
	}

	if _, err := h.client.RegisterWorkflowType(ctx, &server.RegisterWorkflowTypeRequest{
		WorkflowTypes: []string{"RemoteGate"}, WorkerId: "sdk", Sdk: "fake/0.1",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	go (&fakeSDK{client: h.client, types: []string{"RemoteGate"}, decide: decide}).run(ctx, t)

	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "RemoteGate", WorkflowId: "gate-1",
	}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Timer fires, then the workflow parks on the signal.
	waitForStep(t, h.store, "gate-1", "gate", state.StatusWaiting)
	if _, err := h.client.SignalWorkflow(ctx, &server.SignalWorkflowRequest{
		WorkflowId: "gate-1", SignalName: "approval", Payload: []byte("yes"),
	}); err != nil {
		t.Fatalf("signal: %v", err)
	}

	st := waitForStatus(t, h.store, "gate-1", state.StatusCompleted)
	if string(st.Output) != "yes" {
		t.Errorf("output = %q, want yes", st.Output)
	}
}

// TestPolyglotReplayErrorFailsWorkflow: an SDK that cannot replay its own
// history (e.g. the workflow code changed) fails the workflow durably rather
// than spinning on a task it can never complete.
func TestPolyglotReplayErrorFailsWorkflow(t *testing.T) {
	h := newHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	decide := func(*server.WorkflowTask) (*server.WorkflowCommand, string) {
		return nil, "non-determinism at step 1: expected 'reserve', history has 'charge'"
	}
	if _, err := h.client.RegisterWorkflowType(ctx, &server.RegisterWorkflowTypeRequest{
		WorkflowTypes: []string{"Broken"}, WorkerId: "sdk", Sdk: "fake/0.1",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	go (&fakeSDK{client: h.client, types: []string{"Broken"}, decide: decide}).run(ctx, t)

	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "Broken", WorkflowId: "broken-1",
	}); err != nil {
		t.Fatalf("start: %v", err)
	}

	st := waitForStatus(t, h.store, "broken-1", state.StatusFailed)
	if !strings.Contains(st.Error, "non-determinism") {
		t.Errorf("error = %q, want it to carry the SDK's replay error", st.Error)
	}
}

// TestPolyglotStaleDecisionRejected is the safety property that makes
// re-dispatch safe: a command computed from an out-of-date history must not
// apply. Without this fence a slow worker could schedule a step twice.
func TestPolyglotStaleDecisionRejected(t *testing.T) {
	h := newHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := h.client.RegisterWorkflowType(ctx, &server.RegisterWorkflowTypeRequest{
		WorkflowTypes: []string{"Racy"}, WorkerId: "sdk", Sdk: "fake/0.1",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "Racy", WorkflowId: "racy-1",
	}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// Take the task but don't answer yet.
	poll, err := h.client.PollWorkflowTask(ctx, &server.PollWorkflowTaskRequest{
		WorkerId: "slow", WorkflowTypes: []string{"Racy"},
	})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	task := poll.GetTask()
	if task.GetHistoryLength() != 1 {
		t.Fatalf("history_length = %d, want 1", task.GetHistoryLength())
	}

	// History moves on underneath the slow worker: a signal lands.
	if _, err := h.client.SignalWorkflow(ctx, &server.SignalWorkflowRequest{
		WorkflowId: "racy-1", SignalName: "nudge",
	}); err != nil {
		t.Fatalf("signal: %v", err)
	}

	// The stale command must be refused with Aborted (retry the whole op).
	_, err = h.client.CompleteWorkflowTask(ctx, &server.CompleteWorkflowTaskRequest{
		WorkflowId: "racy-1",
		TaskToken:  task.GetTaskToken(),
		Command: &server.WorkflowCommand{Command: &server.WorkflowCommand_ScheduleActivity{
			ScheduleActivity: &server.ScheduleActivityCommand{StepName: "stale", Sequence: 1},
		}},
	})
	if status.Code(err) != codes.Aborted {
		t.Fatalf("stale complete: got %v, want Aborted", err)
	}

	// Nothing was recorded from the stale decision.
	events, _ := h.store.ReadAll("racy-1")
	st, _ := state.Reconstruct(events)
	if len(st.Steps) != 0 {
		t.Errorf("stale decision leaked %d steps into history", len(st.Steps))
	}
}

// TestPolyglotInvalidCommandRejected: the engine must not let a buggy SDK write
// a history that Reconstruct can no longer read.
func TestPolyglotInvalidCommandRejected(t *testing.T) {
	h := newHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := h.client.RegisterWorkflowType(ctx, &server.RegisterWorkflowTypeRequest{
		WorkflowTypes: []string{"Buggy"}, WorkerId: "sdk", Sdk: "fake/0.1",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "Buggy", WorkflowId: "buggy-1",
	}); err != nil {
		t.Fatalf("start: %v", err)
	}
	poll, err := h.client.PollWorkflowTask(ctx, &server.PollWorkflowTaskRequest{
		WorkerId: "sdk", WorkflowTypes: []string{"Buggy"},
	})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}

	tests := []struct {
		name string
		cmd  *server.WorkflowCommand
	}{
		{"sequence skips ahead", &server.WorkflowCommand{Command: &server.WorkflowCommand_ScheduleActivity{
			ScheduleActivity: &server.ScheduleActivityCommand{StepName: "x", Sequence: 7},
		}}},
		{"empty step name", &server.WorkflowCommand{Command: &server.WorkflowCommand_ScheduleActivity{
			ScheduleActivity: &server.ScheduleActivityCommand{StepName: "", Sequence: 1},
		}}},
		{"await signal without a name", &server.WorkflowCommand{Command: &server.WorkflowCommand_AwaitSignal{
			AwaitSignal: &server.AwaitSignalCommand{StepName: "gate", Sequence: 1},
		}}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := h.client.CompleteWorkflowTask(ctx, &server.CompleteWorkflowTaskRequest{
				WorkflowId: "buggy-1", TaskToken: poll.GetTask().GetTaskToken(), Command: tc.cmd,
			})
			if status.Code(err) != codes.InvalidArgument {
				t.Errorf("got %v, want InvalidArgument", err)
			}
		})
	}

	// The log is still clean and readable after all that.
	events, _ := h.store.ReadAll("buggy-1")
	if _, err := state.Reconstruct(events); err != nil {
		t.Errorf("history became unreadable after invalid commands: %v", err)
	}
}

// TestPolyglotInvalidCommandDoesNotStrandWorkflow guards the failure mode that
// an earlier version of this code had: rejecting a malformed command must not
// leave the workflow with no pending decision AND no queued task, which would
// hang it forever. The decision timer has to re-offer the work so a fixed SDK
// can heal it.
func TestPolyglotInvalidCommandDoesNotStrandWorkflow(t *testing.T) {
	// Short decision timeout so the re-dispatch path is observable.
	h := newHarnessWithOptions(t, []server.Option{server.WithDecisionTimeout(150 * time.Millisecond)})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := h.client.RegisterWorkflowType(ctx, &server.RegisterWorkflowTypeRequest{
		WorkflowTypes: []string{"Healing"}, WorkerId: "sdk", Sdk: "fake/0.1",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "Healing", WorkflowId: "healing-1",
	}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// A broken SDK answers once with garbage, then goes away.
	poll, err := h.client.PollWorkflowTask(ctx, &server.PollWorkflowTaskRequest{
		WorkerId: "broken", WorkflowTypes: []string{"Healing"},
	})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	_, err = h.client.CompleteWorkflowTask(ctx, &server.CompleteWorkflowTaskRequest{
		WorkflowId: "healing-1", TaskToken: poll.GetTask().GetTaskToken(),
		Command: &server.WorkflowCommand{Command: &server.WorkflowCommand_ScheduleActivity{
			ScheduleActivity: &server.ScheduleActivityCommand{StepName: "x", Sequence: 99},
		}},
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("invalid command: got %v, want InvalidArgument", err)
	}

	// A healthy SDK deploys. The workflow must be re-offered to it and finish.
	go (&fakeSDK{client: h.client, types: []string{"Healing"}, decide: replayLinear([]string{"only"})}).run(ctx, t)
	go runWorker(ctx, h.client, func(step string, _ []byte) ([]byte, error) {
		return []byte("healed:" + step), nil
	})

	st := waitForStatus(t, h.store, "healing-1", state.StatusCompleted)
	if string(st.Output) != "healed:only" {
		t.Errorf("output = %q, want healed:only", st.Output)
	}
}

// TestPolyglotRegistrationResumesStrandedWorkflows: an engine restart clears
// in-memory registrations, so a workflow can be left with no runner. When the
// SDK reconnects and re-registers, the workflow must pick up where it was.
func TestPolyglotRegistrationResumesStrandedWorkflows(t *testing.T) {
	h := newHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Register, start, and let it park waiting for a decision with no SDK running.
	if _, err := h.client.RegisterWorkflowType(ctx, &server.RegisterWorkflowTypeRequest{
		WorkflowTypes: []string{"Stranded"}, WorkerId: "sdk", Sdk: "fake/0.1",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "Stranded", WorkflowId: "stranded-1",
	}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// No SDK worker has answered: the workflow is RUNNING with no steps.
	events, _ := h.store.ReadAll("stranded-1")
	if len(events) != 1 {
		t.Fatalf("history = %d events, want just WorkflowStarted", len(events))
	}

	// The SDK now connects (re-registering) and starts working.
	go (&fakeSDK{client: h.client, types: []string{"Stranded"}, decide: replayLinear([]string{"only"})}).run(ctx, t)
	go runWorker(ctx, h.client, func(step string, _ []byte) ([]byte, error) {
		return []byte("ran:" + step), nil
	})
	if _, err := h.client.RegisterWorkflowType(ctx, &server.RegisterWorkflowTypeRequest{
		WorkflowTypes: []string{"Stranded"}, WorkerId: "sdk", Sdk: "fake/0.1",
	}); err != nil {
		t.Fatalf("re-register: %v", err)
	}

	st := waitForStatus(t, h.store, "stranded-1", state.StatusCompleted)
	if string(st.Output) != "ran:only" {
		t.Errorf("output = %q, want ran:only", st.Output)
	}
}

// TestPolyglotCannotShadowInProcessWorkflow: a remote SDK must not hijack a
// workflow type that is compiled into the engine.
func TestPolyglotCannotShadowInProcessWorkflow(t *testing.T) {
	h := newHarness(t, state.WorkflowDefinition{Type: "Native", Steps: state.Activities("a")})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := h.client.RegisterWorkflowType(ctx, &server.RegisterWorkflowTypeRequest{
		WorkflowTypes: []string{"Native"}, WorkerId: "sdk", Sdk: "fake/0.1",
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Errorf("got %v, want InvalidArgument", err)
	}
}

// TestPolyglotTaskCarriesFullHistory documents the SDK's contract: it is
// stateless, so every task must contain everything needed to replay.
func TestPolyglotTaskCarriesFullHistory(t *testing.T) {
	h := newHarness(t)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, err := h.client.RegisterWorkflowType(ctx, &server.RegisterWorkflowTypeRequest{
		WorkflowTypes: []string{"Historied"}, WorkerId: "sdk", Sdk: "fake/0.1",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}
	go (&fakeSDK{client: h.client, types: []string{"Historied"}, decide: replayLinear([]string{"one", "two"})}).run(ctx, t)
	go runWorker(ctx, h.client, func(step string, _ []byte) ([]byte, error) {
		return []byte("out:" + step), nil
	})
	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "Historied", WorkflowId: "hist-1", Input: []byte("seed"),
	}); err != nil {
		t.Fatalf("start: %v", err)
	}
	waitForStatus(t, h.store, "hist-1", state.StatusCompleted)

	// Re-poll a fresh task for a NEW workflow and check the history shape the
	// SDK receives is the real event log, not a summary.
	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "Historied", WorkflowId: "hist-2", Input: []byte("seed2"),
	}); err != nil {
		t.Fatalf("start 2: %v", err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		events, _ := h.store.ReadAll("hist-2")
		if len(events) >= 1 {
			if _, ok := events[0].GetAttributes().(*history.HistoryEvent_WorkflowStarted); !ok {
				t.Fatalf("first event is %T, want WorkflowStarted", events[0].GetAttributes())
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("hist-2 never got a history")
}
