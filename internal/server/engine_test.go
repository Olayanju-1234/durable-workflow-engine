package server_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// harness is a full server stack wired over an in-memory listener: a real
// Engine + gRPC server reachable through a real gRPC client, with no TCP port.
// This exercises the actual network edge (marshalling, status codes, streaming
// poll) the way a deployed worker would, but stays fast and deterministic.
type harness struct {
	client server.WorkflowServiceClient
	store  *history.HistoryStore
	engine *server.Engine
}

func newHarness(t *testing.T, defs ...state.WorkflowDefinition) *harness {
	t.Helper()
	return newHarnessWithOptions(t, nil, defs...)
}

// newHarnessWithOptions is newHarness with engine tuning — short lease or
// decision timeouts let tests exercise the expiry paths in milliseconds.
func newHarnessWithOptions(t *testing.T, opts []server.Option, defs ...state.WorkflowDefinition) *harness {
	t.Helper()

	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := server.NewEngine(store, logger, opts...)
	t.Cleanup(engine.Close)
	for _, d := range defs {
		engine.Register(d)
	}

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	server.RegisterWorkflowServiceServer(gs, server.NewGRPCServer(engine, logger))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return &harness{client: server.NewWorkflowServiceClient(conn), store: store, engine: engine}
}

// runWorker is the canonical external worker: long-poll for a task, run it, then
// report the result — forever, until its context is cancelled. exec turns a step
// name + input into an output, or an error to fail the step.
func runWorker(ctx context.Context, client server.WorkflowServiceClient, exec func(step string, input []byte) ([]byte, error)) {
	for {
		resp, err := client.PollTask(ctx, &server.PollTaskRequest{WorkerId: "test-worker"})
		if err != nil {
			return // ctx cancelled / server gone
		}
		task := resp.GetTask()
		out, execErr := exec(task.GetStepName(), task.GetInput())

		req := &server.CompleteTaskRequest{
			WorkflowId: task.GetWorkflowId(),
			StepName:   task.GetStepName(),
			Sequence:   task.GetSequence(),
			LeaseToken: task.GetLeaseToken(),
		}
		if execErr != nil {
			req.Result = &server.CompleteTaskRequest_Error{Error: execErr.Error()}
		} else {
			req.Result = &server.CompleteTaskRequest_Output{Output: out}
		}
		if _, err := client.CompleteTask(ctx, req); err != nil {
			return
		}
	}
}

// waitForStatus polls the durable history until the workflow reaches want. Tests
// read the source of truth (the log), not in-process state, so they prove the
// effect actually persisted.
func waitForStatus(t *testing.T, store *history.HistoryStore, workflowID string, want state.Status) *state.State {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		events, err := store.ReadAll(workflowID)
		if err != nil {
			t.Fatalf("read history: %v", err)
		}
		st, err := state.Reconstruct(events)
		if err != nil {
			t.Fatalf("reconstruct: %v", err)
		}
		if st.Status == want {
			return st
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("workflow %s did not reach %s within deadline", workflowID, want)
	return nil
}

// TestEndToEndTwoStepWorkflow is the Week-3 deliverable: a two-step workflow runs
// to completion through the gRPC server and a real polling worker, end to end.
func TestEndToEndTwoStepWorkflow(t *testing.T) {
	h := newHarness(t, state.WorkflowDefinition{Type: "TwoStep", Steps: state.Activities("reserve", "ship")})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Worker succeeds every step, echoing the step name into its output.
	go runWorker(ctx, h.client, func(step string, _ []byte) ([]byte, error) {
		return []byte("done:" + step), nil
	})

	resp, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "TwoStep", Input: []byte("order-1"),
	})
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}
	workflowID := resp.GetWorkflowId()

	st := waitForStatus(t, h.store, workflowID, state.StatusCompleted)
	if got := string(st.Output); got != "done:ship" {
		t.Errorf("final output: got %q, want done:ship", got)
	}

	// started + 2×(scheduled+completed) + completed = 6 durable events.
	events, _ := h.store.ReadAll(workflowID)
	if len(events) != 6 {
		t.Errorf("history length: got %d, want 6", len(events))
	}
}

// TestEndToEndStepFailureFailsWorkflow covers the error path: a worker that fails
// a step must drive the whole workflow to FAILED (Phase 1 has no retry yet).
func TestEndToEndStepFailureFailsWorkflow(t *testing.T) {
	h := newHarness(t, state.WorkflowDefinition{Type: "Flaky", Steps: state.Activities("reserve", "charge")})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go runWorker(ctx, h.client, func(step string, _ []byte) ([]byte, error) {
		if step == "charge" {
			return nil, errors.New("card declined")
		}
		return []byte("reserved"), nil
	})

	resp, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "Flaky", Input: []byte("x"),
	})
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}

	st := waitForStatus(t, h.store, resp.GetWorkflowId(), state.StatusFailed)
	if st.Error != "card declined" {
		t.Errorf("failure error: got %q, want %q", st.Error, "card declined")
	}
}

// waitForStep polls the log until the tail step has the given name and status —
// used to wait for a specific point in a multi-step workflow, not just any WAITING.
func waitForStep(t *testing.T, store *history.HistoryStore, id, stepName string, want state.Status) *state.State {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		events, err := store.ReadAll(id)
		if err != nil {
			t.Fatalf("read history: %v", err)
		}
		st, err := state.Reconstruct(events)
		if err != nil {
			t.Fatalf("reconstruct: %v", err)
		}
		if st.Status == want && st.Tail() != nil && st.Tail().Name == stepName {
			return st
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("workflow %s did not reach %s at step %q", id, want, stepName)
	return nil
}

// TestSignalDeliveredToWaitingWorkflow exercises the SignalWorkflow RPC end to end:
// a workflow parks on an AwaitSignal step, an external SignalWorkflow call delivers
// the event, and the workflow resumes with the signal payload chained downstream.
func TestSignalDeliveredToWaitingWorkflow(t *testing.T) {
	def := state.WorkflowDefinition{Type: "Approval", Steps: []state.StepDef{
		state.Activity("submit"),
		state.AwaitSignal("await-approval", "approve"),
		state.Activity("finalize"),
	}}
	h := newHarness(t, def)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go runWorker(ctx, h.client, func(step string, _ []byte) ([]byte, error) {
		return []byte("done:" + step), nil
	})

	resp, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{WorkflowType: "Approval", Input: []byte("doc")})
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}
	id := resp.GetWorkflowId()

	// Wait until it is actually parked on the signal step, then deliver the signal.
	waitForStep(t, h.store, id, "await-approval", state.StatusWaiting)
	if _, err := h.client.SignalWorkflow(ctx, &server.SignalWorkflowRequest{
		WorkflowId: id, SignalName: "approve", Payload: []byte("approved"),
	}); err != nil {
		t.Fatalf("signal workflow: %v", err)
	}

	st := waitForStatus(t, h.store, id, state.StatusCompleted)
	if string(st.Output) != "done:finalize" {
		t.Errorf("final output: got %q, want done:finalize", st.Output)
	}
	if string(st.Steps[1].Output) != "approved" {
		t.Errorf("signal step output: got %q, want approved", st.Steps[1].Output)
	}
}

// TestSignalTerminalWorkflowRejected: signalling a finished workflow is a
// precondition failure, not a silent no-op.
func TestSignalTerminalWorkflowRejected(t *testing.T) {
	h := newHarness(t, state.WorkflowDefinition{Type: "One", Steps: state.Activities("only")})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	go runWorker(ctx, h.client, func(step string, _ []byte) ([]byte, error) { return []byte("ok"), nil })

	resp, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{WorkflowType: "One", Input: []byte("x")})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	waitForStatus(t, h.store, resp.GetWorkflowId(), state.StatusCompleted)

	_, err = h.client.SignalWorkflow(ctx, &server.SignalWorkflowRequest{WorkflowId: resp.GetWorkflowId(), SignalName: "late"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("signal terminal: got code %v (err %v), want FailedPrecondition", status.Code(err), err)
	}
}

// TestStartUnknownWorkflowTypeRejected: starting an unregistered type is a client
// error, surfaced as InvalidArgument rather than a silently stuck workflow.
func TestStartUnknownWorkflowTypeRejected(t *testing.T) {
	h := newHarness(t) // nothing registered

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{WorkflowType: "Nope"})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("got code %v (err %v), want InvalidArgument", status.Code(err), err)
	}
}

// TestHeartbeatRPC exercises the Heartbeat endpoint end to end: a dispatched
// task's token is accepted; a wrong token is a FailedPrecondition — the signal
// a worker uses to learn it no longer owns the task.
func TestHeartbeatRPC(t *testing.T) {
	h := newHarness(t, state.WorkflowDefinition{Type: "One", Steps: state.Activities("only")})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{WorkflowType: "One"}); err != nil {
		t.Fatalf("start: %v", err)
	}
	poll, err := h.client.PollTask(ctx, &server.PollTaskRequest{WorkerId: "hb-worker"})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	task := poll.GetTask()
	if task.GetLeaseToken() == "" || task.GetLeaseDurationMs() <= 0 {
		t.Fatalf("dispatched task carries no lease: token %q, duration %dms",
			task.GetLeaseToken(), task.GetLeaseDurationMs())
	}

	if _, err := h.client.Heartbeat(ctx, &server.HeartbeatRequest{
		WorkflowId: task.GetWorkflowId(), LeaseToken: task.GetLeaseToken(), WorkerId: "hb-worker",
	}); err != nil {
		t.Fatalf("heartbeat with valid token: %v", err)
	}
	if _, err := h.client.Heartbeat(ctx, &server.HeartbeatRequest{
		WorkflowId: task.GetWorkflowId(), LeaseToken: "wrong-token", WorkerId: "imposter",
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("heartbeat with wrong token: got code %v (err %v), want FailedPrecondition", status.Code(err), err)
	}
}

// TestDuplicateCompleteRejected proves the completion side of "no duplicate
// dispatch": once a step's result is recorded, a stale/replayed CompleteTask is
// rejected as a precondition failure instead of corrupting the history.
func TestDuplicateCompleteRejected(t *testing.T) {
	h := newHarness(t, state.WorkflowDefinition{Type: "One", Steps: state.Activities("only")})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{WorkflowType: "One", Input: []byte("x")})
	if err != nil {
		t.Fatalf("start workflow: %v", err)
	}

	poll, err := h.client.PollTask(ctx, &server.PollTaskRequest{WorkerId: "w"})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	task := poll.GetTask()

	complete := &server.CompleteTaskRequest{
		WorkflowId: task.GetWorkflowId(),
		StepName:   task.GetStepName(),
		Sequence:   task.GetSequence(),
		LeaseToken: task.GetLeaseToken(),
		Result:     &server.CompleteTaskRequest_Output{Output: []byte("ok")},
	}
	if _, err := h.client.CompleteTask(ctx, complete); err != nil {
		t.Fatalf("first complete: %v", err)
	}
	waitForStatus(t, h.store, resp.GetWorkflowId(), state.StatusCompleted)

	// The single step is already done (and its lease released); replaying the
	// same completion must fail.
	if _, err := h.client.CompleteTask(ctx, complete); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("duplicate complete: got code %v (err %v), want FailedPrecondition", status.Code(err), err)
	}
}
