package server_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// TestListAndGetWorkflows drives one workflow to completion and parks another
// on a signal wait, then checks that the visibility API reports both truthfully
// — status, type, steps, and the status filter.
func TestListAndGetWorkflows(t *testing.T) {
	h := newHarness(t,
		state.WorkflowDefinition{Type: "VisDone", Steps: state.Activities("only")},
		state.WorkflowDefinition{Type: "VisWait", Steps: []state.StepDef{state.AwaitSignal("gate", "approval")}},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go runWorker(ctx, h.client, func(step string, _ []byte) ([]byte, error) {
		return []byte("done:" + step), nil
	})

	done, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "VisDone", Input: []byte("in-1"), WorkflowId: "vis-done",
	})
	if err != nil {
		t.Fatalf("start VisDone: %v", err)
	}
	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "VisWait", WorkflowId: "vis-wait",
	}); err != nil {
		t.Fatalf("start VisWait: %v", err)
	}
	waitForStatus(t, h.store, done.GetWorkflowId(), state.StatusCompleted)

	list, err := h.client.ListWorkflows(ctx, &server.ListWorkflowsRequest{})
	if err != nil {
		t.Fatalf("list workflows: %v", err)
	}
	byID := map[string]*server.WorkflowSummary{}
	for _, s := range list.GetWorkflows() {
		byID[s.GetWorkflowId()] = s
	}
	if len(byID) != 2 {
		t.Fatalf("list: got %d workflows, want 2", len(byID))
	}
	if got := byID["vis-done"].GetStatus(); got != server.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
		t.Errorf("vis-done status = %v, want COMPLETED", got)
	}
	if got := byID["vis-wait"].GetStatus(); got != server.WorkflowStatus_WORKFLOW_STATUS_WAITING {
		t.Errorf("vis-wait status = %v, want WAITING", got)
	}
	if byID["vis-done"].GetStartedAt() == 0 || byID["vis-done"].GetUpdatedAt() < byID["vis-done"].GetStartedAt() {
		t.Errorf("vis-done timestamps: started_at=%d updated_at=%d",
			byID["vis-done"].GetStartedAt(), byID["vis-done"].GetUpdatedAt())
	}

	// The filter returns only matching workflows.
	filtered, err := h.client.ListWorkflows(ctx, &server.ListWorkflowsRequest{
		StatusFilter: server.WorkflowStatus_WORKFLOW_STATUS_WAITING,
	})
	if err != nil {
		t.Fatalf("filtered list: %v", err)
	}
	if n := len(filtered.GetWorkflows()); n != 1 || filtered.GetWorkflows()[0].GetWorkflowId() != "vis-wait" {
		t.Errorf("filtered list: got %d rows (want just vis-wait)", n)
	}

	// GetWorkflow: full step-level detail, as replay reconstructs it.
	got, err := h.client.GetWorkflow(ctx, &server.GetWorkflowRequest{WorkflowId: "vis-done"})
	if err != nil {
		t.Fatalf("get workflow: %v", err)
	}
	if got.GetSummary().GetWorkflowType() != "VisDone" {
		t.Errorf("type = %q, want VisDone", got.GetSummary().GetWorkflowType())
	}
	if string(got.GetInput()) != "in-1" || string(got.GetOutput()) != "done:only" {
		t.Errorf("input/output = %q/%q", got.GetInput(), got.GetOutput())
	}
	steps := got.GetSteps()
	if len(steps) != 1 {
		t.Fatalf("steps: got %d, want 1", len(steps))
	}
	s := steps[0]
	if s.GetStepName() != "only" || s.GetKind() != "ACTIVITY" || s.GetStatus() != "COMPLETED" || s.GetAttempts() != 1 {
		t.Errorf("step = %v", s)
	}

	// The signal wait shows which signal it is parked on.
	gotWait, err := h.client.GetWorkflow(ctx, &server.GetWorkflowRequest{WorkflowId: "vis-wait"})
	if err != nil {
		t.Fatalf("get vis-wait: %v", err)
	}
	if ws := gotWait.GetSteps(); len(ws) != 1 || ws[0].GetKind() != "SIGNAL" || ws[0].GetSignalName() != "approval" {
		t.Errorf("vis-wait steps = %v", ws)
	}

	// Unknown ids are NotFound, not empty responses.
	if _, err := h.client.GetWorkflow(ctx, &server.GetWorkflowRequest{WorkflowId: "no-such"}); status.Code(err) != codes.NotFound {
		t.Errorf("get unknown: got %v, want NotFound", err)
	}
}

// TestTerminateWorkflow: the operator kill switch durably fails the workflow,
// marks it TERMINATED in the API, and refuses double-kills and unknown ids.
func TestTerminateWorkflow(t *testing.T) {
	h := newHarness(t, state.WorkflowDefinition{Type: "Doomed", Steps: state.Activities("stuck")})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// No worker: the workflow parks WAITING on its first step, like a real
	// stuck execution an operator would kill.
	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "Doomed", WorkflowId: "doomed-1",
	}); err != nil {
		t.Fatalf("start: %v", err)
	}

	if _, err := h.client.TerminateWorkflow(ctx, &server.TerminateWorkflowRequest{
		WorkflowId: "doomed-1", Reason: "manual cleanup",
	}); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	// Durable effect: the log says FAILED with the terminated marker and reason.
	st := waitForStatus(t, h.store, "doomed-1", state.StatusFailed)
	if !st.Terminated {
		t.Error("state.Terminated = false, want true")
	}
	if !strings.Contains(st.Error, "manual cleanup") {
		t.Errorf("error = %q, want it to carry the reason", st.Error)
	}

	// The API presents operator kills as TERMINATED, not plain FAILED.
	got, err := h.client.GetWorkflow(ctx, &server.GetWorkflowRequest{WorkflowId: "doomed-1"})
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if s := got.GetSummary().GetStatus(); s != server.WorkflowStatus_WORKFLOW_STATUS_TERMINATED {
		t.Errorf("status = %v, want TERMINATED", s)
	}

	// A terminal workflow can't be killed again, signalled, or found by a typo.
	if _, err := h.client.TerminateWorkflow(ctx, &server.TerminateWorkflowRequest{WorkflowId: "doomed-1"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("double terminate: got %v, want FailedPrecondition", err)
	}
	if _, err := h.client.SignalWorkflow(ctx, &server.SignalWorkflowRequest{WorkflowId: "doomed-1", SignalName: "x"}); status.Code(err) != codes.FailedPrecondition {
		t.Errorf("signal after terminate: got %v, want FailedPrecondition", err)
	}
	if _, err := h.client.TerminateWorkflow(ctx, &server.TerminateWorkflowRequest{WorkflowId: "no-such"}); status.Code(err) != codes.NotFound {
		t.Errorf("terminate unknown: got %v, want NotFound", err)
	}
}

// TestTerminateFencesInFlightWorker: terminating a workflow whose task a worker
// is actively holding must reject that worker's late result — the same zombie
// fencing as lease expiry, so a kill can never be overwritten by a completion.
func TestTerminateFencesInFlightWorker(t *testing.T) {
	h := newHarness(t, state.WorkflowDefinition{Type: "Fenced", Steps: state.Activities("slow")})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{
		WorkflowType: "Fenced", WorkflowId: "fenced-1",
	}); err != nil {
		t.Fatalf("start: %v", err)
	}

	// The worker claims the task (and its lease), then the operator kills the
	// workflow out from under it.
	poll, err := h.client.PollTask(ctx, &server.PollTaskRequest{WorkerId: "w1"})
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	task := poll.GetTask()
	if _, err := h.client.TerminateWorkflow(ctx, &server.TerminateWorkflowRequest{
		WorkflowId: "fenced-1", Reason: "kill while in flight",
	}); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	_, err = h.client.CompleteTask(ctx, &server.CompleteTaskRequest{
		WorkflowId: task.GetWorkflowId(),
		StepName:   task.GetStepName(),
		Sequence:   task.GetSequence(),
		LeaseToken: task.GetLeaseToken(),
		Result:     &server.CompleteTaskRequest_Output{Output: []byte("too late")},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("late complete: got %v, want FailedPrecondition", err)
	}

	// The kill, not the worker's result, is what history remembers.
	st := waitForStatus(t, h.store, "fenced-1", state.StatusFailed)
	if !st.Terminated {
		t.Error("state.Terminated = false, want true")
	}
}

// TestMetricsEndToEnd scrapes the engine's registry over HTTP after driving
// real traffic, asserting the counters agree with what durably happened.
func TestMetricsEndToEnd(t *testing.T) {
	h := newHarness(t,
		state.WorkflowDefinition{Type: "Metered", Steps: state.Activities("a", "b")},
		// The to-be-killed workflow parks on a signal (no worker task), so the
		// shared worker can never race the terminate and skew the counters.
		state.WorkflowDefinition{Type: "MeteredWait", Steps: []state.StepDef{state.AwaitSignal("gate", "go")}},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go runWorker(ctx, h.client, func(step string, _ []byte) ([]byte, error) {
		return []byte(step), nil
	})

	run, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{WorkflowType: "Metered", WorkflowId: "m-done"})
	if err != nil {
		t.Fatalf("start m-done: %v", err)
	}
	waitForStatus(t, h.store, run.GetWorkflowId(), state.StatusCompleted)

	if _, err := h.client.StartWorkflow(ctx, &server.StartWorkflowRequest{WorkflowType: "MeteredWait", WorkflowId: "m-kill"}); err != nil {
		t.Fatalf("start m-kill: %v", err)
	}
	if _, err := h.client.TerminateWorkflow(ctx, &server.TerminateWorkflowRequest{WorkflowId: "m-kill", Reason: "test"}); err != nil {
		t.Fatalf("terminate m-kill: %v", err)
	}

	// Scrape via the same HTTP handler workflowd mounts at /metrics. Counter
	// increments trail the durable append by nanoseconds (same critical
	// section), so poll briefly instead of asserting one instant snapshot.
	wants := []string{
		"workflowd_workflows_started_total 2",
		`workflowd_workflows_finished_total{status="completed"} 1`,
		`workflowd_workflows_finished_total{status="terminated"} 1`,
		"workflowd_workflows_active 0",
		"workflowd_tasks_dispatched_total 2",
		`workflowd_task_results_total{result="completed"} 2`,
		"workflowd_step_duration_seconds_count 2",
		"workflowd_task_queue_depth 0",
	}
	deadline := time.Now().Add(3 * time.Second)
	var body string
	for {
		rec := httptest.NewRecorder()
		h.engine.Metrics().Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
		body = rec.Body.String()
		missing := ""
		for _, w := range wants {
			if !strings.Contains(body, w+"\n") {
				missing = w
				break
			}
		}
		if missing == "" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("metric %q not observed; scrape:\n%s", missing, body)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
