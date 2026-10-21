package main

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

func TestBrowseURL(t *testing.T) {
	tests := map[string]string{
		":8233":            "http://localhost:8233",
		"0.0.0.0:8233":     "http://localhost:8233",
		"[::]:8233":        "http://localhost:8233",
		"127.0.0.1:8233":   "http://127.0.0.1:8233",
		"[::1]:8233":       "http://[::1]:8233",
		"example.com:8233": "http://example.com:8233",
	}
	for addr, want := range tests {
		if got := browseURL(addr); got != want {
			t.Errorf("browseURL(%q) = %q, want %q", addr, got, want)
		}
	}
}

// demoHarness is an engine with the built-in demo types and a fast demo worker.
func demoHarness(t *testing.T, flakyRate float64) *server.Engine {
	t.Helper()
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := server.NewEngine(store, logger)
	t.Cleanup(engine.Close)
	registerDemoTypes(engine)

	w := newDemoWorker(engine, logger)
	w.pause = func() time.Duration { return 0 }
	w.flakyRate = flakyRate
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { w.run(ctx, 2); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return engine
}

func waitForTerminal(t *testing.T, e *server.Engine, id string) *server.GetWorkflowResponse {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := e.GetWorkflow(id)
		if err != nil {
			t.Fatalf("get %s: %v", id, err)
		}
		switch resp.GetSummary().GetStatus() {
		case server.WorkflowStatus_WORKFLOW_STATUS_COMPLETED, server.WorkflowStatus_WORKFLOW_STATUS_FAILED:
			return resp
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("workflow %s did not finish", id)
	return nil
}

func stepAttempts(resp *server.GetWorkflowResponse) map[string]int32 {
	out := map[string]int32{}
	for _, s := range resp.GetSteps() {
		out[s.GetStepName()] = s.GetAttempts()
	}
	return out
}

// TestDemoWorkerCompletesWorkflows runs both demo types end to end. With every
// first charge failing, the saga must still complete on its retry.
func TestDemoWorkerCompletesWorkflows(t *testing.T) {
	e := demoHarness(t, 1.0)

	for _, typ := range []string{"OrderWorkflow", "OrderSaga"} {
		id, err := e.StartWorkflow(typ, []byte("order-7"), "")
		if err != nil {
			t.Fatalf("start %s: %v", typ, err)
		}
		resp := waitForTerminal(t, e, id)
		if got := resp.GetSummary().GetStatus(); got != server.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
			t.Fatalf("%s: status %s (%s), want COMPLETED", typ, got, resp.GetSummary().GetError())
		}
		if string(resp.GetOutput()) != "shipped:order-7" {
			t.Errorf("%s: output %q, want shipped:order-7", typ, resp.GetOutput())
		}
		if n := stepAttempts(resp)["charge"]; n != 2 {
			t.Errorf("%s: charge attempts = %d, want 2 (one transient failure, one retry)", typ, n)
		}
	}
}

// TestDemoWorkerDeclineCompensates proves a "decline" input exhausts the charge
// retries and the saga releases the reservation before failing.
func TestDemoWorkerDeclineCompensates(t *testing.T) {
	e := demoHarness(t, 0)

	id, err := e.StartWorkflow("OrderSaga", []byte("order-9-decline"), "")
	if err != nil {
		t.Fatal(err)
	}
	resp := waitForTerminal(t, e, id)
	if got := resp.GetSummary().GetStatus(); got != server.WorkflowStatus_WORKFLOW_STATUS_FAILED {
		t.Fatalf("status %s, want FAILED", got)
	}
	if !strings.Contains(resp.GetSummary().GetError(), "card declined") {
		t.Errorf("error %q does not mention the decline", resp.GetSummary().GetError())
	}
	attempts := stepAttempts(resp)
	if attempts["charge"] != 3 || attempts["release"] != 1 {
		t.Errorf("attempts = %v, want charge=3 release=1", attempts)
	}
}

// TestDemoWorkerIgnoresOtherSteps proves the demo worker leaves a task it does
// not implement on the queue for whichever worker does.
func TestDemoWorkerIgnoresOtherSteps(t *testing.T) {
	e := demoHarness(t, 0)
	e.Register(state.WorkflowDefinition{Type: "Custom", Steps: state.Activities("send-email")})

	if _, err := e.StartWorkflow("Custom", nil, "custom-1"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	if depth := e.Stats().TaskQueueDepth; depth != 1 {
		t.Fatalf("task queue depth = %d, want 1 (send-email untouched)", depth)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	task, err := e.PollTask(ctx, "email-worker", "send-email")
	if err != nil || task.GetWorkflowId() != "custom-1" {
		t.Fatalf("email worker poll = %v, %v; want custom-1", task, err)
	}
}
