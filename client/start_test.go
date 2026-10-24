package client_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// Start's policies travel to the engine: a taken id fails by default, comes
// back as the existing run with UseExisting, and a finished one may be reused.
func TestStartPolicies(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	e := server.NewEngine(store, logger)
	t.Cleanup(e.Close)
	e.Register(state.WorkflowDefinition{Type: "Wait", Steps: []state.StepDef{state.AwaitSignal("w", "go")}})
	gs := grpc.NewServer()
	server.RegisterWorkflowServiceServer(gs, server.NewGRPCServer(e, logger))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	c, err := client.Dial(client.Options{Addrs: []string{lis.Addr().String()}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	first, err := c.Start(ctx, "Wait", nil, client.WithWorkflowID("order-7"))
	if err != nil || !first.Started || first.RunID == "" {
		t.Fatalf("first start: %+v %v", first, err)
	}
	if _, err := c.Start(ctx, "Wait", nil, client.WithWorkflowID("order-7")); !errors.Is(err, client.ErrAlreadyExists) {
		t.Fatalf("second start: %v, want ErrAlreadyExists", err)
	}
	again, err := c.Start(ctx, "Wait", nil, client.WithWorkflowID("order-7"), client.WithIDConflictPolicy(client.UseExisting))
	if err != nil || again.Started || again.RunID != first.RunID {
		t.Fatalf("start with UseExisting: %+v %v, want the first run back", again, err)
	}

	if err := c.SignalWorkflow(ctx, "order-7", "go", nil); err != nil {
		t.Fatal(err)
	}
	if _, err := c.WaitForResult(ctx, "order-7"); err != nil {
		t.Fatal(err)
	}
	reused, err := c.Start(ctx, "Wait", nil, client.WithWorkflowID("order-7"), client.WithIDReusePolicy(client.AllowDuplicate))
	if err != nil || !reused.Started || reused.RunID == first.RunID {
		t.Fatalf("reuse: %+v %v", reused, err)
	}
	runs, err := c.ListWorkflowRuns(ctx, "order-7")
	if err != nil || len(runs) != 2 || runs[0].RunID != reused.RunID || !runs[0].Current || runs[1].Status != client.StatusCompleted {
		t.Fatalf("runs %+v %v", runs, err)
	}
	old, err := c.DescribeWorkflowRun(ctx, "order-7", first.RunID)
	if err != nil || old.Status != client.StatusCompleted || old.RunID != first.RunID {
		t.Fatalf("describe the first run: %+v %v", old, err)
	}
}
