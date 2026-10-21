package client_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// TestSignalRetriedAfterALostReplyIsRecordedOnce plays a leader that records a
// signal and loses leadership before it can reply: the first SignalWorkflow
// runs on the engine and then answers Unavailable, so the client retries the
// same call. The retry must not record the signal a second time.
func TestSignalRetriedAfterALostReplyIsRecordedOnce(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	e := server.NewEngine(store, logger)
	t.Cleanup(e.Close)
	e.Register(state.WorkflowDefinition{Type: "Wait", Steps: []state.StepDef{state.AwaitSignal("w", "never")}})

	var lost atomic.Bool
	gs := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
		resp, err := h(ctx, req)
		if info.FullMethod == "/workflow.v1.WorkflowService/SignalWorkflow" && err == nil && lost.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "leadership lost before the reply")
		}
		return resp, err
	}))
	server.RegisterWorkflowServiceServer(gs, server.NewGRPCServer(e, logger))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	c, err := client.Dial(client.Options{Addrs: []string{lis.Addr().String()},
		DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	id, err := c.StartWorkflow(ctx, "Wait", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.SignalWorkflow(ctx, id, "approve", []byte("yes")); err != nil {
		t.Fatalf("signal: %v", err)
	}
	if !lost.Load() {
		t.Fatal("the first reply was never lost; the test did not exercise a retry")
	}
	evs, err := store.ReadAll(id)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, ev := range evs {
		if ev.GetSignalReceived() != nil {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("the signal was recorded %d times, want exactly once", n)
	}
}
