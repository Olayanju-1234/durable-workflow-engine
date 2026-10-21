package server_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// authClient is a gRPC client for an engine whose server carries GRPCAuth.
func authClient(t *testing.T, token string, publicReads bool) server.WorkflowServiceClient {
	t.Helper()
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := server.NewEngine(store, logger)
	t.Cleanup(engine.Close)
	engine.Register(state.WorkflowDefinition{Type: "Flow", Steps: state.Activities("one")})
	if _, err := engine.StartWorkflow("Flow", nil, "seed"); err != nil {
		t.Fatal(err)
	}

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(server.GRPCAuth(token, publicReads)...)
	server.RegisterWorkflowServiceServer(gs, server.NewGRPCServer(engine, logger))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return server.NewWorkflowServiceClient(conn)
}

// calls covers a write, both reads, a worker poll and a terminate.
var authCalls = map[string]func(context.Context, server.WorkflowServiceClient) error{
	"StartWorkflow": func(ctx context.Context, c server.WorkflowServiceClient) error {
		_, err := c.StartWorkflow(ctx, &server.StartWorkflowRequest{WorkflowType: "Flow"})
		return err
	},
	"ListWorkflows": func(ctx context.Context, c server.WorkflowServiceClient) error {
		_, err := c.ListWorkflows(ctx, &server.ListWorkflowsRequest{})
		return err
	},
	"GetWorkflow": func(ctx context.Context, c server.WorkflowServiceClient) error {
		_, err := c.GetWorkflow(ctx, &server.GetWorkflowRequest{WorkflowId: "seed"})
		return err
	},
	"PollTask": func(ctx context.Context, c server.WorkflowServiceClient) error {
		_, err := c.PollTask(ctx, &server.PollTaskRequest{WorkerId: "w"})
		return err
	},
	"TerminateWorkflow": func(ctx context.Context, c server.WorkflowServiceClient) error {
		_, err := c.TerminateWorkflow(ctx, &server.TerminateWorkflowRequest{WorkflowId: "nope"})
		return err
	},
}

func callWith(t *testing.T, c server.WorkflowServiceClient, method, authorization string) codes.Code {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if authorization != "" {
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", authorization)
	}
	return status.Code(authCalls[method](ctx, c))
}

func TestGRPCAuth(t *testing.T) {
	c := authClient(t, "sekrit", false)
	for method := range authCalls {
		t.Run(method, func(t *testing.T) {
			for _, bad := range []string{"", "Bearer wrong", "bearer sekrit", "sekrit"} {
				if got := callWith(t, c, method, bad); got != codes.Unauthenticated {
					t.Errorf("authorization %q: got %v, want Unauthenticated", bad, got)
				}
			}
			if got := callWith(t, c, method, "Bearer sekrit"); got == codes.Unauthenticated {
				t.Errorf("valid token rejected")
			}
		})
	}
}

func TestGRPCAuthPublicReads(t *testing.T) {
	c := authClient(t, "sekrit", true)
	want := map[string]bool{"ListWorkflows": true, "GetWorkflow": true} // open without a token
	for method := range authCalls {
		got := callWith(t, c, method, "")
		if open := got != codes.Unauthenticated; open != want[method] {
			t.Errorf("%s without a token: %v, want open=%v", method, got, want[method])
		}
	}
}

func TestGRPCAuthDisabled(t *testing.T) {
	if opts := server.GRPCAuth("", true); len(opts) != 0 {
		t.Fatalf("no token should add no server options, got %d", len(opts))
	}
	c := authClient(t, "", false)
	if got := callWith(t, c, "StartWorkflow", ""); got != codes.OK {
		t.Errorf("no token configured: StartWorkflow = %v, want OK", got)
	}
}
