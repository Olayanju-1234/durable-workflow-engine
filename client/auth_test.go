package client_test

import (
	"context"
	"crypto/tls"
	"errors"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/internal/tlsconfig"
	"github.com/olayanju-1234/durable-workflow-engine/internal/tlsconfig/tlstest"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// secureEngine serves gRPC on a real port with TLS and a required token, as
// workflowd --tls-cert --tls-key --auth-token-file does.
func secureEngine(t *testing.T, token string) (addr string, roots *tls.Config) {
	t.Helper()
	dir := t.TempDir()
	ca, err := tlstest.NewCA(dir, "client")
	if err != nil {
		t.Fatal(err)
	}
	cert, err := ca.Issue(dir, "server")
	if err != nil {
		t.Fatal(err)
	}
	srvTLS, err := tlsconfig.Server(cert.CertFile, cert.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := tlsconfig.LoadCAs(ca.CertFile)
	if err != nil {
		t.Fatal(err)
	}

	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := server.NewEngine(store, logger)
	t.Cleanup(engine.Close)
	engine.Register(state.WorkflowDefinition{Type: "Flow", Steps: state.Activities("one")})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(append(server.GRPCAuth(token, false), grpc.Creds(credentials.NewTLS(srvTLS)))...)
	server.RegisterWorkflowServiceServer(gs, server.NewGRPCServer(engine, logger))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return lis.Addr().String(), &tls.Config{RootCAs: pool}
}

func dial(t *testing.T, opts client.Options) *client.Client {
	t.Helper()
	c, err := client.Dial(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func TestClientOverTLSWithToken(t *testing.T) {
	addr, roots := secureEngine(t, "sekrit")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	ok := dial(t, client.Options{Addrs: []string{addr}, TLS: roots, AuthToken: "sekrit"})
	id, err := ok.StartWorkflow(ctx, "Flow", []byte("x"))
	if err != nil {
		t.Fatalf("TLS + token: %v", err)
	}
	if _, err := ok.DescribeWorkflow(ctx, id); err != nil {
		t.Fatalf("describe over TLS: %v", err)
	}

	noToken := dial(t, client.Options{Addrs: []string{addr}, TLS: roots})
	if _, err := noToken.StartWorkflow(ctx, "Flow", nil); !errors.Is(err, client.ErrUnauthenticated) {
		t.Fatalf("no token: %v, want ErrUnauthenticated", err)
	}
	wrong := dial(t, client.Options{Addrs: []string{addr}, TLS: roots, AuthToken: "nope"})
	if _, err := wrong.StartWorkflow(ctx, "Flow", nil); !errors.Is(err, client.ErrUnauthenticated) {
		t.Fatalf("wrong token: %v, want ErrUnauthenticated", err)
	}

	short, cancelShort := context.WithTimeout(ctx, 2*time.Second)
	defer cancelShort()
	plain := dial(t, client.Options{Addrs: []string{addr}, AuthToken: "sekrit"})
	if _, err := plain.StartWorkflow(short, "Flow", nil); err == nil {
		t.Fatal("a plaintext client reached a TLS-only engine")
	}
	untrusting := dial(t, client.Options{Addrs: []string{addr}, TLS: &tls.Config{}, AuthToken: "sekrit"})
	if _, err := untrusting.StartWorkflow(short, "Flow", nil); err == nil {
		t.Fatal("a client that does not trust the CA completed a call")
	}
}

// A worker with a rejected token stops at once instead of retrying forever.
func TestWorkerRunFailsOnRejectedToken(t *testing.T) {
	addr, roots := secureEngine(t, "sekrit")
	w := worker.New(dial(t, client.Options{Addrs: []string{addr}, TLS: roots, AuthToken: "nope"}), worker.Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	w.RegisterWorkflow("Order", func(ctx workflow.Context, in []byte) ([]byte, error) { return in, nil })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := w.Run(ctx)
	if ctx.Err() != nil {
		t.Fatal("Run kept retrying a rejected token")
	}
	if status.Code(errors.Unwrap(err)) != codes.Unauthenticated {
		t.Fatalf("Run = %v, want Unauthenticated", err)
	}
}
