// Package embedded runs a workflowd engine inside your own process, on a data
// directory you choose, for apps that want durable workflows without a separate
// server and for integration tests.
//
//	srv, err := embedded.Start(embedded.Options{DataDir: "data"})
//	defer srv.Close()
//	w := worker.New(srv.Client(), worker.Options{})
//	w.RegisterWorkflow("Order", Order)
//	go w.Run(ctx)
//	id, err := srv.Client().StartWorkflow(ctx, "Order", input)
//
// The engine is the same one workflowd runs: histories are fsync'd to DataDir and
// survive a restart of your process, and Start recovers whatever was in flight.
// It is reached through an in-memory gRPC connection, so workers and clients use
// exactly the code paths they use against a remote engine. The data directory
// must not be shared with another engine at the same time.
package embedded

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// Options configures an embedded engine.
type Options struct {
	// DataDir holds one history file per workflow. Required.
	DataDir string
	// LeaseDuration is how long a worker may hold an activity without
	// heartbeating before the task is retried elsewhere. Default 30s.
	LeaseDuration time.Duration
	// Logger receives engine logs. Nil discards them.
	Logger *slog.Logger
}

// Server is a running embedded engine.
type Server struct {
	store  *history.HistoryStore
	engine *server.Engine
	grpc   *grpc.Server
	lis    *bufconn.Listener
	client *client.Client
	served chan error
}

// Start opens the data directory, recovers in-flight workflows and serves the
// engine on an in-memory connection.
func Start(opts Options) (*Server, error) {
	if opts.DataDir == "" {
		return nil, errors.New("embedded: DataDir is required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	store, err := history.NewHistoryStore(opts.DataDir)
	if err != nil {
		return nil, fmt.Errorf("embedded: %w", err)
	}
	var engineOpts []server.Option
	if opts.LeaseDuration > 0 {
		engineOpts = append(engineOpts, server.WithLeaseDuration(opts.LeaseDuration))
	}
	engine := server.NewEngine(store, logger, engineOpts...)
	if err := engine.Recover(); err != nil {
		engine.Close()
		store.Close()
		return nil, fmt.Errorf("embedded: recover: %w", err)
	}

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	server.RegisterWorkflowServiceServer(gs, server.NewGRPCServer(engine, logger))
	s := &Server{store: store, engine: engine, grpc: gs, lis: lis, served: make(chan error, 1)}
	go func() { s.served <- gs.Serve(lis) }()

	s.client, err = client.Dial(client.Options{
		Addrs: []string{"passthrough:///workflowd-embedded"},
		DialOptions: []grpc.DialOption{
			grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		Logger: logger,
	})
	if err != nil {
		s.Close()
		return nil, fmt.Errorf("embedded: %w", err)
	}
	return s, nil
}

// Client returns a client connected to the embedded engine. Pass it to
// worker.New to run workflows; it is closed by Close.
func (s *Server) Client() *client.Client { return s.client }

// Close stops the engine and closes its data directory. Stop workers first:
// their pending polls end with an error once the engine is gone.
func (s *Server) Close() error {
	var errs []error
	if s.client != nil {
		errs = append(errs, s.client.Close())
	}
	s.grpc.Stop()
	if err := <-s.served; err != nil && !errors.Is(err, grpc.ErrServerStopped) {
		errs = append(errs, err)
	}
	s.engine.Close()
	errs = append(errs, s.store.Close())
	return errors.Join(errs...)
}
