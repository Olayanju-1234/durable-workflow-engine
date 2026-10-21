// Package client starts, signals, terminates and inspects workflows on a
// workflowd engine over gRPC.
//
//	c, err := client.Dial(client.Options{Addrs: []string{"localhost:7233"}})
//	id, err := c.StartWorkflow(ctx, "Order", []byte("order-7"))
//	out, err := c.WaitForResult(ctx, id)
//
// Against a cluster, list every node in Options.Addrs: calls follow the leader
// hints followers return, so any reachable node is a valid entry point.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/internal/rpc"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// Errors a call can be matched against with errors.Is.
var (
	ErrNotFound       = errors.New("client: workflow not found")
	ErrAlreadyExists  = errors.New("client: workflow id already in use")
	ErrNotRunning     = errors.New("client: workflow is not running")
	ErrInvalid        = errors.New("client: invalid request")
	ErrUnavailable    = errors.New("client: no workflowd node is serving")
	ErrWorkflowFailed = errors.New("client: workflow did not complete")
)

func init() {
	rpc.ConnOf = func(c any) *rpc.Conn { return c.(*Client).conn }
}

// Options configures a Client.
type Options struct {
	// Addrs are workflowd gRPC addresses, e.g. "localhost:7233". For a cluster,
	// list every node.
	Addrs []string
	// DialOptions replace the default (plaintext; workflowd has no TLS of its
	// own, so put a TLS-terminating proxy in front of it across networks).
	DialOptions []grpc.DialOption
	// Logger receives connection-level debug logs. Nil discards them.
	Logger *slog.Logger
}

// Client is safe for concurrent use. Close it when done.
type Client struct {
	conn *rpc.Conn
	log  *slog.Logger
}

// Dial creates a client. It does not contact the engine; the first call does.
func Dial(opts Options) (*Client, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	dialOpts := opts.DialOptions
	if len(dialOpts) == 0 {
		dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := rpc.New(opts.Addrs, dialOpts, logger)
	if err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}
	return &Client{conn: conn, log: logger}, nil
}

// Close releases the client's connections.
func (c *Client) Close() error { return c.conn.Close() }

// StartOption customises StartWorkflow.
type StartOption func(*startOptions)

type startOptions struct{ id string }

// WithWorkflowID starts the workflow under a caller-chosen id, for example an
// order number, so a second start for the same id fails with ErrAlreadyExists.
func WithWorkflowID(id string) StartOption {
	return func(o *startOptions) { o.id = id }
}

// StartWorkflow starts a workflow of a registered type and returns its id.
func (c *Client) StartWorkflow(ctx context.Context, workflowType string, input []byte, opts ...StartOption) (string, error) {
	var so startOptions
	for _, o := range opts {
		o(&so)
	}
	req := &server.StartWorkflowRequest{WorkflowType: workflowType, Input: input, WorkflowId: so.id}
	generated := req.WorkflowId == ""
	if generated {
		// Chosen here, not on the server, so a retry after a lost reply reuses it.
		req.WorkflowId = fmt.Sprintf("%s-%s", workflowType, randomSuffix())
	}
	attempts, err := c.conn.Call(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
		_, err := s.StartWorkflow(ctx, req, opt)
		return err
	})
	if generated && attempts > 1 && status.Code(err) == codes.AlreadyExists {
		// Only an earlier attempt of this call can hold a fresh random id: a
		// leader accepted it and then lost leadership before replying.
		return req.WorkflowId, nil
	}
	if err != nil {
		return "", wrap("start workflow", err)
	}
	return req.WorkflowId, nil
}

// SignalWorkflow sends a named signal with a payload to a running workflow. If
// leadership changes while the call is in flight, the retry may record the
// signal twice; workflows that care should make signal handling idempotent.
func (c *Client) SignalWorkflow(ctx context.Context, workflowID, signal string, payload []byte) error {
	req := &server.SignalWorkflowRequest{WorkflowId: workflowID, SignalName: signal, Payload: payload}
	_, err := c.conn.Call(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
		_, err := s.SignalWorkflow(ctx, req, opt)
		return err
	})
	return wrap("signal workflow", err)
}

// TerminateWorkflow ends a running workflow durably, recording the reason.
func (c *Client) TerminateWorkflow(ctx context.Context, workflowID, reason string) error {
	req := &server.TerminateWorkflowRequest{WorkflowId: workflowID, Reason: reason}
	_, err := c.conn.Call(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
		_, err := s.TerminateWorkflow(ctx, req, opt)
		return err
	})
	return wrap("terminate workflow", err)
}

// DescribeWorkflow returns a workflow's status, steps, input and output.
func (c *Client) DescribeWorkflow(ctx context.Context, workflowID string) (*WorkflowDescription, error) {
	var resp *server.GetWorkflowResponse
	_, err := c.conn.Call(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
		var err error
		resp, err = s.GetWorkflow(ctx, &server.GetWorkflowRequest{WorkflowId: workflowID}, opt)
		return err
	})
	if err != nil {
		return nil, wrap("describe workflow", err)
	}
	return describe(resp), nil
}

// WaitForResult polls until the workflow finishes and returns its output. A
// workflow that failed or was terminated returns an error matching
// ErrWorkflowFailed whose message carries the workflow's own error. It keeps
// waiting while the engine is unreachable (a restart, an election); bound it
// with ctx.
func (c *Client) WaitForResult(ctx context.Context, workflowID string) ([]byte, error) {
	delay := 20 * time.Millisecond
	for {
		d, err := c.DescribeWorkflow(ctx, workflowID)
		if errors.Is(err, ErrUnavailable) && ctx.Err() == nil {
			c.log.Debug("engine unavailable; still waiting", "workflow_id", workflowID, "err", err)
			d = &WorkflowDescription{}
		} else if err != nil {
			return nil, err
		}
		switch d.Status {
		case StatusCompleted:
			return d.Output, nil
		case StatusFailed, StatusTerminated, StatusTimedOut, StatusUnknown:
			return nil, fmt.Errorf("%w: %s %s: %s", ErrWorkflowFailed, workflowID, d.Status, d.Error)
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("client: wait for %s: %w", workflowID, ctx.Err())
		case <-time.After(delay):
		}
		delay = min(delay*2, time.Second)
	}
}

// wrap turns a gRPC status into an error that matches this package's sentinels.
func wrap(op string, err error) error {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return fmt.Errorf("client: %s: %w", op, err)
	}
	var kind error
	switch st.Code() {
	case codes.NotFound:
		kind = ErrNotFound
	case codes.AlreadyExists:
		kind = ErrAlreadyExists
	case codes.FailedPrecondition:
		kind = ErrNotRunning
	case codes.InvalidArgument:
		kind = ErrInvalid
	case codes.Unavailable:
		kind = ErrUnavailable
	default:
		return fmt.Errorf("client: %s: %w", op, err)
	}
	return fmt.Errorf("client: %s: %w: %s", op, kind, st.Message())
}

func randomSuffix() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b) // crypto/rand.Read never fails on supported platforms
	return hex.EncodeToString(b)
}
