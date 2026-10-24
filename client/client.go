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
	"crypto/tls"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/internal/rpc"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// Errors a call can be matched against with errors.Is.
var (
	ErrNotFound        = errors.New("client: workflow not found")
	ErrAlreadyExists   = errors.New("client: workflow id already in use")
	ErrNotRunning      = errors.New("client: workflow is not running")
	ErrInvalid         = errors.New("client: invalid request")
	ErrUnavailable     = errors.New("client: no workflowd node is serving")
	ErrUnauthenticated = errors.New("client: missing or invalid auth token")
	ErrWorkflowFailed  = errors.New("client: workflow did not complete")
)

func init() {
	rpc.ConnOf = func(c any) *rpc.Conn { return c.(*Client).conn }
}

// maxReceiveBytes is the largest reply a client accepts: room for a full
// history at the engine's default limit, with headroom.
const maxReceiveBytes = 128 << 20

// Options configures a Client.
type Options struct {
	// Addrs are workflowd gRPC addresses, e.g. "localhost:7233". For a cluster,
	// list every node.
	Addrs []string
	// TLS, if set, dials with TLS (e.g. &tls.Config{} for public CAs, or one
	// whose RootCAs hold your CA). Nil dials plaintext.
	TLS *tls.Config
	// AuthToken is sent as "authorization: Bearer <token>" on every call, for
	// an engine started with --auth-token-file.
	AuthToken string
	// DialOptions are added after the ones TLS and AuthToken produce, so a
	// transport credential here wins.
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
	creds := insecure.NewCredentials()
	if opts.TLS != nil {
		creds = credentials.NewTLS(opts.TLS)
	}
	// A workflow task carries its whole history, which may be tens of
	// megabytes (the engine's --max-history-bytes), past gRPC's 4 MiB default.
	dialOpts := []grpc.DialOption{grpc.WithTransportCredentials(creds),
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maxReceiveBytes))}
	if opts.AuthToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(bearerToken(opts.AuthToken)))
	}
	dialOpts = append(dialOpts, opts.DialOptions...)
	conn, err := rpc.New(opts.Addrs, dialOpts, logger)
	if err != nil {
		return nil, fmt.Errorf("client: %w", err)
	}
	return &Client{conn: conn, log: logger}, nil
}

// bearerToken attaches the engine's auth token to every call. It is allowed
// over plaintext so a token still works behind a TLS-terminating proxy.
type bearerToken string

func (t bearerToken) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + string(t)}, nil
}

func (bearerToken) RequireTransportSecurity() bool { return false }

// Close releases the client's connections.
func (c *Client) Close() error { return c.conn.Close() }

// StartOption customises StartWorkflow.
type StartOption func(*startOptions)

type startOptions struct {
	id               string
	executionTimeout time.Duration
	reuse            server.IdReusePolicy
	conflict         server.IdConflictPolicy
}

// IDReusePolicy decides whether a workflow id whose run has finished may start
// a new run; the finished run is kept as an earlier run (ListWorkflowRuns).
type IDReusePolicy int

const (
	// RejectDuplicate refuses (ErrAlreadyExists): the default.
	RejectDuplicate IDReusePolicy = iota
	// AllowDuplicate starts a new run whatever the finished one's outcome.
	AllowDuplicate
	// AllowDuplicateFailedOnly starts one only if the finished run failed,
	// timed out, was cancelled or terminated.
	AllowDuplicateFailedOnly
)

// IDConflictPolicy decides what a start does when its id is taken: a run is
// still going, or a finished one the reuse policy does not let it replace.
type IDConflictPolicy int

const (
	// FailOnConflict fails with ErrAlreadyExists: the default.
	FailOnConflict IDConflictPolicy = iota
	// UseExisting returns the run that holds the id instead (Started.Started
	// is false): "start it, or give me the one already there".
	UseExisting
)

// WithIDReusePolicy sets whether a finished workflow id may start a new run.
func WithIDReusePolicy(p IDReusePolicy) StartOption {
	return func(o *startOptions) {
		o.reuse = map[IDReusePolicy]server.IdReusePolicy{
			RejectDuplicate:          server.IdReusePolicy_ID_REUSE_POLICY_REJECT_DUPLICATE,
			AllowDuplicate:           server.IdReusePolicy_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			AllowDuplicateFailedOnly: server.IdReusePolicy_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		}[p]
	}
}

// WithIDConflictPolicy sets what a start does when its id is taken.
func WithIDConflictPolicy(p IDConflictPolicy) StartOption {
	return func(o *startOptions) {
		if p == UseExisting {
			o.conflict = server.IdConflictPolicy_ID_CONFLICT_POLICY_USE_EXISTING
		} else {
			o.conflict = server.IdConflictPolicy_ID_CONFLICT_POLICY_FAIL
		}
	}
}

// Started is what Start did.
type Started struct {
	WorkflowID string
	RunID      string
	// Started is false when UseExisting returned the run that held the id.
	Started bool
}

// WithWorkflowID starts the workflow under a caller-chosen id, for example an
// order number, so a second start for the same id fails with ErrAlreadyExists.
func WithWorkflowID(id string) StartOption {
	return func(o *startOptions) { o.id = id }
}

// WithExecutionTimeout ends the workflow with StatusTimedOut if it has not
// finished d after it starts. Whatever it has in flight is abandoned.
func WithExecutionTimeout(d time.Duration) StartOption {
	return func(o *startOptions) { o.executionTimeout = d }
}

// StartWorkflow starts a workflow of a registered type and returns its id.
func (c *Client) StartWorkflow(ctx context.Context, workflowType string, input []byte, opts ...StartOption) (string, error) {
	s, err := c.Start(ctx, workflowType, input, opts...)
	return s.WorkflowID, err
}

// Start is StartWorkflow that also says which run it began, or with
// UseExisting which run already held the id.
func (c *Client) Start(ctx context.Context, workflowType string, input []byte, opts ...StartOption) (Started, error) {
	var so startOptions
	for _, o := range opts {
		o(&so)
	}
	req := &server.StartWorkflowRequest{
		WorkflowType: workflowType, Input: input, WorkflowId: so.id,
		ExecutionTimeoutMs: so.executionTimeout.Milliseconds(),
		IdReusePolicy:      so.reuse, IdConflictPolicy: so.conflict,
	}
	generated := req.WorkflowId == ""
	if generated {
		// Chosen here, not on the server, so a retry after a lost reply reuses it.
		req.WorkflowId = fmt.Sprintf("%s-%s", workflowType, randomSuffix())
	}
	var resp *server.StartWorkflowResponse
	attempts, err := c.conn.Call(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
		var err error
		resp, err = s.StartWorkflow(ctx, req, opt)
		return err
	})
	if generated && attempts > 1 && status.Code(err) == codes.AlreadyExists {
		// Only an earlier attempt of this call can hold a fresh random id: a
		// leader accepted it and then lost leadership before replying.
		return Started{WorkflowID: req.WorkflowId, Started: true}, nil
	}
	if err != nil {
		return Started{}, wrap("start workflow", err)
	}
	// An engine before 0.3.0 sends neither field; it never returns an existing run.
	started := resp.GetStarted() || resp.GetRunId() == ""
	return Started{WorkflowID: req.WorkflowId, RunID: resp.GetRunId(), Started: started}, nil
}

// SignalOption customises SignalWorkflow.
type SignalOption func(*signalOptions)

type signalOptions struct{ requestID string }

// WithSignalRequestID sets the signal's idempotency key, for a caller that
// may send the same signal again from another process (a retried job, a
// redelivered message): every send with the key is recorded once.
func WithSignalRequestID(id string) SignalOption {
	return func(o *signalOptions) { o.requestID = id }
}

// SignalWorkflow sends a named signal with a payload to a running workflow.
// It is recorded exactly once: the call carries an idempotency key (generated
// here unless WithSignalRequestID gives one) that every retry reuses, so a
// retry after a lost reply or a leader change is recognised by the engine.
func (c *Client) SignalWorkflow(ctx context.Context, workflowID, signal string, payload []byte, opts ...SignalOption) error {
	so := signalOptions{requestID: randomSuffix()}
	for _, o := range opts {
		o(&so)
	}
	req := &server.SignalWorkflowRequest{WorkflowId: workflowID, SignalName: signal, Payload: payload, RequestId: so.requestID}
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

// CancelOption customises CancelWorkflow.
type CancelOption func(*server.CancelWorkflowRequest)

// Graceful tells the workflow code instead of ending the workflow at once:
// the steps it waits on fail with workflow.ErrWorkflowCancelled, and it may
// compensate before it returns that error to end CANCELLED (or returns
// normally, to complete). A workflow whose code does not handle it carries on.
func Graceful() CancelOption {
	return func(r *server.CancelWorkflowRequest) { r.Graceful = true }
}

// CancelWorkflow ends a running workflow with StatusCancelled, recording the
// reason. Workers still running its activities are told to stop at their next
// heartbeat or completion (the activity's ctx is cancelled with cause
// worker.ErrActivityCancelled). It fails with ErrNotRunning if the workflow has
// already finished.
func (c *Client) CancelWorkflow(ctx context.Context, workflowID, reason string, opts ...CancelOption) error {
	req := &server.CancelWorkflowRequest{WorkflowId: workflowID, Reason: reason}
	for _, o := range opts {
		o(req)
	}
	_, err := c.conn.Call(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
		_, err := s.CancelWorkflow(ctx, req, opt)
		return err
	})
	return wrap("cancel workflow", err)
}

// DescribeWorkflow returns a workflow's status, steps, input and output: its
// current run's.
func (c *Client) DescribeWorkflow(ctx context.Context, workflowID string) (*WorkflowDescription, error) {
	return c.DescribeWorkflowRun(ctx, workflowID, "")
}

// DescribeWorkflowRun describes one run of a workflow id, current or earlier
// (see ListWorkflowRuns); an empty runID means the current one.
func (c *Client) DescribeWorkflowRun(ctx context.Context, workflowID, runID string) (*WorkflowDescription, error) {
	var resp *server.GetWorkflowResponse
	_, err := c.conn.Call(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
		var err error
		resp, err = s.GetWorkflow(ctx, &server.GetWorkflowRequest{WorkflowId: workflowID, RunId: runID}, opt)
		return err
	})
	if err != nil {
		return nil, wrap("describe workflow", err)
	}
	return describe(resp), nil
}

// ListWorkflowRuns lists the runs kept for a workflow id: the current one,
// then earlier ones (continued as new), newest first.
func (c *Client) ListWorkflowRuns(ctx context.Context, workflowID string) ([]Run, error) {
	var resp *server.ListWorkflowRunsResponse
	_, err := c.conn.Call(ctx, func(ctx context.Context, s server.WorkflowServiceClient, opt grpc.CallOption) error {
		var err error
		resp, err = s.ListWorkflowRuns(ctx, &server.ListWorkflowRunsRequest{WorkflowId: workflowID}, opt)
		return err
	})
	if err != nil {
		return nil, wrap("list workflow runs", err)
	}
	runs := make([]Run, 0, len(resp.GetRuns()))
	for _, r := range resp.GetRuns() {
		st, ok := statuses[r.GetStatus()]
		if !ok {
			st = StatusUnknown
		}
		runs = append(runs, Run{RunID: r.GetRunId(), Status: st, StartedAt: unixNanos(r.GetStartedAt()),
			ClosedAt: unixNanos(r.GetClosedAt()), Current: r.GetCurrent()})
	}
	return runs, nil
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
		case StatusFailed, StatusTerminated, StatusTimedOut, StatusCancelled, StatusUnknown:
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
	case codes.Unauthenticated:
		kind = ErrUnauthenticated
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
