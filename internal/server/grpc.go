package server

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Trailers a follower sets when it turns a call away, naming the leader.
const (
	TrailerLeader     = "workflowd-leader"
	TrailerLeaderGRPC = "workflowd-leader-grpc"
	TrailerLeaderHTTP = "workflowd-leader-http"
)

// GRPCServer adapts the Engine to the generated WorkflowServiceServer interface.
// It is a thin translation layer — decode the request, call the Engine, encode
// the reply — so all workflow logic stays in one place (the Engine) and the
// network edge stays trivial. Embedding UnimplementedWorkflowServiceServer is
// gRPC's forward-compatibility requirement: new RPCs added to the proto won't
// break this type until we implement them.
type GRPCServer struct {
	UnimplementedWorkflowServiceServer
	src EngineSource
	log *slog.Logger
}

// NewGRPCServer wraps an Engine as a gRPC handler. Register it on a *grpc.Server
// with RegisterWorkflowServiceServer.
func NewGRPCServer(engine *Engine, logger *slog.Logger) *GRPCServer {
	return NewGRPCServerFrom(staticSource{engine}, logger)
}

// NewGRPCServerFrom serves whichever engine src resolves per call: in a
// cluster, the current leader's, and a leader hint everywhere else.
func NewGRPCServerFrom(src EngineSource, logger *slog.Logger) *GRPCServer {
	return &GRPCServer{src: src, log: logger}
}

func (s *GRPCServer) engine(ctx context.Context) (*Scope, error) {
	e, err := s.src.Engine()
	if err != nil {
		return nil, s.notLeader(ctx, err)
	}
	return e.In(grantFrom(ctx).Namespace), nil
}

// notLeader turns a call away as Unavailable, the conventional "try another
// server" code, with the leader's addresses in trailers.
func (s *GRPCServer) notLeader(ctx context.Context, err error) error {
	h := leaderHint(s.src, err)
	_ = grpc.SetTrailer(ctx, metadata.Pairs(TrailerLeader, h.ID, TrailerLeaderGRPC, h.GRPCAddr, TrailerLeaderHTTP, h.HTTPAddr))
	return status.Error(codes.Unavailable, (&NotLeaderError{Leader: h}).Error())
}

func (s *GRPCServer) StartWorkflow(ctx context.Context, req *StartWorkflowRequest) (*StartWorkflowResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	started, err := e.Start(req.GetWorkflowType(), req.GetInput(), req.GetWorkflowId(),
		WithExecutionTimeout(time.Duration(req.GetExecutionTimeoutMs())*time.Millisecond),
		WithIDReusePolicy(req.GetIdReusePolicy()), WithIDConflictPolicy(req.GetIdConflictPolicy()),
		WithTaskQueue(req.GetTaskQueue()))
	switch {
	case err == nil:
	case errors.Is(err, ErrNotLeader):
		return nil, s.notLeader(ctx, err)
	case errors.Is(err, ErrUnknownType), errors.Is(err, ErrInvalidArgument):
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, ErrPayloadTooLarge), errors.Is(err, ErrQuotaExceeded):
		return nil, status.Errorf(codes.ResourceExhausted, "%v", err)
	case errors.Is(err, ErrAlreadyExists):
		return nil, status.Errorf(codes.AlreadyExists, "%v", err)
	default:
		// History I/O failed server-side; a retry may succeed.
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &StartWorkflowResponse{WorkflowId: started.WorkflowID, RunId: started.RunID, Started: !started.Existing}, nil
}

func (s *GRPCServer) PollTask(ctx context.Context, req *PollTaskRequest) (*PollTaskResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	task, err := e.PollTask(ctx, req.GetWorkerId(), req.GetTaskQueue(), req.GetStepNames()...)
	if errors.Is(err, ErrNotLeader) {
		return nil, s.notLeader(ctx, err)
	}
	if err != nil {
		// A cancelled/expired context is the worker leaving or the poll timing
		// out, not a server fault: translate it to the matching gRPC status.
		return nil, status.FromContextError(err).Err()
	}
	return &PollTaskResponse{Task: task}, nil
}

func (s *GRPCServer) CompleteTask(ctx context.Context, req *CompleteTaskRequest) (*CompleteTaskResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	switch r := req.GetResult().(type) {
	case *CompleteTaskRequest_Output:
		err = e.CompleteTask(req.GetWorkflowId(), req.GetStepName(), req.GetSequence(), req.GetLeaseToken(), r.Output)
	case *CompleteTaskRequest_Error:
		err = e.FailTask(req.GetWorkflowId(), req.GetStepName(), req.GetSequence(), req.GetLeaseToken(), TaskFailure{
			Message: r.Error, Type: req.GetErrorType(), NonRetryable: req.GetNonRetryable(),
		})
	default:
		return nil, status.Error(codes.InvalidArgument, "complete task: a result (output or error) is required")
	}
	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			return nil, s.notLeader(ctx, err)
		}
		// The step result didn't match the in-flight step (stale/duplicate), the
		// lease was lost (task reassigned), the step was cancelled, or the
		// workflow wasn't waiting — a precondition the client must fix.
		return nil, leaseStatus(err)
	}
	return &CompleteTaskResponse{}, nil
}

func (s *GRPCServer) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	if err := e.Heartbeat(req.GetWorkflowId(), req.GetLeaseToken()); err != nil {
		if errors.Is(err, ErrNotLeader) {
			return nil, s.notLeader(ctx, err)
		}
		// Lost lease or cancelled: the worker should stop working on this task.
		return nil, leaseStatus(err)
	}
	return &HeartbeatResponse{}, nil
}

// leaseStatus is FailedPrecondition for a worker that must stop, with an
// ActivityCancelled detail when the work is no longer wanted at all. Workers
// that predate the detail stop on the code alone.
func leaseStatus(err error) error {
	st := status.New(codes.FailedPrecondition, err.Error())
	if errors.Is(err, ErrActivityCancelled) {
		if withDetail, derr := st.WithDetails(&ActivityCancelled{Reason: err.Error()}); derr == nil {
			st = withDetail
		}
	}
	return st.Err()
}

func (s *GRPCServer) SignalWorkflow(ctx context.Context, req *SignalWorkflowRequest) (*SignalWorkflowResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	err = e.SignalWorkflowOnce(req.GetWorkflowId(), req.GetSignalName(), req.GetRequestId(), req.GetPayload())
	switch {
	case err == nil:
	case errors.Is(err, ErrNotLeader):
		return nil, s.notLeader(ctx, err)
	case errors.Is(err, ErrInvalidArgument):
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, ErrPayloadTooLarge):
		return nil, status.Errorf(codes.ResourceExhausted, "%v", err)
	case errors.Is(err, ErrNotFound):
		return nil, status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, ErrAlreadyTerminal), errors.Is(err, ErrHistoryLimit):
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	default:
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &SignalWorkflowResponse{}, nil
}

func (s *GRPCServer) RegisterWorkflowType(ctx context.Context, req *RegisterWorkflowTypeRequest) (*RegisterWorkflowTypeResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	if err := e.RegisterWorkflowTypes(req.GetWorkflowTypes(), req.GetWorkerId(), req.GetSdk()); err != nil {
		if errors.Is(err, ErrNotLeader) {
			return nil, s.notLeader(ctx, err)
		}
		// Empty type name, or a clash with a compiled-in Go workflow.
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &RegisterWorkflowTypeResponse{}, nil
}

func (s *GRPCServer) PollWorkflowTask(ctx context.Context, req *PollWorkflowTaskRequest) (*PollWorkflowTaskResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	task, err := e.PollWorkflowTask(ctx, req.GetWorkerId(), req.GetTaskQueue(), req.GetWorkflowTypes())
	if errors.Is(err, ErrNotLeader) {
		return nil, s.notLeader(ctx, err)
	}
	if err != nil {
		// Long-poll timed out or the worker disconnected.
		return nil, status.FromContextError(err).Err()
	}
	return &PollWorkflowTaskResponse{Task: task}, nil
}

func (s *GRPCServer) CompleteWorkflowTask(ctx context.Context, req *CompleteWorkflowTaskRequest) (*CompleteWorkflowTaskResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	cmds := req.GetCommands()
	if req.GetCommand() != nil {
		if len(cmds) > 0 {
			return nil, status.Error(codes.InvalidArgument, "complete workflow task: set command or commands, not both")
		}
		cmds = []*WorkflowCommand{req.GetCommand()}
	}
	err = e.CompleteWorkflowTask(req.GetWorkflowId(), req.GetTaskToken(), cmds, req.GetReplayError())
	switch {
	case errors.Is(err, ErrNotLeader):
		return nil, s.notLeader(ctx, err)
	case errors.Is(err, ErrStaleDecision):
		// Routine under re-dispatch: the SDK should just poll again. Aborted is
		// the conventional "you lost a race, retry the whole operation" code.
		return nil, status.Errorf(codes.Aborted, "%v", err)
	case errors.Is(err, ErrInvalidCommand):
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	case err != nil:
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &CompleteWorkflowTaskResponse{}, nil
}

func (s *GRPCServer) ListWorkflows(ctx context.Context, req *ListWorkflowsRequest) (*ListWorkflowsResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	summaries, err := e.ListWorkflows(req.GetStatusFilter())
	if err != nil {
		// Listing only fails on a store-level fault (e.g. unreadable data dir).
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &ListWorkflowsResponse{Workflows: summaries}, nil
}

func (s *GRPCServer) GetWorkflow(ctx context.Context, req *GetWorkflowRequest) (*GetWorkflowResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := e.GetWorkflowRun(req.GetWorkflowId(), req.GetRunId())
	switch {
	case errors.Is(err, ErrNotFound):
		return nil, status.Errorf(codes.NotFound, "%v", err)
	case err != nil:
		// Unreadable or structurally invalid history is a server-side fault.
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return resp, nil
}

func (s *GRPCServer) TerminateWorkflow(ctx context.Context, req *TerminateWorkflowRequest) (*TerminateWorkflowResponse, error) {
	if err := s.endWorkflow(ctx, req.GetWorkflowId(), req.GetReason(), (*Scope).TerminateWorkflow); err != nil {
		return nil, err
	}
	return &TerminateWorkflowResponse{}, nil
}

func (s *GRPCServer) CancelWorkflow(ctx context.Context, req *CancelWorkflowRequest) (*CancelWorkflowResponse, error) {
	end := (*Scope).CancelWorkflow
	if req.GetGraceful() {
		end = (*Scope).RequestCancelWorkflow
	}
	if err := s.endWorkflow(ctx, req.GetWorkflowId(), req.GetReason(), end); err != nil {
		return nil, err
	}
	return &CancelWorkflowResponse{}, nil
}

func (s *GRPCServer) endWorkflow(ctx context.Context, id, reason string, end func(*Scope, string, string) error) error {
	e, err := s.engine(ctx)
	if err != nil {
		return err
	}
	err = end(e, id, reason)
	switch {
	case err == nil:
		return nil
	case errors.Is(err, ErrNotLeader):
		return s.notLeader(ctx, err)
	case errors.Is(err, ErrNotFound):
		return status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, ErrAlreadyTerminal):
		return status.Errorf(codes.FailedPrecondition, "%v", err)
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}

func (s *GRPCServer) ListWorkflowRuns(ctx context.Context, req *ListWorkflowRunsRequest) (*ListWorkflowRunsResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	runs, err := e.ListWorkflowRuns(req.GetWorkflowId())
	switch {
	case errors.Is(err, ErrNotFound):
		return nil, status.Errorf(codes.NotFound, "%v", err)
	case err != nil:
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &ListWorkflowRunsResponse{Runs: runs}, nil
}
