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

func (s *GRPCServer) engine(ctx context.Context) (*Engine, error) {
	e, err := s.src.Engine()
	if err != nil {
		return nil, s.notLeader(ctx, err)
	}
	return e, nil
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
	id, err := e.StartWorkflow(req.GetWorkflowType(), req.GetInput(), req.GetWorkflowId(),
		WithExecutionTimeout(time.Duration(req.GetExecutionTimeoutMs())*time.Millisecond))
	switch {
	case err == nil:
	case errors.Is(err, ErrNotLeader):
		return nil, s.notLeader(ctx, err)
	case errors.Is(err, ErrUnknownType), errors.Is(err, ErrInvalidArgument):
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, ErrAlreadyExists):
		return nil, status.Errorf(codes.AlreadyExists, "%v", err)
	default:
		// History I/O failed server-side; a retry may succeed.
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &StartWorkflowResponse{WorkflowId: id}, nil
}

func (s *GRPCServer) PollTask(ctx context.Context, req *PollTaskRequest) (*PollTaskResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	task, err := e.PollTask(ctx, req.GetWorkerId(), req.GetStepNames()...)
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
	var (
		output  []byte
		stepErr string
		failed  bool
	)
	switch r := req.GetResult().(type) {
	case *CompleteTaskRequest_Output:
		output = r.Output
	case *CompleteTaskRequest_Error:
		stepErr, failed = r.Error, true
	default:
		return nil, status.Error(codes.InvalidArgument, "complete task: a result (output or error) is required")
	}
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	if err := e.CompleteTask(req.GetWorkflowId(), req.GetStepName(), req.GetSequence(), req.GetLeaseToken(), output, stepErr, failed); err != nil {
		if errors.Is(err, ErrNotLeader) {
			return nil, s.notLeader(ctx, err)
		}
		// The step result didn't match the in-flight step (stale/duplicate), the
		// lease was lost (task reassigned), or the workflow wasn't waiting — a
		// precondition the client must fix.
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &CompleteTaskResponse{}, nil
}

func (s *GRPCServer) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	if err := e.Heartbeat(req.GetWorkflowId(), req.GetLeaseToken()); err != nil {
		// Lost lease: the worker should stop working on this task.
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &HeartbeatResponse{}, nil
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
	case errors.Is(err, ErrNotFound):
		return nil, status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, ErrAlreadyTerminal):
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
	task, err := e.PollWorkflowTask(ctx, req.GetWorkerId(), req.GetWorkflowTypes())
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
	resp, err := e.GetWorkflow(req.GetWorkflowId())
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
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	err = e.TerminateWorkflow(req.GetWorkflowId(), req.GetReason())
	switch {
	case errors.Is(err, ErrNotLeader):
		return nil, s.notLeader(ctx, err)
	case errors.Is(err, ErrNotFound):
		return nil, status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, ErrAlreadyTerminal):
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	case err != nil:
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &TerminateWorkflowResponse{}, nil
}
