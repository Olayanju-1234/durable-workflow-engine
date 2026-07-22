package server

import (
	"context"
	"errors"
	"log/slog"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GRPCServer adapts the Engine to the generated WorkflowServiceServer interface.
// It is a thin translation layer — decode the request, call the Engine, encode
// the reply — so all workflow logic stays in one place (the Engine) and the
// network edge stays trivial. Embedding UnimplementedWorkflowServiceServer is
// gRPC's forward-compatibility requirement: new RPCs added to the proto won't
// break this type until we implement them.
type GRPCServer struct {
	UnimplementedWorkflowServiceServer
	engine *Engine
	log    *slog.Logger
}

// NewGRPCServer wraps an Engine as a gRPC handler. Register it on a *grpc.Server
// with RegisterWorkflowServiceServer.
func NewGRPCServer(engine *Engine, logger *slog.Logger) *GRPCServer {
	return &GRPCServer{engine: engine, log: logger}
}

func (s *GRPCServer) StartWorkflow(ctx context.Context, req *StartWorkflowRequest) (*StartWorkflowResponse, error) {
	id, err := s.engine.StartWorkflow(req.GetWorkflowType(), req.GetInput(), req.GetWorkflowId())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	return &StartWorkflowResponse{WorkflowId: id}, nil
}

func (s *GRPCServer) PollTask(ctx context.Context, req *PollTaskRequest) (*PollTaskResponse, error) {
	task, err := s.engine.PollTask(ctx, req.GetWorkerId())
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
	if err := s.engine.CompleteTask(req.GetWorkflowId(), req.GetStepName(), req.GetSequence(), req.GetLeaseToken(), output, stepErr, failed); err != nil {
		// The step result didn't match the in-flight step (stale/duplicate), the
		// lease was lost (task reassigned), or the workflow wasn't waiting — a
		// precondition the client must fix.
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &CompleteTaskResponse{}, nil
}

func (s *GRPCServer) Heartbeat(ctx context.Context, req *HeartbeatRequest) (*HeartbeatResponse, error) {
	if err := s.engine.Heartbeat(req.GetWorkflowId(), req.GetLeaseToken()); err != nil {
		// Lost lease: the worker should stop working on this task.
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &HeartbeatResponse{}, nil
}

func (s *GRPCServer) SignalWorkflow(ctx context.Context, req *SignalWorkflowRequest) (*SignalWorkflowResponse, error) {
	if err := s.engine.SignalWorkflow(req.GetWorkflowId(), req.GetSignalName(), req.GetPayload()); err != nil {
		// Unknown or terminal workflow: a precondition the caller must fix.
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	}
	return &SignalWorkflowResponse{}, nil
}

func (s *GRPCServer) ListWorkflows(ctx context.Context, req *ListWorkflowsRequest) (*ListWorkflowsResponse, error) {
	summaries, err := s.engine.ListWorkflows(req.GetStatusFilter())
	if err != nil {
		// Listing only fails on a store-level fault (e.g. unreadable data dir).
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &ListWorkflowsResponse{Workflows: summaries}, nil
}

func (s *GRPCServer) GetWorkflow(ctx context.Context, req *GetWorkflowRequest) (*GetWorkflowResponse, error) {
	resp, err := s.engine.GetWorkflow(req.GetWorkflowId())
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
	err := s.engine.TerminateWorkflow(req.GetWorkflowId(), req.GetReason())
	switch {
	case errors.Is(err, ErrNotFound):
		return nil, status.Errorf(codes.NotFound, "%v", err)
	case errors.Is(err, ErrAlreadyTerminal):
		return nil, status.Errorf(codes.FailedPrecondition, "%v", err)
	case err != nil:
		return nil, status.Errorf(codes.Internal, "%v", err)
	}
	return &TerminateWorkflowResponse{}, nil
}
