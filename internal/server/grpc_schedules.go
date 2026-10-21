package server

import (
	"context"
	"errors"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *GRPCServer) CreateSchedule(ctx context.Context, req *CreateScheduleRequest) (*CreateScheduleResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	if err := e.CreateSchedule(req.GetSchedule()); err != nil {
		return nil, s.scheduleError(ctx, err)
	}
	return &CreateScheduleResponse{}, nil
}

func (s *GRPCServer) DeleteSchedule(ctx context.Context, req *DeleteScheduleRequest) (*DeleteScheduleResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	if err := e.DeleteSchedule(req.GetScheduleId()); err != nil {
		return nil, s.scheduleError(ctx, err)
	}
	return &DeleteScheduleResponse{}, nil
}

func (s *GRPCServer) ListSchedules(ctx context.Context, _ *ListSchedulesRequest) (*ListSchedulesResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	scheds, err := e.ListSchedules()
	if err != nil {
		return nil, s.scheduleError(ctx, err)
	}
	return &ListSchedulesResponse{Schedules: scheds}, nil
}

func (s *GRPCServer) scheduleError(ctx context.Context, err error) error {
	switch {
	case errors.Is(err, ErrNotLeader):
		return s.notLeader(ctx, err)
	case errors.Is(err, ErrInvalidArgument):
		return status.Errorf(codes.InvalidArgument, "%v", err)
	case errors.Is(err, ErrNotFound):
		return status.Errorf(codes.NotFound, "%v", err)
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}
