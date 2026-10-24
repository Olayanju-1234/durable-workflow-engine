package server

import (
	"context"
	"errors"
	"time"

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
	case errors.Is(err, ErrQuotaExceeded):
		return status.Errorf(codes.ResourceExhausted, "%v", err)
	default:
		return status.Errorf(codes.Internal, "%v", err)
	}
}

func (s *GRPCServer) PauseSchedule(ctx context.Context, req *PauseScheduleRequest) (*PauseScheduleResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	if err := e.PauseSchedule(req.GetScheduleId(), req.GetNote()); err != nil {
		return nil, s.scheduleError(ctx, err)
	}
	return &PauseScheduleResponse{}, nil
}

func (s *GRPCServer) ResumeSchedule(ctx context.Context, req *ResumeScheduleRequest) (*ResumeScheduleResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	if err := e.ResumeSchedule(req.GetScheduleId()); err != nil {
		return nil, s.scheduleError(ctx, err)
	}
	return &ResumeScheduleResponse{}, nil
}

func (s *GRPCServer) BackfillSchedule(ctx context.Context, req *BackfillScheduleRequest) (*BackfillScheduleResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	res, err := e.BackfillSchedule(req.GetScheduleId(), time.Unix(0, req.GetStartTime()), time.Unix(0, req.GetEndTime()), req.GetOverlapPolicy())
	if err != nil {
		return nil, s.scheduleError(ctx, err)
	}
	return &BackfillScheduleResponse{Started: int32(res.Started), Existing: int32(res.Existing), Skipped: int32(res.Skipped)}, nil
}

func (s *GRPCServer) PreviewSchedule(ctx context.Context, req *PreviewScheduleRequest) (*PreviewScheduleResponse, error) {
	e, err := s.engine(ctx)
	if err != nil {
		return nil, err
	}
	p, err := e.PreviewSchedule(req.GetScheduleId(), req.GetSchedule(), int(req.GetCount()))
	if err != nil {
		return nil, s.scheduleError(ctx, err)
	}
	out := &PreviewScheduleResponse{Paused: p.Paused}
	for i := range p.Fires {
		out.FireTimes = append(out.FireTimes, p.Fires[i].UnixNano())
		out.StartTimes = append(out.StartTimes, p.Starts[i].UnixNano())
	}
	return out, nil
}
