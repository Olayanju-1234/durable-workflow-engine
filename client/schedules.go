package client

import (
	"context"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// Schedule starts a workflow each time a cron expression fires. Every run gets
// the workflow id "<ID>-<fire time, UTC, 20060102T150405Z>", so a fire is never
// started twice, even across restarts and leader changes.
type Schedule struct {
	// ID names the schedule and prefixes every run's workflow id.
	ID string
	// Cron is a five-field expression ("0 9 * * MON-FRI"), a macro (@hourly,
	// @daily, @weekly, @monthly, @yearly) or "@every <duration>" (>= 1s).
	Cron string
	// Timezone is the IANA zone the cron fields are in; empty means UTC.
	Timezone     string
	WorkflowType string
	Input        []byte
	// SkipIfRunning skips a fire while an earlier run is still going.
	SkipIfRunning bool
	// TaskQueue is the task queue the runs start on; empty is the default.
	TaskQueue string
}

// ScheduleDescription is a schedule as the engine reports it.
type ScheduleDescription struct {
	Schedule
	CreatedAt time.Time
	// NextFireAt is zero if the schedule never fires again.
	NextFireAt time.Time
}

// CreateSchedule creates a schedule, or replaces the one with the same ID.
// The workflow type need not be registered yet; fires wait for a worker.
func (c *Client) CreateSchedule(ctx context.Context, s Schedule) error {
	req := &server.CreateScheduleRequest{Schedule: &server.Schedule{
		ScheduleId: s.ID, Cron: s.Cron, Timezone: s.Timezone,
		WorkflowType: s.WorkflowType, Input: s.Input, SkipIfRunning: s.SkipIfRunning, TaskQueue: s.TaskQueue,
	}}
	_, err := c.conn.Call(ctx, func(ctx context.Context, stub server.WorkflowServiceClient, opt grpc.CallOption) error {
		_, err := stub.CreateSchedule(ctx, req, opt)
		return err
	})
	return wrap("create schedule", err)
}

// DeleteSchedule stops a schedule; runs it already started carry on.
func (c *Client) DeleteSchedule(ctx context.Context, id string) error {
	attempts, err := c.conn.Call(ctx, func(ctx context.Context, stub server.WorkflowServiceClient, opt grpc.CallOption) error {
		_, err := stub.DeleteSchedule(ctx, &server.DeleteScheduleRequest{ScheduleId: id}, opt)
		return err
	})
	if attempts > 1 && status.Code(err) == codes.NotFound {
		return nil // an earlier attempt deleted it before a leader change hid the reply
	}
	return wrap("delete schedule", err)
}

// ListSchedules returns every schedule, sorted by ID.
func (c *Client) ListSchedules(ctx context.Context) ([]ScheduleDescription, error) {
	var resp *server.ListSchedulesResponse
	_, err := c.conn.Call(ctx, func(ctx context.Context, stub server.WorkflowServiceClient, opt grpc.CallOption) error {
		var err error
		resp, err = stub.ListSchedules(ctx, &server.ListSchedulesRequest{}, opt)
		return err
	})
	if err != nil {
		return nil, wrap("list schedules", err)
	}
	out := make([]ScheduleDescription, 0, len(resp.GetSchedules()))
	for _, info := range resp.GetSchedules() {
		s := info.GetSchedule()
		d := ScheduleDescription{
			Schedule: Schedule{
				ID: s.GetScheduleId(), Cron: s.GetCron(), Timezone: s.GetTimezone(),
				WorkflowType: s.GetWorkflowType(), Input: s.GetInput(), SkipIfRunning: s.GetSkipIfRunning(),
				TaskQueue: s.GetTaskQueue(),
			},
			CreatedAt: time.Unix(0, info.GetCreatedAt()).UTC(),
		}
		if info.GetNextFireAt() != 0 {
			d.NextFireAt = time.Unix(0, info.GetNextFireAt()).UTC()
		}
		out = append(out, d)
	}
	return out, nil
}
