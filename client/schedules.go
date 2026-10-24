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
	// Overlap decides what a fire does while an earlier run is still going
	// (SkipIfRunning is the same as OverlapSkip).
	Overlap OverlapPolicy
	// Jitter starts each run up to this long after its fire time, by an
	// offset fixed per fire; run ids keep the fire time.
	Jitter time.Duration
	// Paused schedules do not fire; Note says why.
	Paused bool
	Note   string
}

// OverlapPolicy is what a schedule's fire does while an earlier run is going.
type OverlapPolicy int

const (
	// OverlapDefault: OverlapAllowAll, or OverlapSkip with SkipIfRunning.
	OverlapDefault OverlapPolicy = iota
	// OverlapSkip skips the fire.
	OverlapSkip
	// OverlapBufferOne holds the fire and starts it when the running run
	// finishes; fires while one is held are skipped.
	OverlapBufferOne
	// OverlapAllowAll starts it anyway.
	OverlapAllowAll
)

// Backfilled is what a backfill did.
type Backfilled struct {
	Started, Existing, Skipped int
}

// PauseSchedule stops a schedule firing until ResumeSchedule. note says why.
func (c *Client) PauseSchedule(ctx context.Context, id, note string) error {
	_, err := c.conn.Call(ctx, func(ctx context.Context, stub server.WorkflowServiceClient, opt grpc.CallOption) error {
		_, err := stub.PauseSchedule(ctx, &server.PauseScheduleRequest{ScheduleId: id, Note: note}, opt)
		return err
	})
	return wrap("pause schedule", err)
}

// ResumeSchedule makes a paused schedule fire again.
func (c *Client) ResumeSchedule(ctx context.Context, id string) error {
	_, err := c.conn.Call(ctx, func(ctx context.Context, stub server.WorkflowServiceClient, opt grpc.CallOption) error {
		_, err := stub.ResumeSchedule(ctx, &server.ResumeScheduleRequest{ScheduleId: id}, opt)
		return err
	})
	return wrap("resume schedule", err)
}

// BackfillSchedule starts the runs the schedule would have started for its
// fire times after from and up to to (both past), once each: runs that exist
// already are counted, not started again. skip starts a fire only if no run
// of the schedule is going then.
func (c *Client) BackfillSchedule(ctx context.Context, id string, from, to time.Time, skip bool) (Backfilled, error) {
	req := &server.BackfillScheduleRequest{ScheduleId: id, StartTime: from.UnixNano(), EndTime: to.UnixNano()}
	if skip {
		req.OverlapPolicy = server.OverlapPolicy_OVERLAP_POLICY_SKIP
	}
	var resp *server.BackfillScheduleResponse
	_, err := c.conn.Call(ctx, func(ctx context.Context, stub server.WorkflowServiceClient, opt grpc.CallOption) error {
		var err error
		resp, err = stub.BackfillSchedule(ctx, req, opt)
		return err
	})
	if err != nil {
		return Backfilled{}, wrap("backfill schedule", err)
	}
	return Backfilled{Started: int(resp.GetStarted()), Existing: int(resp.GetExisting()), Skipped: int(resp.GetSkipped())}, nil
}

// PreviewSchedule returns the next n fire times of a schedule, and when each
// run would start with its jitter.
func (c *Client) PreviewSchedule(ctx context.Context, id string, n int) (fires, starts []time.Time, err error) {
	var resp *server.PreviewScheduleResponse
	_, err = c.conn.Call(ctx, func(ctx context.Context, stub server.WorkflowServiceClient, opt grpc.CallOption) error {
		var err error
		resp, err = stub.PreviewSchedule(ctx, &server.PreviewScheduleRequest{ScheduleId: id, Count: int32(n)}, opt)
		return err
	})
	if err != nil {
		return nil, nil, wrap("preview schedule", err)
	}
	for i, f := range resp.GetFireTimes() {
		fires = append(fires, time.Unix(0, f).UTC())
		starts = append(starts, time.Unix(0, resp.GetStartTimes()[i]).UTC())
	}
	return fires, starts, nil
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
		OverlapPolicy: server.OverlapPolicy(s.Overlap), JitterMs: s.Jitter.Milliseconds(), Paused: s.Paused, Note: s.Note,
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
				TaskQueue: s.GetTaskQueue(), Overlap: OverlapPolicy(s.GetOverlapPolicy()),
				Jitter: time.Duration(s.GetJitterMs()) * time.Millisecond, Paused: s.GetPaused(), Note: s.GetNote(),
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
