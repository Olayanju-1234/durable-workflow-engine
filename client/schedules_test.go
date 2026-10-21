package client_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/embedded"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

func TestSchedulesThroughTheClient(t *testing.T) {
	srv, err := embedded.Start(embedded.Options{DataDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { srv.Close() })
	c := srv.Client()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	w := worker.New(c, worker.Options{})
	w.RegisterWorkflow("Echo", func(_ workflow.Context, in []byte) ([]byte, error) { return in, nil })
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(w.Stop)

	if err := c.CreateSchedule(ctx, client.Schedule{ID: "bad", Cron: "every minute", WorkflowType: "Echo"}); !errors.Is(err, client.ErrInvalid) {
		t.Fatalf("bad cron: %v, want ErrInvalid", err)
	}
	if err := c.CreateSchedule(ctx, client.Schedule{ID: "ping", Cron: "@every 1s", WorkflowType: "Echo", Input: []byte("tick")}); err != nil {
		t.Fatal(err)
	}
	scheds, err := c.ListSchedules(ctx)
	if err != nil || len(scheds) != 1 || scheds[0].ID != "ping" || scheds[0].NextFireAt.IsZero() {
		t.Fatalf("ListSchedules = %+v, %v", scheds, err)
	}
	run := "ping-" + scheds[0].NextFireAt.Format("20060102T150405Z")
	var out []byte
	for out == nil && ctx.Err() == nil { // the run does not exist until it fires
		out, err = c.WaitForResult(ctx, run)
		if errors.Is(err, client.ErrNotFound) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if string(out) != "tick" {
		t.Fatalf("run %s returned %q, want tick", run, out)
	}
	if err := c.DeleteSchedule(ctx, "ping"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteSchedule(ctx, "ping"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("second delete: %v, want ErrNotFound", err)
	}
	if scheds, _ := c.ListSchedules(ctx); len(scheds) != 0 {
		t.Fatalf("after delete: %+v", scheds)
	}
}
