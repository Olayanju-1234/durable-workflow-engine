package worker_test

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

// TestCancelReachesTheActivity: CancelWorkflow ends the workflow, and the
// worker running its activity learns from its next heartbeat that the work was
// cancelled (not merely reassigned): the activity's ctx ends with that cause.
func TestCancelReachesTheActivity(t *testing.T) {
	srv := startEmbedded(t, embedded.Options{LeaseDuration: 300 * time.Millisecond})
	c := srv.Client()
	w := worker.New(c, worker.Options{Logger: quiet})
	w.RegisterWorkflow("Provision", func(ctx workflow.Context, in []byte) ([]byte, error) {
		return ctx.ExecuteActivity("provision", in)
	})
	started := make(chan struct{})
	cause := make(chan error, 1)
	w.RegisterActivity("provision", func(ctx context.Context, in []byte) ([]byte, error) {
		close(started)
		<-ctx.Done()
		cause <- context.Cause(ctx)
		return nil, ctx.Err()
	})
	run(t, w)
	ctx := testCtx(t)

	id, err := c.StartWorkflow(ctx, "Provision", nil)
	if err != nil {
		t.Fatal(err)
	}
	<-started
	if err := c.CancelWorkflow(ctx, id, "tenant deleted"); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-cause:
		if !errors.Is(got, worker.ErrActivityCancelled) {
			t.Fatalf("activity ctx cause %v, want worker.ErrActivityCancelled", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the activity was never told it was cancelled")
	}
	d, err := c.DescribeWorkflow(ctx, id)
	if err != nil || d.Status != client.StatusCancelled || d.Error != "cancelled: tenant deleted" || d.Steps[0].Status != "CANCELLED" {
		t.Fatalf("describe: %+v %v", d, err)
	}
	if _, err := c.WaitForResult(ctx, id); !errors.Is(err, client.ErrWorkflowFailed) {
		t.Fatalf("WaitForResult: %v, want ErrWorkflowFailed for a cancelled workflow", err)
	}
	if err := c.CancelWorkflow(ctx, id, ""); !errors.Is(err, client.ErrNotRunning) {
		t.Fatalf("second cancel: %v, want ErrNotRunning", err)
	}
}
