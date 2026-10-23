package worker_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/embedded"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// TestTimeoutsThroughTheSDK: an activity's ctx ends at its start-to-close
// timeout, the workflow sees the failure, and a workflow past its execution
// timeout is TIMED_OUT for the client.
func TestTimeoutsThroughTheSDK(t *testing.T) {
	srv := startEmbedded(t, embedded.Options{})
	c := srv.Client()
	w := worker.New(c, worker.Options{Logger: quiet})
	w.RegisterWorkflow("Bounded", func(ctx workflow.Context, in []byte) ([]byte, error) {
		_, err := ctx.ExecuteActivity("stall", in, workflow.WithStartToCloseTimeout(200*time.Millisecond))
		if err == nil {
			return nil, errors.New("stall cannot succeed")
		}
		return []byte(err.Error()), nil
	})
	w.RegisterWorkflow("Forever", func(ctx workflow.Context, in []byte) ([]byte, error) {
		return ctx.WaitForSignal("never", "never-sent"), nil
	})
	sawDeadline := make(chan time.Duration, 1)
	w.RegisterActivity("stall", func(ctx context.Context, in []byte) ([]byte, error) {
		if dl, ok := ctx.Deadline(); ok {
			sawDeadline <- time.Until(dl)
		}
		<-ctx.Done() // a well-behaved activity stops when its time is up
		return nil, ctx.Err()
	})
	run(t, w)
	ctx := testCtx(t)

	id, err := c.StartWorkflow(ctx, "Bounded", nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.WaitForResult(ctx, id)
	if err != nil || (!strings.Contains(string(out), "start-to-close timeout") && !strings.Contains(string(out), "deadline exceeded")) {
		t.Fatalf("got %q %v, want the workflow to see the attempt time out", out, err)
	}
	if left := <-sawDeadline; left <= 0 || left > 200*time.Millisecond {
		t.Fatalf("activity ctx deadline %s away, want within the 200ms start-to-close", left)
	}

	id, err = c.StartWorkflow(ctx, "Forever", nil, client.WithExecutionTimeout(150*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.WaitForResult(ctx, id)
	if !errors.Is(err, client.ErrWorkflowFailed) || !strings.Contains(err.Error(), "TIMED_OUT") {
		t.Fatalf("got %v, want ErrWorkflowFailed with TIMED_OUT", err)
	}
	d, err := c.DescribeWorkflow(ctx, id)
	if err != nil || d.Status != client.StatusTimedOut || !d.Status.Done() {
		t.Fatalf("describe: %v %v, want StatusTimedOut", d, err)
	}
}
