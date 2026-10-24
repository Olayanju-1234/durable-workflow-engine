package worker_test

import (
	"context"
	"testing"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/embedded"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// Two Go workers implement the same activity name on different task queues:
// a workflow runs its activities on its own queue's worker, and on another
// queue's only when it names that queue.
func TestTaskQueuesThroughTheSDK(t *testing.T) {
	srv := startEmbedded(t, embedded.Options{})
	c := srv.Client()
	ctx := testCtx(t)

	billing := worker.New(c, worker.Options{Logger: quiet, TaskQueue: "billing"})
	billing.RegisterWorkflow("Invoice", func(ctx workflow.Context, in []byte) ([]byte, error) {
		own, err := ctx.ExecuteActivity("send", in)
		if err != nil {
			return nil, err
		}
		other, err := ctx.ExecuteActivity("send", in, workflow.WithTaskQueue("default"))
		if err != nil {
			return nil, err
		}
		return []byte(string(own) + " / " + string(other) + " / " + ctx.Info().TaskQueue), nil
	})
	billing.RegisterActivity("send", func(context.Context, []byte) ([]byte, error) { return []byte("billing"), nil })
	run(t, billing)
	general := worker.New(c, worker.Options{Logger: quiet})
	general.RegisterActivity("send", func(context.Context, []byte) ([]byte, error) { return []byte("default"), nil })
	run(t, general)

	id, err := c.StartWorkflow(ctx, "Invoice", nil, client.WithTaskQueue("billing"))
	if err != nil {
		t.Fatal(err)
	}
	out, err := c.WaitForResult(ctx, id)
	if err != nil || string(out) != "billing / default / billing" {
		t.Fatalf("result %q, %v", out, err)
	}
	d, err := c.DescribeWorkflow(ctx, id)
	if err != nil || d.TaskQueue != "billing" || d.Steps[0].TaskQueue != "billing" || d.Steps[1].TaskQueue != "default" {
		t.Fatalf("describe = %+v, %v", d, err)
	}
}
