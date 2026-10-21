package cluster

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// The public client and worker, given one follower's address, follow leader
// hints in, ride out a leader kill mid-workflow, and re-register their workflow
// type with the new leader's engine, which starts with no registrations.
func TestSDKFollowsLeaderAcrossFailover(t *testing.T) {
	tc := newCluster(t, 3, 0, true)
	old, _ := tc.leader()
	// Seed only one follower: the leader is reachable through its hints alone.
	var seed string
	for id, a := range tc.grpcAddrs {
		if id != old.id {
			seed = a
			break
		}
	}

	c, err := client.Dial(client.Options{Addrs: []string{seed}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	release := make(chan struct{})
	w := worker.New(c, worker.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), PollTimeout: time.Second})
	w.RegisterWorkflow("Pipeline", func(ctx workflow.Context, in []byte) ([]byte, error) {
		a, err := ctx.ExecuteActivity("a", in)
		if err != nil {
			return nil, err
		}
		return ctx.ExecuteActivity("b", a)
	})
	w.RegisterActivity("a", func(_ context.Context, in []byte) ([]byte, error) { return append(in, 'a'), nil })
	w.RegisterActivity("b", func(ctx context.Context, in []byte) ([]byte, error) {
		select {
		case <-release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return append(in, 'b'), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	id, err := c.StartWorkflow(ctx, "Pipeline", []byte(">"))
	if err != nil {
		t.Fatal(err)
	}
	for { // b is in flight on the old leader
		d, err := c.DescribeWorkflow(ctx, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(d.Steps) == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	tc.stop(old.id)
	close(release)
	out, err := c.WaitForResult(ctx, id)
	if err != nil || string(out) != ">ab" {
		t.Fatalf("after failover: %q %v", out, err)
	}

	tc.start(old.id)
	evs := tc.waitIdentical(id, 6) // started, a scheduled+completed, b scheduled+completed, completed
	got := completions(evs)
	if got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("completions %v, want each step exactly once in history", got)
	}
}
