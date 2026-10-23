package worker_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/embedded"
	"github.com/olayanju-1234/durable-workflow-engine/testsuite"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// webhook retries generously, as liftoff's deliveries do, and reports how the
// delivery ended.
func webhook(ctx workflow.Context, in []byte) ([]byte, error) {
	_, err := ctx.ExecuteActivity("post", in, workflow.WithRetryPolicy(workflow.RetryPolicy{
		MaxAttempts: 6, InitialBackoff: 10 * time.Millisecond, NonRetryableErrorTypes: []string{"Gone"},
	}))
	var ae *workflow.ActivityError
	if !errors.As(err, &ae) {
		return nil, fmt.Errorf("want *workflow.ActivityError, got %v", err)
	}
	return []byte(fmt.Sprintf("%s|%t|%s", ae.Type, ae.NonRetryable, ae.Message)), nil
}

func post(calls *atomic.Int32) worker.ActivityFunc {
	return func(_ context.Context, in []byte) ([]byte, error) {
		calls.Add(1)
		switch string(in) {
		case "404":
			return nil, worker.NonRetryable(errors.New("webhook answered 404"))
		case "410":
			return nil, worker.Typed("Gone", errors.New("webhook answered 410"))
		}
		return nil, errors.New("webhook answered 503")
	}
}

// TestNonRetryableThroughTheSDK: worker.NonRetryable and worker.Typed reach
// the engine over gRPC, so a 4xx fails the step on its first attempt while a
// 503 is retried; the workflow sees a *workflow.ActivityError saying which.
func TestNonRetryableThroughTheSDK(t *testing.T) {
	srv := startEmbedded(t, embedded.Options{})
	c := srv.Client()
	w := worker.New(c, worker.Options{Logger: quiet})
	w.RegisterWorkflow("Webhook", webhook)
	var calls atomic.Int32
	w.RegisterActivity("post", post(&calls))
	run(t, w)
	ctx := testCtx(t)
	for _, tc := range []struct{ in, want string }{
		{"404", "|true|webhook answered 404"},
		{"410", "Gone|false|webhook answered 410"},
		{"503", "|false|webhook answered 503"},
	} {
		calls.Store(0)
		id, err := c.StartWorkflow(ctx, "Webhook", []byte(tc.in))
		if err != nil {
			t.Fatal(err)
		}
		out, err := c.WaitForResult(ctx, id)
		wantCalls := int32(1)
		if tc.in == "503" {
			wantCalls = 6
		}
		if err != nil || string(out) != tc.want || calls.Load() != wantCalls {
			t.Fatalf("%s: %q %v after %d calls, want %q after %d", tc.in, out, err, calls.Load(), tc.want, wantCalls)
		}
	}
}

// TestNonRetryableInTheTestsuite: the testsuite applies the same rules.
func TestNonRetryableInTheTestsuite(t *testing.T) {
	for in, wantCalls := range map[string]int32{"404": 1, "410": 1, "503": 6} {
		var calls atomic.Int32
		env := testsuite.NewEnv()
		env.RegisterActivity("post", post(&calls))
		if _, err := env.ExecuteWorkflow(webhook, []byte(in)); err != nil || calls.Load() != wantCalls {
			t.Fatalf("%s: %v after %d calls, want %d", in, err, calls.Load(), wantCalls)
		}
	}
}
