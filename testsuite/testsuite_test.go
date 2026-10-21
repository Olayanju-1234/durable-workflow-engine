package testsuite_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/testsuite"
	"github.com/olayanju-1234/durable-workflow-engine/worker"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// subscription bills, waits a week (or until cancelled), and bills again.
func subscription(ctx workflow.Context, input []byte) ([]byte, error) {
	if _, err := ctx.ExecuteActivity("bill", input); err != nil {
		return nil, err
	}
	ctx.Sleep("billing-period", 7*24*time.Hour)
	if ctx.WaitForSignal("renewal", "renew") == nil {
		return nil, errors.New("no renewal answer")
	}
	started := ctx.Now()
	if _, err := ctx.ExecuteActivity("bill", input); err != nil {
		ctx.ExecuteActivity("notify", []byte("payment failed"))
		return nil, err
	}
	return []byte("renewed at " + started.Format(time.RFC3339)), nil
}

func TestTimeIsVirtual(t *testing.T) {
	env := testsuite.NewEnv()
	bills := 0
	env.RegisterActivity("bill", func(context.Context, []byte) ([]byte, error) { bills++; return nil, nil })
	// Sent mid-sleep: buffered until the workflow waits for it.
	env.SignalAfter(time.Hour, "renew", []byte("yes"))

	start := time.Now()
	out, err := env.ExecuteWorkflow(subscription, []byte("acct-1"))
	if err != nil {
		t.Fatal(err)
	}
	if real := time.Since(start); real > 5*time.Second {
		t.Fatalf("a week of virtual time took %s of real time", real)
	}
	if env.Elapsed() != 7*24*time.Hour || bills != 2 {
		t.Fatalf("elapsed %s, bills %d; want 168h and 2", env.Elapsed(), bills)
	}
	if want := "renewed at 2026-01-08T00:00:00Z"; string(out) != want {
		t.Fatalf("output %q, want %q (ctx.Now is virtual)", out, want)
	}
}

func TestMockedFailureRunsCompensation(t *testing.T) {
	env := testsuite.NewEnv()
	attempt := 0
	var notified string
	env.RegisterActivity("bill", func(context.Context, []byte) ([]byte, error) {
		attempt++
		if attempt == 2 {
			return nil, errors.New("card expired")
		}
		return nil, nil
	})
	env.RegisterActivity("notify", func(_ context.Context, in []byte) ([]byte, error) { notified = string(in); return nil, nil })
	env.SignalAfter(8*24*time.Hour, "renew", []byte("yes"))

	_, err := env.ExecuteWorkflow(subscription, nil)
	if !errors.Is(err, testsuite.ErrWorkflowFailed) || !strings.Contains(err.Error(), "card expired") {
		t.Fatalf("got %v, want the workflow to fail with the billing error", err)
	}
	if notified != "payment failed" || env.Elapsed() != 8*24*time.Hour {
		t.Fatalf("notified %q after %s", notified, env.Elapsed())
	}
}

func TestBlockedOnMissingSignal(t *testing.T) {
	env := testsuite.NewEnv()
	env.RegisterActivity("bill", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	if _, err := env.ExecuteWorkflow(subscription, nil); !errors.Is(err, testsuite.ErrBlocked) {
		t.Fatalf("got %v, want ErrBlocked", err)
	}
}

func TestUnregisteredActivity(t *testing.T) {
	env := testsuite.NewEnv()
	if _, err := env.ExecuteWorkflow(subscription, nil); err == nil || !strings.Contains(err.Error(), `"bill" is not registered`) {
		t.Fatalf("got %v", err)
	}
}

// Branching on state outside the history makes replay take another path; the
// environment replays every decision, so the test catches it.
func TestCatchesNonDeterminism(t *testing.T) {
	env := testsuite.NewEnv()
	env.RegisterActivity("a", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	env.RegisterActivity("b", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	runs := 0
	flaky := func(ctx workflow.Context, in []byte) ([]byte, error) {
		runs++ // stands in for time.Now, rand or a global
		step := "a"
		if runs > 1 {
			step = "b"
		}
		ctx.ExecuteActivity(step, in)
		return ctx.ExecuteActivity("a", in)
	}
	if _, err := env.ExecuteWorkflow(flaky, nil); err == nil || !strings.Contains(err.Error(), "non-determinism") {
		t.Fatalf("got %v, want a non-determinism error", err)
	}
}

func TestVersionAndSideEffect(t *testing.T) {
	env := testsuite.NewEnv()
	var info worker.ActivityInfo
	env.RegisterActivity("charge", func(ctx context.Context, in []byte) ([]byte, error) {
		info, _ = worker.GetActivityInfo(ctx)
		return in, nil
	})
	sideEffects := 0
	wf := func(ctx workflow.Context, in []byte) ([]byte, error) {
		v := ctx.GetVersion("idempotency-key", workflow.DefaultVersion, 1)
		key := ctx.SideEffect("key", func() []byte { sideEffects++; return []byte("k-42") })
		out, err := ctx.ExecuteActivity("charge", key)
		return []byte(fmt.Sprintf("v%d %s", v, out)), err
	}
	out, err := env.ExecuteWorkflow(wf, nil)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "v1 k-42" || sideEffects != 1 {
		t.Fatalf("output %q, side effect ran %d times", out, sideEffects)
	}
	if info.WorkflowID != testsuite.WorkflowID || info.StepName != "charge" || info.Sequence != 3 {
		t.Fatalf("activity info %+v", info)
	}
}

func TestRetryPolicyInVirtualTime(t *testing.T) {
	env := testsuite.NewEnv()
	calls := 0
	env.RegisterActivity("charge", func(context.Context, []byte) ([]byte, error) {
		if calls++; calls < 3 {
			return nil, errors.New("declined")
		}
		return []byte("charged"), nil
	})
	out, err := env.ExecuteWorkflow(func(ctx workflow.Context, in []byte) ([]byte, error) {
		return ctx.ExecuteActivity("charge", in, workflow.WithRetryPolicy(workflow.RetryPolicy{
			MaxAttempts: 3, InitialBackoff: time.Minute,
		}))
	}, nil)
	if err != nil || string(out) != "charged" {
		t.Fatalf("got %q, %v; want charged", out, err)
	}
	if env.Elapsed() != 3*time.Minute { // 1m, then 2m
		t.Errorf("elapsed %s, want 3m of backoff", env.Elapsed())
	}

	env = testsuite.NewEnv()
	env.RegisterActivity("charge", func(context.Context, []byte) ([]byte, error) { return nil, errors.New("declined") })
	_, err = env.ExecuteWorkflow(func(ctx workflow.Context, in []byte) ([]byte, error) {
		return ctx.ExecuteActivity("charge", in, workflow.WithRetryPolicy(workflow.RetryPolicy{MaxAttempts: 2}))
	}, nil)
	if !errors.Is(err, testsuite.ErrWorkflowFailed) {
		t.Fatalf("exhausted retries: %v, want ErrWorkflowFailed", err)
	}
}
