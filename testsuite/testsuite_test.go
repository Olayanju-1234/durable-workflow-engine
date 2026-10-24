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

func TestParallelStepsInVirtualTime(t *testing.T) {
	env := testsuite.NewEnv()
	var order []string
	for _, name := range []string{"a", "b", "c"} {
		env.RegisterActivity(name, func(_ context.Context, in []byte) ([]byte, error) {
			order = append(order, name)
			if name == "b" {
				return nil, errors.New("b failed")
			}
			return []byte(name), nil
		})
	}
	env.SignalAfter(90*time.Minute, "late", nil)
	out, err := env.ExecuteWorkflow(func(ctx workflow.Context, in []byte) ([]byte, error) {
		a := ctx.ExecuteActivityAsync("a", nil)
		b := ctx.ExecuteActivityAsync("b", nil, workflow.WithRetryPolicy(workflow.RetryPolicy{MaxAttempts: 2, InitialBackoff: time.Hour}))
		c := ctx.ExecuteActivityAsync("c", nil)
		sig := ctx.WaitForSignalAsync("late", "late")
		deadline := ctx.NewTimer("deadline", 2*time.Hour)
		if _, err := b.Get(); err == nil {
			return nil, errors.New("b should fail after its retry")
		}
		first := ctx.WaitAny(sig, deadline)
		ra, _ := a.Get()
		rc, _ := c.Get()
		return []byte(fmt.Sprintf("%s%s:%d", ra, rc, first)), nil
	}, nil)
	if err != nil || string(out) != "ac:0" {
		t.Fatalf("got %q, %v; want ac:0 (the signal at 1h30 beats the 2h deadline)", out, err)
	}
	if strings.Join(order, "") != "abcb" {
		t.Fatalf("activities ran in order %v, want a b c, then b's retry an hour later", order)
	}
	if env.Elapsed() != 90*time.Minute {
		t.Fatalf("elapsed %s, want 1h30 (b's retry at 1h, then the signal)", env.Elapsed())
	}
}

// TestCancelStepInVirtualTime: the deadline wins, the losing signal wait is
// cancelled, and a later signal goes to the wait that still wants it.
func TestCancelStepInVirtualTime(t *testing.T) {
	env := testsuite.NewEnv()
	env.SignalAfter(2*time.Hour, "reply", []byte("late"))
	out, err := env.ExecuteWorkflow(func(ctx workflow.Context, in []byte) ([]byte, error) {
		reply := ctx.WaitForSignalAsync("reply", "reply")
		deadline := ctx.NewTimer("deadline", time.Hour)
		if ctx.WaitAny(reply, deadline) == 1 {
			ctx.CancelStep(reply)
		}
		if _, err := reply.Get(); !errors.Is(err, workflow.ErrCancelled) {
			return nil, err
		}
		return ctx.WaitForSignal("again", "reply"), nil
	}, nil)
	if err != nil || string(out) != "late" || env.Elapsed() != 2*time.Hour {
		t.Fatalf("out %q err %v elapsed %s", out, err, env.Elapsed())
	}
}

// The test environment follows continue-as-new and suggests it the way the
// engine does near its limits.
func TestContinueAsNewInVirtualTime(t *testing.T) {
	env := testsuite.NewEnv()
	env.RegisterActivity("tick", func(context.Context, []byte) ([]byte, error) { return nil, nil })
	env.SuggestContinueAsNewAfter(6)
	out, err := env.ExecuteWorkflow(func(ctx workflow.Context, in []byte) ([]byte, error) {
		total := len(in)
		for i := 0; ; i++ {
			if total >= 10 {
				return []byte(fmt.Sprintf("%d ticks over runs, this one continued from %s", total, ctx.Info().ContinuedFromRunID)), nil
			}
			if ctx.ContinueAsNewSuggested() {
				return nil, workflow.ContinueAsNew(make([]byte, total))
			}
			if _, err := ctx.ExecuteActivity("tick", nil); err != nil {
				return nil, err
			}
			total++
		}
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if env.Runs() < 3 || !strings.HasPrefix(string(out), "10 ticks over runs, this one continued from test-run-") {
		t.Fatalf("%d runs, output %q", env.Runs(), out)
	}
}

// A graceful cancel in virtual time: the code is told at its wait, its
// compensation runs, and the workflow ends cancelled.
func TestGracefulCancelInVirtualTime(t *testing.T) {
	env := testsuite.NewEnv()
	released := false
	env.RegisterActivity("reserve", func(context.Context, []byte) ([]byte, error) { return []byte("r"), nil })
	env.RegisterActivity("release", func(context.Context, []byte) ([]byte, error) { released = true; return nil, nil })
	env.RequestCancelAfter(time.Hour, "customer left")
	_, err := env.ExecuteWorkflow(func(ctx workflow.Context, in []byte) ([]byte, error) {
		reserved, err := ctx.ExecuteActivity("reserve", in)
		if err != nil {
			return nil, err
		}
		if _, err := ctx.NewTimer("settle", 24*time.Hour).Get(); err != nil {
			ctx.ExecuteActivity("release", reserved)
			return nil, err
		}
		return reserved, nil
	}, nil)
	if !errors.Is(err, testsuite.ErrWorkflowCancelled) || !released || env.Elapsed() != time.Hour {
		t.Fatalf("err %v, released %v, elapsed %s", err, released, env.Elapsed())
	}
}
