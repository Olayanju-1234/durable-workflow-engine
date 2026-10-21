package server

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

func codeRetryWorkflow(backoff time.Duration) workflow.Func {
	return func(ctx workflow.Context, input []byte) ([]byte, error) {
		return ctx.ExecuteActivity("charge", input, workflow.WithRetryPolicy(workflow.RetryPolicy{
			MaxAttempts: 5, InitialBackoff: backoff,
		}))
	}
}

// TestCodeRetryPolicyRetriesActivity: a policy set by the workflow code applies
// with no policy configured on the engine, and the workflow never sees the
// transient failures.
func TestCodeRetryPolicyRetriesActivity(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e := NewEngine(store, discardLogger())
	t.Cleanup(e.Close)
	e.RegisterWorkflow("CodeRetry", replay.Planner{Fn: codeRetryWorkflow(time.Millisecond)})

	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runEngineWorker(ctx, e, func(string, []byte) ([]byte, error) {
		if calls.Add(1) < 3 {
			return nil, errors.New("flaky")
		}
		return []byte("charged"), nil
	})
	id, err := e.StartWorkflow("CodeRetry", []byte("in"), "")
	if err != nil {
		t.Fatal(err)
	}
	final := waitCompleted(t, e, id, 5*time.Second)
	if final.Status != state.StatusCompleted || string(final.Output) != "charged" {
		t.Fatalf("status %s output %q, want COMPLETED charged", final.Status, final.Output)
	}
	if got := final.Steps[0].Attempts; got != 3 {
		t.Errorf("attempts = %d, want 3", got)
	}
	if p := final.Steps[0].Retry; p == nil || p.MaxAttempts != 5 {
		t.Errorf("recorded policy = %+v, want MaxAttempts 5", p)
	}
}

// TestCodeRetryPolicySurvivesRestart: the policy lives in history, so a restarted
// engine with no policy of its own still retries the failed attempt.
func TestCodeRetryPolicySurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	store1, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	e1 := NewEngine(store1, discardLogger())
	e1.RegisterWorkflow("CodeRetry", replay.Planner{Fn: codeRetryWorkflow(time.Hour)})
	ctx1, cancel1 := context.WithCancel(context.Background())
	go runEngineWorker(ctx1, e1, func(string, []byte) ([]byte, error) { return nil, errors.New("down") })
	id, err := e1.StartWorkflow("CodeRetry", []byte("in"), "")
	if err != nil {
		t.Fatal(err)
	}
	waitForState(t, e1, id, func(st *state.State) bool {
		return st.Tail() != nil && st.Tail().Status == state.StepFailed
	}, 3*time.Second)
	cancel1()
	e1.Close()
	store1.Close()

	store2, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store2.Close() })
	e2 := NewEngine(store2, discardLogger())
	t.Cleanup(e2.Close)
	e2.RegisterWorkflow("CodeRetry", replay.Planner{Fn: codeRetryWorkflow(time.Hour)})
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go runEngineWorker(ctx2, e2, func(string, []byte) ([]byte, error) { return []byte("charged"), nil })
	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}
	final := waitCompleted(t, e2, id, 5*time.Second)
	if final.Status != state.StatusCompleted || final.Steps[0].Attempts != 2 {
		t.Fatalf("after restart: status %s attempts %d, want COMPLETED after 2 attempts", final.Status, final.Steps[0].Attempts)
	}
}

// TestCodeRetryPolicyOverridesEngine: the call's policy wins over the engine's
// policy for the same step name.
func TestCodeRetryPolicyOverridesEngine(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e := NewEngine(store, discardLogger())
	t.Cleanup(e.Close)
	e.SetRetryPolicy("charge", RetryPolicy{MaxAttempts: 1})
	e.RegisterWorkflow("CodeRetry", replay.Planner{Fn: codeRetryWorkflow(time.Millisecond)})
	var calls atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runEngineWorker(ctx, e, func(string, []byte) ([]byte, error) {
		if calls.Add(1) < 2 {
			return nil, errors.New("flaky")
		}
		return []byte("ok"), nil
	})
	id, err := e.StartWorkflow("CodeRetry", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if final := waitCompleted(t, e, id, 5*time.Second); final.Status != state.StatusCompleted {
		t.Fatalf("status %s (%s), want COMPLETED: the engine's no-retry policy won", final.Status, final.Error)
	}
}

func TestRetryFromCommandValidates(t *testing.T) {
	for _, p := range []*history.RetryPolicy{
		{MaxAttempts: 0, InitialBackoffMs: 10},
		{MaxAttempts: 2, InitialBackoffMs: -1},
		{MaxAttempts: 2, MaxBackoffMs: -5},
	} {
		if _, err := retryFromCommand(p); !errors.Is(err, ErrInvalidCommand) {
			t.Errorf("retryFromCommand(%v) = %v, want ErrInvalidCommand", p, err)
		}
	}
	got, err := retryFromCommand(&history.RetryPolicy{MaxAttempts: 3, InitialBackoffMs: 100, MaxBackoffMs: 1000})
	if err != nil || got.MaxAttempts != 3 || got.Backoff != 100*time.Millisecond || got.MaxBackoff != time.Second {
		t.Errorf("valid policy = %+v, %v", got, err)
	}
}
