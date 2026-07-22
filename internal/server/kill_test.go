package server

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// The Kill Test is the Phase-1 finale: prove a workflow survives an uncatchable
// SIGKILL of the whole engine process. A child process starts a 5-step workflow,
// drives it to WAITING at step 3, and reports ready; the parent SIGKILLs it (not a
// graceful shutdown — the child gets no chance to clean up), then opens a fresh
// engine over the same data dir, recovers, and drives steps 3–5 home. The proof:
// steps 1–2 are never re-run and steps 4–5 do run, all from what was on disk.

const (
	killEnvActive = "WORKFLOW_KILL_HELPER" // "1" marks the re-exec'd child (the victim)
	killEnvDir    = "WORKFLOW_KILL_DIR"    // data dir shared between parent and child
	killEnvReady  = "WORKFLOW_KILL_READY"  // sentinel file the child writes at WAITING/step3
)

const killWorkflowID = "kill-test-wf"

var killTestDef = state.WorkflowDefinition{
	Type:  "FiveStep",
	Steps: state.Activities("step1", "step2", "step3", "step4", "step5"),
}

// TestKillHelperVictim is the child half of the Kill Test. Under normal `go test`
// it skips instantly; only the parent's re-exec (with killEnvActive set) runs the
// body. It drives the workflow to step 3, signals ready, then blocks to be killed.
func TestKillHelperVictim(t *testing.T) {
	if os.Getenv(killEnvActive) != "1" {
		t.Skip("child-only: run via re-exec from TestKillTest")
	}
	dir := os.Getenv(killEnvDir)
	readyPath := os.Getenv(killEnvReady)

	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("child: new store: %v", err)
	}
	e := NewEngine(store, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	e.Register(killTestDef)
	if err := e.Recover(); err != nil { // models the real daemon boot; a no-op here
		t.Fatalf("child: recover: %v", err)
	}
	if _, err := e.StartWorkflow(killTestDef.Type, []byte("order-42"), killWorkflowID); err != nil {
		t.Fatalf("child: start workflow: %v", err)
	}

	// Act as the worker for steps 1 and 2. When step 3 comes up it is already
	// durably scheduled (WAITING on disk), so signal ready and wait to be killed
	// WITHOUT completing it — that leaves the crash exactly at step 3.
	ctx := context.Background()
	for {
		task, err := e.PollTask(ctx, "child-worker")
		if err != nil {
			t.Fatalf("child: poll: %v", err)
		}
		if task.GetStepName() == "step3" {
			if err := os.WriteFile(readyPath, []byte("ready"), 0o644); err != nil {
				t.Fatalf("child: write ready sentinel: %v", err)
			}
			// Block until the parent SIGKILLs us. The timeout is only a safety net
			// against being orphaned; the normal path never reaches it.
			<-time.After(60 * time.Second)
			return
		}
		out := []byte("done:" + task.GetStepName())
		if err := e.CompleteTask(task.GetWorkflowId(), task.GetStepName(), task.GetSequence(), task.GetLeaseToken(), out, "", false); err != nil {
			t.Fatalf("child: complete %s: %v", task.GetStepName(), err)
		}
	}
}

func TestKillTest(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "data")
	readyPath := filepath.Join(base, "ready")

	// Re-exec this test binary, running only the child victim, over a shared dir.
	child := exec.Command(os.Args[0], "-test.run=^TestKillHelperVictim$", "-test.v")
	child.Env = append(os.Environ(),
		killEnvActive+"=1", killEnvDir+"="+dir, killEnvReady+"="+readyPath)
	var childOut bytes.Buffer
	child.Stdout, child.Stderr = &childOut, &childOut
	if err := child.Start(); err != nil {
		t.Fatalf("start child: %v", err)
	}
	// Always reap the child so a failure can't leak a process.
	defer func() {
		_ = child.Process.Kill()
		_ = child.Wait()
	}()

	// Wait until the child has durably reached WAITING at step 3.
	if !waitForFile(readyPath, 15*time.Second) {
		t.Fatalf("child never reached step 3.\nchild output:\n%s", childOut.String())
	}

	// The uncatchable kill: no defer, no fsync, no graceful shutdown in the child.
	if err := child.Process.Kill(); err != nil {
		t.Fatalf("kill child: %v", err)
	}
	_ = child.Wait() // reap; "signal: killed" is expected and ignored

	// Restart: a brand-new engine (empty queue) over the same on-disk history.
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	e := NewEngine(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(e.Close)
	e.Register(killTestDef)

	// Sanity-check what the kill left on disk: WAITING at step 3, steps 1–2 done.
	preEvents, err := store.ReadAll(killWorkflowID)
	if err != nil {
		t.Fatalf("read history after kill: %v", err)
	}
	preState, err := state.Reconstruct(preEvents)
	if err != nil {
		t.Fatalf("reconstruct after kill: %v", err)
	}
	if preState.Status != state.StatusWaiting {
		t.Fatalf("post-kill status: got %s, want WAITING at step 3", preState.Status)
	}
	if tail := preState.Tail(); tail.Name != "step3" || tail.Sequence != 3 {
		t.Fatalf("post-kill in-flight step: got %s/seq%d, want step3/seq3", tail.Name, tail.Sequence)
	}

	// Recover rebuilds the queue from disk; step 3 must be re-enqueued.
	if err := e.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}

	// Drive the rest home as the worker: steps 3, 4, 5 → COMPLETED.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	workerErr := make(chan error, 1)
	go func() {
		for {
			task, err := e.PollTask(ctx, "recovery-worker")
			if err != nil {
				workerErr <- err
				return
			}
			out := []byte("done:" + task.GetStepName())
			if err := e.CompleteTask(task.GetWorkflowId(), task.GetStepName(), task.GetSequence(), task.GetLeaseToken(), out, "", false); err != nil {
				workerErr <- err
				return
			}
		}
	}()

	final := waitCompleted(t, e, killWorkflowID, 8*time.Second)
	cancel()
	<-workerErr // let the worker goroutine drain out on the cancelled context

	if final.Status != state.StatusCompleted {
		t.Fatalf("final status: got %s, want COMPLETED", final.Status)
	}
	if got := string(final.Output); got != "done:step5" {
		t.Errorf("final output: got %q, want done:step5", got)
	}

	// No duplicates: exactly WorkflowStarted + 5×(scheduled+completed) + Completed.
	events, _ := store.ReadAll(killWorkflowID)
	if len(events) != 12 {
		t.Fatalf("history length: got %d, want 12 (no duplicate steps)", len(events))
	}
	// Every step ran exactly once, in order — and steps 4 & 5 (post-recovery) are present.
	if len(final.Steps) != 5 {
		t.Fatalf("step count: got %d, want 5", len(final.Steps))
	}
	for i, want := range killTestDef.Steps {
		step := final.Steps[i]
		if step.Name != want.Name || step.Sequence != int64(i+1) {
			t.Errorf("step %d: got %s/seq%d, want %s/seq%d", i, step.Name, step.Sequence, want.Name, i+1)
		}
		if step.Status != state.StepCompleted {
			t.Errorf("step %s: status %s, want COMPLETED", step.Name, step.Status)
		}
	}
}

// waitForFile reports whether path appears within d.
func waitForFile(path string, d time.Duration) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// waitCompleted polls the durable log until the workflow reaches a terminal state.
func waitCompleted(t *testing.T, e *Engine, wfID string, d time.Duration) *state.State {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		events, err := e.store.ReadAll(wfID)
		if err != nil {
			t.Fatalf("read history: %v", err)
		}
		st, err := state.Reconstruct(events)
		if err != nil {
			t.Fatalf("reconstruct: %v", err)
		}
		if st.Status.IsTerminal() {
			return st
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("workflow %s did not terminate within %s", wfID, d)
	return nil
}
