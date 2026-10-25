package server

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// countdown runs one activity per run and continues as new until its input
// counts down to zero: each run's history stays a handful of events.
func countdown(ctx workflow.Context, in []byte) ([]byte, error) {
	n, _ := strconv.Atoi(string(in))
	if _, err := ctx.ExecuteActivity("tick", in); err != nil {
		return nil, err
	}
	if n > 0 {
		return nil, workflow.ContinueAsNew([]byte(strconv.Itoa(n - 1)))
	}
	return []byte("done after " + ctx.Info().ContinuedFromRunID), nil
}

// waitFinished waits, through the engine, for the workflow id's current run
// to end, and returns it.
func waitFinished(t *testing.T, e *Engine, id string, d time.Duration) *GetWorkflowResponse {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		resp, err := e.GetWorkflow(id)
		if err == nil && isTerminalStatus(resp.GetSummary().GetStatus()) {
			return resp
		}
		time.Sleep(5 * time.Millisecond)
	}
	resp, err := e.GetWorkflow(id)
	t.Fatalf("%s did not finish in %s: %v %v", id, d, resp.GetSummary(), err)
	return nil
}

func runWorker(t *testing.T, e *Engine) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { driveWorker(ctx, e); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
}

// A workflow that continues as new keeps its id, gets a new run with an
// empty history each time, and keeps every finished run for inspection.
func TestContinueAsNewStartsFreshRuns(t *testing.T) {
	dir := t.TempDir()
	e := timeoutEngine(t, newStore(t, dir), map[string]workflow.Func{"Countdown": countdown})
	runWorker(t, e)
	started, err := e.Start("Countdown", []byte("3"), "countdown-1")
	if err != nil {
		t.Fatal(err)
	}
	resp := waitFinished(t, e, "countdown-1", 5*time.Second)
	sum := resp.GetSummary()
	if sum.GetStatus() != WorkflowStatus_WORKFLOW_STATUS_COMPLETED || sum.GetHistoryLength() != 4 {
		t.Fatalf("final run: %v, want COMPLETED with a 4-event history", sum)
	}
	if !strings.HasPrefix(string(resp.GetOutput()), "done after ") || resp.GetContinuedFromRunId() == "" ||
		string(resp.GetOutput()) != "done after "+resp.GetContinuedFromRunId() {
		t.Fatalf("output %q continued from %q", resp.GetOutput(), resp.GetContinuedFromRunId())
	}

	runs, err := e.ListWorkflowRuns("countdown-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 4 || !runs[0].GetCurrent() || runs[0].GetRunId() != sum.GetRunId() {
		t.Fatalf("runs %v, want the current run then 3 earlier ones", runs)
	}
	seen := map[string]bool{}
	for i, r := range runs {
		if seen[r.GetRunId()] {
			t.Fatalf("run id %s used twice", r.GetRunId())
		}
		seen[r.GetRunId()] = true
		if i > 0 && (r.GetStatus() != WorkflowStatus_WORKFLOW_STATUS_CONTINUED_AS_NEW || r.GetClosedAt() == 0) {
			t.Fatalf("earlier run %d: %v", i, r)
		}
	}
	if runs[3].GetRunId() != started.RunID {
		t.Fatalf("oldest run %s, want the one Start returned (%s)", runs[3].GetRunId(), started.RunID)
	}
	// Each earlier run names its successor, and can be read in full.
	for i := 1; i < len(runs); i++ {
		old, err := e.GetWorkflowRun("countdown-1", runs[i].GetRunId())
		if err != nil {
			t.Fatal(err)
		}
		if old.GetContinuedAsNewRunId() != runs[i-1].GetRunId() || old.GetSummary().GetRunId() != runs[i].GetRunId() {
			t.Fatalf("run %s continued as %s, want %s", runs[i].GetRunId(), old.GetContinuedAsNewRunId(), runs[i-1].GetRunId())
		}
	}
	// Archived runs are not workflows: the list shows one entry for the id.
	all, err := e.ListWorkflows(WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED)
	if err != nil || len(all) != 1 {
		t.Fatalf("ListWorkflows: %v %v, want just countdown-1", all, err)
	}
	if f, _ := history.ReadFormat(dir); f != history.FormatRuns {
		t.Fatalf("recorded format %d, want %d once a run continued as new", f, history.FormatRuns)
	}
}

// writeRaw writes events exactly as given, the state a crash leaves behind.
func writeRaw(t *testing.T, dir, id string, evs ...*history.HistoryEvent) {
	t.Helper()
	s := newStore(t, dir)
	for i, ev := range evs {
		ev.EventId, ev.Timestamp = int64(i+1), time.Now().UnixNano()
		if err := s.Append(id, ev); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
}

func continuedHistory(id string) []*history.HistoryEvent {
	return []*history.HistoryEvent{
		{Attributes: &history.HistoryEvent_WorkflowStarted{WorkflowStarted: &history.WorkflowStartedEvent{
			WorkflowId: id, WorkflowType: "Countdown", Input: []byte("1"), RunId: "run-one",
		}}},
		{Attributes: &history.HistoryEvent_WorkflowCompleted{WorkflowCompleted: &history.WorkflowCompletedEvent{
			ContinuedAsNew: &history.ContinuedAsNew{RunId: "run-two", WorkflowType: "Countdown", Input: []byte("0")},
		}}, MinReaderFormat: history.FormatRuns},
	}
}

// A crash between ending a run and starting the next leaves the finished run
// current; recovery starts the run it names. Also after a crash that kept
// the old run under its archive name but had not replaced it yet.
func TestRecoveryFinishesInterruptedContinueAsNew(t *testing.T) {
	for _, linked := range []bool{false, true} {
		t.Run(fmt.Sprintf("archive linked %v", linked), func(t *testing.T) {
			dir := t.TempDir()
			writeRaw(t, dir, "cd", continuedHistory("cd")...)
			if linked {
				s := newStore(t, dir)
				stored, _ := s.ReadAll("cd")
				s.Close()
				name := archiveID("cd", "run-one", stored[1].GetTimestamp())
				if err := os.Link(filepath.Join(dir, "cd.history"), filepath.Join(dir, name+".history")); err != nil {
					t.Fatal(err)
				}
			}
			e := timeoutEngine(t, newStore(t, dir), map[string]workflow.Func{"Countdown": countdown})
			if err := e.Recover(); err != nil {
				t.Fatal(err)
			}
			runWorker(t, e)
			resp := waitFinished(t, e, "cd", 5*time.Second)
			if string(resp.GetOutput()) != "done after run-one" || resp.GetSummary().GetRunId() != "run-two" {
				t.Fatalf("recovered run: %v output %q", resp.GetSummary(), resp.GetOutput())
			}
			runs, err := e.ListWorkflowRuns("cd")
			if err != nil || len(runs) != 2 || runs[1].GetRunId() != "run-one" {
				t.Fatalf("runs %v %v", runs, err)
			}
		})
	}
}

// A timer left over from a run that continued as new must not fire into the
// next run, even at the same step with the same name.
func TestTimerFromEarlierRunDoesNotFireIntoTheNext(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Twice": func(ctx workflow.Context, in []byte) ([]byte, error) {
			if string(in) == "first" {
				ctx.NewTimer("t", 150*time.Millisecond) // abandoned by the continue-as-new below
				if _, err := ctx.ExecuteActivity("a", nil); err != nil {
					return nil, err
				}
				return nil, workflow.ContinueAsNew([]byte("second"))
			}
			ctx.Sleep("t", time.Hour)
			return []byte("woke early"), nil
		},
	})
	if _, err := e.Start("Twice", []byte("first"), "twice"); err != nil {
		t.Fatal(err)
	}
	task := mustPoll(t, e, "w", time.Second)
	if err := e.CompleteTask("twice", "a", task.GetSequence(), task.GetLeaseToken(), nil, "", false); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond) // past the first run's timer
	resp, err := e.GetWorkflow("twice")
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_WAITING || resp.GetContinuedFromRunId() == "" {
		t.Fatalf("second run %v: the first run's timer fired into it", resp.GetSummary())
	}
}

// At its event limit a workflow fails, saying why, instead of growing; past
// the warning share, code that checks ContinueAsNewSuggested keeps going.
func TestHistoryEventLimit(t *testing.T) {
	looping := func(continueWhenSuggested bool) workflow.Func {
		return func(ctx workflow.Context, in []byte) ([]byte, error) {
			for i := 0; ; i++ {
				if continueWhenSuggested && ctx.ContinueAsNewSuggested() {
					return nil, workflow.ContinueAsNew(in)
				}
				if _, err := ctx.ExecuteActivity("tick", nil); err != nil {
					return nil, err
				}
			}
		}
	}
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Grows": looping(false), "Continues": looping(true),
	}, WithLimits(Limits{MaxHistoryEvents: 20}))
	runWorker(t, e)

	if _, err := e.Start("Grows", nil, "grows"); err != nil {
		t.Fatal(err)
	}
	resp := waitFinished(t, e, "grows", 5*time.Second)
	sum := resp.GetSummary()
	if sum.GetStatus() != WorkflowStatus_WORKFLOW_STATUS_FAILED || sum.GetHistoryLength() > 20 ||
		!strings.Contains(sum.GetError(), "history limit reached: 19 events (limit 20)") ||
		!strings.Contains(sum.GetError(), "continue as new") {
		t.Fatalf("at the limit: %v", sum)
	}
	if got := e.metrics.historyLimit.With("failed").Value(); got != 1 {
		t.Fatalf("history_limit_total{failed} = %v", got)
	}

	if _, err := e.Start("Continues", nil, "continues"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		runs, err := e.ListWorkflowRuns("continues")
		if err == nil && len(runs) >= 3 {
			if runs[0].GetStatus() == WorkflowStatus_WORKFLOW_STATUS_FAILED {
				t.Fatalf("a workflow that continues as new when told failed: %v", runs[0])
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("never continued as new: %v %v", runs, err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := e.metrics.historyLimit.With("warned").Value(); got < 3 {
		t.Fatalf("history_limit_total{warned} = %v, want one per run that crossed the warning share", got)
	}
}

// The byte limit holds too, counted as stored.
func TestHistoryByteLimit(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Big": func(ctx workflow.Context, in []byte) ([]byte, error) {
			for {
				if _, err := ctx.ExecuteActivity("big", nil); err != nil {
					return nil, err
				}
			}
		},
	}, WithLimits(Limits{MaxHistoryBytes: 16 << 10, MaxPayloadBytes: 4 << 10}))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for {
			task, err := e.PollTask(ctx, "w")
			if err != nil {
				return
			}
			_ = e.CompleteTask(task.GetWorkflowId(), task.GetStepName(), task.GetSequence(), task.GetLeaseToken(),
				make([]byte, 1000), "", false)
		}
	}()
	if _, err := e.Start("Big", nil, "big"); err != nil {
		t.Fatal(err)
	}
	sum := waitFinished(t, e, "big", 5*time.Second).GetSummary()
	if sum.GetStatus() != WorkflowStatus_WORKFLOW_STATUS_FAILED || !strings.Contains(sum.GetError(), "bytes (limit 16384)") ||
		sum.GetHistorySizeBytes() > 16<<10+1024 {
		t.Fatalf("at the byte limit: %v", sum)
	}
}

// Payloads over the limit are refused at the door, and ones produced inside
// (an activity's output, the workflow's) fail clearly instead of being written.
func TestPayloadLimit(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{
		"Echo": func(ctx workflow.Context, in []byte) ([]byte, error) {
			out, err := ctx.ExecuteActivity("produce", in)
			if err != nil {
				var ae *workflow.ActivityError
				if errors.As(err, &ae) {
					return []byte(ae.Type + " " + strconv.FormatBool(ae.NonRetryable)), nil
				}
				return nil, err
			}
			return append(out, out...), nil // twice the size: over the limit
		},
	}, WithLimits(Limits{MaxPayloadBytes: 100}))
	big := make([]byte, 101)
	if _, err := e.Start("Echo", big, ""); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("start with a big input: %v", err)
	}

	id, err := e.StartWorkflow("Echo", []byte("x"), "too-big-output")
	if err != nil {
		t.Fatal(err)
	}
	task := mustPoll(t, e, "w", time.Second)
	if err := e.CompleteTask(id, "produce", task.GetSequence(), task.GetLeaseToken(), big, "", false); err != nil {
		t.Fatal(err)
	}
	if got := string(waitFinished(t, e, id, 2*time.Second).GetOutput()); got != ErrorTypePayloadTooLarge+" true" {
		t.Fatalf("activity output over the limit: workflow saw %q", got)
	}

	id, err = e.StartWorkflow("Echo", []byte("x"), "too-big-result")
	if err != nil {
		t.Fatal(err)
	}
	task = mustPoll(t, e, "w", time.Second)
	if err := e.CompleteTask(id, "produce", task.GetSequence(), task.GetLeaseToken(), make([]byte, 60), "", false); err != nil {
		t.Fatal(err)
	}
	sum := waitFinished(t, e, id, 2*time.Second).GetSummary()
	if sum.GetStatus() != WorkflowStatus_WORKFLOW_STATUS_FAILED || !strings.Contains(sum.GetError(), "workflow output is 120 bytes") {
		t.Fatalf("workflow output over the limit: %v", sum)
	}

	if err := e.SignalWorkflow("too-big-result", "s", big); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("signal to a finished workflow: %v", err)
	}
	id, _ = e.StartWorkflow("Echo", []byte("x"), "")
	if err := e.SignalWorkflow(id, "s", big); !errors.Is(err, ErrPayloadTooLarge) {
		t.Fatalf("big signal: %v", err)
	}
}

// Retention removes archived runs past the cutoff, and now cancelled
// workflows too; a run that continued as new but whose next run has not
// started (a crash in between) is never purged.
func TestRetentionPurgesArchivedRuns(t *testing.T) {
	dir := t.TempDir()
	e := timeoutEngine(t, newStore(t, dir), map[string]workflow.Func{"Countdown": countdown, "Slow": waitsForever})
	runWorker(t, e)
	if _, err := e.Start("Countdown", []byte("2"), "cd"); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, e, "cd", 5*time.Second)
	// On a queue no worker polls: the test's worker takes any task on the default one.
	id, _ := e.StartWorkflow("Slow", nil, "", WithTaskQueue("nobody-polls-this"))
	if err := e.CancelWorkflow(id, ""); err != nil {
		t.Fatal(err)
	}
	n, err := e.PurgeTerminated(time.Now().Add(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if n != 4 { // two archived runs, the finished current run, the cancelled workflow
		t.Fatalf("purged %d, want 4", n)
	}
	ids, _ := e.store.ListWorkflows()
	if len(ids) != 0 {
		t.Fatalf("left behind: %v", ids)
	}

	dir2 := t.TempDir()
	writeRaw(t, dir2, "mid", continuedHistory("mid")...)
	e2 := timeoutEngine(t, newStore(t, dir2), nil)
	if n, err := e2.PurgeTerminated(time.Now().Add(time.Hour)); err != nil || n != 0 {
		t.Fatalf("purged %d (%v) from a directory whose only run is mid continue-as-new", n, err)
	}
}

// Recovery reads the run id of a history from before run ids from its start
// time, the same on every read, and the state carries it.
func TestLegacyRunID(t *testing.T) {
	st, err := state.Reconstruct([]*history.HistoryEvent{{EventId: 1, Timestamp: 0x1234, Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{WorkflowType: "T"},
	}}})
	if err != nil || st.RunID != "0000000000001234" {
		t.Fatalf("legacy run id %q (%v)", st.RunID, err)
	}
	id := archiveID("order~7", "abc", 255)
	wf, run, at, ok := parseArchiveID(id)
	if !ok || wf != "order~7" || run != "abc" || at != 255 || !isReservedID(id) {
		t.Fatalf("archive id %q parsed as %q %q %d %v", id, wf, run, at, ok)
	}
}
