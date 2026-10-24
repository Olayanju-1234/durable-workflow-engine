package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// scanSummaries is what ListWorkflows returned before the index: read and
// replay every history file.
func scanSummaries(t *testing.T, store *history.HistoryStore) []*WorkflowSummary {
	t.Helper()
	ids, err := store.ListWorkflows()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(ids)
	var out []*WorkflowSummary
	for _, id := range ids {
		events, err := store.ReadAll(id)
		if err != nil {
			t.Fatal(err)
		}
		st, err := state.Reconstruct(events)
		if err != nil {
			t.Fatal(err)
		}
		var bytes int64
		for _, ev := range events {
			bytes += eventSize(ev)
		}
		out = append(out, summaryFrom(id, events, st, bytes))
	}
	return out
}

func sameSummaries(t *testing.T, got, want []*WorkflowSummary) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("index lists %d workflows, the files hold %d", len(got), len(want))
	}
	for i := range want {
		if !proto.Equal(got[i], want[i]) {
			t.Fatalf("workflow %d: index %v, files %v", i, got[i], want[i])
		}
	}
}

// TestIndexMatchesScan: after concurrent starts, steps, signals, terminations
// and purges, the index lists exactly what a full scan of the files would.
func TestIndexMatchesScan(t *testing.T) {
	e, store := newLockingEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runEngineWorker(ctx, e, func(string, []byte) ([]byte, error) { return []byte("ok"), nil })

	var wg sync.WaitGroup
	for g := range 6 {
		wg.Go(func() {
			for i := range 8 {
				id, err := e.StartWorkflow("Parked", nil, fmt.Sprintf("g%d-%d", g, i))
				if err != nil {
					t.Error(err)
					return
				}
				_ = e.SignalWorkflow(id, "noise", nil)
				if i%3 == 0 {
					_ = e.TerminateWorkflow(id, "test")
				}
			}
		})
	}
	wg.Wait()
	if _, err := e.PurgeTerminated(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	// Let every surviving workflow reach its signal wait, so the files stop changing.
	for g := range 6 {
		for i := range 8 {
			if i%3 != 0 {
				waitForSteps(t, e, fmt.Sprintf("g%d-%d", g, i), 2)
			}
		}
	}
	got, err := e.ListWorkflows(WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED)
	if err != nil {
		t.Fatal(err)
	}
	sameSummaries(t, got, scanSummaries(t, store))
	if len(got) != 6*8-6*3 {
		t.Fatalf("listed %d, want %d (terminated ones purged)", len(got), 6*8-6*3)
	}
}

// TestIndexBuiltByRecoverNotByListing: a restarted engine lists from the
// index its recovery built, without reading history files again.
func TestIndexBuiltByRecoverNotByListing(t *testing.T) {
	dir := t.TempDir()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e1 := NewEngine(store, discardLogger())
	e1.Register(parked)
	for i := range 5 {
		if _, err := e1.StartWorkflow("Parked", nil, fmt.Sprint("wf-", i)); err != nil {
			t.Fatal(err)
		}
	}
	e1.Close()

	e2 := NewEngine(store, discardLogger())
	t.Cleanup(e2.Close)
	e2.Register(parked)
	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}
	want := scanSummaries(t, store)
	// Remove a file behind the engine's back: a listing that scanned the
	// directory would lose it, the index does not.
	if err := os.Remove(filepath.Join(dir, "wf-0.history")); err != nil {
		t.Fatal(err)
	}
	got, err := e2.ListWorkflows(WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED)
	if err != nil {
		t.Fatal(err)
	}
	sameSummaries(t, got, want)

	// An engine that never ran Recover builds the index on first use.
	e3 := NewEngine(store, discardLogger())
	t.Cleanup(e3.Close)
	got, err = e3.ListWorkflows(WorkflowStatus_WORKFLOW_STATUS_WAITING)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("lazy index lists %d waiting workflows, want 4", len(got))
	}
}

// TestIndexAfterFailedAppend: a write that landed but reported failure is
// re-read before the next listing, so the index never shows a stale length.
func TestIndexAfterFailedAppend(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	log := &gatedLog{store: store}
	e := NewEngine(store, discardLogger(), WithHistoryLog(log))
	t.Cleanup(e.Close)
	e.Register(parked)
	id, err := e.StartWorkflow("Parked", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := e.ListWorkflows(0); err != nil {
		t.Fatal(err)
	}
	log.failAt.Store(3)
	if err := e.SignalWorkflow(id, "noise", nil); err == nil {
		t.Fatal("injected failure not reported")
	}
	got, err := e.ListWorkflows(0)
	if err != nil {
		t.Fatal(err)
	}
	sameSummaries(t, got, scanSummaries(t, store))
}

func TestReservedIDsAreNotWorkflows(t *testing.T) {
	e, _ := newLockingEngine(t)
	if _, err := e.StartWorkflow("Parked", nil, reservedPrefix+"-x"); err == nil {
		t.Fatal("started a workflow under a reserved id")
	}
}

// waitForSteps waits through the engine (so under the workflow's lock, after
// any operation in flight) until the workflow has n steps.
func waitForSteps(t *testing.T, e *Engine, id string, n int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if wf, err := e.GetWorkflow(id); err == nil && len(wf.GetSteps()) == n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("%s did not reach %d steps", id, n)
}
