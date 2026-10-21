package server

import (
	"fmt"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

// seedHistories writes n finished five-step workflows straight to the store,
// one in ten still running, the shape of a busy data directory.
func seedHistories(b *testing.B, store *history.HistoryStore, n int) {
	b.Helper()
	now := time.Now().UnixNano()
	for i := range n {
		id := fmt.Sprintf("wf-%06d", i)
		evs := []*history.HistoryEvent{{Attributes: &history.HistoryEvent_WorkflowStarted{
			WorkflowStarted: &history.WorkflowStartedEvent{WorkflowId: id, WorkflowType: "Seed", Input: []byte("order")},
		}}}
		for seq := int64(1); seq <= 5; seq++ {
			evs = append(evs,
				&history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{StepScheduled: &history.StepScheduledEvent{StepName: fmt.Sprint("s", seq), Sequence: seq}}},
				&history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{StepCompleted: &history.StepCompletedEvent{StepName: fmt.Sprint("s", seq), Sequence: seq, Output: []byte("ok")}}})
		}
		if i%10 != 0 {
			evs = append(evs, &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowCompleted{WorkflowCompleted: &history.WorkflowCompletedEvent{Output: []byte("ok")}}})
		}
		for j, ev := range evs {
			ev.EventId, ev.Timestamp = int64(j+1), now
			if err := store.AppendBuffered(id, ev); err != nil {
				b.Fatal(err)
			}
		}
		if err := store.Release(id); err != nil {
			b.Fatal(err)
		}
	}
	if err := store.Sync(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkListWorkflows lists a data directory of 10k workflows, filtered to
// the running ones, the dashboard's most common query.
func BenchmarkListWorkflows(b *testing.B) {
	store, err := history.NewHistoryStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	seedHistories(b, store, 10_000)
	e := NewEngine(store, discardLogger())
	b.Cleanup(e.Close)
	if err := e.Recover(); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for range b.N {
		got, err := e.ListWorkflows(WorkflowStatus_WORKFLOW_STATUS_RUNNING)
		if err != nil || len(got) != 1000 {
			b.Fatalf("listed %d, %v; want 1000", len(got), err)
		}
	}
}

// BenchmarkRecover10k is the startup cost of a data directory of 10k workflows.
func BenchmarkRecover10k(b *testing.B) {
	store, err := history.NewHistoryStore(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { store.Close() })
	seedHistories(b, store, 10_000)
	b.ResetTimer()
	for range b.N {
		e := NewEngine(store, discardLogger())
		if err := e.Recover(); err != nil {
			b.Fatal(err)
		}
		e.Close()
	}
}
