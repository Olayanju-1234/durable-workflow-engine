package history

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"
)

// sampleEvents builds 10 events spanning the event types, in a realistic order.
func sampleEvents() []*HistoryEvent {
	const wfType = "OrderWorkflow"
	base := int64(1_700_000_000_000_000_000)
	mk := func(id int64, attrs isHistoryEvent_Attributes) *HistoryEvent {
		return &HistoryEvent{EventId: id, Timestamp: base + id, Attributes: attrs}
	}
	return []*HistoryEvent{
		mk(1, &HistoryEvent_WorkflowStarted{WorkflowStarted: &WorkflowStartedEvent{WorkflowId: "wf-1", WorkflowType: wfType, Input: []byte(`{"order":42}`)}}),
		mk(2, &HistoryEvent_StepScheduled{StepScheduled: &StepScheduledEvent{StepName: "reserve", Input: []byte("a"), Sequence: 1}}),
		mk(3, &HistoryEvent_StepCompleted{StepCompleted: &StepCompletedEvent{StepName: "reserve", Output: []byte("ok"), Sequence: 1}}),
		mk(4, &HistoryEvent_StepScheduled{StepScheduled: &StepScheduledEvent{StepName: "charge", Input: []byte("b"), Sequence: 2}}),
		mk(5, &HistoryEvent_StepFailed{StepFailed: &StepFailedEvent{StepName: "charge", Error: "card declined", Sequence: 2, RetryCount: 1}}),
		mk(6, &HistoryEvent_StepScheduled{StepScheduled: &StepScheduledEvent{StepName: "charge", Input: []byte("b"), Sequence: 3}}),
		mk(7, &HistoryEvent_StepCompleted{StepCompleted: &StepCompletedEvent{StepName: "charge", Output: []byte("paid"), Sequence: 3}}),
		mk(8, &HistoryEvent_StepScheduled{StepScheduled: &StepScheduledEvent{StepName: "ship", Sequence: 4}}),
		mk(9, &HistoryEvent_StepCompleted{StepCompleted: &StepCompletedEvent{StepName: "ship", Output: []byte("tracking-123"), Sequence: 4}}),
		mk(10, &HistoryEvent_WorkflowCompleted{WorkflowCompleted: &WorkflowCompletedEvent{Output: []byte(`{"status":"done"}`)}}),
	}
}

// TestAppendReadAllSurvivesReopen is the Phase 1 deliverable: append 10 events,
// close the store, reopen from disk, and verify every event round-trips. This is
// the first proof that workflow state survives a process restart.
func TestAppendReadAllSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	const wfID = "wf-restart"
	want := sampleEvents()

	store, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	for i, ev := range want {
		if err := store.Append(wfID, ev); err != nil {
			t.Fatalf("append %d: %v", i, err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Reopen from scratch — this is exactly what happens after a crash/restart.
	reopened, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer reopened.Close()

	got, err := reopened.ReadAll(wfID)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("event count: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if !proto.Equal(want[i], got[i]) {
			t.Errorf("event %d mismatch:\n want %v\n  got %v", i, want[i], got[i])
		}
	}
}

// TestAppendAfterTornTailRecovers simulates a crash mid-append (a few stray
// bytes at the tail), then reopens and appends a fresh event. The store must
// truncate the torn bytes so the new record lands on a clean boundary and the
// whole log still reads back. This is the durability property Week 4 crash
// recovery is built on: without tail truncation the second append lands after
// the garbage and mis-frames every subsequent read.
func TestAppendAfterTornTailRecovers(t *testing.T) {
	dir := t.TempDir()
	const wfID = "wf-torn"

	store, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ev1 := &HistoryEvent{EventId: 1, Attributes: &HistoryEvent_WorkflowStarted{
		WorkflowStarted: &WorkflowStartedEvent{WorkflowId: wfID, WorkflowType: "T"},
	}}
	if err := store.Append(wfID, ev1); err != nil {
		t.Fatalf("append ev1: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Simulate a crash mid-append: a partial record header at the tail.
	path := filepath.Join(dir, wfID+".history")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatalf("open for torn write: %v", err)
	}
	if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE}); err != nil {
		t.Fatalf("torn write: %v", err)
	}
	f.Close()

	reopened, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer reopened.Close()

	ev2 := &HistoryEvent{EventId: 2, Attributes: &HistoryEvent_WorkflowCompleted{
		WorkflowCompleted: &WorkflowCompletedEvent{Output: []byte("ok")},
	}}
	if err := reopened.Append(wfID, ev2); err != nil {
		t.Fatalf("append ev2 after torn tail: %v", err)
	}

	got, err := reopened.ReadAll(wfID)
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("event count: got %d, want 2", len(got))
	}
	if !proto.Equal(ev1, got[0]) {
		t.Errorf("event 0 mismatch:\n want %v\n  got %v", ev1, got[0])
	}
	if !proto.Equal(ev2, got[1]) {
		t.Errorf("event 1 mismatch:\n want %v\n  got %v", ev2, got[1])
	}
}

// TestListWorkflows proves recovery can discover every workflow on disk and
// nothing else: it returns exactly the ids that have a .history file, ignoring
// unrelated files, and yields an empty result for a fresh data dir.
func TestListWorkflows(t *testing.T) {
	dir := t.TempDir()
	store, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	// A brand-new store has no workflows yet.
	if got, err := store.ListWorkflows(); err != nil || len(got) != 0 {
		t.Fatalf("empty store: got %v (err %v), want none", got, err)
	}

	want := map[string]bool{"wf-a": true, "wf-b": true, "wf-c": true}
	for id := range want {
		ev := &HistoryEvent{EventId: 1, Attributes: &HistoryEvent_WorkflowStarted{
			WorkflowStarted: &WorkflowStartedEvent{WorkflowId: id, WorkflowType: "T"},
		}}
		if err := store.Append(id, ev); err != nil {
			t.Fatalf("append %s: %v", id, err)
		}
	}
	// A non-history file in the data dir must be ignored, not mistaken for a workflow.
	if err := os.WriteFile(filepath.Join(dir, "README.txt"), []byte("x"), 0o644); err != nil {
		t.Fatalf("write stray file: %v", err)
	}

	got, err := store.ListWorkflows()
	if err != nil {
		t.Fatalf("list workflows: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("count: got %d (%v), want %d", len(got), got, len(want))
	}
	for _, id := range got {
		if !want[id] {
			t.Errorf("unexpected workflow id %q", id)
		}
	}
}

func TestReadAllMissingWorkflow(t *testing.T) {
	store, err := NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	defer store.Close()

	got, err := store.ReadAll("does-not-exist")
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected no events, got %d", len(got))
	}
}

// TestCorruptRecordDetected proves the CRC check rejects a flipped on-disk byte.
func TestCorruptRecordDetected(t *testing.T) {
	dir := t.TempDir()
	const wfID = "wf-corrupt"

	store, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	ev := &HistoryEvent{EventId: 1, Attributes: &HistoryEvent_WorkflowStarted{
		WorkflowStarted: &WorkflowStartedEvent{WorkflowId: "x", WorkflowType: "T"},
	}}
	if err := store.Append(wfID, ev); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Flip the first payload byte to simulate bit rot / a torn write.
	path := filepath.Join(dir, wfID+".history")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}
	data[headerSize] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write file: %v", err)
	}

	store2, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store2.Close()

	if _, err := store2.ReadAll(wfID); !errors.Is(err, ErrCorruptRecord) {
		t.Fatalf("expected ErrCorruptRecord, got %v", err)
	}
}
