package history

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
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

// TestReleaseThenAppend proves Release closes the cached handle without
// touching the log, and that a later Append transparently reopens it.
func TestReleaseThenAppend(t *testing.T) {
	s, err := NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	events := sampleEvents()
	if err := s.Append("wf-1", events[0]); err != nil {
		t.Fatalf("append: %v", err)
	}
	if n := s.OpenHandles(); n != 1 {
		t.Fatalf("open handles after append = %d, want 1", n)
	}
	if err := s.Release("wf-1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if n := s.OpenHandles(); n != 0 {
		t.Fatalf("open handles after release = %d, want 0", n)
	}
	if err := s.Release("wf-1"); err != nil {
		t.Errorf("second release: %v", err)
	}
	if err := s.Release("never-opened"); err != nil {
		t.Errorf("release of unknown workflow: %v", err)
	}

	if err := s.Append("wf-1", events[1]); err != nil {
		t.Fatalf("append after release: %v", err)
	}
	got, err := s.ReadAll("wf-1")
	if err != nil {
		t.Fatalf("read all: %v", err)
	}
	if len(got) != 2 || !proto.Equal(events[0], got[0]) || !proto.Equal(events[1], got[1]) {
		t.Fatalf("history after release+append = %v, want first two sample events", got)
	}
	if n := s.OpenHandles(); n != 1 {
		t.Errorf("open handles after reopen = %d, want 1", n)
	}
}

// TestAppendAfterFailedWriteRecovers covers a write that fails on a live store.
// The failed Append must drop its cached handle, so the retry reopens the file,
// truncates the partial record and lands on a clean boundary, not after garbage.
func TestAppendAfterFailedWriteRecovers(t *testing.T) {
	tests := []struct {
		name    string
		breakIt func(t *testing.T, s *HistoryStore, path string)
		wantErr error
	}{
		{
			// What ENOSPC or EIO mid-record leaves behind: half a record on disk.
			name: "partial write then ENOSPC",
			breakIt: func(t *testing.T, s *HistoryStore, _ string) {
				s.writeFile = func(f *os.File, b []byte) (int, error) {
					n, err := f.Write(b[:len(b)/2])
					if err != nil {
						return n, err
					}
					return n, syscall.ENOSPC
				}
			},
			wantErr: syscall.ENOSPC,
		},
		{
			name: "torn bytes and a dead descriptor",
			breakIt: func(t *testing.T, s *HistoryStore, path string) {
				f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
				if err != nil {
					t.Fatalf("open for torn write: %v", err)
				}
				if _, err := f.Write([]byte{0xDE, 0xAD, 0xBE}); err != nil {
					t.Fatalf("torn write: %v", err)
				}
				f.Close()
				for _, cached := range s.files {
					cached.Close()
				}
			},
			wantErr: os.ErrClosed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			const wfID = "wf-failed-write"
			s, err := NewHistoryStore(dir)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			events := sampleEvents()
			if err := s.Append(wfID, events[0]); err != nil {
				t.Fatalf("append ev1: %v", err)
			}

			tc.breakIt(t, s, filepath.Join(dir, wfID+historyExt))
			if err := s.Append(wfID, events[1]); !errors.Is(err, tc.wantErr) {
				t.Fatalf("failed append: got %v, want %v", err, tc.wantErr)
			}
			s.writeFile = (*os.File).Write
			if n := s.OpenHandles(); n != 0 {
				t.Errorf("open handles after failed append = %d, want 0", n)
			}

			if err := s.Append(wfID, events[1]); err != nil {
				t.Fatalf("retry append: %v", err)
			}
			got, err := s.ReadAll(wfID)
			if err != nil {
				t.Fatalf("read all after retry: %v", err)
			}
			if len(got) != 2 || !proto.Equal(events[0], got[0]) || !proto.Equal(events[1], got[1]) {
				t.Fatalf("history after retry = %v, want first two sample events", got)
			}
		})
	}
}

// TestDelete proves a deleted history is gone from reads and listings, that the
// cached append handle is released (a later append starts a fresh log), and
// that deleting a missing workflow is not an error.
func TestDelete(t *testing.T) {
	s, err := NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	for _, ev := range sampleEvents() {
		if err := s.Append("wf-1", ev); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if err := s.Delete("wf-1"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got, err := s.ReadAll("wf-1"); err != nil || len(got) != 0 {
		t.Fatalf("ReadAll after delete = %d events, %v; want 0, nil", len(got), err)
	}
	if ids, _ := s.ListWorkflows(); len(ids) != 0 {
		t.Fatalf("ListWorkflows after delete = %v, want none", ids)
	}

	if err := s.Append("wf-1", sampleEvents()[0]); err != nil {
		t.Fatalf("append after delete: %v", err)
	}
	if got, _ := s.ReadAll("wf-1"); len(got) != 1 {
		t.Fatalf("history after re-append = %d events, want 1", len(got))
	}

	if err := s.Delete("never-existed"); err != nil {
		t.Errorf("delete missing: %v", err)
	}
	if err := s.Delete("../escape"); err == nil {
		t.Error("delete with path separator: want error")
	}
}
