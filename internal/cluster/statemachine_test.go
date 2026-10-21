package cluster

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/raft"
)

func newTestSM(t *testing.T) (*stateMachine, *history.HistoryStore) {
	t.Helper()
	dir := t.TempDir()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	return newStateMachine(store, dir, slog.New(slog.NewTextHandler(io.Discard, nil))), store
}

func appendEntry(t *testing.T, index uint64, id string, eventID int64) *raft.LogEntry {
	t.Helper()
	data, err := proto.Marshal(&Command{Command: &Command_Append{Append: &AppendEvent{
		WorkflowId: id, Event: &history.HistoryEvent{EventId: eventID},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	return &raft.LogEntry{Index: index, Term: 1, Data: data}
}

func deleteEntry(t *testing.T, index uint64, id string) *raft.LogEntry {
	t.Helper()
	data, _ := proto.Marshal(&Command{Command: &Command_Delete{Delete: &DeleteWorkflow{WorkflowId: id}}})
	return &raft.LogEntry{Index: index, Term: 1, Data: data}
}

func mustApply(t *testing.T, m *stateMachine, e *raft.LogEntry) error {
	t.Helper()
	res, err := m.Apply(e)
	if err != nil {
		t.Fatalf("apply %d: fatal %v", e.GetIndex(), err)
	}
	if res == nil {
		return nil
	}
	return res.(error)
}

func eventIDs(t *testing.T, store *history.HistoryStore, id string) (ids []int64, stamps []uint64) {
	t.Helper()
	evs, err := store.ReadAll(id)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range evs {
		ids = append(ids, e.GetEventId())
		stamps = append(stamps, e.GetRaftIndex())
	}
	return ids, stamps
}

func TestApplyStampsAndFences(t *testing.T) {
	m, store := newTestSM(t)
	for i, e := range []*raft.LogEntry{appendEntry(t, 3, "wf", 1), appendEntry(t, 5, "wf", 2)} {
		if err := mustApply(t, m, e); err != nil {
			t.Fatalf("entry %d: %v", i, err)
		}
	}
	// A second engine racing for position 2 is rejected, on every node alike.
	if err := mustApply(t, m, appendEntry(t, 6, "wf", 2)); !errors.Is(err, ErrConflict) {
		t.Fatalf("duplicate position: %v, want ErrConflict", err)
	}
	if err := mustApply(t, m, appendEntry(t, 7, "wf", 9)); !errors.Is(err, ErrConflict) {
		t.Fatalf("gap: %v, want ErrConflict", err)
	}
	ids, stamps := eventIDs(t, store, "wf")
	if len(ids) != 2 || stamps[0] != 3 || stamps[1] != 5 {
		t.Fatalf("history ids %v stamps %v, want [1 2] stamped [3 5]", ids, stamps)
	}
}

// After a restart the node re-applies its log over a history directory that
// already holds some of it. The stamps make that a no-op, even with a fresh
// cache (a new state machine over the same store).
func TestReapplyAfterRestartIsIdempotent(t *testing.T) {
	m, store := newTestSM(t)
	log := []*raft.LogEntry{appendEntry(t, 1, "wf", 1), appendEntry(t, 2, "wf", 2), appendEntry(t, 4, "wf", 3)}
	for _, e := range log {
		mustApply(t, m, e)
	}
	restarted := newStateMachine(store, m.raftDir, m.log)
	for _, e := range log {
		if err := mustApply(t, restarted, e); err != nil {
			t.Fatalf("re-apply %d: %v", e.GetIndex(), err)
		}
	}
	if ids, _ := eventIDs(t, store, "wf"); len(ids) != 3 {
		t.Fatalf("history has %d events after re-applying, want 3", len(ids))
	}
}

// Retention deletes a finished workflow; its id may be used again later. A
// replayed delete must not remove the newer workflow.
func TestDeleteThenReuseSurvivesReplay(t *testing.T) {
	m, store := newTestSM(t)
	log := []*raft.LogEntry{
		appendEntry(t, 1, "wf", 1), appendEntry(t, 2, "wf", 2),
		deleteEntry(t, 3, "wf"),
		appendEntry(t, 4, "wf", 1), // a new workflow with the same id
	}
	for _, e := range log {
		if err := mustApply(t, m, e); err != nil {
			t.Fatalf("apply %d: %v", e.GetIndex(), err)
		}
	}
	restarted := newStateMachine(store, m.raftDir, m.log)
	for _, e := range log {
		mustApply(t, restarted, e)
	}
	ids, stamps := eventIDs(t, store, "wf")
	if len(ids) != 1 || stamps[0] != 4 {
		t.Fatalf("after replay: ids %v stamps %v, want only the new workflow's event from index 4", ids, stamps)
	}
}

func TestSnapshotRestoreRoundTrip(t *testing.T) {
	src, _ := newTestSM(t)
	for _, e := range []*raft.LogEntry{appendEntry(t, 1, "a", 1), appendEntry(t, 2, "a", 2), appendEntry(t, 3, "b", 1)} {
		mustApply(t, src, e)
	}
	data, err := src.Snapshot()
	if err != nil {
		t.Fatal(err)
	}

	dst, dstStore := newTestSM(t)
	mustApply(t, dst, appendEntry(t, 1, "stale", 1)) // not in the snapshot: removed
	if err := dst.Restore(&raft.Snapshot{Index: 3, Term: 1, Data: data}); err != nil {
		t.Fatal(err)
	}
	if ids, _ := eventIDs(t, dstStore, "a"); len(ids) != 2 {
		t.Fatalf("a restored with %d events, want 2", len(ids))
	}
	if ids, _ := eventIDs(t, dstStore, "stale"); len(ids) != 0 {
		t.Fatal("a history missing from the snapshot survived the restore")
	}
	// Entries after the snapshot continue from it; entries it covers are no-ops.
	if err := mustApply(t, dst, appendEntry(t, 2, "a", 2)); err != nil {
		t.Fatalf("covered entry: %v", err)
	}
	if err := mustApply(t, dst, appendEntry(t, 4, "a", 3)); err != nil {
		t.Fatalf("entry after the snapshot: %v", err)
	}
}

// At startup the node restores its own snapshot. If the history directory is
// already past it (the marker says so), nothing is rewritten.
func TestRestoreSkippedWhenDirectoryIsAhead(t *testing.T) {
	m, store := newTestSM(t)
	mustApply(t, m, appendEntry(t, 1, "a", 1))
	data, _ := m.Snapshot() // marker = 1
	mustApply(t, m, appendEntry(t, 2, "a", 2))

	if err := m.Restore(&raft.Snapshot{Index: 1, Term: 1, Data: data}); err != nil {
		t.Fatal(err)
	}
	if ids, _ := eventIDs(t, store, "a"); len(ids) != 2 {
		t.Fatalf("restore rewound the directory to %d events, want 2", len(ids))
	}
	if have, _ := m.readMarker(); have != 1 {
		t.Fatalf("marker %d, want 1", have)
	}
}

// History files are written without fsync in cluster mode, so a crash can cut
// a file short. Replaying the log over the damaged directory writes the lost
// events back, in order, with the same stamps.
func TestReplayRestoresLostBufferedWrites(t *testing.T) {
	m, store := newTestSM(t)
	log := []*raft.LogEntry{appendEntry(t, 1, "wf", 1), appendEntry(t, 2, "wf", 2), appendEntry(t, 3, "wf", 3)}
	for _, e := range log {
		mustApply(t, m, e)
	}
	want, _ := store.ReadRaw("wf")

	// Simulate the crash: the last event never reached the disk.
	evs, _ := store.ReadAll("wf")
	store.Delete("wf")
	for _, ev := range evs[:2] {
		store.Append("wf", ev)
	}

	restarted := newStateMachine(store, m.raftDir, m.log)
	for _, e := range log {
		if err := mustApply(t, restarted, e); err != nil {
			t.Fatalf("replay %d: %v", e.GetIndex(), err)
		}
	}
	if got, _ := store.ReadRaw("wf"); !bytes.Equal(got, want) {
		t.Fatal("replay did not restore the history byte for byte")
	}
}
