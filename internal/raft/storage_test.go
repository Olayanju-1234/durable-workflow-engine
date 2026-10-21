package raft

import (
	"os"
	"path/filepath"
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"
)

func TestHardStateSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	s, err := OpenDiskStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.load()
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(st.hardState, &HardState{}) {
		t.Fatalf("fresh storage: got %v, want the zero state", st.hardState)
	}

	want := &HardState{CurrentTerm: 7, VotedFor: "n2"}
	if err := s.saveHardState(want); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenDiskStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err = reopened.load()
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(st.hardState, want) {
		t.Fatalf("after reopen: got %v, want %v", st.hardState, want)
	}
}

// The file is replaced by rename, so a crash mid-save leaves a stray temp file
// next to the intact old state. Loading must ignore it.
func TestHardStateIgnoresInterruptedSave(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenDiskStorage(dir)
	want := &HardState{CurrentTerm: 3, VotedFor: "n1"}
	if err := s.saveHardState(want); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, hardStateFile+".tmp"), []byte("torn"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := s.load()
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(st.hardState, want) {
		t.Fatalf("got %v, want %v", st.hardState, want)
	}
}

// A damaged hard state must stop the node: guessing a term or a vote could
// let it vote twice in one term.
func TestCorruptHardStateRefused(t *testing.T) {
	dir := t.TempDir()
	s, _ := OpenDiskStorage(dir)
	if err := s.saveHardState(&HardState{CurrentTerm: 3, VotedFor: "n1"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, hardStateFile)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := s.load(); err == nil {
		t.Fatal("loaded a corrupt hard state without error")
	}
}

func openLoaded(t *testing.T, dir string) (*DiskStorage, persistedState) {
	t.Helper()
	s, err := OpenDiskStorage(dir)
	if err != nil {
		t.Fatal(err)
	}
	st, err := s.load()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, st
}

func entryTerms(ents []*LogEntry) []uint64 {
	var out []uint64
	for _, e := range ents {
		out = append(out, e.GetTerm())
	}
	return out
}

func TestLogSurvivesReopenAndTruncatesConflicts(t *testing.T) {
	dir := t.TempDir()
	s, _ := openLoaded(t, dir)
	if err := s.appendEntries(logFromTerms("", []uint64{1, 1, 2, 2})); err != nil {
		t.Fatal(err)
	}
	// A follower replaces its conflicting suffix from index 3 on.
	if err := s.appendEntries([]*LogEntry{{Index: 3, Term: 3}, {Index: 4, Term: 3}, {Index: 5, Term: 3}}); err != nil {
		t.Fatal(err)
	}
	s.Close()

	_, st := openLoaded(t, dir)
	if got, want := entryTerms(st.entries), []uint64{1, 1, 3, 3, 3}; !slices.Equal(got, want) {
		t.Fatalf("after reopen: terms %v, want %v", got, want)
	}
}

// A crash mid-append leaves a partial record at the end of the log. The next
// open drops it and appends cleanly after the last complete entry.
func TestLogTornTailIsCut(t *testing.T) {
	dir := t.TempDir()
	s, _ := openLoaded(t, dir)
	if err := s.appendEntries(logFromTerms("", []uint64{1, 1})); err != nil {
		t.Fatal(err)
	}
	s.Close()
	f, err := os.OpenFile(filepath.Join(dir, logFile), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0, 0, 0, 9, 1, 2}) // half a header
	f.Close()

	s, st := openLoaded(t, dir)
	if len(st.entries) != 2 {
		t.Fatalf("got %d entries after a torn tail, want 2", len(st.entries))
	}
	if err := s.appendEntries([]*LogEntry{{Index: 3, Term: 1}}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	if _, st := openLoaded(t, dir); len(st.entries) != 3 {
		t.Fatalf("got %d entries after appending past a cut tail, want 3", len(st.entries))
	}
}

func TestLogRejectsGaps(t *testing.T) {
	s, _ := openLoaded(t, t.TempDir())
	if err := s.appendEntries(logFromTerms("", []uint64{1})); err != nil {
		t.Fatal(err)
	}
	if err := s.appendEntries([]*LogEntry{{Index: 3, Term: 1}}); err == nil {
		t.Fatal("appended index 3 after index 1")
	}
}

func TestSnapshotFileRoundTrip(t *testing.T) {
	dir := t.TempDir()
	s, _ := openLoaded(t, dir)
	want := &Snapshot{Index: 7, Term: 3, Data: []byte("state")}
	if err := s.saveSnapshot(want); err != nil {
		t.Fatal(err)
	}
	s.Close()
	_, st := openLoaded(t, dir)
	if got := st.snapshot; got == nil || got.Index != 7 || got.Term != 3 || string(got.Data) != "state" {
		t.Fatalf("loaded snapshot %+v, want %+v", got, want)
	}
}

// Compaction rewrites the log without the entries a snapshot covers, and the
// result reopens cleanly.
func TestCompactLogThenReopen(t *testing.T) {
	dir := t.TempDir()
	s, _ := openLoaded(t, dir)
	if err := s.appendEntries(logFromTerms("", []uint64{1, 1, 2, 2, 2, 3})); err != nil {
		t.Fatal(err)
	}
	if err := s.saveSnapshot(&Snapshot{Index: 4, Term: 2}); err != nil {
		t.Fatal(err)
	}
	if err := s.compactLog(4); err != nil {
		t.Fatal(err)
	}
	if err := s.appendEntries([]*LogEntry{{Index: 7, Term: 3}}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	_, st := openLoaded(t, dir)
	if got, want := entryTerms(st.entries), []uint64{2, 3, 3}; !slices.Equal(got, want) || st.entries[0].GetIndex() != 5 {
		t.Fatalf("after compaction: entries from %d with terms %v, want from 5 with %v", st.entries[0].GetIndex(), got, want)
	}
}

// A crash between saving a snapshot and compacting leaves the covered entries
// in the log; loading skips them.
func TestLoadSkipsEntriesCoveredBySnapshot(t *testing.T) {
	dir := t.TempDir()
	s, _ := openLoaded(t, dir)
	if err := s.appendEntries(logFromTerms("", []uint64{1, 1, 2, 2})); err != nil {
		t.Fatal(err)
	}
	if err := s.saveSnapshot(&Snapshot{Index: 2, Term: 1}); err != nil {
		t.Fatal(err)
	}
	s.Close() // crash before compactLog
	_, st := openLoaded(t, dir)
	if len(st.entries) != 2 || st.entries[0].GetIndex() != 3 {
		t.Fatalf("got %d entries from %d, want 2 from 3", len(st.entries), st.entries[0].GetIndex())
	}
}

// A crash between saving a leader's snapshot and resetting the log leaves a
// log that doesn't continue the snapshot. Loading discards all of it.
func TestLoadResetsLogThatDivergesFromSnapshot(t *testing.T) {
	dir := t.TempDir()
	s, _ := openLoaded(t, dir)
	if err := s.appendEntries(logFromTerms("", []uint64{1, 1, 1, 1})); err != nil {
		t.Fatal(err)
	}
	if err := s.saveSnapshot(&Snapshot{Index: 3, Term: 2}); err != nil { // term 2 at 3: not our entry
		t.Fatal(err)
	}
	s.Close()
	s, st := openLoaded(t, dir)
	if len(st.entries) != 0 {
		t.Fatalf("kept %d entries that don't follow the snapshot", len(st.entries))
	}
	if err := s.appendEntries([]*LogEntry{{Index: 4, Term: 2}}); err != nil {
		t.Fatalf("append after reset: %v", err)
	}
}
