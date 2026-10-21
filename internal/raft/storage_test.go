package raft

import (
	"os"
	"path/filepath"
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
