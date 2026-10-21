package history

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
)

func appendN(t *testing.T, s *HistoryStore, id string, n int) {
	t.Helper()
	for i := 1; i <= n; i++ {
		if err := s.Append(id, &HistoryEvent{EventId: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
}

// ReadRaw then ReplaceAll copies histories byte for byte into another store,
// and removes histories the snapshot doesn't list.
func TestReadRawReplaceAllRoundTrip(t *testing.T) {
	src, _ := NewHistoryStore(t.TempDir())
	defer src.Close()
	appendN(t, src, "a", 3)
	appendN(t, src, "b", 1)

	dstDir := t.TempDir()
	dst, _ := NewHistoryStore(dstDir)
	defer dst.Close()
	appendN(t, dst, "a", 1)     // stale copy, replaced
	appendN(t, dst, "stray", 2) // not in the snapshot, removed

	files := map[string][]byte{}
	for _, id := range []string{"a", "b"} {
		raw, err := src.ReadRaw(id)
		if err != nil {
			t.Fatal(err)
		}
		files[id] = raw
	}
	if err := dst.ReplaceAll(files); err != nil {
		t.Fatal(err)
	}
	for id, want := range files {
		got, _ := os.ReadFile(filepath.Join(dstDir, id+historyExt))
		if !bytes.Equal(got, want) {
			t.Errorf("%s differs after ReplaceAll", id)
		}
	}
	if evs, _ := dst.ReadAll("stray"); len(evs) != 0 {
		t.Error("history missing from the snapshot was kept")
	}
	// The replaced file takes appends normally: the cached handle was dropped.
	if err := dst.Append("a", &HistoryEvent{EventId: 4}); err != nil {
		t.Fatal(err)
	}
	if evs, _ := dst.ReadAll("a"); len(evs) != 4 {
		t.Fatalf("a has %d events after ReplaceAll + append, want 4", len(evs))
	}
}

func TestReadRawOmitsTornTail(t *testing.T) {
	dir := t.TempDir()
	s, _ := NewHistoryStore(dir)
	defer s.Close()
	appendN(t, s, "a", 2)
	clean, _ := s.ReadRaw("a")
	f, _ := os.OpenFile(filepath.Join(dir, "a"+historyExt), os.O_APPEND|os.O_WRONLY, 0)
	f.Write([]byte{0, 0, 0, 7})
	f.Close()
	got, err := s.ReadRaw("a")
	if err != nil || !bytes.Equal(got, clean) {
		t.Fatalf("ReadRaw with a torn tail: %d bytes, err %v; want the %d clean bytes", len(got), err, len(clean))
	}
	if raw, _ := s.ReadRaw("missing"); raw != nil {
		t.Fatal("missing history should read as nil")
	}
}

// Buffered appends are readable at once; Sync makes them durable, including
// histories whose handles were released in between.
func TestAppendBufferedThenSync(t *testing.T) {
	s, _ := NewHistoryStore(t.TempDir())
	defer s.Close()
	for i := 1; i <= 3; i++ {
		if err := s.AppendBuffered("a", &HistoryEvent{EventId: int64(i)}); err != nil {
			t.Fatal(err)
		}
	}
	if err := s.AppendBuffered("b", &HistoryEvent{EventId: 1}); err != nil {
		t.Fatal(err)
	}
	if evs, _ := s.ReadAll("a"); len(evs) != 3 {
		t.Fatalf("buffered events not readable: got %d", len(evs))
	}
	if err := s.Release("b"); err != nil { // Sync must still reach b
		t.Fatal(err)
	}
	if !s.dirty["b"] {
		t.Fatal("releasing a buffered history forgot it still needs an fsync")
	}
	if err := s.Sync(); err != nil {
		t.Fatal(err)
	}
	if len(s.dirty) != 0 {
		t.Fatalf("still dirty after Sync: %v", s.dirty)
	}
}
