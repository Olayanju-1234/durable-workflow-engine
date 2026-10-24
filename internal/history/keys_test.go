package history

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestKeys(t *testing.T) {
	for _, c := range []struct {
		ns, id, key string
	}{
		{"", "order-1", "order-1"},
		{"default", "order-1", "order-1"},
		{"acme", "order-1", "acme/order-1"},
	} {
		if got := Key(c.ns, c.id); got != c.key {
			t.Errorf("Key(%q, %q) = %q, want %q", c.ns, c.id, got, c.key)
		}
		ns, id := SplitKey(c.key)
		if want := c.ns; (want == "" && ns != DefaultNamespace) || (want != "" && ns != want) || id != c.id {
			t.Errorf("SplitKey(%q) = %q, %q", c.key, ns, id)
		}
	}
	for _, bad := range []string{"", "default/x", "Acme/x", "acme/", "acme/x/y", "a_b/x", "-a/x", "acme/..", `acme/x\y`, "../x"} {
		if validateWorkflowID(bad) == nil {
			t.Errorf("key %q was accepted", bad)
		}
	}
	for _, good := range []string{"x", "acme/x", "a-1/x~y", "0/x"} {
		if err := validateWorkflowID(good); err != nil {
			t.Errorf("key %q: %v", good, err)
		}
	}
}

// A namespace's histories live in its own directory; listing, deleting and
// starting a new run all work there, and the directory's format is raised.
func TestNamespacedHistories(t *testing.T) {
	dir := t.TempDir()
	s, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	appendN(t, s, "order-1", 2)
	if s.Format() > FormatBase {
		t.Fatalf("format %d after a default-namespace history, want unchanged", s.Format())
	}
	appendN(t, s, "acme/order-1", 3)
	if s.Format() != FormatNamespaces {
		t.Fatalf("format %d after a namespaced history, want %d", s.Format(), FormatNamespaces)
	}
	if f, err := ReadFormat(dir); err != nil || f != FormatNamespaces {
		t.Fatalf("recorded format %d, %v", f, err)
	}
	if _, err := os.Stat(filepath.Join(dir, "namespaces", "acme", "order-1.history")); err != nil {
		t.Fatalf("acme's history is not in its directory: %v", err)
	}
	keys, err := s.ListWorkflows()
	if err != nil {
		t.Fatal(err)
	}
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"acme/order-1", "order-1"}) {
		t.Fatalf("keys = %v", keys)
	}
	if evs, _ := s.ReadAll("acme/order-1"); len(evs) != 3 {
		t.Fatalf("acme/order-1 has %d events, want 3", len(evs))
	}
	if evs, _ := s.ReadAll("order-1"); len(evs) != 2 {
		t.Fatalf("order-1 has %d events, want 2", len(evs))
	}

	if err := s.NewRun("acme/order-1", "__workflowd-run~elsewhere", started(1)); err == nil {
		t.Fatal("a namespaced run was archived outside its namespace")
	}
	if err := s.NewRun("acme/order-1", "acme/__workflowd-run~old", started(1)); err != nil {
		t.Fatal(err)
	}
	if evs, _ := s.ReadAll("acme/__workflowd-run~old"); len(evs) != 3 {
		t.Fatalf("archived run has %d events, want 3", len(evs))
	}
	if err := s.Delete("acme/order-1"); err != nil {
		t.Fatal(err)
	}
	if evs, _ := s.ReadAll("order-1"); len(evs) != 2 {
		t.Fatal("deleting acme's order-1 touched default's")
	}

	// A snapshot install keeps namespaces apart and removes what it lacks.
	raw, err := s.ReadRaw("order-1")
	if err != nil {
		t.Fatal(err)
	}
	if err := s.ReplaceAll(map[string][]byte{"beta/order-1": raw}); err != nil {
		t.Fatal(err)
	}
	keys, _ = s.ListWorkflows()
	if !slices.Equal(keys, []string{"beta/order-1"}) {
		t.Fatalf("after ReplaceAll: %v", keys)
	}
}

// The format is recorded before the namespace's directory or history exists:
// if it cannot be, nothing is written that an older binary would ignore.
func TestNamespaceFormatRaisedFirst(t *testing.T) {
	dir := t.TempDir()
	s, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// A directory in the temp file's place makes recording the format fail.
	if err := os.Mkdir(filepath.Join(dir, "FORMAT.tmp"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := s.Append("acme/x", started(1)); err == nil {
		t.Fatal("append succeeded without recording the format")
	}
	if _, err := os.Stat(filepath.Join(dir, "namespaces")); !os.IsNotExist(err) {
		t.Fatalf("the namespace directory exists though the format was not recorded: %v", err)
	}
}
