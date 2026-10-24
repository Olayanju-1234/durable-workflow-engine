package history

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func backupOf(t *testing.T, files map[string][]byte) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, data := range files {
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o644, Size: int64(len(data)), Typeflag: tar.TypeReg}); err != nil {
			t.Fatal(err)
		}
		tw.Write(data)
	}
	tw.Close()
	gz.Close()
	return &buf
}

// A backup restores byte for byte, namespaces and format included.
func TestBackupRestoreRoundTrip(t *testing.T) {
	src, err := NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	appendN(t, src, "a", 3)
	appendN(t, src, "acme/b", 2)
	var buf bytes.Buffer
	if err := src.Backup(&buf); err != nil {
		t.Fatal(err)
	}

	dir := filepath.Join(t.TempDir(), "restored")
	sum, err := Restore(dir, bytes.NewReader(buf.Bytes()))
	if err != nil || sum.Histories != 2 || sum.Format != FormatNamespaces {
		t.Fatalf("restore = %+v, %v", sum, err)
	}
	dst, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer dst.Close()
	keys, _ := dst.ListWorkflows()
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"a", "acme/b"}) {
		t.Fatalf("restored keys %v", keys)
	}
	for _, k := range keys {
		want, _ := src.ReadRaw(k)
		got, _ := dst.ReadRaw(k)
		if !bytes.Equal(want, got) {
			t.Fatalf("%s differs after restore", k)
		}
	}
	if _, err := os.Stat(dir + ".restoring"); !os.IsNotExist(err) {
		t.Fatal("the staging directory was left behind")
	}

	// Never over an existing data directory.
	if _, err := Restore(dir, bytes.NewReader(buf.Bytes())); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("restore over a data directory: %v", err)
	}
}

// A damaged or foreign archive restores nothing.
func TestRestoreRefusesBadBackups(t *testing.T) {
	src, _ := NewHistoryStore(t.TempDir())
	defer src.Close()
	appendN(t, src, "a", 2)
	good, _ := src.ReadRaw("a")
	for name, files := range map[string]map[string][]byte{
		"torn history":       {"a.history": good[:len(good)-3]},
		"path escape":        {"../a.history": good},
		"nested elsewhere":   {"raft/a.history": good},
		"invalid namespace":  {"namespaces/Bad/a.history": good},
		"not a history":      {"notes.txt": []byte("hi")},
		"newer format":       {"a.history": good, "FORMAT": []byte(`{"history_format":99}`)},
		"default namespace ": {"namespaces/default/a.history": good},
	} {
		dir := filepath.Join(t.TempDir(), "d")
		if _, err := Restore(dir, backupOf(t, files)); err == nil {
			t.Errorf("%s: restored", name)
		}
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Errorf("%s: left %s behind", name, dir)
		}
		if _, err := os.Stat(dir + ".restoring"); !os.IsNotExist(err) {
			t.Errorf("%s: left the staging directory behind", name)
		}
	}
}
