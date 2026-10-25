package audit

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRecordReadNewestFirst(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "audit", "audit.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	for i := range 5 {
		if err := l.Record(Entry{Actor: "engine:ab", Action: "workflow.start", Target: fmt.Sprint("w", i), Outcome: "ok", Via: "http"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Record(Entry{Actor: "operator", Action: "config.reload", Outcome: "ok", Via: "signal"}); err != nil {
		t.Fatal(err)
	}
	all, err := l.Read(nil, 0)
	if err != nil || len(all) != 6 || all[0].Action != "config.reload" || all[5].Target != "w0" || all[1].Time.IsZero() {
		t.Fatalf("read %v %v", all, err)
	}
	starts, _ := l.Read(func(e Entry) bool { return e.Action == "workflow.start" }, 2)
	if len(starts) != 2 || starts[0].Target != "w4" || starts[1].Target != "w3" {
		t.Fatalf("filtered %v", starts)
	}
}

// The log rotates by size, keeps a fixed number of earlier files, and reads
// across them.
func TestRotation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "audit.log")
	l, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	l.maxBytes, l.keep = 300, 2
	for i := range 40 {
		if err := l.Record(Entry{Actor: "a", Action: "workflow.signal", Target: fmt.Sprint("w", i), Outcome: "ok", Via: "grpc"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Stat(path + ".2"); err != nil {
		t.Fatalf("no second rotated file: %v", err)
	}
	if _, err := os.Stat(path + ".3"); err == nil {
		t.Fatal("kept more rotated files than asked")
	}
	got, _ := l.Read(nil, 0)
	if len(got) == 0 || got[0].Target != "w39" {
		t.Fatalf("newest entry %v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i-1].Time.Before(got[i].Time) {
			t.Fatal("entries out of order across rotated files")
		}
	}
	if len(got) >= 40 {
		t.Fatalf("read %d entries: the oldest files should be gone", len(got))
	}
}

func TestNilLogRecordsNothing(t *testing.T) {
	var l *Log
	if err := l.Record(Entry{Action: "x"}); err != nil {
		t.Fatal(err)
	}
	if got, err := l.Read(nil, 0); got != nil || err != nil {
		t.Fatal(got, err)
	}
}
