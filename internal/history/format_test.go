package history

import (
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
)

func started(id int64) *HistoryEvent {
	return &HistoryEvent{EventId: id, Attributes: &HistoryEvent_WorkflowStarted{
		WorkflowStarted: &WorkflowStartedEvent{WorkflowId: "wf", WorkflowType: "T"},
	}}
}

func stamped(id int64, format uint32) *HistoryEvent {
	ev := started(id)
	ev.MinReaderFormat = format
	return ev
}

// A directory written by a newer binary is refused when the store opens,
// before anything reads a history, and the message says what is needed.
func TestStoreRefusesDirectoryInNewerFormat(t *testing.T) {
	dir := t.TempDir()
	if err := WriteFormat(dir, FormatVersion+1); err != nil {
		t.Fatal(err)
	}
	_, err := NewHistoryStore(dir)
	if !errors.Is(err, ErrFormatTooNew) {
		t.Fatalf("open: err = %v, want ErrFormatTooNew", err)
	}
	for _, want := range []string{fmt.Sprintf("needs history format %d", FormatVersion+1), fmt.Sprintf("reads up to format %d", FormatVersion), "docs/UPGRADING.md"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
	if err := WriteFormat(dir, FormatVersion); err != nil {
		t.Fatal(err)
	}
	s, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatalf("open at this binary's own format: %v", err)
	}
	s.Close()
}

// The recorded format rises before the first event that needs it is written,
// never falls while a store writes, and is untouched by base-format events.
func TestAppendRaisesRecordedFormatFirst(t *testing.T) {
	dir := t.TempDir()
	s, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.Append("a", started(1)); err != nil {
		t.Fatal(err)
	}
	if f, _ := ReadFormat(dir); f != 0 {
		t.Fatalf("base-format append recorded format %d, want none", f)
	}

	// A write that fails after the check leaves the record high, not low: a
	// crash between the two can only make a rollback look less safe.
	s.writeFile = func(*os.File, []byte) (int, error) { return 0, errors.New("disk on fire") }
	if err := s.Append("b", stamped(1, FormatParallelSteps)); err == nil {
		t.Fatal("append with a failing write succeeded")
	}
	if f, _ := ReadFormat(dir); f != FormatParallelSteps {
		t.Fatalf("after a failed write the recorded format is %d, want %d", f, FormatParallelSteps)
	}
	s.writeFile = (*os.File).Write

	if err := s.Append("c", stamped(1, FormatMarkers)); err != nil {
		t.Fatal(err)
	}
	if f, _ := ReadFormat(dir); f != FormatParallelSteps || s.Format() != FormatParallelSteps {
		t.Fatalf("an older-format event lowered the record to %d", f)
	}

	if err := s.Append("d", stamped(1, FormatVersion+1)); !errors.Is(err, ErrFormatTooNew) {
		t.Fatalf("append of an event from the future: err = %v", err)
	}
	if evs, _ := s.ReadAll("d"); len(evs) != 0 {
		t.Fatalf("a refused event was written: %v", evs)
	}
}

// A snapshot installed in a cluster carries its histories' formats along.
func TestReplaceAllRaisesRecordedFormat(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	a, err := NewHistoryStore(src)
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if err := a.Append("wf", stamped(1, FormatClosedSteps)); err != nil {
		t.Fatal(err)
	}
	raw, err := a.ReadRaw("wf")
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewHistoryStore(dst)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.ReplaceAll(map[string][]byte{"wf": raw}); err != nil {
		t.Fatal(err)
	}
	if f, _ := ReadFormat(dst); f != FormatClosedSteps {
		t.Fatalf("recorded format after install = %d, want %d", f, FormatClosedSteps)
	}
}

func TestRequireFormatWritesARecordOnce(t *testing.T) {
	dir := t.TempDir()
	s, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.RequireFormat(0); err != nil {
		t.Fatal(err)
	}
	if f, _ := ReadFormat(dir); f != FormatBase {
		t.Fatalf("a directory with no record got %d, want the base format", f)
	}
	data, _ := os.ReadFile(dir + "/FORMAT")
	if got := strings.TrimSpace(string(data)); got != `{"history_format":1}` {
		t.Fatalf("FORMAT holds %q", got)
	}
	if ids, _ := s.ListWorkflows(); len(ids) != 0 {
		t.Fatalf("the format record is listed as a workflow: %v", ids)
	}
}
