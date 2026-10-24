package main

import (
	"bytes"
	"strings"
	"testing"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

func writeEvents(t *testing.T, dir, id string, evs ...*history.HistoryEvent) {
	t.Helper()
	s, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i, ev := range evs {
		ev.EventId = int64(i + 1)
		if err := s.Append(id, ev); err != nil {
			t.Fatal(err)
		}
	}
}

// --check-format reports what an operator needs before a rollback: what the
// directory records, what its histories need, and whether this binary serves it.
func TestCheckFormat(t *testing.T) {
	dir := t.TempDir()
	writeEvents(t, dir, "wf",
		&history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{WorkflowStarted: &history.WorkflowStartedEvent{WorkflowType: "T"}}},
		&history.HistoryEvent{Attributes: &history.HistoryEvent_MarkerRecorded{MarkerRecorded: &history.MarkerRecordedEvent{StepName: "v", Sequence: 1, Marker: "version", Data: []byte("1")}}})

	var out bytes.Buffer
	if err := checkFormat(&out, dir, false); err != nil {
		t.Fatalf("check: %v\n%s", err, out.String())
	}
	for _, want := range []string{"recorded format:  none", "histories need:   2", "1 histories scanned", "OK:"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output lacks %q:\n%s", want, out.String())
		}
	}
	if f, _ := history.ReadFormat(dir); f != 0 {
		t.Fatalf("--check-format wrote a record (%d)", f)
	}

	// --rescan-format records what the histories need, lowering a stale record.
	if err := history.WriteFormat(dir, history.FormatClosedSteps); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := checkFormat(&out, dir, true); err != nil {
		t.Fatalf("rescan: %v\n%s", err, out.String())
	}
	if f, _ := history.ReadFormat(dir); f != history.FormatMarkers {
		t.Fatalf("after rescan the record is %d, want %d", f, history.FormatMarkers)
	}

	// A directory from a newer binary is refused, and not rewritten.
	if err := history.WriteFormat(dir, history.FormatVersion+1); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	if err := checkFormat(&out, dir, true); err == nil {
		t.Fatalf("a newer directory passed:\n%s", out.String())
	}
	if !strings.Contains(out.String(), "REFUSED") {
		t.Errorf("output lacks REFUSED:\n%s", out.String())
	}
	if f, _ := history.ReadFormat(dir); f != history.FormatVersion+1 {
		t.Fatalf("a refused rescan rewrote the record to %d", f)
	}
}
