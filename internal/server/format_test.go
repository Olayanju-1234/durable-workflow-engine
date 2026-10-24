package server

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/sdk"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// The engine stamps each event an older binary would misread, and the data
// directory's recorded format rises with the first one.
func TestEngineStampsEventsWithTheirFormat(t *testing.T) {
	dir := t.TempDir()
	wf := sdk.Workflow{Type: "Fan", Fn: func(ctx workflow.Context, _ []byte) ([]byte, error) {
		id := ctx.SideEffect("id", func() []byte { return []byte("x") })
		a := ctx.ExecuteActivityAsync("a", id)
		b := ctx.ExecuteActivityAsync("b", id)
		if _, err := a.Get(); err != nil {
			return nil, err
		}
		return b.Get()
	}}
	e := newFuncEngine(t, dir, wf)
	id, err := e.StartWorkflow("Fan", nil, "fan-1")
	if err != nil {
		t.Fatal(err)
	}
	evs, err := e.store.ReadAll(id)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]uint32{"WorkflowStarted": 0, "marker id": history.FormatMarkers, "step a": 0, "step b": history.FormatParallelSteps}
	got := map[string]uint32{}
	for _, ev := range evs {
		switch {
		case ev.GetWorkflowStarted() != nil:
			got["WorkflowStarted"] = ev.GetMinReaderFormat()
		case ev.GetMarkerRecorded() != nil:
			got["marker id"] = ev.GetMinReaderFormat()
		case ev.GetStepScheduled() != nil:
			got["step "+ev.GetStepScheduled().GetStepName()] = ev.GetMinReaderFormat()
		}
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s stamped %d, want %d (all: %v)", k, got[k], v, got)
		}
	}
	if f, _ := history.ReadFormat(dir); f != history.FormatParallelSteps {
		t.Fatalf("recorded format = %d, want %d", f, history.FormatParallelSteps)
	}
}

// A directory written before formats were recorded learns its format from
// what its histories use, at the first recovery.
func TestRecoverRecordsFormatOfOlderDirectory(t *testing.T) {
	dir := t.TempDir()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeHistory(t, store, "versioned", startedEv("versioned", "T", nil),
		&history.HistoryEvent{Attributes: &history.HistoryEvent_MarkerRecorded{MarkerRecorded: &history.MarkerRecordedEvent{
			StepName: "v", Sequence: 1, Marker: state.MarkerVersion, Data: []byte("1"),
		}}},
		&history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowCompleted{WorkflowCompleted: &history.WorkflowCompletedEvent{}}})
	writeHistory(t, store, "plain", startedEv("plain", "T", nil), scheduledEv("a", 1, nil))
	store.Close()
	if f, _ := history.ReadFormat(dir); f != 0 {
		t.Fatalf("setup recorded format %d", f)
	}

	e := newTestEngine(t, dir)
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	if f, _ := history.ReadFormat(dir); f != history.FormatMarkers {
		t.Fatalf("recorded format after recovery = %d, want %d", f, history.FormatMarkers)
	}
	if st := e.Stats(); st.HistoryFormat != history.FormatMarkers {
		t.Fatalf("Stats().HistoryFormat = %d", st.HistoryFormat)
	}
}

// A history holding an event a newer binary wrote stops recovery with a
// message naming the workflow and the format, instead of misreading it.
func TestRecoverRefusesHistoryFromNewerBinary(t *testing.T) {
	dir := t.TempDir()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeHistory(t, store, "ok", startedEv("ok", "T", nil), scheduledEv("a", 1, nil))
	store.Close()

	// Written byte for byte, as a newer binary would, past this store's check.
	future := scheduledEv("a", 1, nil)
	future.EventId, future.Timestamp, future.MinReaderFormat = 2, time.Now().UnixNano(), history.FormatVersion+1
	first := startedEv("from-the-future", "T", nil)
	first.EventId, first.Timestamp = 1, time.Now().UnixNano()
	var raw []byte
	for _, ev := range []*history.HistoryEvent{first, future} {
		b, err := proto.Marshal(ev)
		if err != nil {
			t.Fatal(err)
		}
		rec, err := record.Encode(b)
		if err != nil {
			t.Fatal(err)
		}
		raw = append(raw, rec...)
	}
	if err := os.WriteFile(filepath.Join(dir, "from-the-future.history"), raw, 0o644); err != nil {
		t.Fatal(err)
	}

	e := newTestEngine(t, dir)
	err = e.Recover()
	if !errors.Is(err, history.ErrFormatTooNew) {
		t.Fatalf("recover: err = %v, want ErrFormatTooNew", err)
	}
	for _, want := range []string{"from-the-future", fmt.Sprintf("needs history format %d", history.FormatVersion+1)} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %q", err, want)
		}
	}
}
