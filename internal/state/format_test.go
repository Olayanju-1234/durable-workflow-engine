package state

import (
	"errors"
	"testing"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

func marker(name string, seq int64) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_MarkerRecorded{
		MarkerRecorded: &history.MarkerRecordedEvent{StepName: name, Sequence: seq, Marker: MarkerSideEffect},
	}}
}

func retrying(name string, seq int64) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
		StepFailed: &history.StepFailedEvent{StepName: name, Sequence: seq, Error: "boom", WillRetry: true},
	}}
}

func cancelledStep(name string, seq int64) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_StepFailed{
		StepFailed: &history.StepFailedEvent{StepName: name, Sequence: seq, Error: "cancelled", Cancelled: true},
	}}
}

// Histories written before events were stamped still get the format of the
// features they use, so a directory from an older binary is classified right.
func TestFormatOfUnstampedHistories(t *testing.T) {
	tests := []struct {
		name   string
		events []*history.HistoryEvent
		want   uint32
	}{
		{"sequential steps", []*history.HistoryEvent{
			started("T", ""), scheduled("a", 1, ""), completed("a", 1, ""), scheduled("b", 2, ""),
			failed("b", 2, "x"), wfFailed("x"),
		}, history.FormatBase},
		{"retry, sequential", []*history.HistoryEvent{
			started("T", ""), scheduled("a", 1, ""), retrying("a", 1), scheduled("a", 1, ""), completed("a", 1, ""),
		}, history.FormatBase},
		{"a marker", []*history.HistoryEvent{
			started("T", ""), marker("id", 1),
		}, history.FormatMarkers},
		{"two steps in flight", []*history.HistoryEvent{
			started("T", ""), scheduled("a", 1, ""), timerStarted("t", 2, 1, ""),
		}, history.FormatParallelSteps},
		{"a step opened while another retries", []*history.HistoryEvent{
			started("T", ""), scheduled("a", 1, ""), retrying("a", 1), scheduled("b", 2, ""),
		}, history.FormatParallelSteps},
		{"a cancelled step", []*history.HistoryEvent{
			started("T", ""), timerStarted("t", 1, 1, ""), cancelledStep("t", 1),
		}, history.FormatClosedSteps},
		{"a final failure closing a step between attempts", []*history.HistoryEvent{
			started("T", ""), scheduled("a", 1, ""), retrying("a", 1), failed("a", 1, "deadline"),
		}, history.FormatClosedSteps},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := NewFolder()
			for i, ev := range tt.events {
				ev.EventId = int64(i + 1)
				if err := f.Apply(ev); err != nil {
					t.Fatalf("apply %d: %v", i, err)
				}
			}
			if got := f.Format(); got != tt.want {
				t.Fatalf("Format() = %d, want %d", got, tt.want)
			}
		})
	}
}

// A stamp is honoured, and one newer than this binary poisons the fold with
// ErrFormatTooNew instead of being read as something it is not.
func TestFormatStampsAndEventsFromTheFuture(t *testing.T) {
	f := NewFolder()
	ev := started("T", "")
	ev.EventId, ev.MinReaderFormat = 1, history.FormatRuns
	if err := f.Apply(ev); err != nil {
		t.Fatal(err)
	}
	if f.Format() != history.FormatRuns {
		t.Fatalf("Format() = %d, want the stamp %d", f.Format(), history.FormatRuns)
	}

	future := scheduled("a", 1, "")
	future.EventId, future.MinReaderFormat = 2, history.FormatVersion+1
	if err := f.Apply(future); !errors.Is(err, history.ErrFormatTooNew) {
		t.Fatalf("event stamped with a newer format: err = %v", err)
	}

	// An event kind added by a newer binary decodes with no attributes and an
	// unknown field; it must not be mistaken for a damaged history.
	b, err := proto.Marshal(&history.HistoryEvent{EventId: 2})
	if err != nil {
		t.Fatal(err)
	}
	b = protowire.AppendTag(b, 99, protowire.BytesType)
	b = protowire.AppendBytes(b, []byte{0x0a, 0x01, 'x'})
	unknown := &history.HistoryEvent{}
	if err := proto.Unmarshal(b, unknown); err != nil {
		t.Fatal(err)
	}
	g := NewFolder()
	first := started("T", "")
	first.EventId = 1
	if err := g.Apply(first); err != nil {
		t.Fatal(err)
	}
	if err := g.Apply(unknown); !errors.Is(err, history.ErrFormatTooNew) {
		t.Fatalf("unknown event kind: err = %v, want ErrFormatTooNew", err)
	}
}
