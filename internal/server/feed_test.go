package server

import (
	"io"
	"log/slog"
	"testing"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

func TestSubscriberThatFallsBehindIsToldToResync(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e := NewEngine(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer e.Close()
	sub, err := e.In("").Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	// Nobody reads: the buffer fills, and publishing must neither block nor lose the fact.
	for range subscriptionBuffer + 10 {
		e.feed.publish(DefaultNamespace, Change{Key: "w"})
	}
	if len(sub.C()) != subscriptionBuffer {
		t.Fatalf("buffered %d changes, want %d", len(sub.C()), subscriptionBuffer)
	}
	if !sub.Lagged() {
		t.Fatal("a subscriber that fell behind was not told so")
	}
	if sub.Lagged() {
		t.Fatal("Lagged did not clear once reported")
	}
	e.feed.publish("other", Change{Key: "other/w"})
	if sub.Lagged() {
		t.Fatal("another namespace's change reached this subscriber")
	}
}
