package server

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// parked waits for a signal nobody sends, so it stays live for as long as a
// test needs; the activity before it gives workers something to complete.
var parked = state.WorkflowDefinition{Type: "Parked", Steps: []state.StepDef{
	state.Activity("work"), state.AwaitSignal("park", "never"),
}}

func newLockingEngine(t *testing.T, opts ...Option) (*Engine, *history.HistoryStore) {
	t.Helper()
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e := NewEngine(store, discardLogger(), opts...)
	t.Cleanup(e.Close)
	e.Register(parked)
	return e, store
}

// checkContiguous fails unless the workflow's event ids are exactly 1..n.
func checkContiguous(t *testing.T, store *history.HistoryStore, id string) []*history.HistoryEvent {
	t.Helper()
	events, err := store.ReadAll(id)
	if err != nil {
		t.Fatal(err)
	}
	for i, ev := range events {
		if ev.GetEventId() != int64(i+1) {
			t.Fatalf("%s: event %d has id %d: concurrent appends interleaved", id, i+1, ev.GetEventId())
		}
	}
	return events
}

// TestConcurrentWritesToOneWorkflowStaySerial: many callers appending to one
// workflow at once still produce one contiguous history with every write in it.
func TestConcurrentWritesToOneWorkflowStaySerial(t *testing.T) {
	e, store := newLockingEngine(t)
	id, err := e.StartWorkflow("Parked", nil, "busy")
	if err != nil {
		t.Fatal(err)
	}
	const senders, each = 20, 10
	var wg sync.WaitGroup
	for range senders {
		wg.Go(func() {
			for range each {
				if err := e.SignalWorkflow(id, "noise", nil); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	events := checkContiguous(t, store, id)
	if want := 2 + senders*each; len(events) != want { // started + scheduled + signals
		t.Fatalf("history has %d events, want %d", len(events), want)
	}
}

// TestCachedStateMatchesHistory: after concurrent traffic on several
// workflows, the state the engine holds in memory equals a fresh replay of
// what is on disk, event for event.
func TestCachedStateMatchesHistory(t *testing.T) {
	e, store := newLockingEngine(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runEngineWorker(ctx, e, func(string, []byte) ([]byte, error) { return []byte("done"), nil })

	var ids []string
	for i := range 8 {
		id, err := e.StartWorkflow("Parked", []byte(fmt.Sprint(i)), "")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	var wg sync.WaitGroup
	for _, id := range ids {
		wg.Go(func() {
			for range 25 {
				if err := e.SignalWorkflow(id, "noise", []byte("x")); err != nil {
					t.Error(err)
				}
			}
		})
	}
	wg.Wait()
	for _, id := range ids {
		waitForState(t, e, id, func(st *state.State) bool { return len(st.Steps) == 2 }, 3*time.Second)
		events := checkContiguous(t, store, id)
		want, err := state.Reconstruct(events)
		if err != nil {
			t.Fatal(err)
		}
		w := e.lockWorkflow(id)
		cached, st, err := e.historyLocked(w)
		e.unlockWorkflow(w)
		if err != nil {
			t.Fatal(err)
		}
		if len(cached) != len(events) {
			t.Fatalf("%s: cache holds %d events, disk %d", id, len(cached), len(events))
		}
		for i := range events {
			if !proto.Equal(cached[i], events[i]) {
				t.Fatalf("%s: event %d differs: cache %v, disk %v", id, i, cached[i], events[i])
			}
		}
		if !reflect.DeepEqual(st, want) {
			t.Fatalf("%s: cached state differs from replay:\ncache %+v\ndisk  %+v", id, st, want)
		}
	}
}

// gatedLog blocks appends to one workflow until released, and can fail the
// next append after writing it (the outcome-unknown case).
type gatedLog struct {
	store   *history.HistoryStore
	gateID  string
	entered chan struct{}
	release chan struct{}
	failAt  atomic.Int64 // fail the append with this event id, after writing it
}

func (l *gatedLog) NewRun(id, archive string, ev *history.HistoryEvent) error {
	return l.store.NewRun(id, archive, ev)
}

func (l *gatedLog) Append(id string, ev *history.HistoryEvent) error {
	if id == l.gateID {
		l.entered <- struct{}{}
		<-l.release
	}
	if err := l.store.Append(id, ev); err != nil {
		return err
	}
	if l.failAt.CompareAndSwap(ev.GetEventId(), 0) {
		return errors.New("injected: write landed but the caller was told it failed")
	}
	return nil
}

func (l *gatedLog) Delete(id string) error { return l.store.Delete(id) }

// TestWorkflowsDoNotWaitForEachOther: while one workflow's write is stuck,
// another starts, runs and is signalled.
func TestWorkflowsDoNotWaitForEachOther(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	log := &gatedLog{store: store, gateID: "slow", entered: make(chan struct{}), release: make(chan struct{})}
	e := NewEngine(store, discardLogger(), WithHistoryLog(log))
	t.Cleanup(e.Close)
	e.Register(parked)

	stuck := make(chan error, 1)
	go func() {
		_, err := e.StartWorkflow("Parked", nil, "slow")
		stuck <- err
	}()
	<-log.entered // "slow" now holds its lock, blocked inside Append

	done := make(chan error, 1)
	go func() {
		id, err := e.StartWorkflow("Parked", nil, "fast")
		if err == nil {
			err = e.SignalWorkflow(id, "noise", nil)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a stuck write to one workflow blocked another workflow")
	}
	close(log.release)
	go func() {
		for range log.entered {
		}
	}()
	if err := <-stuck; err != nil {
		t.Fatal(err)
	}
}

// TestFailedAppendRereadsHistory: an append that reported failure may still
// have reached the log, so the engine must not trust its cached copy after one.
func TestFailedAppendRereadsHistory(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	log := &gatedLog{store: store}
	e := NewEngine(store, discardLogger(), WithHistoryLog(log))
	t.Cleanup(e.Close)
	e.Register(parked)
	id, err := e.StartWorkflow("Parked", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	log.failAt.Store(3) // the first signal: written, but reported as failed
	if err := e.SignalWorkflow(id, "noise", nil); err == nil {
		t.Fatal("injected failure not reported")
	}
	if err := e.SignalWorkflow(id, "noise", nil); err != nil {
		t.Fatal(err)
	}
	events := checkContiguous(t, store, id)
	if len(events) != 4 {
		t.Fatalf("history has %d events, want 4", len(events))
	}
}

// TestCacheIsBounded: finished workflows leave the cache at once, and idle
// ones beyond its size are evicted (and re-read correctly when used again).
func TestCacheIsBounded(t *testing.T) {
	e, store := newLockingEngine(t, WithStateCacheSize(2))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runEngineWorker(ctx, e, func(string, []byte) ([]byte, error) { return nil, nil })
	var ids []string
	for range 5 {
		id, err := e.StartWorkflow("Parked", nil, "")
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	for _, id := range ids {
		waitForState(t, e, id, func(st *state.State) bool { return len(st.Steps) == 2 }, 3*time.Second)
	}
	for round := range 3 {
		for _, id := range ids {
			if err := e.SignalWorkflow(id, "noise", []byte(fmt.Sprint(round))); err != nil {
				t.Fatal(err)
			}
		}
	}
	e.wfMu.Lock()
	cached := len(e.wfs)
	e.wfMu.Unlock()
	if cached > 2 {
		t.Fatalf("%d workflows cached, want at most 2", cached)
	}
	for _, id := range ids {
		// started, work scheduled and completed, park scheduled, 3 signals
		if n := len(checkContiguous(t, store, id)); n != 7 {
			t.Fatalf("%s: %d events, want 7", id, n)
		}
		if err := e.TerminateWorkflow(id, "done"); err != nil {
			t.Fatal(err)
		}
	}
	e.wfMu.Lock()
	cached = len(e.wfs)
	e.wfMu.Unlock()
	if cached != 0 {
		t.Fatalf("%d finished workflows still cached", cached)
	}
}
