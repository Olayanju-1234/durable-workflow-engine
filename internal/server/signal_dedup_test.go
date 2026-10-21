package server

import (
	"errors"
	"testing"
)

func countSignals(t *testing.T, e *Engine, id string) int {
	t.Helper()
	evs, err := e.store.ReadAll(id)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, ev := range evs {
		if ev.GetSignalReceived() != nil {
			n++
		}
	}
	return n
}

// TestSignalRequestIDIsRecordedOnce: a signal retried with the same key is
// recorded once, by the same engine or by the next one (a restart or a new
// leader), and even after the workflow has finished.
func TestSignalRequestIDIsRecordedOnce(t *testing.T) {
	dir := t.TempDir()
	e1 := reopen(t, dir)
	id, err := e1.StartWorkflow("Parked", nil, "")
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := e1.SignalWorkflowOnce(id, "noise", "req-1", []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	if err := e1.SignalWorkflowOnce(id, "other", "req-1", nil); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("reusing a key for another signal: %v, want ErrInvalidArgument", err)
	}
	if err := e1.SignalWorkflow(id, "noise", nil); err != nil { // no key: always recorded
		t.Fatal(err)
	}
	e1.Close()

	e2 := reopen(t, dir)
	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := e2.SignalWorkflowOnce(id, "noise", "req-1", []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err := e2.TerminateWorkflow(id, "done"); err != nil {
		t.Fatal(err)
	}
	if err := e2.SignalWorkflowOnce(id, "noise", "req-1", []byte("x")); err != nil {
		t.Fatalf("retry after the workflow finished: %v; the first attempt was recorded, so it succeeded", err)
	}
	if err := e2.SignalWorkflowOnce(id, "noise", "req-2", nil); !errors.Is(err, ErrAlreadyTerminal) {
		t.Fatalf("a new signal to a finished workflow: %v, want ErrAlreadyTerminal", err)
	}
	if n := countSignals(t, e2, id); n != 2 {
		t.Fatalf("%d signals recorded, want 2 (req-1 once, plus the one without a key)", n)
	}
}
