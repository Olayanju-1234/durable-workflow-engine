package server

import (
	"bytes"
	"context"
	"crypto/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

func testKeyring(t *testing.T) *history.Keyring {
	t.Helper()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	k, err := history.NewKeyring("k1", map[string][]byte{"k1": key})
	if err != nil {
		t.Fatal(err)
	}
	return k
}

// capturingLog is the store as a HistoryLog, keeping what it was handed: in
// a cluster this is what goes into the Raft log.
type capturingLog struct {
	*history.HistoryStore
	mu   sync.Mutex
	seen []*history.HistoryEvent
}

func (l *capturingLog) Append(id string, ev *history.HistoryEvent) error {
	l.mu.Lock()
	l.seen = append(l.seen, ev)
	l.mu.Unlock()
	return l.HistoryStore.Append(id, ev)
}

func (l *capturingLog) NewRun(id, archive string, first *history.HistoryEvent, more ...*history.HistoryEvent) error {
	l.mu.Lock()
	l.seen = append(append(l.seen, first), more...)
	l.mu.Unlock()
	return l.HistoryStore.NewRun(id, archive, first, more...)
}

const cardNumber = "4242-4242-4242-4242"

// A workflow runs as before with encryption on: the engine and its APIs see
// plaintext, the disk and the history log only ciphertext, and a restart
// reads it all back.
func TestWorkflowsRunEncryptedAtRest(t *testing.T) {
	dir := t.TempDir()
	keys := testKeyring(t)
	store := newStore(t, dir)
	store.SetKeyring(keys)
	log := &capturingLog{HistoryStore: store}
	e := timeoutEngine(t, store, map[string]workflow.Func{"Pipeline": pipeline}, WithHistoryLog(log))
	broken := true
	w := newStepWorker(func(step string) bool { return step == "c" && broken })
	if _, err := e.Start("Pipeline", []byte(cardNumber), "pay-1"); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		w.runOne(t, e)
	}
	failed := waitFinished(t, e, "pay-1", 3*time.Second)
	if failed.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_FAILED || string(failed.GetInput()) != cardNumber {
		t.Fatalf("describe %v %q", failed.GetSummary().GetStatus(), failed.GetInput())
	}
	broken = false
	if _, err := e.ResetWorkflow("pay-1", ResetOptions{}); err != nil {
		t.Fatal(err)
	}
	w.runOne(t, e)
	done := waitFinished(t, e, "pay-1", 3*time.Second)
	if string(done.GetOutput()) != cardNumber+">a>b>c" {
		t.Fatalf("output %q", done.GetOutput())
	}

	log.mu.Lock()
	for _, ev := range log.seen {
		if !history.IsSealed(ev) {
			t.Fatalf("the history log was handed event %d in plaintext: %v", ev.GetEventId(), ev)
		}
	}
	log.mu.Unlock()
	entries, _ := os.ReadDir(dir)
	for _, ent := range entries {
		data, _ := os.ReadFile(filepath.Join(dir, ent.Name()))
		if bytes.Contains(data, []byte(cardNumber)) || bytes.Contains(data, []byte("Pipeline")) {
			t.Fatalf("%s shows the workflow's payload or type on disk", ent.Name())
		}
	}
	e.Close()

	store2 := newStore(t, dir)
	store2.SetKeyring(keys)
	e2 := timeoutEngine(t, store2, map[string]workflow.Func{"Pipeline": pipeline})
	if err := e2.Recover(); err != nil {
		t.Fatal(err)
	}
	evs, err := e2.In("").History("pay-1", "")
	if err != nil || evs[0].GetWorkflowStarted().GetWorkflowType() != "Pipeline" {
		t.Fatalf("history after a restart: %v %v", evs, err)
	}
	runs, err := e2.ListWorkflowRuns("pay-1")
	if err != nil || len(runs) != 2 {
		t.Fatalf("runs after a restart: %v %v", runs, err)
	}
	if got := e2.Stats().EncryptionKey; got != "k1" {
		t.Fatalf("health says encryption key %q", got)
	}
}

// Without the key the engine does not start: it would not know what its
// workflows are doing.
func TestRecoveryRefusesHistoriesItCannotOpen(t *testing.T) {
	dir := t.TempDir()
	store := newStore(t, dir)
	store.SetKeyring(testKeyring(t))
	e := timeoutEngine(t, store, map[string]workflow.Func{"Pipeline": pipeline})
	if _, err := e.Start("Pipeline", nil, "pay-2"); err != nil {
		t.Fatal(err)
	}
	e.Close()
	e2 := timeoutEngine(t, newStore(t, dir), map[string]workflow.Func{"Pipeline": pipeline})
	if err := e2.Recover(); err == nil {
		t.Fatal("recovered histories it has no key for")
	}
}

// An archived copy is the history file as it was on disk: ciphertext, which
// the engine opens again when it is read back.
func TestArchivedRunsStayEncrypted(t *testing.T) {
	e, mem := archiveEngine(t)
	e.store.SetKeyring(testKeyring(t))
	finish(t, e.In("acme"), "card-"+cardNumber[:4])
	if n, err := e.PurgeTerminated(time.Now().Add(time.Hour)); n != 1 || err != nil {
		t.Fatalf("purged %d (%v)", n, err)
	}
	for key, data := range mem.objs {
		if bytes.Contains(data, []byte("in:card")) || bytes.Contains(data, []byte("Flow")) {
			t.Fatalf("archive object %s is plaintext", key)
		}
	}
	d, err := e.In("acme").DescribeArchived(context.Background(), "card-"+cardNumber[:4], "")
	if err != nil || string(d.GetInput()) != "in:card-4242" {
		t.Fatalf("describe archived = %v, %v", d, err)
	}
}
