package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/archive"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// memArchive is an archive store that can fail, and checks at every Put that
// the history it is given is still on disk: archived before it is deleted.
type memArchive struct {
	t     *testing.T
	store *history.HistoryStore
	mu    sync.Mutex
	objs  map[string][]byte
	fail  bool
	puts  int
}

func (m *memArchive) Put(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.fail {
		return errors.New("archive unavailable")
	}
	run, ok := archive.ParseKey(key)
	if !ok {
		m.t.Errorf("put under a malformed key %q", key)
	}
	keys, _ := m.store.ListWorkflows()
	onDisk := false
	for _, k := range keys {
		if raw, _ := m.store.ReadRaw(k); bytes.Equal(raw, data) && strings.Contains(k, run.WorkflowID) {
			onDisk = true
		}
	}
	if !onDisk {
		m.t.Errorf("archived %s, which is no longer on disk", key)
	}
	m.objs[key] = append([]byte(nil), data...)
	m.puts++
	return nil
}

func (m *memArchive) Get(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	d, ok := m.objs[key]
	if !ok {
		return nil, archive.ErrNotFound
	}
	return d, nil
}

func (m *memArchive) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for k := range m.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (m *memArchive) String() string { return "mem" }

// flakyDeletes fails the first Delete of each key: a crash between archiving
// and deleting.
type flakyDeletes struct {
	*history.HistoryStore
	mu     sync.Mutex
	failed map[string]bool
}

func (f *flakyDeletes) Delete(id string) error {
	f.mu.Lock()
	first := !f.failed[id]
	f.failed[id] = true
	f.mu.Unlock()
	if first {
		return errors.New("disk said no")
	}
	return f.HistoryStore.Delete(id)
}

func archiveEngine(t *testing.T, opts ...Option) (*Engine, *memArchive) {
	t.Helper()
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	mem := &memArchive{t: t, store: store, objs: map[string][]byte{}}
	e := NewEngine(store, discardLogger(), append([]Option{WithArchive(mem)}, opts...)...)
	t.Cleanup(e.Close)
	e.Register(flowDef)
	e.In("acme").RegisterWorkflow(flowDef.Type, flowDef)
	return e, mem
}

func finish(t *testing.T, sc *Scope, id string) {
	t.Helper()
	if _, err := sc.Start("Flow", []byte("in:"+id), id); err != nil {
		t.Fatal(err)
	}
	if err := sc.TerminateWorkflow(id, "done"); err != nil {
		t.Fatal(err)
	}
}

// Retention deletes a finished history only once the archive has it: while
// the archive fails, the history stays and is retried every pass.
func TestArchiveBeforeDelete(t *testing.T) {
	e, mem := archiveEngine(t)
	finish(t, e.In(""), "d1")
	finish(t, e.In("acme"), "a1")
	raw, _ := e.store.ReadRaw("acme/a1")

	mem.fail = true
	if n, _ := e.PurgeTerminated(time.Now().Add(time.Hour)); n != 0 {
		t.Fatalf("purged %d while the archive was down", n)
	}
	for _, sc := range []*Scope{e.In(""), e.In("acme")} {
		if _, err := sc.GetWorkflowRun(map[string]string{"default": "d1", "acme": "a1"}[sc.Namespace()], ""); err != nil {
			t.Fatalf("%s's history went while the archive was down: %v", sc.Namespace(), err)
		}
	}
	mem.fail = false
	if n, err := e.PurgeTerminated(time.Now().Add(time.Hour)); n != 2 || err != nil {
		t.Fatalf("purged %d (%v), want 2", n, err)
	}
	if _, err := e.In("acme").GetWorkflowRun("a1", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("a1 still on disk: %v", err)
	}

	// The archive reads back what was on disk, by namespace.
	run, events, err := e.In("acme").ArchivedHistory(context.Background(), "a1", "")
	if err != nil || len(events) != 3 || run.Namespace != "acme" {
		t.Fatalf("archived a1 = %+v, %d events, %v", run, len(events), err)
	}
	if !bytes.Equal(mem.objs[run.Key()], raw) {
		t.Fatal("the archive holds something other than the history file")
	}
	d, err := e.In("acme").DescribeArchived(context.Background(), "a1", run.RunID)
	if err != nil || d.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_TERMINATED || string(d.GetInput()) != "in:a1" {
		t.Fatalf("describe archived = %v, %v", d, err)
	}
	if _, _, err := e.In("").ArchivedHistory(context.Background(), "a1", ""); !errors.Is(err, ErrNotFound) {
		t.Fatalf("default sees acme's archived run: %v", err)
	}
	if runs, _ := e.In("").ArchivedRuns(context.Background(), "", 0); len(runs) != 1 || runs[0].WorkflowID != "d1" {
		t.Fatalf("default's archived runs = %v", runs)
	}
}

// A crash between archiving and deleting archives again, harmlessly, then
// deletes.
func TestArchiveThenDeleteFails(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	mem := &memArchive{t: t, store: store, objs: map[string][]byte{}}
	e := NewEngine(store, discardLogger(), WithArchive(mem),
		WithHistoryLog(&flakyDeletes{HistoryStore: store, failed: map[string]bool{}}))
	t.Cleanup(e.Close)
	e.Register(flowDef)
	finish(t, e.In(""), "w")
	if n, _ := e.PurgeTerminated(time.Now().Add(time.Hour)); n != 0 || mem.puts != 1 {
		t.Fatalf("first pass purged %d with %d puts", n, mem.puts)
	}
	if _, err := e.GetWorkflow("w"); err != nil {
		t.Fatalf("a failed delete lost the workflow: %v", err)
	}
	if n, _ := e.PurgeTerminated(time.Now().Add(time.Hour)); n != 1 || mem.puts != 2 || len(mem.objs) != 1 {
		t.Fatalf("second pass purged %d with %d puts, %d objects", n, mem.puts, len(mem.objs))
	}
}

// Earlier runs (continue-as-new) are archived too, each under its run id.
func TestArchiveEarlierRuns(t *testing.T) {
	e, mem := archiveEngine(t)
	e.RegisterWorkflow("Twice", replay.Planner{Fn: func(ctx workflow.Context, in []byte) ([]byte, error) {
		if len(in) == 0 {
			return nil, workflow.ContinueAsNew([]byte("second"))
		}
		return in, nil
	}})
	if _, err := e.Start("Twice", nil, "t"); err != nil {
		t.Fatal(err)
	}
	mem.fail = true
	if n, _ := e.PurgeTerminated(time.Now().Add(time.Hour)); n != 0 {
		t.Fatalf("purged %d runs while the archive was down", n)
	}
	if runs, _ := e.ListWorkflowRuns("t"); len(runs) != 2 {
		t.Fatalf("%d runs kept while the archive was down, want 2", len(runs))
	}
	mem.fail = false
	if n, err := e.PurgeTerminated(time.Now().Add(time.Hour)); n != 2 || err != nil {
		t.Fatalf("purged %d, %v; want both runs", n, err)
	}
	runs, err := e.In("").ArchivedRuns(context.Background(), "t", 0)
	if err != nil || len(runs) != 2 || runs[0].ClosedAt < runs[1].ClosedAt {
		t.Fatalf("archived runs = %+v, %v; want 2, newest first", runs, err)
	}
	last, err := e.In("").DescribeArchived(context.Background(), "t", "")
	if err != nil || string(last.GetOutput()) != "second" {
		t.Fatalf("the latest archived run = %v, %v", last, err)
	}
	if len(mem.objs) != 2 {
		t.Fatalf("%d objects", len(mem.objs))
	}
}

// Without an archive, retention deletes as it always has, and the read path
// says archiving is off.
func TestNoArchive(t *testing.T) {
	e := nsEngine(t, t.TempDir())
	finish(t, e.In(""), "w")
	if n, _ := e.PurgeTerminated(time.Now().Add(time.Hour)); n != 1 {
		t.Fatalf("purged %d", n)
	}
	if _, err := e.In("").ArchivedRuns(context.Background(), "", 0); !errors.Is(err, ErrArchiveOff) {
		t.Fatalf("archived runs without an archive: %v", err)
	}
}

func TestArchiveHTTP(t *testing.T) {
	e, _ := archiveEngine(t)
	finish(t, e.In("acme"), "h1")
	if _, err := e.PurgeTerminated(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	set, _ := NewNamespaceSet([]NamespaceConfig{{Name: "acme", Tokens: []string{"at"}}})
	api := NewHTTPAPI(e, discardLogger(), WithNamespaceSet(set))
	srv := httptest.NewServer(api.Wrap(api.Handler()))
	t.Cleanup(srv.Close)
	get := func(path, token string) (int, string) {
		req, _ := http.NewRequest("GET", srv.URL+path, nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		var b strings.Builder
		_, _ = fmt.Fprint(&b, readAll(res))
		return res.StatusCode, b.String()
	}
	if code, body := get("/api/archive/workflows", "at"); code != 200 || !strings.Contains(body, `"workflow_id":"h1"`) {
		t.Fatalf("list: %d %s", code, body)
	}
	if code, body := get("/api/archive/workflows/h1", "at"); code != 200 || !strings.Contains(body, `"status":"TERMINATED"`) {
		t.Fatalf("describe: %d %s", code, body)
	}
	if code, body := get("/api/archive/workflows/h1/history", "at"); code != 200 || !strings.Contains(body, "workflow_started") {
		t.Fatalf("history: %d %s", code, body)
	}
	if code, _ := get("/api/archive/workflows/h1", ""); code != 404 {
		t.Fatalf("default namespace sees acme's archive: %d", code)
	}
}

func readAll(res *http.Response) string {
	var b bytes.Buffer
	_, _ = b.ReadFrom(res.Body)
	return b.String()
}
