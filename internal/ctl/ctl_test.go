package ctl

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/archive"
	"github.com/olayanju-1234/durable-workflow-engine/internal/audit"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

// harness serves a real engine over its HTTP API with an engine token
// ("admin") and an "acme" namespace with its own token.
type harness struct {
	t      *testing.T
	url    string
	dir    string
	engine *server.Engine
	env    map[string]string
}

func newHarness(t *testing.T, opts ...server.Option) *harness {
	t.Helper()
	dir := t.TempDir()
	store, err := history.NewHistoryStore(filepath.Join(dir, "data"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	set, err := server.NewNamespaceSet([]server.NamespaceConfig{{Name: "acme", Tokens: []string{"acme-token"}}})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	e := server.NewEngine(store, logger, append([]server.Option{server.WithNamespaces(set)}, opts...)...)
	t.Cleanup(e.Close)
	approval := replay.Planner{Fn: func(ctx workflow.Context, in []byte) ([]byte, error) {
		got := ctx.WaitForSignal("approval", "approve")
		return []byte("approved by " + string(got)), nil
	}}
	for _, ns := range []string{"", "acme"} {
		e.In(ns).RegisterWorkflow("Approval", approval)
	}
	auditLog, err := audit.Open(filepath.Join(dir, "audit.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { auditLog.Close() })
	api := server.NewHTTPAPI(e, logger, server.WithAuthToken("admin"), server.WithNamespaceSet(set), server.WithAudit(auditLog))
	srv := httptest.NewServer(api.Wrap(api.Handler()))
	t.Cleanup(srv.Close)
	for name, tok := range map[string]string{"admin.token": "admin", "acme.token": "acme-token"} {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(tok+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return &harness{t: t, url: srv.URL, dir: dir, engine: e,
		env: map[string]string{envAddr: srv.URL, envTokenFile: filepath.Join(dir, "admin.token")}}
}

// run runs `workflowd ctl args...` and returns its exit status and output.
func (h *harness) run(stdin string, args ...string) (int, string, string) {
	h.t.Helper()
	var out, errOut bytes.Buffer
	code := Main(args, strings.NewReader(stdin), &out, &errOut, func(k string) string { return h.env[k] })
	return code, out.String(), errOut.String()
}

func (h *harness) ok(args ...string) string {
	h.t.Helper()
	code, out, errOut := h.run("", args...)
	if code != 0 {
		h.t.Fatalf("ctl %v: exit %d\n%s%s", args, code, out, errOut)
	}
	return out
}

func contains(t *testing.T, what, got string, want ...string) {
	t.Helper()
	for _, w := range want {
		if !strings.Contains(got, w) {
			t.Fatalf("%s lacks %q:\n%s", what, w, got)
		}
	}
}

func TestWorkflowCommands(t *testing.T) {
	h := newHarness(t)
	contains(t, "start", h.ok("start", "Approval", "--id", "a1", "--input", "order-1"), "started a1 (run ")
	contains(t, "start again", h.ok("start", "Approval", "--id", "a1", "--id-conflict-policy", "USE_EXISTING"), "a1 is already running")
	contains(t, "list", h.ok("list"), "WORKFLOW ID", "a1", "Approval", "WAITING")
	contains(t, "list by type", h.ok("list", "--type", "Approval", "--status", "waiting"), "a1")
	if out := h.ok("list", "--type", "Nope"); strings.Contains(out, "a1") {
		t.Fatalf("list --type Nope:\n%s", out)
	}
	// Flags may follow the arguments.
	contains(t, "describe", h.ok("describe", "a1", "--timeout", "5s"), "Approval", "WAITING", "order-1", "approval", "waiting for approve")
	contains(t, "signal", h.ok("signal", "a1", "approve", "--payload", "ops"), "signalled a1: approve")
	deadline := time.Now().Add(5 * time.Second)
	for !strings.Contains(h.ok("describe", "a1"), "COMPLETED") {
		if time.Now().After(deadline) {
			t.Fatal("a1 never completed")
		}
		time.Sleep(20 * time.Millisecond)
	}
	contains(t, "describe done", h.ok("describe", "a1"), `approved by ops`)
	contains(t, "history", h.ok("history", "a1"), "WorkflowStarted", `input="order-1"`, "SignalReceived", `payload="ops"`, "WorkflowCompleted")
	contains(t, "runs", h.ok("runs", "a1"), "COMPLETED", "current")

	// --json is the engine's JSON, for scripts.
	var got struct {
		Status string `json:"status"`
		Output string `json:"output"`
	}
	if err := json.Unmarshal([]byte(h.ok("describe", "a1", "--json")), &got); err != nil || got.Status != "COMPLETED" {
		t.Fatalf("describe --json: %+v %v", got, err)
	}
	var hist struct {
		Events []map[string]any `json:"events"`
	}
	if err := json.Unmarshal([]byte(h.ok("history", "a1", "--json")), &hist); err != nil || len(hist.Events) < 4 ||
		hist.Events[0]["workflow_started"] == nil {
		t.Fatalf("history --json: %v %v", hist, err)
	}

	// Input from standard input; cancel and terminate.
	if code, out, errOut := h.run("from stdin", "start", "Approval", "--id", "a2", "--input-file", "-"); code != 0 {
		t.Fatalf("start from stdin: %d %s%s", code, out, errOut)
	}
	contains(t, "stdin input", h.ok("describe", "a2"), "from stdin")
	contains(t, "cancel", h.ok("cancel", "a2", "--reason", "not needed"), "cancelled a2")
	contains(t, "after cancel", h.ok("describe", "a2"), "CANCELLED")
	h.ok("start", "Approval", "--id", "a3")
	contains(t, "terminate", h.ok("terminate", "a3", "--reason", "stuck"), "terminated a3")
	contains(t, "terminated", h.ok("describe", "a3"), "TERMINATED", "stuck")
}

func TestNamespacesAndErrors(t *testing.T) {
	h := newHarness(t)
	h.ok("start", "Approval", "--id", "shared", "-n", "acme", "--input", "acme's")
	h.ok("start", "Approval", "--id", "shared", "--input", "default's")
	contains(t, "acme by flag", h.ok("describe", "shared", "-n", "acme"), "acme's")
	h.env[envTokenFile] = filepath.Join(h.dir, "acme.token")
	contains(t, "acme by token", h.ok("describe", "shared"), "acme's")
	contains(t, "namespaces", h.ok("namespaces"), "* acme")

	code, _, errOut := h.run("", "list", "--namespace", "default")
	if code != 1 || !strings.Contains(errOut, "HTTP 403") {
		t.Fatalf("acme token in default: exit %d, %s", code, errOut)
	}
	code, _, errOut = h.run("", "describe", "nope")
	if code != 1 || !strings.Contains(errOut, "not found") {
		t.Fatalf("describe a missing workflow: exit %d, %s", code, errOut)
	}
	for _, args := range [][]string{{"describe"}, {"signal", "x"}, {"frobnicate"}, {"list", "--bogus"}, {"restore", "x.tgz"}} {
		if code, _, errOut := h.run("", args...); code != 2 {
			t.Errorf("ctl %v: exit %d, want 2 (usage)\n%s", args, code, errOut)
		}
	}
	if code, out, _ := h.run("", "start", "--help"); code != 0 || !strings.Contains(out, "-task-queue string") {
		t.Fatalf("start --help: %d\n%s", code, out)
	}
	delete(h.env, envTokenFile)
	if code, _, errOut := h.run("", "list"); code != 1 || !strings.Contains(errOut, "HTTP 401") {
		t.Fatalf("no token: exit %d, %s", code, errOut)
	}
}

func TestScheduleCommands(t *testing.T) {
	h := newHarness(t)
	contains(t, "create", h.ok("schedule", "create", "nightly", "--cron", "0 3 * * *", "--type", "Approval", "--timezone", "Europe/London"),
		"created schedule nightly")
	contains(t, "list", h.ok("schedules", "list"), "nightly", "0 3 * * * (Europe/London)", "Approval", "active")
	if code, _, _ := h.run("", "schedule", "create", "x", "--type", "Approval"); code != 2 {
		t.Fatalf("create without --cron: exit %d, want 2", code)
	}
	contains(t, "delete", h.ok("schedule", "delete", "nightly"), "deleted schedule nightly")
	if out := h.ok("schedule", "list"); strings.Contains(out, "nightly") {
		t.Fatalf("still listed:\n%s", out)
	}
}

// A backup from the engine (engine token only) restores into a new data
// directory that a fresh engine serves, namespaces included.
func TestBackupAndRestore(t *testing.T) {
	h := newHarness(t)
	h.ok("start", "Approval", "--id", "b1", "--input", "one")
	h.ok("start", "Approval", "--id", "b2", "-n", "acme")
	file := filepath.Join(h.dir, "backup.tar.gz")
	if code, _, errOut := h.run("", "backup", file); code != 0 || !strings.Contains(errOut, "backed up to") {
		t.Fatalf("backup: %d %s", code, errOut)
	}
	h.env[envTokenFile] = filepath.Join(h.dir, "acme.token")
	other := filepath.Join(h.dir, "nope.tar.gz")
	if code, _, errOut := h.run("", "backup", other); code != 1 || !strings.Contains(errOut, "HTTP 403") {
		t.Fatalf("backup with a namespace token: %d %s", code, errOut)
	}
	if _, err := os.Stat(other); !os.IsNotExist(err) {
		t.Fatal("a refused backup left a file behind")
	}

	restored := filepath.Join(h.dir, "restored")
	contains(t, "restore", h.ok("restore", "--data-dir", restored, file), "restored 2 histories", "history format 6")
	if code, _, errOut := h.run("", "restore", "--data-dir", restored, file); code != 1 || !strings.Contains(errOut, "not empty") {
		t.Fatalf("restore over a data dir: %d %s", code, errOut)
	}
	store, err := history.NewHistoryStore(restored)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	e := server.NewEngine(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer e.Close()
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	if r, err := e.In("acme").GetWorkflowRun("b2", ""); err != nil || r.GetSummary().GetWorkflowType() != "Approval" {
		t.Fatalf("restored acme/b2: %v %v", r, err)
	}

	// Offline, from a data directory.
	offline := filepath.Join(h.dir, "offline.tar.gz")
	h.ok("backup", "--data-dir", filepath.Join(h.dir, "data"), offline)
	contains(t, "offline restore", h.ok("restore", "--data-dir", filepath.Join(h.dir, "restored2"), offline), "restored 2 histories")
}

// Finished runs that retention archived are listed, described and printed
// from the archive, in their namespace.
func TestArchiveCommands(t *testing.T) {
	dir, err := archive.OpenDir(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, server.WithArchive(dir))
	h.ok("start", "Approval", "--id", "old-1", "--input", "archived input", "-n", "acme")
	h.ok("terminate", "old-1", "--reason", "done", "-n", "acme")
	if code, _, errOut := h.run("", "archive", "list", "-n", "acme"); code != 0 {
		t.Fatalf("archive list before any purge: %d %s", code, errOut)
	}
	if n, err := h.engine.PurgeTerminated(time.Now().Add(time.Hour)); n != 1 || err != nil {
		t.Fatalf("purged %d, %v", n, err)
	}
	contains(t, "archive list", h.ok("archive", "list", "-n", "acme"), "old-1")
	contains(t, "archive describe", h.ok("archive", "describe", "old-1", "-n", "acme"), "TERMINATED", "archived input")
	contains(t, "archive history", h.ok("archive", "history", "old-1", "-n", "acme"), "WorkflowStarted", "WorkflowFailed")
	if code, _, errOut := h.run("", "archive", "describe", "old-1"); code != 1 || !strings.Contains(errOut, "HTTP 404") {
		t.Fatalf("default namespace reading acme's archive: %d %s", code, errOut)
	}
}

func TestScheduleControlCommands(t *testing.T) {
	h := newHarness(t)
	contains(t, "dry run", h.ok("schedule", "create", "hourly", "--cron", "0 * * * *", "--type", "Approval", "--jitter", "30s", "--dry-run"),
		"FIRE", "RUN STARTS")
	if out := h.ok("schedule", "list"); strings.Contains(out, "hourly") {
		t.Fatalf("a dry run created the schedule:\n%s", out)
	}
	h.ok("schedule", "create", "hourly", "--cron", "0 * * * *", "--type", "Approval", "--overlap", "buffer_one", "--jitter", "30s")
	contains(t, "list", h.ok("schedule", "list"), "hourly", "BUFFER_ONE", "±30s", "active")
	contains(t, "pause", h.ok("schedule", "pause", "hourly", "--note", "migrating"), "paused schedule hourly")
	contains(t, "paused list", h.ok("schedule", "list"), "paused: migrating")
	contains(t, "preview", h.ok("schedule", "preview", "hourly", "--count", "3"), "paused:", "FIRE")
	contains(t, "backfill", h.ok("schedule", "backfill", "hourly", "--from", "3h"), "started", "already there")
	contains(t, "backfill again", h.ok("schedule", "backfill", "hourly", "--from", "3h"), "0 started")
	contains(t, "resume", h.ok("schedule", "resume", "hourly"), "resumed schedule hourly")
	if code, _, errOut := h.run("", "schedule", "backfill", "hourly", "--from", "nonsense"); code != 2 {
		t.Fatalf("bad --from: exit %d %s", code, errOut)
	}
	if code, _, errOut := h.run("", "schedule", "backfill", "hourly", "--from", "1h", "--to", "-1h"); code != 1 || !strings.Contains(errOut, "HTTP 400") {
		t.Fatalf("future --to: exit %d %s", code, errOut)
	}
}

func TestBulkActionsAndReset(t *testing.T) {
	h := newHarness(t)
	for _, id := range []string{"b1", "b2", "b3"} {
		h.ok("start", "Approval", "--id", id)
	}
	// Several workflows: asked first, and nothing happens without a yes.
	code, _, errOut := h.run("no\n", "terminate", "b1", "b2", "--reason", "cleanup")
	if code == 0 || !strings.Contains(errOut, "terminate 2 workflows: b1, b2") || !strings.Contains(errOut, "nothing changed") {
		t.Fatalf("declined bulk terminate: exit %d\n%s", code, errOut)
	}
	contains(t, "still waiting", h.ok("describe", "b1"), "WAITING")
	code, out, errOut := h.run("yes\n", "terminate", "b1", "b2", "--reason", "cleanup")
	if code != 0 {
		t.Fatalf("bulk terminate: exit %d\n%s%s", code, out, errOut)
	}
	contains(t, "bulk terminate", out, "terminated b1", "terminated b2", "2 terminated, 0 failed")
	// By filter, with --yes; a workflow that already finished fails, and the exit says so.
	code, out, errOut = h.run("", "cancel", "--type", "Approval", "--yes")
	if code != 1 || !strings.Contains(out, "cancelled b3") || !strings.Contains(out, "1 cancelled, 2 failed") || !strings.Contains(errOut, "b1:") {
		t.Fatalf("cancel by filter: exit %d\n%s%s", code, out, errOut)
	}
	if code, _, errOut := h.run("", "cancel", "b1", "--type", "Approval"); code != 2 || !strings.Contains(errOut, "not both") {
		t.Fatalf("ids and a filter: exit %d %s", code, errOut)
	}

	// Reset: the terminated run starts again, waiting for its signal.
	contains(t, "reset", h.ok("reset", "b1", "--reason", "retry"), "b1: new run", "from event 2")
	contains(t, "after reset", h.ok("describe", "b1"), "WAITING")
	contains(t, "runs after reset", h.ok("runs", "b1"), "TERMINATED", "current")
	if code, _, errOut := h.run("", "reset", "b1"); code != 1 || !strings.Contains(errOut, "still running") && !strings.Contains(errOut, "only a finished run") {
		t.Fatalf("reset of a live run: exit %d %s", code, errOut)
	}
	if code, _, errOut := h.run("", "reset", "b2", "b3", "--event", "2"); code != 2 || !strings.Contains(errOut, "exactly one") {
		t.Fatalf("--event with two ids: exit %d %s", code, errOut)
	}
	var res struct {
		Results []struct {
			WorkflowID string         `json:"workflow_id"`
			OK         bool           `json:"ok"`
			Result     map[string]any `json:"result"`
		} `json:"results"`
	}
	if err := json.Unmarshal([]byte(h.ok("reset", "--status", "cancelled", "--yes", "--json")), &res); err != nil ||
		len(res.Results) != 1 || res.Results[0].WorkflowID != "b3" || !res.Results[0].OK || res.Results[0].Result["run_id"] == "" {
		t.Fatalf("reset --json by status: %+v %v", res, err)
	}
}

func TestAuditCommand(t *testing.T) {
	h := newHarness(t)
	h.ok("start", "Approval", "--id", "au1", "--input", "private")
	h.ok("terminate", "au1", "--reason", "cleanup")
	out := h.ok("audit")
	contains(t, "audit", out, "ACTOR", "workflow.terminate", "au1", "reason=cleanup", "workflow.start", "engine:")
	if strings.Contains(out, "private") {
		t.Fatalf("the audit shows an input:\n%s", out)
	}
	if strings.Index(out, "workflow.terminate") > strings.Index(out, "workflow.start") {
		t.Fatalf("not newest first:\n%s", out)
	}
	contains(t, "audit --action", h.ok("audit", "--action", "workflow.start"), "workflow.start")
	if out := h.ok("audit", "--action", "workflow.start"); strings.Contains(out, "terminate") {
		t.Fatalf("--action did not filter:\n%s", out)
	}
	// A namespace token sees its namespace only.
	h.env[envTokenFile] = filepath.Join(h.dir, "acme.token")
	if out := h.ok("audit"); strings.Contains(out, "au1") {
		t.Fatalf("acme's token sees the default namespace's actions:\n%s", out)
	}
}
