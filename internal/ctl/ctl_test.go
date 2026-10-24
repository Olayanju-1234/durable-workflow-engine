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

func newHarness(t *testing.T) *harness {
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
	e := server.NewEngine(store, logger, server.WithNamespaces(set))
	t.Cleanup(e.Close)
	approval := replay.Planner{Fn: func(ctx workflow.Context, in []byte) ([]byte, error) {
		got := ctx.WaitForSignal("approval", "approve")
		return []byte("approved by " + string(got)), nil
	}}
	for _, ns := range []string{"", "acme"} {
		e.In(ns).RegisterWorkflow("Approval", approval)
	}
	api := server.NewHTTPAPI(e, logger, server.WithAuthToken("admin"), server.WithNamespaceSet(set))
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
