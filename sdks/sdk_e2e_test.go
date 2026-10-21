// Package sdks_test runs the TypeScript and Python SDKs end to end against a
// real engine. Each language has a script that drives the same scenario and
// exits non-zero on any mismatch; a test is skipped when its runtime is not
// installed.
package sdks_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// e2eEngine serves the HTTP API of an engine that the test can restart in
// place, plus a fake cluster follower that only ever answers 503 naming it.
type e2eEngine struct {
	t     *testing.T
	store *history.HistoryStore
	log   *slog.Logger

	mu      sync.Mutex
	engine  *server.Engine
	handler http.Handler

	leader   *httptest.Server
	follower *httptest.Server
}

func newE2EEngine(t *testing.T) *e2eEngine {
	t.Helper()
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	h := &e2eEngine{t: t, store: store, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
	h.leader = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.mu.Lock()
		next := h.handler
		h.mu.Unlock()
		next.ServeHTTP(w, r)
	}))
	t.Cleanup(h.leader.Close)
	leaderAddr := strings.TrimPrefix(h.leader.URL, "http://")
	h.follower = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Workflowd-Leader", "n1")
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprintf(w, `{"error":"not the leader","leader_id":"n1","leader_http":%q}`, leaderAddr)
	}))
	t.Cleanup(h.follower.Close)
	h.restart()
	t.Cleanup(func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.engine.Close()
	})
	return h
}

// restart replaces the engine with a fresh one over the same history, the way
// a process restart or a new leader does: registrations are forgotten.
func (h *e2eEngine) restart() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.engine != nil {
		h.engine.Close()
	}
	// A short lease, so an activity that runs longer survives only by heartbeating.
	e := server.NewEngine(h.store, h.log, server.WithLeaseDuration(600*time.Millisecond))
	if err := e.Recover(); err != nil {
		h.t.Fatalf("recover: %v", err)
	}
	h.engine, h.handler = e, server.NewHTTPAPI(e, h.log).Handler()
}

// runScript runs an SDK scenario script. It prints "RESTART" when it wants the
// engine restarted and waits for a line on stdin; "PASS" means it succeeded.
func runScript(t *testing.T, h *e2eEngine, name string, args ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, append(args, h.follower.URL, h.leader.URL)...)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	passed := false
	sc := bufio.NewScanner(stdout)
	for sc.Scan() {
		line := sc.Text()
		t.Log(line)
		switch line {
		case "RESTART":
			h.restart()
			fmt.Fprintln(stdin, "go")
		case "PASS":
			passed = true
		}
	}
	err = cmd.Wait()
	if err != nil || !passed {
		t.Fatalf("%s %v: exit %v, passed=%v", name, args, err, passed)
	}
}

func TestTypeScriptSDK(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}
	runScript(t, newE2EEngine(t), node, "typescript/e2e.mjs")
}

func TestPythonSDK(t *testing.T) {
	py, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not installed")
	}
	runScript(t, newE2EEngine(t), py, "python/e2e.py")
}
