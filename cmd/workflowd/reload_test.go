package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/audit"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

type reloadHarness struct {
	dir         string
	adminFile   string
	acmeFile    string
	metricsFile string
	nsFile      string
	sec         security
	store       *history.HistoryStore
	r           *reloader
	api         *httptest.Server
	metrics     *httptest.Server
	audit       *audit.Log
}

func write(t *testing.T, path, content string) {
	t.Helper()
	// Written beside and renamed, as an operator's tooling should: a reader
	// never sees half a file.
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, path); err != nil {
		t.Fatal(err)
	}
}

func newReloadHarness(t *testing.T, interval time.Duration) *reloadHarness {
	t.Helper()
	dir := t.TempDir()
	h := &reloadHarness{dir: dir, adminFile: filepath.Join(dir, "admin.token"), acmeFile: filepath.Join(dir, "acme.token"),
		metricsFile: filepath.Join(dir, "metrics.token"), nsFile: filepath.Join(dir, "namespaces.json")}
	write(t, h.adminFile, "admin-1\n")
	write(t, h.acmeFile, "acme-1\n")
	write(t, h.metricsFile, "metrics-1\n")
	write(t, h.nsFile, `{"namespaces":[{"name":"acme","token_file":"acme.token"}]}`)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	f := registerSecurityFlags(fs)
	if err := fs.Parse([]string{"--auth-token-file", h.adminFile, "--metrics-auth-token-file", h.metricsFile, "--namespaces", h.nsFile}); err != nil {
		t.Fatal(err)
	}
	sec, err := f.resolve(logger)
	if err != nil {
		t.Fatal(err)
	}
	if sec.audit, err = audit.Open(filepath.Join(dir, "data", "audit", "audit.log")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sec.audit.Close() })
	h.audit = sec.audit
	if h.store, err = history.NewHistoryStore(filepath.Join(dir, "data")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { h.store.Close() })
	e := server.NewEngine(h.store, logger, server.WithNamespaces(sec.namespaces))
	t.Cleanup(e.Close)
	api := server.NewHTTPAPI(e, logger, sec.httpOptions()...)
	h.api = httptest.NewServer(api.Wrap(api.Handler()))
	t.Cleanup(h.api.Close)
	h.metrics = httptest.NewServer(sec.metricsHandler(e.Metrics().Handler()))
	t.Cleanup(h.metrics.Close)
	h.sec = sec
	h.r = newReloader(reloadConfig{authTokenFile: h.adminFile, metricsTokenFile: h.metricsFile, namespacesFile: h.nsFile, interval: interval},
		&h.sec, h.store, logger)
	return h
}

// status of GET path with a bearer token.
func (h *reloadHarness) status(t *testing.T, base, path, token string) int {
	t.Helper()
	req, _ := http.NewRequest("GET", base+path, nil)
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	return res.StatusCode
}

func hash16(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:8])
}

// A rotated engine token, namespace token and metrics token take effect
// without a restart: the old ones stop working at once, and the audit log
// says what changed.
func TestReloadSwapsTokensWithoutARestart(t *testing.T) {
	h := newReloadHarness(t, 0)
	for _, c := range []struct{ token string }{{"admin-1"}, {"acme-1"}} {
		if code := h.status(t, h.api.URL, "/api/workflows", c.token); code != http.StatusOK {
			t.Fatalf("%s before the reload: %d", c.token, code)
		}
	}
	write(t, h.adminFile, "admin-2\n")
	write(t, h.acmeFile, "acme-2\n")
	write(t, h.metricsFile, "metrics-2\n")
	if err := h.r.reload("signal"); err != nil {
		t.Fatal(err)
	}
	for token, want := range map[string]int{"admin-1": 401, "admin-2": 200, "acme-1": 401, "acme-2": 200} {
		if code := h.status(t, h.api.URL, "/api/workflows", token); code != want {
			t.Errorf("%s after the reload: %d, want %d", token, code, want)
		}
	}
	if code := h.status(t, h.metrics.URL, "/", "metrics-1"); code != 401 {
		t.Errorf("old metrics token: %d", code)
	}
	if code := h.status(t, h.metrics.URL, "/", "metrics-2"); code != 200 {
		t.Errorf("new metrics token: %d", code)
	}
	got, _ := h.audit.Read(func(e audit.Entry) bool { return e.Action == "config.reload" }, 0)
	if len(got) != 1 || got[0].Actor != "operator" || got[0].Via != "signal" || got[0].Outcome != "ok" ||
		got[0].Details["engine_token"] != "replaced" || got[0].Details["metrics_token"] != "replaced" ||
		got[0].Details["namespaces_changed"] != "acme" {
		t.Fatalf("audit %+v", got)
	}
	raw, _ := os.ReadFile(h.audit.Path())
	if strings.Contains(string(raw), "admin-2") || strings.Contains(string(raw), "acme-2") {
		t.Fatal("the audit log holds a token")
	}
	// The new tokens' actions are recorded under their hashes.
	req, _ := http.NewRequest("POST", h.api.URL+"/api/workflows", strings.NewReader(`{"type":"Nope"}`))
	req.Header.Set("Authorization", "Bearer admin-2")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if last, _ := h.audit.Read(nil, 1); last[0].Actor != "engine:"+hash16("admin-2") {
		t.Fatalf("actor %q", last[0].Actor)
	}

	// Reloading files that did not change records nothing.
	if err := h.r.reload("signal"); err != nil {
		t.Fatal(err)
	}
	if again, _ := h.audit.Read(func(e audit.Entry) bool { return e.Action == "config.reload" }, 0); len(again) != 1 {
		t.Fatalf("a no-op reload was recorded: %+v", again)
	}
}

// A file that does not parse changes nothing, not even the files that did.
func TestReloadRefusesABadFileAndKeepsEverything(t *testing.T) {
	h := newReloadHarness(t, 0)
	write(t, h.adminFile, "admin-2\n")
	write(t, h.nsFile, `{"namespaces":[{"name":"acme","token_file":"acme.token"`) // cut short
	if err := h.r.reload("file"); err == nil {
		t.Fatal("a broken namespaces file was applied")
	}
	if code := h.status(t, h.api.URL, "/api/workflows", "admin-1"); code != 200 {
		t.Fatalf("the old engine token stopped working: %d", code)
	}
	if code := h.status(t, h.api.URL, "/api/workflows", "acme-1"); code != 200 {
		t.Fatalf("the old namespace token stopped working: %d", code)
	}
	got, _ := h.audit.Read(nil, 1)
	if len(got) != 1 || got[0].Outcome != "error" || got[0].Via != "file" || got[0].Error == "" {
		t.Fatalf("audit %+v", got)
	}
}

// A running engine picks changes up by itself, and on SIGHUP.
func TestReloadOnChangeAndOnSIGHUP(t *testing.T) {
	h := newReloadHarness(t, 30*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { h.r.run(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	time.Sleep(50 * time.Millisecond) // the handler is installed

	write(t, h.adminFile, "admin-2\n")
	waitStatus(t, h, "admin-2", 200, "a changed file")

	// Now only the signal: the watcher is told the next change is already applied.
	h.r.mu.Lock()
	write(t, h.adminFile, "admin-3\n")
	h.r.sums[h.adminFile] = sha256.Sum256([]byte("admin-3\n")) // the watcher would think it is applied
	h.r.mu.Unlock()
	if code := h.status(t, h.api.URL, "/api/workflows", "admin-3"); code != 401 {
		t.Fatalf("admin-3 before the signal: %d", code)
	}
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, h, "admin-3", 200, "SIGHUP")
}

func waitStatus(t *testing.T, h *reloadHarness, token string, want int, what string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for h.status(t, h.api.URL, "/api/workflows", token) != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s: %s never got %d", what, token, want)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// Rotating the encryption key without a restart: new events are sealed with
// the new active key.
func TestReloadEncryptionKeys(t *testing.T) {
	h := newReloadHarness(t, 0)
	keys := newKeys(t, "k1", "k2")
	path := filepath.Join(h.dir, "keys.json")
	write(t, path, `{"active":"k1","keys":[{"id":"k1","key":"`+keys["k1"]+`"}]}`)
	k1, err := loadKeys(path)
	if err != nil {
		t.Fatal(err)
	}
	h.store.SetKeyring(k1)
	h.r.cfg.keysFile = path
	write(t, path, `{"active":"k2","keys":[{"id":"k1","key":"`+keys["k1"]+`"},{"id":"k2","key":"`+keys["k2"]+`"}]}`)
	if err := h.r.reload("signal"); err != nil {
		t.Fatal(err)
	}
	if got := h.store.Keyring().Active(); got != "k2" {
		t.Fatalf("active key %q after the reload", got)
	}
	last, _ := h.audit.Read(nil, 1)
	if last[0].Details["active_key"] != "k2" || last[0].Details["keys_added"] != "k2" {
		t.Fatalf("audit %+v", last[0])
	}
	write(t, path, `{"active":"k3","keys":[]}`)
	if err := h.r.reload("signal"); err == nil || h.store.Keyring().Active() != "k2" {
		t.Fatalf("a key file without its active key: %v, active %q", err, h.store.Keyring().Active())
	}
}
