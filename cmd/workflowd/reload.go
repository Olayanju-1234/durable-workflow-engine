package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/audit"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// reloadConfig names the files a running engine re-reads (ADR-0023): the
// engine token, the metrics token, the namespaces (their tokens and settings)
// and the encryption keys. Secrets given by flag or environment are fixed.
type reloadConfig struct {
	authTokenFile, metricsTokenFile, namespacesFile, keysFile string
	// interval is how often the files are checked for changes (0: only on SIGHUP).
	interval time.Duration
}

func (c reloadConfig) files() []string {
	var out []string
	for _, f := range []string{c.authTokenFile, c.metricsTokenFile, c.namespacesFile, c.keysFile} {
		if f != "" {
			out = append(out, f)
		}
	}
	return out
}

// reloader applies a reloadConfig to a running engine. A reload reads every
// file first and changes nothing unless all of them parse, so a half-written
// or mistyped file leaves the engine as it was (and says why).
type reloader struct {
	cfg    reloadConfig
	sec    *security
	store  *history.HistoryStore
	audit  *audit.Log
	logger *slog.Logger

	mu   sync.Mutex
	sums map[string][32]byte // each file's content when last applied
}

func newReloader(cfg reloadConfig, sec *security, store *history.HistoryStore, logger *slog.Logger) *reloader {
	r := &reloader{cfg: cfg, sec: sec, store: store, audit: sec.audit, logger: logger, sums: map[string][32]byte{}}
	for _, f := range cfg.files() {
		if data, err := os.ReadFile(f); err == nil {
			r.sums[f] = sha256.Sum256(data)
		}
	}
	return r
}

// run reloads on SIGHUP, and whenever a file's content changes, until ctx ends.
func (r *reloader) run(ctx context.Context) {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	defer signal.Stop(hup)
	var tick <-chan time.Time
	if r.cfg.interval > 0 && len(r.cfg.files()) > 0 {
		t := time.NewTicker(r.cfg.interval)
		defer t.Stop()
		tick = t.C
	}
	for {
		select {
		case <-ctx.Done():
			return
		case <-hup:
			_ = r.reload("signal")
		case <-tick:
			if r.changed() {
				_ = r.reload("file")
			}
		}
	}
}

// changed reports whether any file's content differs from what was applied.
func (r *reloader) changed() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, f := range r.cfg.files() {
		data, err := os.ReadFile(f)
		if err != nil {
			continue // mid-replace, or gone: the next check sees the new one
		}
		if sha256.Sum256(data) != r.sums[f] {
			return true
		}
	}
	return false
}

// reload re-reads every file and applies them together. via says what asked:
// "signal" (SIGHUP) or "file" (a change was seen).
func (r *reloader) reload(via string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.cfg.files()) == 0 {
		r.logger.Info("reload: nothing to reload (no token, namespaces or key files were given)")
		return nil
	}
	details := map[string]string{}
	var (
		admin, metrics string
		namespaces     *server.NamespaceSet
		keys           *history.Keyring
		errs           []error
		sums           = map[string][32]byte{}
	)
	read := func(path string) {
		if data, err := os.ReadFile(path); err == nil {
			sums[path] = sha256.Sum256(data)
		}
	}
	if f := r.cfg.authTokenFile; f != "" {
		var err error
		if admin, err = readSecretFile("--auth-token-file", f, r.logger); err != nil {
			errs = append(errs, err)
		}
		read(f)
	}
	if f := r.cfg.metricsTokenFile; f != "" {
		var err error
		if metrics, err = readSecretFile("--metrics-auth-token-file", f, r.logger); err != nil {
			errs = append(errs, err)
		}
		read(f)
	}
	if f := r.cfg.namespacesFile; f != "" {
		var err error
		if namespaces, err = server.LoadNamespaces(f); err != nil {
			errs = append(errs, err)
		}
		read(f)
	}
	if f := r.cfg.keysFile; f != "" {
		var err error
		if keys, err = history.LoadKeyring(f); err != nil {
			errs = append(errs, err)
		}
		read(f)
	}
	entry := audit.Entry{Actor: "operator", Action: "config.reload", Via: via, Outcome: "ok", Details: details}
	if err := errors.Join(errs...); err != nil {
		entry.Outcome, entry.Error = "error", err.Error()
		r.record(entry)
		r.logger.Error("reload refused: the engine keeps its tokens, namespaces and keys as they were", "via", via, "err", err)
		return err
	}

	var changed []string
	if r.cfg.authTokenFile != "" && admin != r.sec.authz.AdminToken() {
		r.sec.authz.SetAdminToken(admin)
		changed = append(changed, "engine token")
		details["engine_token"] = "replaced"
	}
	if r.cfg.metricsTokenFile != "" && metrics != *r.sec.metrics.Load() {
		r.sec.metrics.Store(&metrics)
		changed = append(changed, "metrics token")
		details["metrics_token"] = "replaced"
	}
	if namespaces != nil && r.sec.namespaces != nil {
		added, removed, edited := r.sec.namespaces.Diff(namespaces)
		r.sec.namespaces.Replace(namespaces)
		for k, v := range map[string][]string{"namespaces_added": added, "namespaces_removed": removed, "namespaces_changed": edited} {
			if len(v) > 0 {
				details[k] = strings.Join(v, ",")
				changed = append(changed, k)
			}
		}
	}
	if keys != nil {
		old := r.store.Keyring()
		var added, removed []string
		for _, id := range keys.IDs() {
			if !old.Has(id) {
				added = append(added, id)
			}
		}
		for _, id := range old.IDs() {
			if !keys.Has(id) {
				removed = append(removed, id)
			}
		}
		if len(added)+len(removed) > 0 || keys.Active() != old.Active() {
			changed = append(changed, "encryption keys")
			details["active_key"] = keys.Active()
			if len(added) > 0 {
				details["keys_added"] = strings.Join(added, ",")
			}
			if len(removed) > 0 {
				details["keys_removed"] = strings.Join(removed, ",")
				r.logger.Warn("reload: keys removed from the key file; events they sealed can no longer be read (run --check-format first)",
					"removed", removed)
			}
		}
		r.store.SetKeyring(keys)
	}
	r.sums = sums
	if len(changed) == 0 {
		r.logger.Info("reload: nothing changed", "via", via)
		return nil
	}
	slices.Sort(changed)
	details["changed"] = strings.Join(changed, ",")
	r.record(entry)
	r.logger.Info("reload applied", "via", via, "changed", changed)
	return nil
}

func (r *reloader) record(e audit.Entry) {
	if err := r.audit.Record(e); err != nil {
		r.logger.Error("audit log write failed", "action", e.Action, "err", err)
	}
}

// describe says what a reload covers, for the startup log.
func (c reloadConfig) describe() string {
	if len(c.files()) == 0 {
		return "nothing (no token, namespaces or key files)"
	}
	how := "on SIGHUP"
	if c.interval > 0 {
		how = fmt.Sprintf("on SIGHUP and when they change (checked every %s)", c.interval)
	}
	return strings.Join(c.files(), ", ") + ", " + how
}
