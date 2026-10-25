// Package audit records operator actions (ADR-0023): who started, signalled,
// cancelled, terminated or reset a workflow, changed a schedule, took a
// backup or reloaded the engine's tokens, keys and namespaces, and when. One
// JSON object per line, appended and fsync'd before the action's answer goes
// out, in files that rotate by size.
package audit

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

// Entry is one operator action.
type Entry struct {
	Time time.Time `json:"time"`
	// Actor is who did it: "engine:<hash>" (the engine token), "token:<hash>"
	// (a namespace token), "anonymous", or "operator" for a reload. A hash is
	// the first 16 hex digits of the token's SHA-256, never the token.
	Actor     string `json:"actor"`
	Namespace string `json:"namespace,omitempty"`
	// Action is what: workflow.start, workflow.signal, workflow.cancel,
	// workflow.request_cancel, workflow.terminate, workflow.reset,
	// schedule.create|delete|pause|resume|backfill, admin.backup, config.reload.
	Action string `json:"action"`
	// Target is the workflow or schedule id acted on.
	Target  string `json:"target,omitempty"`
	RunID   string `json:"run_id,omitempty"`
	Outcome string `json:"outcome"` // "ok" or "error"
	// Status is the answer's HTTP status (gRPC calls are mapped to one).
	Status int    `json:"status,omitempty"`
	Error  string `json:"error,omitempty"`
	Remote string `json:"remote,omitempty"`
	// Via is how it came: http, grpc, signal (SIGHUP) or file (a file changed).
	Via string `json:"via"`
	// Details are the request's settings: a reason, a signal's name, a
	// workflow type. Never a payload or an input.
	Details map[string]string `json:"details,omitempty"`
}

// Log is an append-only audit log. A nil *Log records nothing.
type Log struct {
	path     string
	maxBytes int64
	keep     int

	mu   sync.Mutex
	f    *os.File
	size int64
}

// Defaults: rotate at 16 MiB, keep the current file and 4 earlier ones.
const (
	DefaultMaxBytes = 16 << 20
	DefaultKeep     = 4
)

// Open opens (or creates) the log at path, creating its directory.
func Open(path string) (*Log, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return nil, fmt.Errorf("audit: %w", err)
	}
	l := &Log{path: path, maxBytes: DefaultMaxBytes, keep: DefaultKeep}
	if err := l.openLocked(); err != nil {
		return nil, err
	}
	return l, nil
}

func (l *Log) openLocked() error {
	f, err := os.OpenFile(l.path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return fmt.Errorf("audit: open %s: %w", l.path, err)
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return fmt.Errorf("audit: %w", err)
	}
	l.f, l.size = f, st.Size()
	return nil
}

// Path is where the current file is.
func (l *Log) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

// Record appends e durably (written and fsync'd) and rotates the file once it
// has grown past its limit. A nil log records nothing.
func (l *Log) Record(e Entry) error {
	if l == nil {
		return nil
	}
	if e.Time.IsZero() {
		e.Time = time.Now()
	}
	e.Time = e.Time.UTC()
	line, err := json.Marshal(e)
	if err != nil {
		return fmt.Errorf("audit: encode: %w", err)
	}
	line = append(line, '\n')
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return errors.New("audit: log is closed")
	}
	if _, err := l.f.Write(line); err != nil {
		return fmt.Errorf("audit: write: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("audit: fsync: %w", err)
	}
	l.size += int64(len(line))
	if l.size >= l.maxBytes {
		return l.rotateLocked()
	}
	return nil
}

// rotateLocked renames audit.log to audit.log.1 (shifting older ones up and
// dropping the oldest) and starts a fresh file.
func (l *Log) rotateLocked() error {
	if err := l.f.Close(); err != nil {
		return fmt.Errorf("audit: close for rotation: %w", err)
	}
	l.f = nil
	os.Remove(l.rotated(l.keep))
	for i := l.keep - 1; i >= 1; i-- {
		if err := os.Rename(l.rotated(i), l.rotated(i+1)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("audit: rotate: %w", err)
		}
	}
	if err := os.Rename(l.path, l.rotated(1)); err != nil {
		return fmt.Errorf("audit: rotate: %w", err)
	}
	return l.openLocked()
}

func (l *Log) rotated(i int) string { return l.path + "." + strconv.Itoa(i) }

// Read returns entries that match, newest first, at most limit (0: all).
func (l *Log) Read(match func(Entry) bool, limit int) ([]Entry, error) {
	if l == nil {
		return nil, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Entry
	files := []string{l.path}
	for i := 1; i <= l.keep; i++ {
		files = append(files, l.rotated(i))
	}
	for _, path := range files {
		data, err := os.ReadFile(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("audit: read %s: %w", path, err)
		}
		var entries []Entry
		sc := bufio.NewScanner(bytes.NewReader(data))
		sc.Buffer(make([]byte, 64<<10), 4<<20)
		for sc.Scan() {
			var e Entry
			if json.Unmarshal(sc.Bytes(), &e) == nil {
				entries = append(entries, e)
			}
		}
		for i := len(entries) - 1; i >= 0; i-- {
			if match == nil || match(entries[i]) {
				out = append(out, entries[i])
				if limit > 0 && len(out) == limit {
					return out, nil
				}
			}
		}
	}
	return out, nil
}

// Close closes the current file.
func (l *Log) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f == nil {
		return nil
	}
	err := l.f.Close()
	l.f = nil
	return err
}
