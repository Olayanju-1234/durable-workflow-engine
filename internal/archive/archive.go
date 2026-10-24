// Package archive keeps finished workflow histories outside the data
// directory (ADR-0019): retention exports a history here, and only deletes it
// once the archive has it. A store is a local directory or an S3-compatible
// bucket (AWS S3, Cloudflare R2, MinIO), signed by hand with SigV4.
//
// An archived history is the history file's bytes, unchanged, under
//
//	<namespace>/<workflow id>/<closed at, 16 hex digits>~<run id>.history
//
// so names sort by when the run closed and every binary that reads history
// files reads an archived one.
package archive

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
)

// ErrNotFound: the archive holds nothing under that key.
var ErrNotFound = errors.New("archive: not found")

// Store is where archived histories live.
type Store interface {
	// Put stores data under key, replacing anything there, and returns only
	// once the store has it durably and intact.
	Put(ctx context.Context, key string, data []byte) error
	// Get returns what Put stored, or ErrNotFound.
	Get(ctx context.Context, key string) ([]byte, error)
	// List returns every key that starts with prefix, sorted.
	List(ctx context.Context, prefix string) ([]string, error)
	// String says where the store is, without secrets.
	String() string
}

// Run names one archived run.
type Run struct {
	Namespace, WorkflowID, RunID string
	ClosedAt                     int64 // unix nanoseconds
}

const ext = ".history"

// Key is where a run is archived.
func (r Run) Key() string {
	return fmt.Sprintf("%s/%s/%016x~%s%s", r.Namespace, r.WorkflowID, r.ClosedAt, r.RunID, ext)
}

// ParseKey is Key's inverse; ok is false for anything else in the store.
func ParseKey(key string) (Run, bool) {
	parts := strings.Split(key, "/")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" {
		return Run{}, false
	}
	name, ok := strings.CutSuffix(parts[2], ext)
	if !ok {
		return Run{}, false
	}
	at, run, ok := strings.Cut(name, "~")
	if !ok || len(at) != 16 || run == "" {
		return Run{}, false
	}
	n, err := strconv.ParseUint(at, 16, 64)
	if err != nil {
		return Run{}, false
	}
	return Run{Namespace: parts[0], WorkflowID: parts[1], RunID: run, ClosedAt: int64(n)}, true
}

// Credentials sign requests to an S3-compatible store.
type Credentials struct {
	AccessKeyID, SecretAccessKey string
}

// Open opens a store from its --archive spec: a directory (a path, or
// file:///path), or s3://bucket[/prefix][?endpoint=URL&region=R] (region
// defaults to us-east-1, or auto with an R2 endpoint).
func Open(spec string, creds Credentials) (Store, error) {
	switch {
	case strings.HasPrefix(spec, "s3://"):
		u, err := url.Parse(spec)
		if err != nil {
			return nil, fmt.Errorf("archive: %q: %w", spec, err)
		}
		return newS3(u, creds)
	case strings.HasPrefix(spec, "file://"):
		u, err := url.Parse(spec)
		if err != nil {
			return nil, fmt.Errorf("archive: %q: %w", spec, err)
		}
		return OpenDir(u.Path)
	case strings.Contains(spec, "://"):
		return nil, fmt.Errorf("archive: %q: want a directory, file:///path or s3://bucket/prefix", spec)
	default:
		return OpenDir(spec)
	}
}

// Dir archives into a local directory, one file per key.
type Dir struct{ root string }

// OpenDir opens (creating) a directory store.
func OpenDir(root string) (*Dir, error) {
	if root == "" {
		return nil, errors.New("archive: empty directory")
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("archive: %w", err)
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, fmt.Errorf("archive: %w", err)
	}
	return &Dir{root: abs}, nil
}

func (d *Dir) String() string { return "dir:" + d.root }

// path maps a key to a file under the root, refusing anything that would
// leave it.
func (d *Dir) path(key string) (string, error) {
	for _, part := range strings.Split(key, "/") {
		if part == "" || part == "." || part == ".." || strings.ContainsRune(part, '\\') {
			return "", fmt.Errorf("archive: invalid key %q", key)
		}
	}
	return filepath.Join(d.root, filepath.FromSlash(key)), nil
}

// Put writes the file whole (temp file, fsync, rename, fsync directory).
func (d *Dir) Put(_ context.Context, key string, data []byte) error {
	p, err := d.path(key)
	if err != nil {
		return err
	}
	dir := filepath.Dir(p)
	created := false
	if _, err := os.Stat(dir); errors.Is(err, os.ErrNotExist) {
		created = true
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("archive: %w", err)
	}
	if created {
		// The new directories must survive a crash as well as the file.
		for parent := dir; parent != d.root && strings.HasPrefix(parent, d.root); parent = filepath.Dir(parent) {
			if err := record.SyncDir(filepath.Dir(parent)); err != nil {
				return fmt.Errorf("archive: %w", err)
			}
		}
	}
	if err := record.ReplaceFile(dir, filepath.Base(p), data); err != nil {
		return fmt.Errorf("archive: put %s: %w", key, err)
	}
	return nil
}

func (d *Dir) Get(_ context.Context, key string) ([]byte, error) {
	p, err := d.path(key)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("%w: %s", ErrNotFound, key)
	}
	if err != nil {
		return nil, fmt.Errorf("archive: get %s: %w", key, err)
	}
	return data, nil
}

func (d *Dir) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	err := filepath.WalkDir(d.root, func(p string, e fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if e.IsDir() || strings.HasSuffix(p, ".tmp") {
			return nil
		}
		rel, err := filepath.Rel(d.root, p)
		if err != nil {
			return err
		}
		if key := filepath.ToSlash(rel); strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("archive: list: %w", err)
	}
	sort.Strings(keys)
	return keys, nil
}
