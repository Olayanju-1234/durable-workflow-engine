package history

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
)

// On-disk framing is package record's, repeated once per event:
//
//	[4B big-endian length N][4B big-endian CRC32C of payload][N-byte protobuf payload]
const (
	headerSize = record.HeaderSize
	// historyExt is the on-disk suffix for a workflow's log; the file's base name
	// (minus this suffix) is the workflow id. ListWorkflows relies on this mapping.
	historyExt = ".history"
)

// ErrCorruptRecord is returned when a complete record's CRC fails to verify.
var ErrCorruptRecord = record.ErrCorrupt

// HistoryStore is an append-only, per-workflow event log persisted to disk. It
// is the single source of truth for workflow state and is safe for concurrent use.
type HistoryStore struct {
	dir string

	mu    sync.Mutex
	files map[string]*os.File // workflowID -> open append handle
	dirty map[string]bool     // written by AppendBuffered and not yet fsync'd
	// dirDirty: AppendBuffered created a file whose directory entry is not yet
	// fsync'd (Sync does it, with the files).
	dirDirty bool

	// writeFile is (*os.File).Write; tests swap it to inject a torn write.
	writeFile func(f *os.File, b []byte) (int, error)
}

// NewHistoryStore opens a store rooted at dir, creating the directory if needed.
func NewHistoryStore(dir string) (*HistoryStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("history: create data dir: %w", err)
	}
	return &HistoryStore{dir: dir, files: make(map[string]*os.File), dirty: make(map[string]bool), writeFile: (*os.File).Write}, nil
}

// Append serializes event and writes it as a single durable record to the
// workflow's history file, fsyncing before returning. A successful Append means
// the event has survived to disk and will be recovered after a crash.
func (s *HistoryStore) Append(workflowID string, event *HistoryEvent) error {
	return s.append(workflowID, event, true)
}

// AppendBuffered is Append without the fsync: the record is written, readable
// at once, but may not survive a crash until Sync. It is for callers that keep
// their own write-ahead log and can replay what a crash loses, such as a
// cluster node, whose Raft log holds every event until a snapshot syncs it.
func (s *HistoryStore) AppendBuffered(workflowID string, event *HistoryEvent) error {
	return s.append(workflowID, event, false)
}

// Sync fsyncs every history written by AppendBuffered since the last Sync,
// and the directory entries of any files it created.
func (s *HistoryStore) Sync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.dirDirty {
		if err := s.syncDir(); err != nil {
			return err
		}
		s.dirDirty = false
	}
	for id := range s.dirty {
		f, cached := s.files[id]
		if !cached {
			// Released since it was written: fsync through a fresh descriptor,
			// which flushes the same file.
			var err error
			if f, err = os.Open(s.path(id)); err != nil {
				return fmt.Errorf("history: open %s to fsync: %w", id, err)
			}
		}
		err := f.Sync()
		if !cached {
			f.Close()
		}
		if err != nil {
			return fmt.Errorf("history: fsync %s: %w", id, err)
		}
		delete(s.dirty, id)
	}
	return nil
}

func (s *HistoryStore) append(workflowID string, event *HistoryEvent, sync bool) error {
	if err := validateWorkflowID(workflowID); err != nil {
		return err
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("history: marshal event: %w", err)
	}
	// One buffer, one write call to keep the torn-write window as small as possible.
	rec, err := record.Encode(payload)
	if err != nil {
		return fmt.Errorf("history: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := s.fileLocked(workflowID, sync)
	if err != nil {
		return err
	}
	// os.File.Write errors whenever it writes short, so err alone catches a torn
	// record. Dropping the handle makes the next Append reopen and truncate it.
	if _, err := s.writeFile(f, rec); err != nil {
		s.releaseLocked(workflowID)
		return fmt.Errorf("history: write record: %w", err)
	}
	if !sync {
		s.dirty[workflowID] = true
		return nil
	}
	if err := f.Sync(); err != nil {
		s.releaseLocked(workflowID)
		return fmt.Errorf("history: fsync: %w", err)
	}
	return nil
}

// ReadAll returns every event in the workflow's history, in append order. A
// missing history file yields an empty slice. A trailing torn record (a crash
// mid-append) is treated as the end of the log; a CRC mismatch on an otherwise
// complete record is a hard error.
func (s *HistoryStore) ReadAll(workflowID string) ([]*HistoryEvent, error) {
	if err := validateWorkflowID(workflowID); err != nil {
		return nil, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	f, err := os.Open(s.path(workflowID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("history: open: %w", err)
	}
	defer f.Close()

	var events []*HistoryEvent
	if _, err := record.Scan(bufio.NewReader(f), func(payload []byte) error {
		event := &HistoryEvent{}
		if err := proto.Unmarshal(payload, event); err != nil {
			return fmt.Errorf("history: unmarshal record %d: %w", len(events), err)
		}
		events = append(events, event)
		return nil
	}); err != nil {
		return nil, err
	}
	return events, nil
}

// ReadRaw returns a workflow's history file exactly as stored, up to the last
// complete record (a torn tail is left out). A missing file yields nil. It is
// how a cluster snapshot copies histories without decoding them.
func (s *HistoryStore) ReadRaw(workflowID string) ([]byte, error) {
	if err := validateWorkflowID(workflowID); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path(workflowID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("history: read raw: %w", err)
	}
	end, err := record.Scan(bufio.NewReader(bytes.NewReader(data)), nil)
	if err != nil {
		return nil, err
	}
	return data[:end], nil
}

// ReplaceAll makes the store hold exactly the given histories, keyed by
// workflow id, as raw bytes from ReadRaw. Each file is written to a temp file
// and renamed into place, and every history not listed is removed. It installs
// a cluster snapshot; a crash part-way leaves a mix of old and new files, which
// the caller repairs by installing the same snapshot again.
func (s *HistoryStore) ReplaceAll(files map[string][]byte) error {
	for id, data := range files {
		if err := validateWorkflowID(id); err != nil {
			return err
		}
		if _, err := record.Scan(bufio.NewReader(bytes.NewReader(data)), nil); err != nil {
			return fmt.Errorf("history: replace %s: %w", id, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.files {
		s.releaseLocked(id)
	}
	clear(s.dirty) // every history is about to be rewritten durably
	for id, data := range files {
		if err := record.ReplaceFile(s.dir, id+historyExt, data); err != nil {
			return fmt.Errorf("history: replace %s: %w", id, err)
		}
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("history: read data dir: %w", err)
	}
	for _, e := range entries {
		id, ok := strings.CutSuffix(e.Name(), historyExt)
		if !ok || e.IsDir() {
			continue
		}
		if _, keep := files[id]; !keep {
			if err := os.Remove(filepath.Join(s.dir, e.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("history: remove %s: %w", id, err)
			}
		}
	}
	return s.syncDir()
}

// Close flushes and closes all open file handles. The store must not be used afterward.
func (s *HistoryStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for id, f := range s.files {
		if s.dirty[id] {
			if err := f.Sync(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := s.releaseLocked(id); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Release closes and forgets the workflow's cached append handle, leaving its
// file untouched; a later Append reopens it. Call it once a workflow is finished
// so a long-running server doesn't hold one descriptor per workflow ever run.
func (s *HistoryStore) Release(workflowID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.releaseLocked(workflowID)
}

// releaseLocked is Release for callers that already hold s.mu. A buffered
// history stays marked dirty; Sync reopens it.
func (s *HistoryStore) releaseLocked(workflowID string) error {
	f, ok := s.files[workflowID]
	if !ok {
		return nil
	}
	delete(s.files, workflowID)
	if err := f.Close(); err != nil {
		return fmt.Errorf("history: release %s: %w", workflowID, err)
	}
	return nil
}

// OpenHandles reports how many append handles are cached, i.e. open descriptors.
func (s *HistoryStore) OpenHandles() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.files)
}

// Delete removes a workflow's history for good, closing its cached append
// handle and fsyncing the directory so the removal survives a crash. It exists
// for retention of finished workflows; deleting a live workflow loses it.
func (s *HistoryStore) Delete(workflowID string) error {
	if err := validateWorkflowID(workflowID); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.dirty, workflowID) // no point syncing what is about to be removed
	s.releaseLocked(workflowID)
	if err := os.Remove(s.path(workflowID)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("history: delete %s: %w", workflowID, err)
	}
	return s.syncDir()
}

// fileLocked returns a cached append handle for the workflow, opening one on
// first use. On the first open of an existing file it recovers a torn tail; on
// the first creation of a new file it fsyncs the parent directory (or, with
// syncDir false, leaves that to Sync). Callers must hold s.mu.
func (s *HistoryStore) fileLocked(workflowID string, syncDir bool) (*os.File, error) {
	if f, ok := s.files[workflowID]; ok {
		return f, nil
	}
	path := s.path(workflowID)
	_, statErr := os.Stat(path)
	isNew := errors.Is(statErr, os.ErrNotExist)

	// O_RDWR (not O_WRONLY) so recovery can read the file back to find the clean
	// tail; O_APPEND still forces every write to the physical end of the file.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("history: open append: %w", err)
	}

	if isNew && !syncDir {
		s.dirDirty = true
	} else if isNew {
		// A new file's name isn't durable until its parent directory is fsync'd;
		// without this a crash could lose the file despite a synced first append.
		if err := s.syncDir(); err != nil {
			f.Close()
			return nil, err
		}
	} else if err := recoverTornTail(f); err != nil {
		// Drop a torn record left by a crash mid-append, so the next append lands
		// on a clean boundary instead of after garbage that mis-frames every read.
		f.Close()
		return nil, err
	}

	s.files[workflowID] = f
	return f, nil
}

// recoverTornTail truncates any partial trailing record so the file ends on a
// record boundary. It is a no-op on a cleanly-closed file. A CRC mismatch or an
// oversized length mid-log is left in place and surfaced as an error: that is
// corruption, not a torn tail, and must never be silently discarded.
func recoverTornTail(f *os.File) error {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("history: seek for recovery: %w", err)
	}
	cleanEnd, err := record.Scan(bufio.NewReader(f), nil)
	if err != nil {
		return err
	}
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("history: size for recovery: %w", err)
	}
	if cleanEnd == size {
		return nil
	}
	if err := f.Truncate(cleanEnd); err != nil {
		return fmt.Errorf("history: truncate torn tail: %w", err)
	}
	// Make the truncation durable before any new append builds on it.
	if err := f.Sync(); err != nil {
		return fmt.Errorf("history: fsync after truncate: %w", err)
	}
	return nil
}

// syncDir fsyncs the data directory so a newly-created history file is durably
// linked into it. Callers must hold s.mu.
func (s *HistoryStore) syncDir() error {
	d, err := os.Open(s.dir)
	if err != nil {
		return fmt.Errorf("history: open data dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("history: fsync data dir: %w", err)
	}
	return nil
}

func (s *HistoryStore) path(workflowID string) string {
	return filepath.Join(s.dir, workflowID+historyExt)
}

// ListWorkflows returns the id of every workflow with a history file in the data
// directory, in unspecified order. The data dir is the durable index of all
// workflows, so this is how crash recovery discovers what to replay after a
// restart — the in-memory task queue is gone, but the logs on disk are not.
func (s *HistoryStore) ListWorkflows() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("history: read data dir: %w", err)
	}
	var ids []string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		if !strings.HasSuffix(name, historyExt) {
			continue // ignore sentinels, temp files, anything not a history log
		}
		ids = append(ids, strings.TrimSuffix(name, historyExt))
	}
	return ids, nil
}

// validateWorkflowID rejects IDs that are empty or could escape the data dir.
func validateWorkflowID(id string) error {
	if id == "" {
		return errors.New("history: empty workflow ID")
	}
	if id == "." || id == ".." || strings.ContainsAny(id, `/\`) {
		return fmt.Errorf("history: invalid workflow ID %q", id)
	}
	return nil
}
