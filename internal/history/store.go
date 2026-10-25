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
	// dirtyDirs: directories in which AppendBuffered created a file whose
	// entry is not yet fsync'd (Sync does it, with the files).
	dirtyDirs map[string]bool

	// format is the history format recorded for the directory (0: none yet).
	// Guarded by mu.
	format uint32

	// writeFile is (*os.File).Write; tests swap it to inject a torn write.
	writeFile func(f *os.File, b []byte) (int, error)
}

// NewHistoryStore opens a store rooted at dir, creating the directory if needed.
// It refuses a directory whose recorded history format is newer than this
// binary reads, before anything can read or write a history there.
func NewHistoryStore(dir string) (*HistoryStore, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("history: create data dir: %w", err)
	}
	format, err := ReadFormat(dir)
	if err != nil {
		return nil, err
	}
	if format > FormatVersion {
		return nil, TooNewError(format, "data directory "+dir)
	}
	return &HistoryStore{dir: dir, format: format, files: make(map[string]*os.File), dirty: make(map[string]bool),
		dirtyDirs: make(map[string]bool), writeFile: (*os.File).Write}, nil
}

// Format is the history format recorded for the data directory, 0 if none is
// recorded yet (a directory written only by binaries before 0.3.0).
func (s *HistoryStore) Format() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.format
}

// RequireFormat records that the directory holds history needing format
// need, unless it already says so. It is how the directory's format rises:
// before the first event that needs it is written, so a crash in between
// can only leave the record too high, never too low.
func (s *HistoryStore) RequireFormat(need uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requireFormatLocked(need)
}

func (s *HistoryStore) requireFormatLocked(need uint32) error {
	need = max(need, FormatBase)
	if need > FormatVersion {
		return TooNewError(need, "an event")
	}
	if need <= s.format {
		return nil
	}
	if err := WriteFormat(s.dir, need); err != nil {
		return err
	}
	s.format = need
	return nil
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
	for dir := range s.dirtyDirs {
		if err := syncDir(dir); err != nil {
			return err
		}
		delete(s.dirtyDirs, dir)
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
	if need := event.GetMinReaderFormat(); need > FormatBase {
		if err := s.RequireFormat(need); err != nil {
			return err
		}
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
	f, err := s.fileLocked(workflowID, sync)
	s.mu.Unlock()
	if err != nil {
		return err
	}
	// The write and fsync run without s.mu, so appends to different workflows
	// share the disk instead of queueing on one lock. Callers serialise appends
	// to one workflow (the engine's per-workflow lock, the cluster's apply loop),
	// so nothing else uses or closes f until this returns.
	//
	// os.File.Write errors whenever it writes short, so err alone catches a torn
	// record. Dropping the handle makes the next Append reopen and truncate it.
	if _, err := s.writeFile(f, rec); err != nil {
		s.dropHandle(workflowID, f)
		return fmt.Errorf("history: write record: %w", err)
	}
	if !sync {
		s.mu.Lock()
		s.dirty[workflowID] = true
		s.mu.Unlock()
		return nil
	}
	if err := f.Sync(); err != nil {
		s.dropHandle(workflowID, f)
		return fmt.Errorf("history: fsync: %w", err)
	}
	return nil
}

// dropHandle forgets f as the workflow's append handle after a failed write.
func (s *HistoryStore) dropHandle(workflowID string, f *os.File) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.files[workflowID] == f {
		s.releaseLocked(workflowID)
	}
}

// ReadAll returns every event in the workflow's history, in append order. A
// missing history file yields an empty slice. A trailing torn record (a crash
// mid-append) is treated as the end of the log; a CRC mismatch on an otherwise
// complete record is a hard error.
func (s *HistoryStore) ReadAll(workflowID string) ([]*HistoryEvent, error) {
	if err := validateWorkflowID(workflowID); err != nil {
		return nil, err
	}

	// No lock: a read racing an append sees the new record whole or not at
	// all (a partial tail reads as the end of the log).
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
// store key, as raw bytes from ReadRaw. Each file is written to a temp file
// and renamed into place, and every history not listed is removed. It installs
// a cluster snapshot; a crash part-way leaves a mix of old and new files, which
// the caller repairs by installing the same snapshot again.
func (s *HistoryStore) ReplaceAll(files map[string][]byte) error {
	need := FormatBase
	for id, data := range files {
		if err := validateWorkflowID(id); err != nil {
			return err
		}
		if Namespaced(id) {
			need = max(need, FormatNamespaces)
		}
		if _, err := record.Scan(bufio.NewReader(bytes.NewReader(data)), func(payload []byte) error {
			ev := &HistoryEvent{}
			if err := proto.Unmarshal(payload, ev); err != nil {
				return err
			}
			need = max(need, ev.GetMinReaderFormat())
			return nil
		}); err != nil {
			return fmt.Errorf("history: replace %s: %w", id, err)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireFormatLocked(need); err != nil {
		return err
	}
	for id := range s.files {
		s.releaseLocked(id)
	}
	clear(s.dirty) // every history is about to be rewritten durably
	for id, data := range files {
		dir := s.dirOf(id)
		if Namespaced(id) {
			ns, _ := SplitKey(id)
			if err := s.namespaceDirLocked(ns); err != nil {
				return err
			}
		}
		_, name := SplitKey(id)
		if err := record.ReplaceFile(dir, name+historyExt, data); err != nil {
			return fmt.Errorf("history: replace %s: %w", id, err)
		}
	}
	existing, err := s.listLocked()
	if err != nil {
		return err
	}
	dirs := map[string]bool{s.dir: true}
	for _, id := range existing {
		if _, keep := files[id]; keep {
			continue
		}
		if err := os.Remove(s.path(id)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("history: remove %s: %w", id, err)
		}
		dirs[s.dirOf(id)] = true
	}
	for dir := range dirs {
		if err := syncDir(dir); err != nil {
			return err
		}
	}
	return nil
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
	return syncDir(s.dirOf(workflowID))
}

// NewRun ends a workflow's current history file and starts the next: the
// finished history is kept under archiveID, and the workflow's history becomes
// first, then more (a reset run starts with the events it keeps). A crash at
// any point leaves the old history current (the caller redoes the whole step:
// it reads a finished run) or the whole new one, never neither and never part.
//
//  1. link the current file to the archive name, and fsync the directory;
//  2. write the events to <id>.history.next and fsync it;
//  3. rename it over the current file, and fsync the directory.
//
// Redoing it after a crash between 1 and 3 finds the archive already linked
// to the same file, which is fine. It always fsyncs, even for a cluster's
// buffered store: it is rare, and the rename must not outrun the data.
func (s *HistoryStore) NewRun(workflowID, archiveID string, first *HistoryEvent, more ...*HistoryEvent) error {
	if err := validateWorkflowID(workflowID); err != nil {
		return err
	}
	if err := validateWorkflowID(archiveID); err != nil {
		return err
	}
	if s.dirOf(archiveID) != s.dirOf(workflowID) {
		return fmt.Errorf("history: run archive %q is not in the namespace of %q", archiveID, workflowID)
	}
	var rec []byte
	need := FormatBase
	for _, ev := range append([]*HistoryEvent{first}, more...) {
		payload, err := proto.Marshal(ev)
		if err != nil {
			return fmt.Errorf("history: marshal event: %w", err)
		}
		r, err := record.Encode(payload)
		if err != nil {
			return fmt.Errorf("history: %w", err)
		}
		rec = append(rec, r...)
		need = max(need, ev.GetMinReaderFormat())
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.requireFormatLocked(need); err != nil {
		return err
	}
	cur, arch := s.path(workflowID), s.path(archiveID)

	// The finished run's buffered tail must be on disk before it is renamed.
	if f, ok := s.files[workflowID]; ok {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("history: fsync %s before a new run: %w", workflowID, err)
		}
	} else if s.dirty[workflowID] {
		if err := syncFile(cur); err != nil {
			return err
		}
	}
	delete(s.dirty, workflowID)
	if err := s.releaseLocked(workflowID); err != nil {
		return err
	}

	if err := os.Link(cur, arch); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("history: keep %s as %s: %w", workflowID, archiveID, err)
		}
		a, aerr := os.Stat(arch)
		c, cerr := os.Stat(cur)
		if aerr != nil || cerr != nil || !os.SameFile(a, c) {
			return fmt.Errorf("history: keep %s as %s: a different history already has that name", workflowID, archiveID)
		}
	}
	if err := syncDir(s.dirOf(workflowID)); err != nil {
		return err
	}
	next := cur + ".next"
	f, err := os.OpenFile(next, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("history: create %s: %w", next, err)
	}
	_, werr := s.writeFile(f, rec)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr != nil {
		return fmt.Errorf("history: write the start of %s's next run: %w", workflowID, werr)
	}
	if err := os.Rename(next, cur); err != nil {
		return fmt.Errorf("history: start %s's next run: %w", workflowID, err)
	}
	return syncDir(s.dirOf(workflowID))
}

// syncFile fsyncs a file through a fresh descriptor.
func syncFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("history: open %s to fsync: %w", path, err)
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("history: fsync %s: %w", path, err)
	}
	return nil
}

// fileLocked returns a cached append handle for the workflow, opening one on
// first use. On the first open of an existing file it recovers a torn tail; on
// the first creation of a new file it fsyncs the parent directory (or, with
// syncDir false, leaves that to Sync). Callers must hold s.mu.
func (s *HistoryStore) fileLocked(workflowID string, syncDirNow bool) (*os.File, error) {
	if f, ok := s.files[workflowID]; ok {
		return f, nil
	}
	path := s.path(workflowID)
	_, statErr := os.Stat(path)
	isNew := errors.Is(statErr, os.ErrNotExist)
	if isNew && Namespaced(workflowID) {
		ns, _ := SplitKey(workflowID)
		if err := s.namespaceDirLocked(ns); err != nil {
			return nil, err
		}
	}

	// O_RDWR (not O_WRONLY) so recovery can read the file back to find the clean
	// tail; O_APPEND still forces every write to the physical end of the file.
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("history: open append: %w", err)
	}

	if isNew && !syncDirNow {
		s.dirtyDirs[s.dirOf(workflowID)] = true
	} else if isNew {
		// A new file's name isn't durable until its parent directory is fsync'd;
		// without this a crash could lose the file despite a synced first append.
		if err := syncDir(s.dirOf(workflowID)); err != nil {
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

// syncDir fsyncs a directory so a file newly created, renamed or removed in
// it is durably so.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("history: open data dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("history: fsync data dir: %w", err)
	}
	return nil
}

// dirOf is the directory that holds a key's history.
func (s *HistoryStore) dirOf(key string) string {
	if !Namespaced(key) {
		return s.dir
	}
	ns, _ := SplitKey(key)
	return filepath.Join(s.dir, namespacesDir, ns)
}

func (s *HistoryStore) path(key string) string {
	_, id := SplitKey(key)
	return filepath.Join(s.dirOf(key), id+historyExt)
}

// namespaceDirLocked makes a namespace's directory exist, durably, before a
// history is created in it. The directory's format is raised first: a binary
// before 0.4.0 would not see the namespace's workflows at all. The caller
// holds s.mu.
func (s *HistoryStore) namespaceDirLocked(ns string) error {
	if err := s.requireFormatLocked(FormatNamespaces); err != nil {
		return err
	}
	dir := filepath.Join(s.dir, namespacesDir, ns)
	if _, err := os.Stat(dir); err == nil {
		return nil
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("history: create namespace %s: %w", ns, err)
	}
	for _, d := range []string{filepath.Join(s.dir, namespacesDir), s.dir} {
		if err := syncDir(d); err != nil {
			return err
		}
	}
	return nil
}

// ListWorkflows returns the key of every history in the data directory, the
// default namespace's and every other namespace's, in unspecified order. The
// data dir is the durable index of all workflows, so this is how crash
// recovery discovers what to replay after a restart — the in-memory task
// queue is gone, but the logs on disk are not.
func (s *HistoryStore) ListWorkflows() ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listLocked()
}

func (s *HistoryStore) listLocked() ([]string, error) {
	ids, err := listHistories(s.dir, "")
	if err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(filepath.Join(s.dir, namespacesDir))
	if errors.Is(err, os.ErrNotExist) {
		return ids, nil
	}
	if err != nil {
		return nil, fmt.Errorf("history: read namespaces: %w", err)
	}
	for _, e := range entries {
		if !e.IsDir() || !ValidNamespace(e.Name()) || e.Name() == DefaultNamespace {
			continue
		}
		more, err := listHistories(filepath.Join(s.dir, namespacesDir, e.Name()), e.Name())
		if err != nil {
			return nil, err
		}
		ids = append(ids, more...)
	}
	return ids, nil
}

// listHistories returns the keys of the history files in one directory.
func listHistories(dir, ns string) ([]string, error) {
	entries, err := os.ReadDir(dir)
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
		ids = append(ids, Key(ns, strings.TrimSuffix(name, historyExt)))
	}
	return ids, nil
}
