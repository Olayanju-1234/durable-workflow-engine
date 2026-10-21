package raft

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
)

const (
	// hardStateFile holds the node's term and vote as one CRC-framed record.
	hardStateFile = "hardstate"
	// logFile holds the log, one CRC-framed LogEntry per record, in index order.
	logFile = "log"
	// snapshotFile holds the latest snapshot: a SnapshotMeta record, then the
	// state machine's data as a second record.
	snapshotFile = "snapshot"
)

// unsafeNoFsync turns off the log's fsync; see UnsafeDisableFsyncForTests.
var unsafeNoFsync atomic.Bool

// UnsafeDisableFsyncForTests stops fsyncing the Raft log, so log writes no
// longer survive a machine crash. Only tests that kill processes (whose page
// cache still reaches the disk) may call it, to run in reasonable time on
// laptops where fsync costs tens of milliseconds. Production never does.
func UnsafeDisableFsyncForTests() { unsafeNoFsync.Store(true) }

// DiskStorage keeps a node's Raft state durable in one directory, using the
// same CRC-framed, fsync'd records as the workflow histories. It is used by a
// single goroutine (the Node's) and is not safe for concurrent use.
type DiskStorage struct {
	dir string

	log     *os.File // opened by load, appended to with O_APPEND
	first   uint64   // index of the first entry the log file holds
	offsets []int64  // offsets[i] is where the entry with index first+i starts
	end     int64    // byte length of the valid log
}

// OpenDiskStorage opens (creating if needed) the Raft state directory.
func OpenDiskStorage(dir string) (*DiskStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: create state dir: %w", err)
	}
	return &DiskStorage{dir: dir}, nil
}

// load reads everything a restarting node needs and opens the log for
// appending. A brand-new directory yields the zero state: term 0, no vote,
// no snapshot, empty log.
func (s *DiskStorage) load() (persistedState, error) {
	hs, err := s.loadHardState()
	if err != nil {
		return persistedState{}, err
	}
	snap, err := s.loadSnapshot()
	if err != nil {
		return persistedState{}, err
	}
	ents, err := s.openLog()
	if err != nil {
		return persistedState{}, err
	}
	if snap == nil {
		if len(ents) > 0 && ents[0].GetIndex() != 1 {
			return persistedState{}, fmt.Errorf("raft: log starts at index %d with no snapshot before it", ents[0].GetIndex())
		}
		return persistedState{hardState: hs, entries: ents}, nil
	}
	ents, err = s.entriesAfter(snap, ents)
	if err != nil {
		return persistedState{}, err
	}
	return persistedState{hardState: hs, snapshot: snap, entries: ents}, nil
}

// entriesAfter returns the logged entries that continue snap. The snapshot is
// always written before the log is compacted or reset, so after a crash the
// log may still hold entries the snapshot covers (dropped here, and from the
// file at the next compaction) or, mid-install, entries that don't follow the
// snapshot at all (the whole file is reset).
func (s *DiskStorage) entriesAfter(snap *Snapshot, ents []*LogEntry) ([]*LogEntry, error) {
	if len(ents) > 0 && ents[0].GetIndex() == snap.Index+1 {
		return ents, nil // already compacted
	}
	if len(ents) > 0 && ents[0].GetIndex() > snap.Index+1 {
		return nil, fmt.Errorf("raft: log starts at index %d, after a snapshot ending at %d", ents[0].GetIndex(), snap.Index)
	}
	if len(ents) > 0 && ents[len(ents)-1].GetIndex() >= snap.Index {
		if at := ents[snap.Index-ents[0].GetIndex()]; at.GetTerm() == snap.Term {
			return ents[snap.Index-ents[0].GetIndex()+1:], nil
		}
	}
	return nil, s.resetLog(snap.Index)
}

// Close releases the log file. The storage must not be used afterwards.
func (s *DiskStorage) Close() error {
	if s.log == nil {
		return nil
	}
	err := s.log.Close()
	s.log = nil
	return err
}

// openLog reads every entry in the log file and leaves it open for appends.
// A torn final record (a crash mid-append) is cut off, exactly as a history
// file's would be; corruption anywhere else refuses to start the node.
func (s *DiskStorage) openLog() ([]*LogEntry, error) {
	path := filepath.Join(s.dir, logFile)
	_, statErr := os.Stat(path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("raft: open log: %w", err)
	}
	if errors.Is(statErr, os.ErrNotExist) {
		if err := record.SyncDir(s.dir); err != nil {
			f.Close()
			return nil, err
		}
	}
	var (
		ents    []*LogEntry
		offsets []int64
		off     int64
	)
	end, err := record.Scan(bufio.NewReader(f), func(p []byte) error {
		e := &LogEntry{}
		if err := proto.Unmarshal(p, e); err != nil {
			return fmt.Errorf("raft: decode log entry %d: %w", len(ents), err)
		}
		if n := len(ents); n > 0 && e.GetIndex() != ents[n-1].GetIndex()+1 {
			return fmt.Errorf("raft: log jumps from index %d to %d", ents[n-1].GetIndex(), e.GetIndex())
		}
		ents = append(ents, e)
		offsets = append(offsets, off)
		off += record.HeaderSize + int64(len(p))
		return nil
	})
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("raft: read log: %w", err)
	}
	if err := truncateTail(f, end); err != nil {
		f.Close()
		return nil, err
	}
	s.log, s.offsets, s.end, s.first = f, offsets, end, 1
	if len(ents) > 0 {
		s.first = ents[0].GetIndex()
	}
	return ents, nil
}

// truncateTail cuts f back to end if anything follows it, durably.
func truncateTail(f *os.File, end int64) error {
	size, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("raft: size log: %w", err)
	}
	if size == end {
		return nil
	}
	if err := f.Truncate(end); err != nil {
		return fmt.Errorf("raft: truncate torn log tail: %w", err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("raft: fsync after truncate: %w", err)
	}
	return nil
}

func (s *DiskStorage) lastIndex() uint64 {
	return s.first + uint64(len(s.offsets)) - 1
}

// appendEntries durably writes ents, first discarding anything stored at or
// after ents[0].Index: that is how a follower's conflicting suffix goes away.
// It returns once the entries would survive a crash.
func (s *DiskStorage) appendEntries(ents []*LogEntry) error {
	first := ents[0].GetIndex()
	if len(s.offsets) == 0 {
		s.first = first
	}
	if first > s.lastIndex()+1 || first < s.first {
		return fmt.Errorf("raft: append at index %d to a log holding %d..%d", first, s.first, s.lastIndex())
	}
	if first <= s.lastIndex() {
		cut := s.offsets[first-s.first]
		if err := s.log.Truncate(cut); err != nil {
			return fmt.Errorf("raft: truncate log at index %d: %w", first, err)
		}
		s.offsets, s.end = s.offsets[:first-s.first], cut
	}
	var buf []byte
	offsets := make([]int64, 0, len(ents))
	for _, e := range ents {
		payload, err := proto.Marshal(e)
		if err != nil {
			return fmt.Errorf("raft: encode log entry %d: %w", e.GetIndex(), err)
		}
		rec, err := record.Encode(payload)
		if err != nil {
			return fmt.Errorf("raft: log entry %d: %w", e.GetIndex(), err)
		}
		offsets = append(offsets, s.end+int64(len(buf)))
		buf = append(buf, rec...)
	}
	// One write and one fsync for the whole batch: the leader's batching is
	// what amortizes the cost of fsync across many proposals.
	if _, err := s.log.Write(buf); err != nil {
		return fmt.Errorf("raft: write log: %w", err)
	}
	if !unsafeNoFsync.Load() {
		if err := s.log.Sync(); err != nil {
			return fmt.Errorf("raft: fsync log: %w", err)
		}
	}
	s.offsets = append(s.offsets, offsets...)
	s.end += int64(len(buf))
	return nil
}

// resetLog empties the log file; the next entry appended will be index+1.
// Used after installing a snapshot that replaces the whole log.
func (s *DiskStorage) resetLog(index uint64) error {
	if err := s.log.Truncate(0); err != nil {
		return fmt.Errorf("raft: reset log: %w", err)
	}
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("raft: fsync reset log: %w", err)
	}
	s.first, s.offsets, s.end = index+1, nil, 0
	return nil
}

// compactLog drops every entry up to and including index from the log file,
// which a durable snapshot now covers. The file is rewritten and renamed into
// place, so a crash leaves either the old log or the new one.
func (s *DiskStorage) compactLog(index uint64) error {
	if index < s.first {
		return nil
	}
	keepFrom := s.end
	if index < s.lastIndex() {
		keepFrom = s.offsets[index+1-s.first]
	}
	kept := make([]byte, s.end-keepFrom)
	if _, err := s.log.ReadAt(kept, keepFrom); err != nil {
		return fmt.Errorf("raft: read log for compaction: %w", err)
	}
	if err := record.ReplaceFile(s.dir, logFile, kept); err != nil {
		return fmt.Errorf("raft: rewrite compacted log: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(s.dir, logFile), os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("raft: reopen compacted log: %w", err)
	}
	s.log.Close()
	offsets := make([]int64, 0, s.lastIndex()-index)
	for _, off := range s.offsets[min(index+1-s.first, uint64(len(s.offsets))):] {
		offsets = append(offsets, off-keepFrom)
	}
	s.log, s.first, s.offsets, s.end = f, index+1, offsets, s.end-keepFrom
	return nil
}

func (s *DiskStorage) loadSnapshot() (*Snapshot, error) {
	payloads, err := record.ReadFile(filepath.Join(s.dir, snapshotFile))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("raft: read snapshot: %w", err)
	}
	if len(payloads) != 2 {
		return nil, fmt.Errorf("raft: snapshot file holds %d records, want 2", len(payloads))
	}
	meta := &SnapshotMeta{}
	if err := proto.Unmarshal(payloads[0], meta); err != nil {
		return nil, fmt.Errorf("raft: decode snapshot meta: %w", err)
	}
	return &Snapshot{Index: meta.GetLastIncludedIndex(), Term: meta.GetLastIncludedTerm(), Data: payloads[1]}, nil
}

// saveSnapshot durably replaces the snapshot file. It must complete before the
// log entries it covers are dropped.
func (s *DiskStorage) saveSnapshot(snap *Snapshot) error {
	meta, err := proto.Marshal(&SnapshotMeta{LastIncludedIndex: snap.Index, LastIncludedTerm: snap.Term})
	if err != nil {
		return fmt.Errorf("raft: encode snapshot meta: %w", err)
	}
	if err := record.WriteFile(s.dir, snapshotFile, meta, snap.Data); err != nil {
		return fmt.Errorf("raft: save snapshot: %w", err)
	}
	return nil
}

func (s *DiskStorage) loadHardState() (*HardState, error) {
	payloads, err := record.ReadFile(filepath.Join(s.dir, hardStateFile))
	if errors.Is(err, os.ErrNotExist) {
		return &HardState{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("raft: hard state: %w", err)
	}
	if len(payloads) != 1 {
		return nil, fmt.Errorf("raft: hard state file holds %d records, want 1", len(payloads))
	}
	hs := &HardState{}
	if err := proto.Unmarshal(payloads[0], hs); err != nil {
		return nil, fmt.Errorf("raft: decode hard state: %w", err)
	}
	return hs, nil
}

// saveHardState durably replaces the term and vote. It returns only once the
// new state would survive a crash.
func (s *DiskStorage) saveHardState(hs *HardState) error {
	payload, err := proto.Marshal(hs)
	if err != nil {
		return fmt.Errorf("raft: encode hard state: %w", err)
	}
	if err := record.WriteFile(s.dir, hardStateFile, payload); err != nil {
		return fmt.Errorf("raft: save hard state: %w", err)
	}
	return nil
}
