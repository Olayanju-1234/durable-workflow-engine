package raft

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
)

const (
	// hardStateFile holds the node's term and vote as one CRC-framed record.
	hardStateFile = "hardstate"
	// logFile holds the log, one CRC-framed LogEntry per record, in index order.
	logFile = "log"
)

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
// empty log.
func (s *DiskStorage) load() (persistedState, error) {
	hs, err := s.loadHardState()
	if err != nil {
		return persistedState{}, err
	}
	ents, err := s.openLog()
	if err != nil {
		return persistedState{}, err
	}
	if len(ents) > 0 && ents[0].GetIndex() != 1 {
		return persistedState{}, fmt.Errorf("raft: log starts at index %d, want 1", ents[0].GetIndex())
	}
	return persistedState{hardState: hs, entries: ents}, nil
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
		if err := syncDir(s.dir); err != nil {
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
	if err := s.log.Sync(); err != nil {
		return fmt.Errorf("raft: fsync log: %w", err)
	}
	s.offsets = append(s.offsets, offsets...)
	s.end += int64(len(buf))
	return nil
}

func (s *DiskStorage) loadHardState() (*HardState, error) {
	data, err := os.ReadFile(filepath.Join(s.dir, hardStateFile))
	if errors.Is(err, os.ErrNotExist) {
		return &HardState{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("raft: read hard state: %w", err)
	}
	var payloads [][]byte
	if _, err := record.Scan(bufio.NewReader(bytes.NewReader(data)), func(p []byte) error {
		payloads = append(payloads, p)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("raft: hard state: %w", err)
	}
	// The file is only ever replaced whole, by rename, so anything other than
	// exactly one record is damage, never a crash mid-write.
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
	rec, err := record.Encode(payload)
	if err != nil {
		return fmt.Errorf("raft: hard state: %w", err)
	}
	return writeFileAtomic(s.dir, hardStateFile, rec)
}

// writeFileAtomic replaces dir/name with data so that a crash leaves either the
// old file or the new one, never a mix: write a temp file, fsync it, rename it
// over the target, then fsync the directory so the rename itself is durable.
func writeFileAtomic(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("raft: create %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("raft: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raft: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("raft: rename %s: %w", tmp, err)
	}
	return syncDir(dir)
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("raft: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("raft: fsync dir: %w", err)
	}
	return nil
}
