package raft

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
)

// hardStateFile holds the node's term and vote as one CRC-framed record.
const hardStateFile = "hardstate"

// DiskStorage keeps a node's Raft state durable in one directory, using the
// same CRC-framed, fsync'd records as the workflow histories.
type DiskStorage struct {
	dir string
}

// OpenDiskStorage opens (creating if needed) the Raft state directory.
func OpenDiskStorage(dir string) (*DiskStorage, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("raft: create state dir: %w", err)
	}
	return &DiskStorage{dir: dir}, nil
}

// load reads everything a restarting node needs. A brand-new directory yields
// the zero state: term 0, no vote, empty log.
func (s *DiskStorage) load() (persistedState, error) {
	hs, err := s.loadHardState()
	if err != nil {
		return persistedState{}, err
	}
	return persistedState{hardState: hs}, nil
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
