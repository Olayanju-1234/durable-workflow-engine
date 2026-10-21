// Package record is the on-disk framing shared by every durable file the
// engine writes: workflow histories, the Raft log and Raft's persistent state.
//
//	[4B big-endian length N][4B big-endian CRC32C of payload][N-byte payload]
//
// Length-prefixing lets a reader walk records without delimiters; the CRC tells
// a torn write (a short tail, which is simply the end of the usable file) apart
// from corruption in the body (a complete record whose checksum fails).
package record

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
)

const (
	lengthSize = 4
	crcSize    = 4
	// HeaderSize is the fixed prefix in front of every payload.
	HeaderSize = lengthSize + crcSize
	// MaxSize guards against a corrupt length prefix triggering a huge allocation.
	MaxSize = 64 << 20
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// ErrCorrupt is returned when a complete record's CRC fails to verify.
var ErrCorrupt = errors.New("record: corrupt record (CRC mismatch)")

// Encode frames payload as one record. Callers write the result with a single
// write call, which keeps the torn-write window as small as possible.
func Encode(payload []byte) ([]byte, error) {
	if len(payload) > MaxSize {
		return nil, fmt.Errorf("record: payload too large: %d bytes", len(payload))
	}
	buf := make([]byte, HeaderSize+len(payload))
	binary.BigEndian.PutUint32(buf[0:lengthSize], uint32(len(payload)))
	binary.BigEndian.PutUint32(buf[lengthSize:HeaderSize], crc32.Checksum(payload, crcTable))
	copy(buf[HeaderSize:], payload)
	return buf, nil
}

// Scan reads records from r in order, calling emit with each valid payload, and
// returns the byte offset just past the last valid record: the clean end.
//
// A torn trailing record (a partial header or payload, i.e. a crash mid-append)
// ends the scan without error, because that record never committed. A CRC
// mismatch or an over-large length on an otherwise complete record is a hard
// error: that is corruption in the body, not an interrupted write at the tail.
func Scan(r *bufio.Reader, emit func(payload []byte) error) (int64, error) {
	var offset int64
	for {
		var header [HeaderSize]byte
		if _, err := io.ReadFull(r, header[:]); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return offset, nil // clean EOF or torn header -> end of usable data
			}
			return offset, fmt.Errorf("record: read header: %w", err)
		}
		length := binary.BigEndian.Uint32(header[0:lengthSize])
		wantCRC := binary.BigEndian.Uint32(header[lengthSize:HeaderSize])
		if length > MaxSize {
			return offset, fmt.Errorf("record: record at offset %d length %d exceeds max", offset, length)
		}

		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return offset, nil // torn payload -> last record never committed
			}
			return offset, fmt.Errorf("record: read payload: %w", err)
		}
		if crc32.Checksum(payload, crcTable) != wantCRC {
			return offset, fmt.Errorf("record: record at offset %d: %w", offset, ErrCorrupt)
		}
		if emit != nil {
			if err := emit(payload); err != nil {
				return offset, err
			}
		}
		offset += HeaderSize + int64(length)
	}
}

// WriteFile replaces dir/name with payloads framed as records, so that a crash
// leaves either the old file or the new one, never a mix: write a temp file,
// fsync it, rename it over the target, then fsync the directory so the rename
// itself is durable.
func WriteFile(dir, name string, payloads ...[]byte) error {
	var data []byte
	for _, p := range payloads {
		rec, err := Encode(p)
		if err != nil {
			return err
		}
		data = append(data, rec...)
	}
	return ReplaceFile(dir, name, data)
}

// ReplaceFile is WriteFile for bytes that are already framed records.
func ReplaceFile(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("record: create %s: %w", tmp, err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("record: write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("record: fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("record: close %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("record: rename %s: %w", tmp, err)
	}
	return SyncDir(dir)
}

// ReadFile reads a file written by WriteFile and returns its payloads. A
// missing file returns os.ErrNotExist. Because WriteFile replaces files whole,
// a torn record here is damage, not an interrupted write, and is an error.
func ReadFile(path string) ([][]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var payloads [][]byte
	end, err := Scan(bufio.NewReader(bytes.NewReader(data)), func(p []byte) error {
		payloads = append(payloads, p)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if end != int64(len(data)) {
		return nil, fmt.Errorf("record: %s ends with a partial record", path)
	}
	return payloads, nil
}

// SyncDir fsyncs a directory, making renames and new files in it durable.
func SyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("record: open dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("record: fsync dir: %w", err)
	}
	return nil
}
