package history

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
)

// Backups are gzipped tar files of a data directory's histories, laid out as
// in the directory itself, so `tar xzf` restores one by hand too:
//
//	<id>.history
//	namespaces/<namespace>/<id>.history
//	FORMAT
//
// Histories are append-only, so a copy taken while the engine writes is a
// prefix of each history, ending on a whole record. FORMAT goes in last: it
// only rises, so the backup records at least the format of every history in it.

// Backup writes a backup of every history in the store to w. Raft state and
// anything else in the directory is left out: it backs up a single node.
func (s *HistoryStore) Backup(w io.Writer) error {
	gz := gzip.NewWriter(w)
	tw := tar.NewWriter(gz)
	keys, err := s.ListWorkflows()
	if err != nil {
		return err
	}
	slices.Sort(keys)
	now := time.Now()
	for _, key := range keys {
		data, err := s.ReadRaw(key)
		if err != nil {
			return fmt.Errorf("history: back up %s: %w", key, err)
		}
		if data == nil {
			continue // removed since it was listed
		}
		if err := writeTarFile(tw, backupPath(key), data, now); err != nil {
			return err
		}
	}
	format, err := os.ReadFile(filepath.Join(s.dir, formatFile))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("history: back up %s: %w", formatFile, err)
	}
	if err == nil {
		if err := writeTarFile(tw, formatFile, format, now); err != nil {
			return err
		}
	}
	if err := tw.Close(); err != nil {
		return fmt.Errorf("history: back up: %w", err)
	}
	if err := gz.Close(); err != nil {
		return fmt.Errorf("history: back up: %w", err)
	}
	return nil
}

func writeTarFile(tw *tar.Writer, name string, data []byte, mod time.Time) error {
	hdr := &tar.Header{Name: name, Mode: 0o644, Size: int64(len(data)), ModTime: mod, Typeflag: tar.TypeReg}
	if err := tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("history: back up %s: %w", name, err)
	}
	if _, err := tw.Write(data); err != nil {
		return fmt.Errorf("history: back up %s: %w", name, err)
	}
	return nil
}

// backupPath is a key's path inside a backup (and its data directory).
func backupPath(key string) string {
	ns, id := SplitKey(key)
	if !Namespaced(key) {
		return id + historyExt
	}
	return path.Join(namespacesDir, ns, id+historyExt)
}

// keyOfBackupPath is backupPath's inverse; ok is false for any other path.
func keyOfBackupPath(p string) (string, bool) {
	name, ok := strings.CutSuffix(p, historyExt)
	if !ok {
		return "", false
	}
	parts := strings.Split(name, "/")
	var key string
	switch {
	case len(parts) == 1:
		key = parts[0]
	case len(parts) == 3 && parts[0] == namespacesDir:
		key = Key(parts[1], parts[2])
	default:
		return "", false
	}
	return key, validateWorkflowID(key) == nil && backupPath(key) == p
}

// RestoreSummary is what a restore wrote.
type RestoreSummary struct {
	Histories int
	Format    uint32 // the format recorded for the restored directory
}

// maxRestoreFile bounds one file in a backup, against a damaged or hostile
// archive claiming terabytes.
const maxRestoreFile = 4 << 30

// Restore unpacks a backup into dir, which must not exist or be empty: it
// never overwrites a data directory. It unpacks into a sibling directory
// first, checking every history (whole records, matching checksums) and
// fsyncing every file, and renames it into place only when all of it is good.
func Restore(dir string, r io.Reader) (RestoreSummary, error) {
	var sum RestoreSummary
	if entries, err := os.ReadDir(dir); err == nil && len(entries) > 0 {
		return sum, fmt.Errorf("history: restore into %s: the directory is not empty; restore into a new one", dir)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return sum, fmt.Errorf("history: restore: %w", err)
	}
	gz, err := gzip.NewReader(r)
	if err != nil {
		return sum, fmt.Errorf("history: restore: not a workflowd backup: %w", err)
	}
	tmp := filepath.Clean(dir) + ".restoring"
	if err := os.RemoveAll(tmp); err != nil {
		return sum, fmt.Errorf("history: restore: %w", err)
	}
	if err := os.MkdirAll(tmp, 0o755); err != nil {
		return sum, fmt.Errorf("history: restore: %w", err)
	}
	ok := false
	defer func() {
		if !ok {
			os.RemoveAll(tmp)
		}
	}()

	tr := tar.NewReader(gz)
	need := FormatBase
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return sum, fmt.Errorf("history: restore: %w", err)
		}
		if hdr.Typeflag == tar.TypeDir {
			continue
		}
		if hdr.Typeflag != tar.TypeReg || hdr.Size > maxRestoreFile {
			return sum, fmt.Errorf("history: restore: %s is not a history file of a workflowd backup", hdr.Name)
		}
		data, err := io.ReadAll(tr)
		if err != nil {
			return sum, fmt.Errorf("history: restore %s: %w", hdr.Name, err)
		}
		if hdr.Name == formatFile {
			var m formatMarker
			if err := json.Unmarshal(data, &m); err != nil || m.HistoryFormat == 0 {
				return sum, fmt.Errorf("history: restore: %s holds %q, not a history format", formatFile, data)
			}
			if m.HistoryFormat > FormatVersion {
				return sum, TooNewError(m.HistoryFormat, "the backup")
			}
			need = max(need, m.HistoryFormat)
			continue
		}
		key, ok := keyOfBackupPath(hdr.Name)
		if !ok {
			return sum, fmt.Errorf("history: restore: %q is not a history file of a workflowd backup", hdr.Name)
		}
		if Namespaced(key) {
			need = max(need, FormatNamespaces)
		}
		end, err := record.Scan(bufio.NewReader(bytes.NewReader(data)), func(payload []byte) error {
			ev := &HistoryEvent{}
			if err := proto.Unmarshal(payload, ev); err != nil {
				return err
			}
			need = max(need, ev.GetMinReaderFormat())
			return nil
		})
		if err != nil || end != int64(len(data)) {
			return sum, fmt.Errorf("history: restore %s: damaged history (%d of %d bytes whole, %v)", hdr.Name, end, len(data), err)
		}
		if need > FormatVersion {
			return sum, TooNewError(need, hdr.Name)
		}
		target := filepath.Join(tmp, filepath.FromSlash(hdr.Name))
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return sum, fmt.Errorf("history: restore: %w", err)
		}
		if err := record.ReplaceFile(filepath.Dir(target), filepath.Base(target), data); err != nil {
			return sum, fmt.Errorf("history: restore %s: %w", hdr.Name, err)
		}
		sum.Histories++
	}
	if err := WriteFormat(tmp, need); err != nil {
		return sum, err
	}
	sum.Format = need
	if nsDir := filepath.Join(tmp, namespacesDir); dirExists(nsDir) {
		if err := syncDir(nsDir); err != nil {
			return sum, err
		}
	}
	if err := os.Remove(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
		return sum, fmt.Errorf("history: restore: %w", err)
	}
	if err := os.Rename(tmp, dir); err != nil {
		return sum, fmt.Errorf("history: restore: %w", err)
	}
	ok = true
	return sum, syncDir(filepath.Dir(filepath.Clean(dir)))
}

func dirExists(p string) bool {
	st, err := os.Stat(p)
	return err == nil && st.IsDir()
}

// DecodeRaw reads events from a history file's bytes (ReadRaw, a backup or
// an archive). A torn tail is an error here: such bytes are always whole.
func DecodeRaw(data []byte) ([]*HistoryEvent, error) {
	var events []*HistoryEvent
	end, err := record.Scan(bufio.NewReader(bytes.NewReader(data)), func(payload []byte) error {
		ev := &HistoryEvent{}
		if err := proto.Unmarshal(payload, ev); err != nil {
			return fmt.Errorf("history: unmarshal record %d: %w", len(events), err)
		}
		events = append(events, ev)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if end != int64(len(data)) {
		return nil, fmt.Errorf("history: %d of %d bytes are whole records", end, len(data))
	}
	return events, nil
}
