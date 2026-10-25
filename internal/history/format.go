package history

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
)

// History formats. A format names a change to what history can contain that
// an older binary would refuse or misread. Every binary reads every format up
// to its own FormatVersion; docs/UPGRADING.md is the table operators use.
const (
	// FormatBase is what every workflowd has read: Phase 1–4 histories.
	FormatBase uint32 = 1
	// FormatMarkers: MarkerRecorded (GetVersion/SideEffect), from 14a1fa0.
	FormatMarkers uint32 = 2
	// FormatParallelSteps: a step opened while another is in flight, from f7c18c1.
	FormatParallelSteps uint32 = 3
	// FormatClosedSteps: a cancelled step, or a final StepFailed closing a
	// step between attempts (0.2.0, 81c98f8).
	FormatClosedSteps uint32 = 4
	// FormatRuns: continue-as-new and graceful cancellation (0.3.0).
	FormatRuns uint32 = 5
	// FormatNamespaces: a history outside the default namespace (0.4.0).
	FormatNamespaces uint32 = 6
	// FormatEncrypted: an event encrypted at rest (0.5.0, ADR-0022).
	FormatEncrypted uint32 = 7

	// FormatVersion is the newest format this binary reads and writes.
	FormatVersion = FormatEncrypted
)

// FormatRelease names the first release (and commit) whose binary reads each
// format, for messages that tell an operator which binary they need.
var FormatRelease = map[uint32]string{
	FormatBase:          "any workflowd",
	FormatMarkers:       "workflowd 0.1.0, or a build from 14a1fa0 on",
	FormatParallelSteps: "workflowd 0.1.0, or a build from f7c18c1 on",
	FormatClosedSteps:   "workflowd 0.2.0 or later",
	FormatRuns:          "workflowd 0.3.0 or later",
	FormatNamespaces:    "workflowd 0.4.0 or later",
	FormatEncrypted:     "workflowd 0.5.0 or later",
}

// ErrFormatTooNew means the data directory, or one of its histories, was
// written in a format newer than this binary reads.
var ErrFormatTooNew = errors.New("history: format too new for this binary")

// TooNewError is ErrFormatTooNew with the format that was needed.
func TooNewError(need uint32, what string) error {
	who := FormatRelease[need]
	if who == "" {
		who = "a newer workflowd"
	}
	return fmt.Errorf("%w: %s needs history format %d (%s), but this binary reads up to format %d; "+
		"upgrade the binary, or restore a backup taken before the upgrade (docs/UPGRADING.md)",
		ErrFormatTooNew, what, need, who, FormatVersion)
}

// formatFile records the highest format any history in the data directory
// needs. It only rises while a store writes; --rescan-format may lower it.
const formatFile = "FORMAT"

type formatMarker struct {
	HistoryFormat uint32 `json:"history_format"`
}

// ReadFormat returns the format recorded for a data directory, or 0 if none is.
func ReadFormat(dir string) (uint32, error) {
	data, err := os.ReadFile(filepath.Join(dir, formatFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("history: read %s: %w", formatFile, err)
	}
	var m formatMarker
	if err := json.Unmarshal(data, &m); err != nil || m.HistoryFormat == 0 {
		return 0, fmt.Errorf("history: %s holds %q, not a history format", formatFile, data)
	}
	return m.HistoryFormat, nil
}

// WriteFormat records a data directory's format durably (write, fsync,
// rename, fsync the directory), replacing any earlier record.
func WriteFormat(dir string, format uint32) error {
	data, err := json.Marshal(formatMarker{HistoryFormat: format})
	if err != nil {
		return fmt.Errorf("history: encode %s: %w", formatFile, err)
	}
	if err := record.ReplaceFile(dir, formatFile, append(data, '\n')); err != nil {
		return fmt.Errorf("history: write %s: %w", formatFile, err)
	}
	return nil
}
