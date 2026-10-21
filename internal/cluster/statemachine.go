package cluster

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/raft"
	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
)

// ErrConflict rejects an AppendEvent whose position in the history is not the
// next one. Every node rejects it identically, so histories never diverge.
var ErrConflict = errors.New("cluster: history position conflict")

const (
	snapshotVersion = 1
	// markerFile records the index of the newest snapshot the history
	// directory is known to include, so a restart can skip restoring it.
	markerFile = "applied-snapshot"
)

// stateMachine applies committed Commands to the history store: the history
// directory is the replicated state (ADR-0004). It runs on the Raft node's
// apply goroutine only, so it needs no locking of its own.
type stateMachine struct {
	store   *history.HistoryStore
	raftDir string
	log     *slog.Logger

	// pos caches each touched workflow's length and the Raft index stamped on
	// its last event, so apply doesn't re-read the file for every event.
	pos     map[string]position
	applied uint64 // index of the last entry applied (empty entries skipped)
}

type position struct {
	events    int64
	lastIndex uint64
}

func newStateMachine(store *history.HistoryStore, raftDir string, logger *slog.Logger) *stateMachine {
	return &stateMachine{store: store, raftDir: raftDir, log: logger, pos: make(map[string]position)}
}

// Apply returns nil or an error describing why the command was rejected, as
// the proposer's result. Its own error return is reserved for this node's
// disk failing, which stops the node.
func (m *stateMachine) Apply(e *raft.LogEntry) (any, error) {
	m.applied = e.GetIndex()
	cmd := &Command{}
	if err := proto.Unmarshal(e.GetData(), cmd); err != nil {
		return fmt.Errorf("cluster: undecodable command at index %d: %w", e.GetIndex(), err), nil
	}
	switch c := cmd.GetCommand().(type) {
	case *Command_Append:
		return m.appendEvent(e.GetIndex(), c.Append.GetWorkflowId(), c.Append.GetEvent())
	case *Command_Delete:
		return m.deleteWorkflow(e.GetIndex(), c.Delete.GetWorkflowId())
	default:
		return fmt.Errorf("cluster: empty command at index %d", e.GetIndex()), nil
	}
}

func (m *stateMachine) position(id string) (position, error) {
	if p, ok := m.pos[id]; ok {
		return p, nil
	}
	events, err := m.store.ReadAll(id)
	if err != nil {
		return position{}, fmt.Errorf("cluster: read %s: %w", id, err)
	}
	p := position{events: int64(len(events))}
	if n := len(events); n > 0 {
		p.lastIndex = events[n-1].GetRaftIndex()
	}
	m.pos[id] = p
	return p, nil
}

func (m *stateMachine) appendEvent(index uint64, id string, ev *history.HistoryEvent) (any, error) {
	p, err := m.position(id)
	if err != nil {
		return nil, err
	}
	if index <= p.lastIndex {
		// Written before a restart: the log is being replayed over a history
		// directory that already holds this event.
		return nil, nil
	}
	if ev.GetEventId() != p.events+1 {
		return fmt.Errorf("%w: %s event %d, but the history has %d events", ErrConflict, id, ev.GetEventId(), p.events), nil
	}
	stamped := proto.Clone(ev).(*history.HistoryEvent)
	stamped.RaftIndex = index
	// No fsync per event: the Raft log already holds it durably. A crash can
	// lose the tail of a history file, and replaying the log after restart
	// writes it again (the stamps tell which events are already there).
	if err := m.store.AppendBuffered(id, stamped); err != nil {
		delete(m.pos, id)
		return nil, fmt.Errorf("cluster: append %s: %w", id, err)
	}
	if isTerminal(stamped) {
		if err := m.store.Release(id); err != nil {
			m.log.Warn("release history handle", "workflow_id", id, "err", err)
		}
	}
	m.pos[id] = position{events: p.events + 1, lastIndex: index}
	return nil, nil
}

func (m *stateMachine) deleteWorkflow(index uint64, id string) (any, error) {
	p, err := m.position(id)
	if err != nil {
		return nil, err
	}
	if p.lastIndex > index {
		return nil, nil // the id was reused after this delete; that history stays
	}
	if err := m.store.Delete(id); err != nil {
		delete(m.pos, id)
		return nil, fmt.Errorf("cluster: delete %s: %w", id, err)
	}
	m.pos[id] = position{}
	return nil, nil
}

func isTerminal(ev *history.HistoryEvent) bool {
	switch ev.GetAttributes().(type) {
	case *history.HistoryEvent_WorkflowCompleted, *history.HistoryEvent_WorkflowFailed:
		return true
	}
	return false
}

// Snapshot serializes every history file, byte for byte.
func (m *stateMachine) Snapshot() ([]byte, error) {
	// The marker written below promises the directory durably holds everything
	// applied so far, and once the snapshot is saved the log entries it covers
	// are gone. So the buffered appends must reach disk first.
	if err := m.store.Sync(); err != nil {
		return nil, fmt.Errorf("cluster: sync histories before snapshot: %w", err)
	}
	ids, err := m.store.ListWorkflows()
	if err != nil {
		return nil, fmt.Errorf("cluster: snapshot: %w", err)
	}
	slices.Sort(ids)
	head, err := proto.Marshal(&SnapshotHeader{Version: snapshotVersion, Workflows: uint64(len(ids))})
	if err != nil {
		return nil, fmt.Errorf("cluster: encode snapshot header: %w", err)
	}
	out, err := record.Encode(head)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		raw, err := m.store.ReadRaw(id)
		if err != nil {
			return nil, fmt.Errorf("cluster: snapshot %s: %w", id, err)
		}
		payload, err := proto.Marshal(&SnapshotWorkflow{WorkflowId: id, History: raw})
		if err != nil {
			return nil, fmt.Errorf("cluster: encode snapshot of %s: %w", id, err)
		}
		rec, err := record.Encode(payload)
		if err != nil {
			return nil, fmt.Errorf("cluster: snapshot of %s: %w", id, err)
		}
		out = append(out, rec...)
	}
	if len(out) > record.MaxSize {
		return nil, fmt.Errorf("cluster: snapshot is %d bytes, over the %d limit", len(out), record.MaxSize)
	}
	// The directory holds at least everything applied so far.
	if err := m.writeMarker(m.applied); err != nil {
		return nil, err
	}
	return out, nil
}

// Restore makes the history directory match a snapshot, unless the directory
// is already known to include it (the usual case at startup).
func (m *stateMachine) Restore(snap *raft.Snapshot) error {
	m.pos = make(map[string]position)
	m.applied = max(m.applied, snap.Index)
	have, err := m.readMarker()
	if err != nil {
		return err
	}
	if snap.Index <= have {
		return nil
	}
	files, err := decodeSnapshot(snap.Data)
	if err != nil {
		return err
	}
	if err := m.store.ReplaceAll(files); err != nil {
		return fmt.Errorf("cluster: restore snapshot %d: %w", snap.Index, err)
	}
	m.log.Info("cluster: history restored from snapshot", "index", snap.Index, "workflows", len(files))
	return m.writeMarker(snap.Index)
}

func decodeSnapshot(data []byte) (map[string][]byte, error) {
	var payloads [][]byte
	end, err := record.Scan(bufio.NewReader(bytes.NewReader(data)), func(p []byte) error {
		payloads = append(payloads, p)
		return nil
	})
	if err != nil || end != int64(len(data)) || len(payloads) == 0 {
		return nil, fmt.Errorf("cluster: malformed snapshot (%d records, err %v)", len(payloads), err)
	}
	head := &SnapshotHeader{}
	if err := proto.Unmarshal(payloads[0], head); err != nil {
		return nil, fmt.Errorf("cluster: decode snapshot header: %w", err)
	}
	if head.GetVersion() != snapshotVersion || head.GetWorkflows() != uint64(len(payloads)-1) {
		return nil, fmt.Errorf("cluster: snapshot version %d with %d of %d workflows", head.GetVersion(), len(payloads)-1, head.GetWorkflows())
	}
	files := make(map[string][]byte, len(payloads)-1)
	for _, p := range payloads[1:] {
		wf := &SnapshotWorkflow{}
		if err := proto.Unmarshal(p, wf); err != nil {
			return nil, fmt.Errorf("cluster: decode snapshot workflow: %w", err)
		}
		files[wf.GetWorkflowId()] = wf.GetHistory()
	}
	return files, nil
}

func (m *stateMachine) readMarker() (uint64, error) {
	payloads, err := record.ReadFile(filepath.Join(m.raftDir, markerFile))
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil || len(payloads) != 1 {
		return 0, fmt.Errorf("cluster: read %s: %v", markerFile, err)
	}
	n, err := strconv.ParseUint(string(payloads[0]), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("cluster: parse %s: %w", markerFile, err)
	}
	return n, nil
}

func (m *stateMachine) writeMarker(index uint64) error {
	if err := record.WriteFile(m.raftDir, markerFile, []byte(strconv.FormatUint(index, 10))); err != nil {
		return fmt.Errorf("cluster: write %s: %w", markerFile, err)
	}
	return nil
}
