package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"
)

// Defaults for a Node's timing. With a 100 ms tick a leader heartbeats every
// 100 ms and a follower waits 1–2 s of silence before starting an election.
const (
	DefaultTickInterval   = 100 * time.Millisecond
	DefaultElectionTicks  = 10
	DefaultHeartbeatTicks = 1
	// DefaultSnapshotEvery is how many applied entries pile up before a node
	// snapshots its state machine and compacts its log.
	DefaultSnapshotEvery = 8192
)

var (
	// ErrStopped is returned by a Node that has shut down.
	ErrStopped = errors.New("raft: node stopped")
	// ErrLeadershipLost means this node stopped leading before the proposal
	// was applied. The entry may still commit under the next leader, or be
	// discarded: the outcome is unknown, so the caller must not assume either.
	ErrLeadershipLost = errors.New("raft: leadership lost before the entry was applied; it may or may not commit")
	// ErrProposalDropped means a new leader overwrote the proposal's entry
	// with its own: it will never be applied.
	ErrProposalDropped = errors.New("raft: proposal overwritten by a new leader")
)

// Transport delivers messages to peers. Send must not block: Raft tolerates
// lost messages, so a transport that cannot keep up should drop.
type Transport interface {
	Send(m *Envelope)
}

// StateMachine is what a cluster replicates: every node applies the same
// committed entries, in the same order, so every node ends in the same state.
// All three methods are called from one goroutine, never concurrently.
type StateMachine interface {
	// Apply applies one committed entry and returns a result for the proposer,
	// if it is waiting on this node. After a restart Apply sees again every
	// entry since the latest snapshot, so a state machine that persists its own
	// state must recognise entries it already applied. A non-nil error means
	// this node's copy of the state can no longer be trusted (a failed disk
	// write, say) and stops the node; an outcome to report to the proposer,
	// like a rejected command, belongs in the result instead.
	Apply(entry *LogEntry) (any, error)
	// Snapshot serializes the state as of the last applied entry.
	Snapshot() ([]byte, error)
	// Restore replaces the whole state with a snapshot's: at startup, and when
	// the leader sends one because this node fell too far behind.
	Restore(snap *Snapshot) error
}

// Config configures a Node.
type Config struct {
	ID string
	// Members is every node in the cluster, ID included. Membership is static.
	Members      []string
	Storage      *DiskStorage
	Transport    Transport
	StateMachine StateMachine
	Logger       *slog.Logger

	// Zero values pick the defaults above.
	TickInterval   time.Duration
	ElectionTicks  int
	HeartbeatTicks int
	SnapshotEvery  uint64
}

// Node runs one Raft member. One goroutine owns the core: it turns wall-clock
// time into ticks, persists before it sends, and queues committed entries. A
// second goroutine applies them, so a slow state machine never delays
// heartbeats.
type Node struct {
	id        string
	core      *core
	storage   *DiskStorage
	transport Transport
	sm        StateMachine
	tick      time.Duration
	logger    *slog.Logger

	recvc chan *Envelope
	propc chan *proposal
	snapc chan *Snapshot // local snapshots, from the applier to the run loop
	stopc chan struct{}  // closed to stop both goroutines
	done  chan struct{}  // closed once both have exited
	wg    sync.WaitGroup

	errMu sync.Mutex
	err   error // the first failure that stopped the node

	snapshotEvery uint64

	startOnce sync.Once
	stopOnce  sync.Once

	applyMu    sync.Mutex
	applyq     []applyItem
	applyReady chan struct{} // buffered(1): "applyq is not empty"

	// Owned by the apply goroutine.
	appliedTerm uint64 // term of the last applied entry
	snapIndex   uint64 // index of the latest snapshot this node has

	mu      sync.Mutex
	status  Status
	applied uint64
	changed chan struct{}        // closed and replaced whenever status changes
	waiters map[uint64]*proposal // log index -> proposal waiting for it
}

// applyItem is one unit of work for the applier, in log order: a committed
// entry, or a snapshot from the leader that replaces everything before it.
type applyItem struct {
	entry *LogEntry
	snap  *Snapshot
}

type proposal struct {
	term     uint64
	data     []byte
	index    uint64
	accepted chan error // the run loop's verdict: appended, or refused
	result   chan proposalResult
}

type proposalResult struct {
	value any
	err   error
}

// NewNode loads the node's durable state and builds it, stopped. Call Start.
func NewNode(cfg Config) (*Node, error) {
	if cfg.Storage == nil || cfg.Transport == nil || cfg.StateMachine == nil || cfg.Logger == nil {
		return nil, errors.New("raft: config needs Storage, Transport, StateMachine and Logger")
	}
	tick := cfg.TickInterval
	if tick == 0 {
		tick = DefaultTickInterval
	}
	election, heartbeat, every := cfg.ElectionTicks, cfg.HeartbeatTicks, cfg.SnapshotEvery
	if election == 0 {
		election = DefaultElectionTicks
	}
	if heartbeat == 0 {
		heartbeat = DefaultHeartbeatTicks
	}
	if every == 0 {
		every = DefaultSnapshotEvery
	}
	st, err := cfg.Storage.load()
	if err != nil {
		return nil, err
	}
	if st.snapshot != nil {
		// The state machine starts from the snapshot; only later entries are
		// applied again.
		if err := cfg.StateMachine.Restore(st.snapshot); err != nil {
			return nil, fmt.Errorf("raft: restore snapshot at %d: %w", st.snapshot.Index, err)
		}
	}
	c, err := newCore(coreConfig{
		id:            cfg.ID,
		members:       cfg.Members,
		electionTick:  election,
		heartbeatTick: heartbeat,
		rand:          rand.New(rand.NewPCG(rand.Uint64(), rand.Uint64())),
		logger:        cfg.Logger,
	}, st)
	if err != nil {
		return nil, err
	}
	n := &Node{
		id:         cfg.ID,
		core:       c,
		storage:    cfg.Storage,
		transport:  cfg.Transport,
		sm:         cfg.StateMachine,
		tick:       tick,
		logger:     cfg.Logger.With("node_id", cfg.ID),
		recvc:      make(chan *Envelope, 256),
		propc:      make(chan *proposal, 256),
		snapc:      make(chan *Snapshot, 1),
		stopc:      make(chan struct{}),
		done:       make(chan struct{}),
		applyReady: make(chan struct{}, 1),
		status:     c.status(),
		changed:    make(chan struct{}),
		waiters:    make(map[uint64]*proposal),

		snapshotEvery: every,
		applied:       c.log.handedOut,
		appliedTerm:   c.log.offsetTerm,
		snapIndex:     c.log.offset,
	}
	n.status.Applied = n.applied
	n.logger.Info("raft: node loaded", "term", st.hardState.GetCurrentTerm(),
		"voted_for", st.hardState.GetVotedFor(), "snapshot", c.log.offset,
		"last_index", c.log.lastIndex(), "members", cfg.Members)
	return n, nil
}

// Start launches the node's goroutines.
func (n *Node) Start() {
	n.startOnce.Do(func() {
		n.wg.Add(2)
		go n.run()
		go n.applyLoop()
		go func() {
			n.wg.Wait()
			close(n.done)
		}()
	})
}

// Stop shuts the node down and waits for it. It returns the error that stopped
// the node early, if one did (a failed fsync, for example).
func (n *Node) Stop() error {
	n.stopOnce.Do(func() { close(n.stopc) })
	n.Start() // so done is closed even if Start was never called
	return n.Err()
}

// Done is closed once the node has stopped, for any reason.
func (n *Node) Done() <-chan struct{} { return n.done }

// Err reports why the node stopped; nil after a clean Stop. Valid after Done.
func (n *Node) Err() error {
	<-n.done
	n.errMu.Lock()
	defer n.errMu.Unlock()
	return n.err
}

// fail stops the node because it can no longer guarantee what it promised:
// a write that didn't reach disk, a snapshot that wouldn't restore.
func (n *Node) fail(err error) {
	n.errMu.Lock()
	if n.err == nil {
		n.err = err
	}
	n.errMu.Unlock()
	n.logger.Error("raft: stopping node", "err", err)
	n.stopOnce.Do(func() { close(n.stopc) })
}

// Step hands a message from a peer to the node.
func (n *Node) Step(ctx context.Context, m *Envelope) error {
	select {
	case n.recvc <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

// Propose replicates data and waits until this node has applied it, returning
// what the state machine's Apply returned. term must be the term this node
// leads: a caller acting for an earlier leadership is refused with
// ErrNotLeader, even if the node has since been elected again.
//
// An error other than ErrNotLeader can leave the outcome unknown (see
// ErrLeadershipLost): the entry may still be applied later.
func (n *Node) Propose(ctx context.Context, term uint64, data []byte) (any, error) {
	if len(data) == 0 {
		// Empty entries are reserved for the one a leader opens its term with.
		return nil, errors.New("raft: empty proposal")
	}
	p := &proposal{term: term, data: data, accepted: make(chan error, 1), result: make(chan proposalResult, 1)}
	select {
	case n.propc <- p:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}
	select {
	case err := <-p.accepted:
		if err != nil {
			return nil, err
		}
	case <-n.done:
		return nil, ErrStopped
	}
	select {
	case r := <-p.result:
		return r.value, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-n.done:
		return nil, ErrStopped
	}
}

// Status returns the node's latest published status.
func (n *Node) Status() Status {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.status
}

// Changed returns a channel that is closed the next time Status changes.
func (n *Node) Changed() <-chan struct{} {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.changed
}

func (n *Node) run() {
	defer n.wg.Done()
	defer n.stopOnce.Do(func() { close(n.stopc) }) // an early exit stops the applier too
	ticker := time.NewTicker(n.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.core.tick()
		case m := <-n.recvc:
			n.core.step(m)
		case p := <-n.propc:
			n.propose(p)
		case snap := <-n.snapc:
			if err := n.compact(snap); err != nil {
				n.fail(err)
				return
			}
		case <-n.stopc:
			return
		}
		n.drainInbox()
		if err := n.flush(); err != nil {
			// A node that cannot persist must stop: carrying on could mean voting
			// twice in one term, or acknowledging entries it would then lose.
			n.fail(err)
			return
		}
	}
}

// drainInbox steps every message already waiting, so a burst of messages
// costs one flush (one fsync) instead of one each. Without it a follower that
// fsyncs per message falls behind a busy leader and its acknowledgements
// arrive too late for the leader's quorum check.
func (n *Node) drainInbox() {
	for range cap(n.recvc) {
		select {
		case m := <-n.recvc:
			n.core.step(m)
		default:
			return
		}
	}
}

// compact makes a local snapshot durable, then drops the log entries it covers.
func (n *Node) compact(snap *Snapshot) error {
	if snap.Index <= n.core.log.offset {
		return nil // overtaken by a snapshot installed from the leader
	}
	if err := n.storage.saveSnapshot(snap); err != nil {
		return fmt.Errorf("raft: persist snapshot: %w", err)
	}
	if err := n.storage.compactLog(snap.Index); err != nil {
		return fmt.Errorf("raft: compact log: %w", err)
	}
	n.core.compact(snap)
	n.logger.Info("raft: snapshot taken", "index", snap.Index, "bytes", len(snap.Data))
	return nil
}

// propose hands one proposal, plus any others already queued, to the core in
// one batch, so they share one disk write and one round of messages.
func (n *Node) propose(first *proposal) {
	batch := []*proposal{first}
drain:
	for len(batch) < maxEntriesPerMsg {
		select {
		case p := <-n.propc:
			batch = append(batch, p)
		default:
			break drain
		}
	}
	for len(batch) > 0 {
		// Consecutive proposals for the same term go in together.
		k := 1
		for k < len(batch) && batch[k].term == batch[0].term {
			k++
		}
		group := batch[:k]
		batch = batch[k:]
		data := make([][]byte, len(group))
		for i, p := range group {
			data[i] = p.data
		}
		idx, err := n.core.propose(group[0].term, data...)
		if err != nil {
			for _, p := range group {
				p.accepted <- err
			}
			continue
		}
		n.mu.Lock()
		for i, p := range group {
			p.index = idx + uint64(i)
			n.waiters[p.index] = p
		}
		n.mu.Unlock()
		for _, p := range group {
			p.accepted <- nil
		}
	}
}

// flush handles every pending Ready: persist, then send, then queue for apply.
// The order is the whole point: nothing leaves this node, and nothing is
// applied, until the state it reflects is durable. The one exception is a
// leader's AppendEntries (Ready.LeaderAppend), which replicates entries the
// leader is still writing; its own copy counts only once written.
func (n *Node) flush() error {
	for n.core.hasReady() {
		rd := n.core.ready()
		if rd.HardState != nil {
			if err := n.storage.saveHardState(rd.HardState); err != nil {
				return fmt.Errorf("raft: persist hard state: %w", err)
			}
		}
		if rd.Snapshot != nil {
			// Snapshot first, then drop the log it replaces: a crash in between
			// leaves a log that load() recognises and resets.
			if err := n.storage.saveSnapshot(rd.Snapshot); err != nil {
				return fmt.Errorf("raft: persist leader snapshot: %w", err)
			}
			if err := n.storage.resetLog(rd.Snapshot.Index); err != nil {
				return err
			}
			n.enqueueApply(applyItem{snap: rd.Snapshot})
		}
		if rd.LeaderAppend {
			// Followers write their copies while we write ours: one fsync of
			// latency per commit instead of two.
			for _, m := range rd.Messages {
				n.transport.Send(m)
			}
		}
		if len(rd.Entries) > 0 {
			if err := n.storage.appendEntries(rd.Entries); err != nil {
				return fmt.Errorf("raft: persist entries: %w", err)
			}
		}
		if !rd.LeaderAppend {
			for _, m := range rd.Messages {
				n.transport.Send(m)
			}
		}
		for _, e := range rd.CommittedEntries {
			n.enqueueApply(applyItem{entry: e})
		}
		n.core.advance(rd)
	}
	n.publish()
	return nil
}

func (n *Node) enqueueApply(it applyItem) {
	n.applyMu.Lock()
	n.applyq = append(n.applyq, it)
	n.applyMu.Unlock()
	select {
	case n.applyReady <- struct{}{}:
	default:
	}
}

func (n *Node) applyLoop() {
	defer n.wg.Done()
	for {
		select {
		case <-n.applyReady:
		case <-n.stopc:
			return // anything still queued is applied again after the restart
		}
		n.applyMu.Lock()
		items := n.applyq
		n.applyq = nil
		n.applyMu.Unlock()
		for _, it := range items {
			if it.snap != nil {
				if err := n.sm.Restore(it.snap); err != nil {
					n.fail(fmt.Errorf("raft: restore snapshot at %d: %w", it.snap.Index, err))
					return
				}
				n.appliedTerm, n.snapIndex = it.snap.Term, it.snap.Index
				n.markApplied(it.snap.Index)
				continue
			}
			var v any
			if len(it.entry.GetData()) > 0 {
				var err error
				if v, err = n.sm.Apply(it.entry); err != nil {
					n.fail(fmt.Errorf("raft: apply entry %d: %w", it.entry.GetIndex(), err))
					return
				}
			}
			n.appliedTerm = it.entry.GetTerm()
			n.applied1(it.entry, v)
		}
		n.maybeSnapshot()
	}
}

// maybeSnapshot snapshots the state machine once enough entries have been
// applied since the last snapshot, and hands it to the run loop to persist.
func (n *Node) maybeSnapshot() {
	n.mu.Lock()
	applied := n.applied
	n.mu.Unlock()
	if applied-n.snapIndex < n.snapshotEvery {
		return
	}
	data, err := n.sm.Snapshot()
	if err != nil {
		// Not fatal: the log just keeps growing until a later attempt works.
		n.logger.Error("raft: snapshot failed", "index", applied, "err", err)
		return
	}
	select {
	case n.snapc <- &Snapshot{Index: applied, Term: n.appliedTerm, Data: data}:
		n.snapIndex = applied
	default: // the previous one is still being written; try after the next batch
	}
}

func (n *Node) markApplied(index uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.applied = index
	n.status.Applied = index
	n.notifyLocked()
}

// applied1 records that e was applied and answers its proposer, if waiting.
func (n *Node) applied1(e *LogEntry, v any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if p, ok := n.waiters[e.GetIndex()]; ok {
		delete(n.waiters, e.GetIndex())
		if p.term == e.GetTerm() {
			p.result <- proposalResult{value: v}
		} else {
			p.result <- proposalResult{err: ErrProposalDropped}
		}
	}
	n.applied = e.GetIndex()
	n.status.Applied = n.applied
	n.notifyLocked()
}

func (n *Node) publish() {
	st := n.core.status()
	n.mu.Lock()
	defer n.mu.Unlock()
	st.Applied = n.applied
	if st == n.status {
		return
	}
	prev := n.status
	if st.Role != prev.Role || st.Leader != prev.Leader {
		n.logger.Info("raft: status changed", "role", st.Role.String(), "term", st.Term, "leader", st.Leader)
	}
	if prev.Role == Leader && (st.Role != Leader || st.Term != prev.Term) {
		// Proposals from the term that just ended can't be answered by this
		// node any more. Their entries may still commit elsewhere.
		for idx, p := range n.waiters {
			p.result <- proposalResult{err: ErrLeadershipLost}
			delete(n.waiters, idx)
		}
	}
	n.status = st
	n.notifyLocked()
}

func (n *Node) notifyLocked() {
	close(n.changed)
	n.changed = make(chan struct{})
}
