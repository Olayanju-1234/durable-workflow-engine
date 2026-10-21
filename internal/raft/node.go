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
type StateMachine interface {
	// Apply applies one committed entry and returns a result for the proposer,
	// if it is waiting on this node. After a restart Apply sees again every
	// entry since the start of the log, so a state machine that persists its
	// own state must recognise entries it already applied.
	Apply(entry *LogEntry) any
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
	stopc chan struct{} // closed to stop both goroutines
	done  chan struct{} // closed once both have exited
	err   error         // why the run loop exited; read only after done
	wg    sync.WaitGroup

	startOnce sync.Once
	stopOnce  sync.Once

	applyMu    sync.Mutex
	applyq     []*LogEntry
	applyReady chan struct{} // buffered(1): "applyq is not empty"

	mu      sync.Mutex
	status  Status
	applied uint64
	changed chan struct{}        // closed and replaced whenever status changes
	waiters map[uint64]*proposal // log index -> proposal waiting for it
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
	election, heartbeat := cfg.ElectionTicks, cfg.HeartbeatTicks
	if election == 0 {
		election = DefaultElectionTicks
	}
	if heartbeat == 0 {
		heartbeat = DefaultHeartbeatTicks
	}
	st, err := cfg.Storage.load()
	if err != nil {
		return nil, err
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
		stopc:      make(chan struct{}),
		done:       make(chan struct{}),
		applyReady: make(chan struct{}, 1),
		status:     c.status(),
		changed:    make(chan struct{}),
		waiters:    make(map[uint64]*proposal),
	}
	n.logger.Info("raft: node loaded", "term", st.hardState.GetCurrentTerm(),
		"voted_for", st.hardState.GetVotedFor(), "last_index", c.log.lastIndex(), "members", cfg.Members)
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
	<-n.done
	return n.err
}

// Done is closed once the node has stopped, for any reason.
func (n *Node) Done() <-chan struct{} { return n.done }

// Err reports why the node stopped; nil after a clean Stop. Valid after Done.
func (n *Node) Err() error {
	<-n.done
	return n.err
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
		case <-n.stopc:
			return
		}
		if err := n.flush(); err != nil {
			// A node that cannot persist must stop: carrying on could mean voting
			// twice in one term, or acknowledging entries it would then lose.
			n.err = err
			n.logger.Error("raft: stopping node", "err", err)
			return
		}
	}
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
// applied, until the state it reflects is durable.
func (n *Node) flush() error {
	for n.core.hasReady() {
		rd := n.core.ready()
		if rd.HardState != nil {
			if err := n.storage.saveHardState(rd.HardState); err != nil {
				return fmt.Errorf("raft: persist hard state: %w", err)
			}
		}
		if len(rd.Entries) > 0 {
			if err := n.storage.appendEntries(rd.Entries); err != nil {
				return fmt.Errorf("raft: persist entries: %w", err)
			}
		}
		for _, m := range rd.Messages {
			n.transport.Send(m)
		}
		if len(rd.CommittedEntries) > 0 {
			n.enqueueApply(rd.CommittedEntries)
		}
		n.core.advance(rd)
	}
	n.publish()
	return nil
}

func (n *Node) enqueueApply(ents []*LogEntry) {
	n.applyMu.Lock()
	n.applyq = append(n.applyq, ents...)
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
		ents := n.applyq
		n.applyq = nil
		n.applyMu.Unlock()
		for _, e := range ents {
			var v any
			if len(e.GetData()) > 0 {
				v = n.sm.Apply(e)
			}
			n.applied1(e, v)
		}
	}
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
