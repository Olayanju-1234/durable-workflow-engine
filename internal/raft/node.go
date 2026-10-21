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

// ErrStopped is returned by a Node that has shut down.
var ErrStopped = errors.New("raft: node stopped")

// Transport delivers messages to peers. Send must not block: Raft tolerates
// lost messages, so a transport that cannot keep up should drop.
type Transport interface {
	Send(m *Envelope)
}

// Config configures a Node.
type Config struct {
	ID string
	// Members is every node in the cluster, ID included. Membership is static.
	Members   []string
	Storage   *DiskStorage
	Transport Transport
	Logger    *slog.Logger

	// Zero values pick the defaults above.
	TickInterval   time.Duration
	ElectionTicks  int
	HeartbeatTicks int
}

// Node runs one Raft member: a goroutine that owns the core, turns wall-clock
// time into ticks, persists before it sends, and publishes status changes.
type Node struct {
	id        string
	core      *core
	storage   *DiskStorage
	transport Transport
	tick      time.Duration
	logger    *slog.Logger

	recvc chan *Envelope
	stopc chan struct{}
	done  chan struct{}
	err   error // why run exited; read only after done is closed

	startOnce sync.Once
	stopOnce  sync.Once

	mu      sync.Mutex
	status  Status
	changed chan struct{} // closed and replaced whenever status changes
}

// NewNode loads the node's durable state and builds it, stopped. Call Start.
func NewNode(cfg Config) (*Node, error) {
	if cfg.Storage == nil || cfg.Transport == nil || cfg.Logger == nil {
		return nil, errors.New("raft: config needs Storage, Transport and Logger")
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
		id:        cfg.ID,
		core:      c,
		storage:   cfg.Storage,
		transport: cfg.Transport,
		tick:      tick,
		logger:    cfg.Logger.With("node_id", cfg.ID),
		recvc:     make(chan *Envelope, 256),
		stopc:     make(chan struct{}),
		done:      make(chan struct{}),
		status:    c.status(),
		changed:   make(chan struct{}),
	}
	n.logger.Info("raft: node loaded", "term", st.hardState.GetCurrentTerm(),
		"voted_for", st.hardState.GetVotedFor(), "members", cfg.Members)
	return n, nil
}

// Start launches the node's goroutine.
func (n *Node) Start() {
	n.startOnce.Do(func() { go n.run() })
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
	defer close(n.done)
	ticker := time.NewTicker(n.tick)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			n.core.tick()
		case m := <-n.recvc:
			n.core.step(m)
		case <-n.stopc:
			return
		}
		if err := n.flush(); err != nil {
			// A node that cannot persist must stop: carrying on could mean voting
			// twice in one term after the next restart.
			n.err = err
			n.logger.Error("raft: stopping node", "err", err)
			return
		}
	}
}

// flush persists, then sends, then acknowledges one Ready. The order is the
// whole point: nothing leaves this node until the state it reflects is durable.
func (n *Node) flush() error {
	if n.core.hasReady() {
		rd := n.core.ready()
		if rd.HardState != nil {
			if err := n.storage.saveHardState(rd.HardState); err != nil {
				return fmt.Errorf("raft: persist hard state: %w", err)
			}
		}
		for _, m := range rd.Messages {
			n.transport.Send(m)
		}
		n.core.advance(rd)
	}
	n.publish()
	return nil
}

func (n *Node) publish() {
	st := n.core.status()
	n.mu.Lock()
	defer n.mu.Unlock()
	if st == n.status {
		return
	}
	if st.Role != n.status.Role || st.Leader != n.status.Leader {
		n.logger.Info("raft: status changed", "role", st.Role.String(), "term", st.Term, "leader", st.Leader)
	}
	n.status = st
	close(n.changed)
	n.changed = make(chan struct{})
}
