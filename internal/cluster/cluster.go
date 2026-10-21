// Package cluster runs workflowd as a member of a Raft cluster (ADR-0004).
// Every node applies the replicated history log to its own history store;
// only the leader runs a workflow engine, a fresh one per leadership term,
// and every other node answers client calls with a hint naming the leader.
package cluster

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/metrics"
	"github.com/olayanju-1234/durable-workflow-engine/internal/raft"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// Metadata keys a node attaches to its Raft traffic so peers can point
// clients at it when it leads.
const (
	mdGRPC = "workflowd-grpc"
	mdHTTP = "workflowd-http"
)

// Config configures one cluster member.
type Config struct {
	NodeID string
	// Peers maps every member's id, this node's included, to the address its
	// Raft traffic should be sent to.
	Peers map[string]string
	// RaftAddr is where this node listens for Raft traffic.
	RaftAddr string
	// GRPCAddr and HTTPAddr are this node's client addresses, given to clients
	// that reach a follower.
	GRPCAddr, HTTPAddr string
	DataDir            string
	Store              *history.HistoryStore
	Logger             *slog.Logger

	// NewEngine builds the engine for one leadership term, writing its history
	// through log. It registers workflow types; it must not call Recover.
	NewEngine func(log server.HistoryLog) *server.Engine
	// OnEngine, if set, runs next to each term's engine (the demo worker,
	// retention) until ctx is cancelled at the end of the term.
	OnEngine func(ctx context.Context, e *server.Engine)

	// PeerTLS, if set, is a mutual-TLS config (tlsconfig.Peer) for Raft traffic
	// in both directions: peers without a certificate from its CA are refused.
	PeerTLS *tls.Config

	// Zero values pick raft's defaults.
	TickInterval  time.Duration
	ElectionTicks int
	SnapshotEvery uint64
}

// Cluster is a running member. It is a server.EngineSource.
type Cluster struct {
	cfg       Config
	log       *slog.Logger
	node      *raft.Node
	storage   *raft.DiskStorage
	transport *raft.GRPCTransport
	raftSrv   *grpc.Server

	mu         sync.Mutex
	current    *leadership                  // nil unless this node leads and its engine is up
	failedTerm uint64                       // a term whose engine failed to recover; not retried
	peerAddrs  map[string]server.LeaderHint // client addresses learned from Raft traffic

	stopc    chan struct{}
	watched  chan struct{}
	stopOnce sync.Once
}

// leadership is one term's engine and the helpers running beside it.
type leadership struct {
	term    uint64
	engine  *server.Engine
	cancel  context.CancelFunc
	stopped chan struct{} // closed once OnEngine has returned
}

// Start opens this node's Raft state, starts its Raft listener and joins the
// cluster. The engine comes later, if and when this node becomes leader.
func Start(cfg Config) (*Cluster, error) {
	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := checkBootstrap(raftDir, cfg.Store); err != nil {
		return nil, err
	}
	logger := cfg.Logger.With("node_id", cfg.NodeID)
	c := &Cluster{
		cfg:       cfg,
		log:       logger,
		peerAddrs: map[string]server.LeaderHint{cfg.NodeID: {ID: cfg.NodeID, GRPCAddr: cfg.GRPCAddr, HTTPAddr: cfg.HTTPAddr}},
		stopc:     make(chan struct{}),
		watched:   make(chan struct{}),
	}
	storage, err := raft.OpenDiskStorage(raftDir)
	if err != nil {
		return nil, err
	}
	c.storage = storage
	dialOpts := []grpc.DialOption{grpc.WithUnaryInterceptor(c.advertise)}
	srvOpts := append(raft.ServerOptions(), grpc.UnaryInterceptor(c.observe))
	if cfg.PeerTLS != nil {
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(credentials.NewTLS(cfg.PeerTLS)))
		srvOpts = append(srvOpts, grpc.Creds(credentials.NewTLS(cfg.PeerTLS)))
	}
	c.transport, err = raft.NewGRPCTransport(cfg.NodeID, cfg.Peers, cfg.Logger, dialOpts...)
	if err != nil {
		storage.Close()
		return nil, err
	}
	members := make([]string, 0, len(cfg.Peers))
	for id := range cfg.Peers {
		members = append(members, id)
	}
	slices.Sort(members)
	c.node, err = raft.NewNode(raft.Config{
		ID: cfg.NodeID, Members: members, Storage: storage, Transport: c.transport,
		StateMachine: newStateMachine(cfg.Store, raftDir, logger), Logger: cfg.Logger,
		TickInterval: cfg.TickInterval, ElectionTicks: cfg.ElectionTicks, SnapshotEvery: cfg.SnapshotEvery,
	})
	if err != nil {
		c.transport.Close()
		storage.Close()
		return nil, err
	}
	lis, err := net.Listen("tcp", cfg.RaftAddr)
	if err != nil {
		c.transport.Close()
		storage.Close()
		return nil, fmt.Errorf("cluster: listen %s: %w", cfg.RaftAddr, err)
	}
	c.raftSrv = grpc.NewServer(srvOpts...)
	raft.RegisterServer(c.raftSrv, c.node)
	go func() { _ = c.raftSrv.Serve(lis) }()
	c.node.Start()
	go c.watch()
	logger.Info("cluster: node started", "raft_addr", cfg.RaftAddr, "members", members)
	return c, nil
}

// checkBootstrap refuses a data directory that has workflow histories but no
// Raft state: those workflows exist on this node only, and the other members
// would never learn about them.
func checkBootstrap(raftDir string, store *history.HistoryStore) error {
	if _, err := os.Stat(raftDir); err == nil {
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("cluster: %w", err)
	}
	ids, err := store.ListWorkflows()
	if err != nil {
		return fmt.Errorf("cluster: %w", err)
	}
	if len(ids) > 0 {
		return fmt.Errorf("cluster: the data dir holds %d workflow histories but no Raft state; "+
			"moving a single-node deployment into a cluster is not supported yet, so start cluster nodes on empty data dirs", len(ids))
	}
	return nil
}

// advertise attaches this node's client addresses to every Raft message it
// sends, so whoever it leads can redirect clients to it.
func (c *Cluster) advertise(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	ctx = metadata.AppendToOutgoingContext(ctx, mdGRPC, c.cfg.GRPCAddr, mdHTTP, c.cfg.HTTPAddr)
	return invoker(ctx, method, req, reply, cc, opts...)
}

// observe records a peer's client addresses from its Raft traffic.
func (c *Cluster) observe(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if env, ok := req.(*raft.Envelope); ok {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			h := server.LeaderHint{ID: env.GetFrom(), GRPCAddr: first(md.Get(mdGRPC)), HTTPAddr: first(md.Get(mdHTTP))}
			c.mu.Lock()
			c.peerAddrs[env.GetFrom()] = h
			c.mu.Unlock()
		}
	}
	return handler(ctx, req)
}

func first(vals []string) string {
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}

// Engine returns the engine serving this node's requests: the current term's
// engine on a ready leader, a *server.NotLeaderError everywhere else.
func (c *Cluster) Engine() (*server.Engine, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.current != nil {
		return c.current.engine, nil
	}
	st := c.node.Status()
	hint := c.peerAddrs[st.Leader] // zero value while no leader is known
	hint.ID = st.Leader
	return nil, &server.NotLeaderError{Leader: hint}
}

// Status is this node's Raft status.
func (c *Cluster) Status() raft.Status { return c.node.Status() }

// StatusMap is the node's cluster view for /api/health.
func (c *Cluster) StatusMap() map[string]any {
	st := c.node.Status()
	c.mu.Lock()
	serving := c.current != nil
	lead := c.peerAddrs[st.Leader]
	c.mu.Unlock()
	return map[string]any{
		"node_id":        st.ID,
		"role":           st.Role.String(),
		"term":           st.Term,
		"leader_id":      st.Leader,
		"leader_grpc":    lead.GRPCAddr,
		"leader_http":    lead.HTTPAddr,
		"serving":        serving,
		"commit_index":   st.Commit,
		"applied_index":  st.Applied,
		"snapshot_index": st.Snapshot,
	}
}

// Done is closed if the Raft node stops on its own (a failed disk write).
func (c *Cluster) Done() <-chan struct{} { return c.node.Done() }

// Err is why the Raft node stopped. Valid after Done.
func (c *Cluster) Err() error { return c.node.Err() }

// Stop leaves the cluster: Raft first, so proposals still waiting fail and
// release the engine, then the engine and its helpers.
func (c *Cluster) Stop() error {
	var err error
	c.stopOnce.Do(func() {
		err = c.node.Stop()
		close(c.stopc)
		<-c.watched
		c.raftSrv.Stop()
		c.transport.Close()
		if cerr := c.storage.Close(); err == nil {
			err = cerr
		}
	})
	return err
}

// watch starts and stops engines as leadership comes and goes.
func (c *Cluster) watch() {
	defer close(c.watched)
	defer c.stopLeadership()
	for {
		changed := c.node.Changed()
		c.reconcile(c.node.Status())
		select {
		case <-changed:
		case <-c.stopc:
			return
		case <-c.node.Done():
			return
		}
	}
}

func (c *Cluster) reconcile(st raft.Status) {
	c.mu.Lock()
	cur, failed := c.current, c.failedTerm
	c.mu.Unlock()
	switch {
	case st.LeaderReady() && (cur == nil || cur.term != st.Term) && failed != st.Term:
		c.stopLeadership()
		c.startLeadership(st.Term)
	case cur != nil && !(st.Role == raft.Leader && st.Term == cur.term):
		c.stopLeadership()
	}
}

// startLeadership builds a fresh engine for term and recovers it from the
// history store. The leader has applied every entry committed before its term
// (that's what LeaderReady means), so Recover sees the complete history: a
// leader change is, to the engine, just a restart on another machine.
func (c *Cluster) startLeadership(term uint64) {
	e := c.cfg.NewEngine(&replicatedLog{node: c.node, term: term})
	if err := e.Recover(); err != nil {
		e.Close()
		if errors.Is(err, server.ErrNotLeader) {
			c.log.Warn("cluster: lost leadership while recovering", "term", term, "err", err)
			return
		}
		c.log.Error("cluster: engine recovery failed; not serving this term", "term", term, "err", err)
		c.mu.Lock()
		c.failedTerm = term
		c.mu.Unlock()
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	l := &leadership{term: term, engine: e, cancel: cancel, stopped: make(chan struct{})}
	go func() {
		defer close(l.stopped)
		if c.cfg.OnEngine != nil {
			c.cfg.OnEngine(ctx, e)
		}
	}()
	c.mu.Lock()
	c.current = l
	c.mu.Unlock()
	c.log.Info("cluster: leading; engine recovered and serving", "term", term)
}

func (c *Cluster) stopLeadership() {
	c.mu.Lock()
	l := c.current
	c.current = nil
	c.mu.Unlock()
	if l == nil {
		return
	}
	l.cancel()
	l.engine.Close()
	<-l.stopped
	c.log.Info("cluster: engine stopped; no longer leading", "term", l.term)
}

// MetricsHandler serves Raft gauges for every node plus the current engine's
// metrics on the leader. Engine counters restart with each term's engine, the
// same as after a restart.
func (c *Cluster) MetricsHandler() http.Handler {
	reg := metrics.NewRegistry()
	gauge := func(name, help string, fn func(raft.Status) float64) {
		reg.GaugeFunc(name, help, func() float64 { return fn(c.node.Status()) })
	}
	gauge("workflowd_raft_term", "Current Raft term.", func(s raft.Status) float64 { return float64(s.Term) })
	gauge("workflowd_raft_leader", "1 if this node is the leader.", func(s raft.Status) float64 {
		if s.Role == raft.Leader {
			return 1
		}
		return 0
	})
	gauge("workflowd_raft_commit_index", "Highest log index known committed.", func(s raft.Status) float64 { return float64(s.Commit) })
	gauge("workflowd_raft_applied_index", "Highest log index applied to the history store.", func(s raft.Status) float64 { return float64(s.Applied) })
	gauge("workflowd_raft_snapshot_index", "Last log index covered by this node's snapshot.", func(s raft.Status) float64 { return float64(s.Snapshot) })
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		reg.WriteText(w)
		if e, err := c.Engine(); err == nil {
			e.Metrics().WriteText(w)
		}
	})
}

var _ server.EngineSource = (*Cluster)(nil)
