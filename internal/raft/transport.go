package raft

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
)

const (
	// peerQueueSize bounds the messages waiting for one peer. When a peer is
	// down or slow its queue fills and newer messages are dropped, which Raft
	// absorbs: a follower that misses entries rejects the next message and the
	// leader backs up. Kept short because every queued message is latency
	// added to the ones behind it.
	peerQueueSize = 256
	// sendTimeout bounds one Send call, so a hung peer can't stall the queue.
	sendTimeout = 10 * time.Second
	// maxMessageSize admits a whole snapshot in one InstallSnapshot message
	// (gRPC's default limit is 4 MB). Snapshot data is capped by the record
	// format at 64 MB.
	maxMessageSize = record.MaxSize + 1<<20
)

// ServerOptions are the options a gRPC server carrying RaftService needs.
func ServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{grpc.MaxRecvMsgSize(maxMessageSize)}
}

// GRPCTransport sends Raft messages to peers over gRPC, one Send call per
// message. Each peer gets its own goroutine and queue, so a slow or dead peer
// never delays traffic to the others.
type GRPCTransport struct {
	logger *slog.Logger
	peers  map[string]*peerSender
}

// NewGRPCTransport builds a transport from this node to each peer, keyed by
// node id. Connections are made lazily and re-established automatically.
// extra dial options are added to every peer connection (an interceptor that
// attaches metadata, for example).
func NewGRPCTransport(self string, peerAddrs map[string]string, logger *slog.Logger, extra ...grpc.DialOption) (*GRPCTransport, error) {
	t := &GRPCTransport{logger: logger.With("node_id", self), peers: make(map[string]*peerSender)}
	for id, addr := range peerAddrs {
		if id == self {
			continue
		}
		opts := append([]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultCallOptions(grpc.MaxCallSendMsgSize(maxMessageSize)),
			// gRPC's default reconnect backoff grows to two minutes. A peer that
			// restarts or comes back from a partition must hear from the leader
			// within an election timeout (1 s at the defaults), or it starts a
			// needless election; so the backoff is capped well below that.
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff:           backoff.Config{BaseDelay: 50 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: 500 * time.Millisecond},
				MinConnectTimeout: time.Second,
			}),
		}, extra...)
		conn, err := grpc.NewClient(addr, opts...)
		if err != nil {
			t.Close()
			return nil, fmt.Errorf("raft: dial peer %s at %s: %w", id, addr, err)
		}
		p := &peerSender{
			id:     id,
			addr:   addr,
			conn:   conn,
			client: NewRaftServiceClient(conn),
			queue:  make(chan *Envelope, peerQueueSize),
			stop:   make(chan struct{}),
			done:   make(chan struct{}),
			logger: t.logger.With("peer", id),
		}
		t.peers[id] = p
		go p.run()
	}
	return t, nil
}

// Send queues m for its destination, dropping it if that peer's queue is full.
func (t *GRPCTransport) Send(m *Envelope) {
	p, ok := t.peers[m.GetTo()]
	if !ok {
		t.logger.Error("raft: message for unknown peer", "to", m.GetTo())
		return
	}
	select {
	case p.queue <- m:
	default:
		p.logger.Debug("raft: peer queue full; dropping message")
	}
}

// Close stops every peer goroutine and closes the connections.
func (t *GRPCTransport) Close() {
	for _, p := range t.peers {
		close(p.stop)
		<-p.done
		p.conn.Close()
	}
}

type peerSender struct {
	id, addr string
	conn     *grpc.ClientConn
	client   RaftServiceClient
	queue    chan *Envelope
	stop     chan struct{}
	done     chan struct{}
	logger   *slog.Logger

	unreachable bool      // logged on change, so a dead peer is one line, not thousands
	slowLogged  time.Time // last "slow send" warning, so a slow peer is rate-limited too
}

func (p *peerSender) run() {
	defer close(p.done)
	for {
		select {
		case <-p.stop:
			return
		case m := <-p.queue:
			p.deliver(m)
		}
	}
}

func (p *peerSender) deliver(m *Envelope) {
	ctx, cancel := context.WithTimeout(context.Background(), sendTimeout)
	defer cancel()
	start := time.Now()
	_, err := p.client.Send(ctx, m)
	if took := time.Since(start); took > 200*time.Millisecond && time.Since(p.slowLogged) > 10*time.Second {
		p.slowLogged = time.Now()
		p.logger.Warn("raft: slow send to peer", "addr", p.addr, "took", took, "err", err)
	}
	switch {
	case err != nil && !p.unreachable:
		p.unreachable = true
		p.logger.Warn("raft: peer unreachable", "addr", p.addr, "err", err)
	case err == nil && p.unreachable:
		p.unreachable = false
		p.logger.Info("raft: peer reachable again", "addr", p.addr)
	}
}

// RegisterServer exposes node on gs as the RaftService peers send to.
func RegisterServer(gs *grpc.Server, node *Node) {
	RegisterRaftServiceServer(gs, &grpcServer{node: node})
}

type grpcServer struct {
	UnimplementedRaftServiceServer
	node *Node
}

func (s *grpcServer) Send(ctx context.Context, m *Envelope) (*SendAck, error) {
	if to := m.GetTo(); to != s.node.id {
		// A message for another node means a peer's address map is wrong;
		// acting on it would let one node speak with another's identity.
		return nil, status.Errorf(codes.InvalidArgument, "raft: message for %q delivered to %q", to, s.node.id)
	}
	if err := s.node.Step(ctx, m); err != nil {
		return nil, status.Errorf(codes.Unavailable, "%v", err)
	}
	return &SendAck{}, nil
}
