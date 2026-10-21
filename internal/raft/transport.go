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
)

const (
	// peerQueueSize bounds the messages waiting for one peer. When a peer is
	// down its queue fills and newer messages are dropped, which Raft absorbs:
	// the leader re-sends whatever a follower is missing on the next heartbeat.
	peerQueueSize = 1024
	// sendTimeout bounds one Send call, so a hung peer can't stall the queue.
	sendTimeout = 2 * time.Second
)

// GRPCTransport sends Raft messages to peers over gRPC, one Send call per
// message. Each peer gets its own goroutine and queue, so a slow or dead peer
// never delays traffic to the others.
type GRPCTransport struct {
	logger *slog.Logger
	peers  map[string]*peerSender
}

// NewGRPCTransport builds a transport from this node to each peer, keyed by
// node id. Connections are made lazily and re-established automatically.
func NewGRPCTransport(self string, peerAddrs map[string]string, logger *slog.Logger) (*GRPCTransport, error) {
	t := &GRPCTransport{logger: logger.With("node_id", self), peers: make(map[string]*peerSender)}
	for id, addr := range peerAddrs {
		if id == self {
			continue
		}
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			// gRPC's default reconnect backoff grows to two minutes. A peer that
			// restarts or comes back from a partition must be heard from within an
			// election timeout, so cap the backoff at a second.
			grpc.WithConnectParams(grpc.ConnectParams{
				Backoff:           backoff.Config{BaseDelay: 100 * time.Millisecond, Multiplier: 1.6, Jitter: 0.2, MaxDelay: time.Second},
				MinConnectTimeout: time.Second,
			}),
		)
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

	unreachable bool // logged on change, so a dead peer is one line, not thousands
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
	_, err := p.client.Send(ctx, m)
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
