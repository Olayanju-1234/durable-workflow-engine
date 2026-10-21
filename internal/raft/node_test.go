package raft

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
)

// recordingSM is a state machine that remembers every payload it applied.
type recordingSM struct {
	mu   sync.Mutex
	data []string
}

func (m *recordingSM) Apply(e *LogEntry) any {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append(m.data, string(e.GetData()))
	return len(m.data)
}

func (m *recordingSM) snapshot() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.data)
}

// grpcCluster runs real Nodes, each with its own on-disk storage and its own
// gRPC server on a loopback TCP port, talking through the real transport.
type grpcCluster struct {
	t     *testing.T
	addrs map[string]string
	dirs  map[string]string
	nodes map[string]*grpcMember
}

type grpcMember struct {
	node    *Node
	gs      *grpc.Server
	tr      *GRPCTransport
	storage *DiskStorage
	sm      *recordingSM
}

func newGRPCCluster(t *testing.T, n int) *grpcCluster {
	t.Helper()
	c := &grpcCluster{t: t, addrs: map[string]string{}, dirs: map[string]string{}, nodes: map[string]*grpcMember{}}
	// Reserve every address first: each node must know all of them up front.
	listeners := map[string]net.Listener{}
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		listeners[id] = lis
		c.addrs[id] = lis.Addr().String()
		c.dirs[id] = filepath.Join(t.TempDir(), id)
	}
	for id, lis := range listeners {
		c.start(id, lis)
	}
	t.Cleanup(func() {
		for id := range c.nodes {
			c.stop(id)
		}
	})
	return c
}

func (c *grpcCluster) members() []string {
	var ids []string
	for id := range c.addrs {
		ids = append(ids, id)
	}
	return ids
}

func (c *grpcCluster) start(id string, lis net.Listener) {
	c.t.Helper()
	if lis == nil {
		var err error
		// Same port as before, so peers find the node where they left it.
		if lis, err = net.Listen("tcp", c.addrs[id]); err != nil {
			c.t.Fatalf("relisten %s: %v", id, err)
		}
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	storage, err := OpenDiskStorage(c.dirs[id])
	if err != nil {
		c.t.Fatal(err)
	}
	tr, err := NewGRPCTransport(id, c.addrs, logger)
	if err != nil {
		c.t.Fatal(err)
	}
	sm := &recordingSM{}
	node, err := NewNode(Config{
		ID: id, Members: c.members(), Storage: storage, Transport: tr, StateMachine: sm, Logger: logger,
		TickInterval: 10 * time.Millisecond, ElectionTicks: 10, HeartbeatTicks: 1,
	})
	if err != nil {
		c.t.Fatal(err)
	}
	gs := grpc.NewServer()
	RegisterServer(gs, node)
	go func() { _ = gs.Serve(lis) }()
	node.Start()
	c.nodes[id] = &grpcMember{node: node, gs: gs, tr: tr, storage: storage, sm: sm}
}

func (c *grpcCluster) stop(id string) {
	m := c.nodes[id]
	m.gs.Stop()
	if err := m.node.Stop(); err != nil {
		c.t.Errorf("stop %s: %v", id, err)
	}
	m.tr.Close()
	m.storage.Close()
	delete(c.nodes, id)
}

// agreedLeader returns the leader every running node agrees on, with its term.
func (c *grpcCluster) agreedLeader() (string, uint64) {
	var lead string
	var term uint64
	for _, m := range c.nodes {
		st := m.node.Status()
		if st.Leader == "" || (lead != "" && (st.Leader != lead || st.Term != term)) {
			return "", 0
		}
		lead, term = st.Leader, st.Term
	}
	if m, ok := c.nodes[lead]; !ok || m.node.Status().Role != Leader {
		return "", 0
	}
	return lead, term
}

func (c *grpcCluster) waitLeader(what string) (string, uint64) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if lead, term := c.agreedLeader(); lead != "" {
			return lead, term
		}
		time.Sleep(10 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for %s", what)
	return "", 0
}

func TestGRPCClusterElectsAndFailsOver(t *testing.T) {
	c := newGRPCCluster(t, 3)
	first, firstTerm := c.waitLeader("the first leader")

	c.stop(first)
	second, secondTerm := c.waitLeader("a leader among the survivors")
	if second == first || secondTerm <= firstTerm {
		t.Fatalf("after stopping %s (term %d): leader %s term %d", first, firstTerm, second, secondTerm)
	}

	// The old leader restarts from its directory: it reloads a term at least as
	// high as the one it led, so it can never act on a stale term again.
	c.start(first, nil)
	if got := c.nodes[first].node.Status().Term; got < firstTerm {
		t.Fatalf("restarted %s came back with term %d, below its old term %d", first, got, firstTerm)
	}
	lead, _ := c.waitLeader("all three nodes to agree again")
	if lead == first {
		// Legal (it could win a later election), but with a live leader and the
		// lease rule it should simply rejoin as a follower.
		t.Logf("restarted node %s won a later election", first)
	}
}

// waitReadyLeader waits for a leader that has applied its term's opening entry
// and so may take proposals.
func (c *grpcCluster) waitReadyLeader(what string) (string, uint64) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for id, m := range c.nodes {
			if st := m.node.Status(); st.LeaderReady() {
				return id, st.Term
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	c.t.Fatalf("timed out waiting for %s", what)
	return "", 0
}

// waitApplied waits until every running node's state machine holds want.
func (c *grpcCluster) waitApplied(want []string) {
	c.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, m := range c.nodes {
			if !slices.Equal(m.sm.snapshot(), want) {
				done = false
			}
		}
		if done {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	for id, m := range c.nodes {
		got := m.sm.snapshot()
		c.t.Errorf("%s applied %d entries", id, len(got))
	}
	c.t.Fatalf("timed out waiting for %d applied entries everywhere", len(want))
}

// proposeAll proposes every payload concurrently on lead and returns them in
// the order the log committed them (the order each Apply numbered them).
func (c *grpcCluster) proposeAll(lead string, term uint64, payloads []string) []string {
	c.t.Helper()
	ordered := make([]string, len(payloads))
	var wg sync.WaitGroup
	errs := make(chan error, len(payloads))
	base := len(c.nodes[lead].sm.snapshot())
	for _, p := range payloads {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.nodes[lead].node.Propose(context.Background(), term, []byte(p))
			if err != nil {
				errs <- fmt.Errorf("propose %s: %w", p, err)
				return
			}
			ordered[v.(int)-base-1] = p
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		c.t.Fatal(err)
	}
	return ordered
}

func TestGRPCClusterReplicates(t *testing.T) {
	c := newGRPCCluster(t, 3)
	lead, term := c.waitReadyLeader("a ready leader")

	want := c.proposeAll(lead, term, payloads("a", 200))
	c.waitApplied(want)

	follower := ""
	for id := range c.nodes {
		if id != lead {
			follower = id
		}
	}
	if _, err := c.nodes[follower].node.Propose(context.Background(), term, []byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("propose on a follower: %v, want ErrNotLeader", err)
	}

	// A follower misses a batch while down, then catches up from the leader.
	c.stop(follower)
	want = append(want, c.proposeAll(lead, term, payloads("b", 50))...)
	c.start(follower, nil)
	c.waitApplied(want)

	// The leader dies; a new one takes over with every committed entry and
	// carries on. Proposals for the old term are refused everywhere.
	c.stop(lead)
	newLead, newTerm := c.waitReadyLeader("a new leader")
	if _, err := c.nodes[newLead].node.Propose(context.Background(), term, []byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("propose for the old term on the new leader: %v, want ErrNotLeader", err)
	}
	want = append(want, c.proposeAll(newLead, newTerm, payloads("c", 20))...)
	c.start(lead, nil)
	c.waitApplied(want)
}

// After every node restarts, each rebuilds its state machine from its own log.
func TestGRPCClusterRestartsFromDisk(t *testing.T) {
	c := newGRPCCluster(t, 3)
	lead, term := c.waitReadyLeader("a ready leader")
	want := c.proposeAll(lead, term, payloads("p", 30))
	c.waitApplied(want)

	for _, id := range c.members() {
		c.stop(id)
	}
	for _, id := range c.members() {
		c.start(id, nil)
	}
	c.waitApplied(want)
}
