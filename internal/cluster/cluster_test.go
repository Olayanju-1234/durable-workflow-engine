package cluster

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/raft"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/internal/tlsconfig"
	"github.com/olayanju-1234/durable-workflow-engine/internal/tlsconfig/tlstest"
)

var threeSteps = state.WorkflowDefinition{Type: "Three", Steps: state.Activities("a", "b", "c")}

type member struct {
	id    string
	dir   string
	store *history.HistoryStore
	c     *Cluster
}

type testCluster struct {
	t             *testing.T
	peers         map[string]string
	members       map[string]*member
	snapshotEvery uint64
	peerTLS       *tls.Config
	mu            sync.Mutex // guards each member's c, read by worker goroutines

	// Set by serveGRPC: each member serves the client API on a real address.
	grpcAddrs   map[string]string
	grpcServers map[string]*grpc.Server
}

// serving returns the engine of whichever member currently serves one.
func (tc *testCluster) serving() *server.Engine {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	for _, m := range tc.members {
		if m.c != nil {
			if e, err := m.c.Engine(); err == nil {
				return e
			}
		}
	}
	return nil
}

func freeAddr(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	return lis.Addr().String()
}

func newTestCluster(t *testing.T, n int, snapshotEvery uint64) *testCluster {
	return newCluster(t, n, snapshotEvery, false)
}

func newCluster(t *testing.T, n int, snapshotEvery uint64, serveGRPC bool) *testCluster {
	t.Helper()
	return newClusterTLS(t, n, snapshotEvery, serveGRPC, nil)
}

func newClusterTLS(t *testing.T, n int, snapshotEvery uint64, serveGRPC bool, peerTLS *tls.Config) *testCluster {
	t.Helper()
	tc := &testCluster{t: t, peers: map[string]string{}, members: map[string]*member{}, snapshotEvery: snapshotEvery, peerTLS: peerTLS}
	if serveGRPC {
		tc.grpcAddrs, tc.grpcServers = map[string]string{}, map[string]*grpc.Server{}
	}
	// Hold every listener until all ports are picked, so no two collide.
	var held []net.Listener
	reserve := func() string {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, lis)
		return lis.Addr().String()
	}
	base := t.TempDir()
	for i := 1; i <= n; i++ {
		id := fmt.Sprintf("n%d", i)
		tc.peers[id] = reserve()
		tc.members[id] = &member{id: id, dir: filepath.Join(base, id)}
		if serveGRPC {
			tc.grpcAddrs[id] = reserve()
		}
	}
	for _, lis := range held {
		lis.Close()
	}
	for id := range tc.members {
		tc.start(id)
	}
	t.Cleanup(func() {
		for id, m := range tc.members {
			if m.c != nil {
				tc.stop(id)
			}
		}
	})
	return tc
}

func (tc *testCluster) start(id string) {
	tc.t.Helper()
	m := tc.members[id]
	store, err := history.NewHistoryStore(m.dir)
	if err != nil {
		tc.t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	c, err := Start(Config{
		NodeID: id, Peers: tc.peers, RaftAddr: tc.peers[id],
		GRPCAddr: tc.grpcAddr(id), HTTPAddr: "http-" + id,
		DataDir: m.dir, Store: store, Logger: logger,
		TickInterval: 10 * time.Millisecond, ElectionTicks: 50, SnapshotEvery: tc.snapshotEvery, PeerTLS: tc.peerTLS,
		NewEngine: func(l server.HistoryLog) *server.Engine {
			e := server.NewEngine(store, logger, server.WithHistoryLog(l), server.WithLeaseDuration(time.Second))
			e.Register(threeSteps)
			e.In("acme").RegisterWorkflow(threeSteps.Type, threeSteps)
			return e
		},
	})
	if err != nil {
		tc.t.Fatalf("start %s: %v", id, err)
	}
	tc.mu.Lock()
	m.store, m.c = store, c
	tc.mu.Unlock()
	if tc.grpcAddrs != nil {
		lis, err := net.Listen("tcp", tc.grpcAddrs[id])
		if err != nil {
			tc.t.Fatal(err)
		}
		gs := grpc.NewServer()
		server.RegisterWorkflowServiceServer(gs, server.NewGRPCServerFrom(c, logger))
		go gs.Serve(lis)
		tc.grpcServers[id] = gs
	}
}

func (tc *testCluster) grpcAddr(id string) string {
	if tc.grpcAddrs != nil {
		return tc.grpcAddrs[id]
	}
	return "grpc-" + id
}

func (tc *testCluster) stop(id string) {
	m := tc.members[id]
	tc.mu.Lock()
	c := m.c
	m.c = nil
	tc.mu.Unlock()
	if gs := tc.grpcServers[id]; gs != nil {
		gs.Stop()
		delete(tc.grpcServers, id)
	}
	if err := c.Stop(); err != nil {
		tc.t.Errorf("stop %s: %v", id, err)
	}
	m.store.Close()
}

// leader waits for the one member that serves an engine.
func (tc *testCluster) leader() (*member, *server.Engine) {
	tc.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		for _, m := range tc.members {
			if m.c == nil {
				continue
			}
			if e, err := m.c.Engine(); err == nil {
				return m, e
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	tc.t.Fatal("no member started serving")
	return nil, nil
}

// worker plays an activity worker that always follows the current leader. It
// completes only the steps allow returns true for.
func (tc *testCluster) worker(ctx context.Context, allow func(step string) bool) *sync.WaitGroup {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			e := tc.serving()
			if e == nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			pctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
			task, err := e.PollTask(pctx, "test-worker")
			cancel()
			if err != nil || !allow(task.GetStepName()) {
				continue // a task we may not run: its lease expires and it is retried
			}
			_ = e.CompleteTask(task.GetWorkflowId(), task.GetStepName(), task.GetSequence(),
				task.GetLeaseToken(), []byte("done:"+task.GetStepName()), "", false)
		}
	}()
	return &wg
}

func waitStatus(t *testing.T, e func() (*server.Engine, error), id string, want server.WorkflowStatus) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var last string
	for time.Now().Before(deadline) {
		if eng, err := e(); err == nil {
			if wf, err := eng.GetWorkflow(id); err == nil {
				if wf.GetSummary().GetStatus() == want {
					return
				}
				last = wf.GetSummary().GetStatus().String()
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("workflow %s never reached %s (last %s)", id, want, last)
}

// waitIdentical waits until every running member's copy of the workflow's
// history file is byte-for-byte the same and holds want events.
func (tc *testCluster) waitIdentical(id string, want int) []*history.HistoryEvent {
	tc.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var first []byte
		same := true
		for _, m := range tc.members {
			if m.c == nil {
				continue
			}
			ns, local := history.SplitKey(id)
			path := filepath.Join(m.dir, local+".history")
			if history.Namespaced(id) {
				path = filepath.Join(m.dir, "namespaces", ns, local+".history")
			}
			raw, err := os.ReadFile(path)
			if err != nil || (first != nil && !bytes.Equal(raw, first)) {
				same = false
				break
			}
			first = raw
		}
		if same {
			for _, m := range tc.members {
				if m.c != nil {
					evs, _ := m.store.ReadAll(id)
					if len(evs) == want {
						return evs
					}
					break
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	tc.t.Fatalf("histories of %s never converged to %d identical events", id, want)
	return nil
}

func completions(evs []*history.HistoryEvent) map[string]int {
	out := map[string]int{}
	for _, ev := range evs {
		if c := ev.GetStepCompleted(); c != nil {
			out[c.GetStepName()]++
		}
	}
	return out
}

func TestClusterRunsWorkflowOnLeaderAndReplicates(t *testing.T) {
	tc := newTestCluster(t, 3, 0)
	lead, e := tc.leader()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc.worker(ctx, func(string) bool { return true })

	if _, err := e.StartWorkflow("Three", []byte("order"), "wf-1"); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, lead.c.Engine, "wf-1", server.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	evs := tc.waitIdentical("wf-1", 8)
	for _, ev := range evs {
		if ev.GetRaftIndex() == 0 {
			t.Fatalf("event %d has no Raft index stamp", ev.GetEventId())
		}
	}

	// Followers serve no engine; they name the leader and its client addresses.
	for id, m := range tc.members {
		if id == lead.id {
			continue
		}
		_, err := m.c.Engine()
		var nle *server.NotLeaderError
		if !errors.As(err, &nle) || nle.Leader.ID != lead.id || nle.Leader.GRPCAddr != "grpc-"+lead.id {
			t.Fatalf("follower %s: %v, want a hint naming %s", id, err, lead.id)
		}
	}
}

// With PeerTLS the cluster runs as usual, and its Raft port refuses anyone
// without a certificate from the cluster's CA.
func TestClusterOverMutualTLS(t *testing.T) {
	dir := t.TempDir()
	ca, err := tlstest.NewCA(dir, "cluster")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := ca.Issue(dir, "peer")
	if err != nil {
		t.Fatal(err)
	}
	peerTLS, err := tlsconfig.Peer(peer.CertFile, peer.KeyFile, ca.CertFile)
	if err != nil {
		t.Fatal(err)
	}
	tc := newClusterTLS(t, 3, 0, false, peerTLS)
	lead, e := tc.leader()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc.worker(ctx, func(string) bool { return true })
	if _, err := e.StartWorkflow("Three", []byte("order"), "wf-tls"); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, lead.c.Engine, "wf-tls", server.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	tc.waitIdentical("wf-tls", 8)

	rogueCA, err := tlstest.NewCA(dir, "rogue")
	if err != nil {
		t.Fatal(err)
	}
	rogue, err := rogueCA.Issue(dir, "rogue")
	if err != nil {
		t.Fatal(err)
	}
	rogueCert, err := tls.LoadX509KeyPair(rogue.CertFile, rogue.KeyFile)
	if err != nil {
		t.Fatal(err)
	}
	pool, err := tlsconfig.LoadCAs(ca.CertFile)
	if err != nil {
		t.Fatal(err)
	}
	send := func(creds credentials.TransportCredentials) error {
		conn, err := grpc.NewClient(tc.peers[lead.id], grpc.WithTransportCredentials(creds))
		if err != nil {
			return err
		}
		defer conn.Close()
		sctx, scancel := context.WithTimeout(ctx, 2*time.Second)
		defer scancel()
		// Addressed to a node that doesn't exist, so a call that gets through
		// the handshake is refused by the handler with InvalidArgument.
		_, err = raft.NewRaftServiceClient(conn).Send(sctx, &raft.Envelope{From: "x", To: "nobody"})
		return err
	}
	for name, creds := range map[string]credentials.TransportCredentials{
		"plaintext":                   insecure.NewCredentials(),
		"no client certificate":       credentials.NewTLS(&tls.Config{RootCAs: pool}),
		"certificate from another CA": credentials.NewTLS(&tls.Config{RootCAs: pool, Certificates: []tls.Certificate{rogueCert}}),
	} {
		if err := send(creds); err == nil || status.Code(err) == codes.InvalidArgument {
			t.Errorf("%s: reached the Raft handler (%v)", name, err)
		}
	}
	if err := send(credentials.NewTLS(peerTLS)); status.Code(err) != codes.InvalidArgument {
		t.Errorf("valid peer certificate: %v, want the handler's InvalidArgument", err)
	}
}

// The leader dies with a step in flight. The new leader's engine recovers the
// workflow from the replicated history and finishes it; every step is recorded
// exactly once, and the old leader catches up when it returns.
func TestClusterFailoverMidWorkflow(t *testing.T) {
	tc := newTestCluster(t, 3, 0)
	old, e := tc.leader()
	ctx, cancel := context.WithCancel(context.Background())
	wg := tc.worker(ctx, func(step string) bool { return step == "a" })
	if _, err := e.StartWorkflow("Three", nil, "wf-1"); err != nil {
		t.Fatal(err)
	}
	tc.waitIdentical("wf-1", 4) // started, a scheduled, a completed, b scheduled
	cancel()
	wg.Wait()

	tc.stop(old.id)
	_, e2 := tc.leader()
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	tc.worker(ctx, func(string) bool { return true })
	waitStatus(t, func() (*server.Engine, error) { return e2, nil }, "wf-1", server.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)

	tc.start(old.id)
	evs := tc.waitIdentical("wf-1", 8)
	if got := completions(evs); got["a"] != 1 || got["b"] != 1 || got["c"] != 1 {
		t.Fatalf("step completions %v, want each step exactly once", got)
	}
}

func TestClusterRetentionDeleteReplicates(t *testing.T) {
	tc := newTestCluster(t, 3, 0)
	lead, e := tc.leader()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc.worker(ctx, func(string) bool { return true })
	if _, err := e.StartWorkflow("Three", nil, "wf-1"); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, lead.c.Engine, "wf-1", server.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	tc.waitIdentical("wf-1", 8)

	if n, err := e.PurgeTerminated(time.Now().Add(time.Hour)); err != nil || n != 1 {
		t.Fatalf("purge: %d, %v", n, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for _, m := range tc.members {
		for {
			if _, err := os.Stat(filepath.Join(m.dir, "wf-1.history")); errors.Is(err, os.ErrNotExist) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s still has the purged history", m.id)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// With a small snapshot interval a follower that was down catches up from the
// leader's snapshot of the history directory, then from the log.
func TestClusterFollowerCatchesUpFromHistorySnapshot(t *testing.T) {
	tc := newTestCluster(t, 3, 10)
	lead, e := tc.leader()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tc.worker(ctx, func(string) bool { return true })

	var lagger string
	for id := range tc.members {
		if id != lead.id {
			lagger = id
		}
	}
	tc.stop(lagger)
	ids := []string{"wf-1", "wf-2", "wf-3", "wf-4"}
	for _, id := range ids {
		if _, err := e.StartWorkflow("Three", nil, id); err != nil {
			t.Fatal(err)
		}
		waitStatus(t, lead.c.Engine, id, server.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	}
	if lead.c.Status().Snapshot == 0 {
		deadline := time.Now().Add(5 * time.Second)
		for lead.c.Status().Snapshot == 0 && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	}
	tc.start(lagger)
	for _, id := range ids {
		tc.waitIdentical(id, 8)
	}
	if got, _ := tc.members[lagger].store.ListWorkflows(); len(got) != len(ids) || !slices.Contains(got, "wf-4") {
		t.Fatalf("lagger holds %v", got)
	}
	sm := newStateMachine(tc.members[lagger].store, filepath.Join(tc.members[lagger].dir, "raft"), nil)
	if restored, err := sm.readMarker(); err != nil || restored == 0 {
		t.Fatalf("the lagging follower caught up without restoring a snapshot (marker %d, %v)", restored, err)
	}
}

func TestStartRefusesSingleNodeDataDir(t *testing.T) {
	dir := t.TempDir()
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	if err := store.Append("wf-old", &history.HistoryEvent{EventId: 1}); err != nil {
		t.Fatal(err)
	}
	_, err = Start(Config{NodeID: "n1", Peers: map[string]string{"n1": freeAddr(t)}, DataDir: dir, Store: store,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err == nil {
		t.Fatal("cluster mode started on a single-node data dir")
	}
}

// A leader that loses its majority must stop serving: its engine closes (long
// polls end with "not the leader") and the node answers with a hint instead.
func TestClusterStopsEngineWhenLeadershipLost(t *testing.T) {
	tc := newTestCluster(t, 3, 0)
	lead, e := tc.leader()
	polled := make(chan error, 1)
	go func() {
		_, err := e.PollTask(context.Background(), "parked-worker")
		polled <- err
	}()
	for id := range tc.members {
		if id != lead.id {
			tc.stop(id)
		}
	}
	select {
	case err := <-polled:
		if !errors.Is(err, server.ErrNotLeader) {
			t.Fatalf("parked poll ended with %v, want ErrNotLeader", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the deposed leader's engine kept serving a long poll")
	}
	if _, err := lead.c.Engine(); !errors.Is(err, server.ErrNotLeader) {
		t.Fatalf("deposed leader still hands out an engine: %v", err)
	}
}

// A rejected command must reach the engine as an error, not vanish: the
// engine would otherwise believe an event was recorded when it wasn't.
func TestReplicatedLogReportsRejection(t *testing.T) {
	tc := newTestCluster(t, 3, 0)
	lead, _ := tc.leader()
	l := &replicatedLog{node: lead.c.node, term: lead.c.Status().Term}
	err := l.Append("wf-x", &history.HistoryEvent{EventId: 5}) // the history is empty
	if !errors.Is(err, ErrConflict) {
		t.Fatalf("append at the wrong position: %v, want ErrConflict", err)
	}
	if err := l.Append("wf-x", &history.HistoryEvent{EventId: 1}); err != nil {
		t.Fatalf("append at the right position: %v", err)
	}
	stale := &replicatedLog{node: lead.c.node, term: lead.c.Status().Term - 1}
	if err := stale.Append("wf-x", &history.HistoryEvent{EventId: 2}); !errors.Is(err, server.ErrNotLeader) {
		t.Fatalf("append for an older term: %v, want ErrNotLeader", err)
	}
}

// Namespaced workflows replicate like any other, and a follower that missed
// them gets them, in their namespace, from a snapshot.
func TestClusterReplicatesNamespaces(t *testing.T) {
	tc := newTestCluster(t, 3, 10)
	lead, e := tc.leader()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			e := tc.serving()
			if e == nil {
				time.Sleep(5 * time.Millisecond)
				continue
			}
			pctx, pcancel := context.WithTimeout(ctx, 100*time.Millisecond)
			task, err := e.In("acme").PollTask(pctx, "acme-worker", "")
			pcancel()
			if err == nil {
				_ = e.In("acme").CompleteTask(task.GetWorkflowId(), task.GetStepName(), task.GetSequence(), task.GetLeaseToken(), nil)
			}
		}
	}()
	var lagger string
	for id := range tc.members {
		if id != lead.id {
			lagger = id
		}
	}
	tc.stop(lagger)
	for _, id := range []string{"n-1", "n-2", "n-3", "n-4"} {
		if _, err := e.In("acme").Start("Three", nil, id); err != nil {
			t.Fatal(err)
		}
		waitStatus(t, func() (*server.Engine, error) { return lead.c.Engine() }, "acme/"+id, server.WorkflowStatus_WORKFLOW_STATUS_COMPLETED)
	}
	deadline := time.Now().Add(5 * time.Second)
	for lead.c.Status().Snapshot == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	tc.start(lagger)
	for _, id := range []string{"n-1", "n-4"} {
		tc.waitIdentical("acme/"+id, 8)
	}
	got, _ := tc.members[lagger].store.ListWorkflows()
	if !slices.Contains(got, "acme/n-4") || slices.Contains(got, "n-4") {
		t.Fatalf("lagger holds %v", got)
	}
	if f := tc.members[lagger].store.Format(); f < history.FormatNamespaces {
		t.Fatalf("lagger's format is %d after restoring namespaced histories", f)
	}
}
