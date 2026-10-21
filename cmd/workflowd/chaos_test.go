package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/raft"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// Chaos tests run a real 3-node cluster: every node is its own OS process (this
// test binary re-executed into main), with its own data dir and ports. Every
// Raft link between two nodes goes through a TCP proxy owned by the test, so
// cutting proxies is a real network partition, and SIGKILL is a real crash.

const (
	chaosEnvNode = "WORKFLOWD_CHAOS_NODE" // "1" in a node process
	chaosEnvArgs = "WORKFLOWD_CHAOS_ARGS" // the node's flags, JSON-encoded
)

// TestChaosNodeHelper is the node process. Under a normal `go test` it skips.
func TestChaosNodeHelper(t *testing.T) {
	if os.Getenv(chaosEnvNode) != "1" {
		t.Skip("node process only: started by the chaos tests")
	}
	var args []string
	if err := json.Unmarshal([]byte(os.Getenv(chaosEnvArgs)), &args); err != nil {
		t.Fatal(err)
	}
	os.Args = append([]string{"workflowd"}, args...)
	flag.CommandLine = flag.NewFlagSet("workflowd", flag.ExitOnError)
	// These tests kill processes, never machines: a killed process's writes
	// still reach the disk from the page cache, so the Raft log's fsync (tens
	// of ms on a laptop, three nodes sharing one drive) protects nothing here.
	raft.UnsafeDisableFsyncForTests()
	main() // runs until the parent kills or signals this process
}

func reservePort(t *testing.T) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	return lis.Addr().String()
}

// linkProxy forwards one direction of Raft traffic (node a sending to node b).
type linkProxy struct {
	lis    net.Listener
	target string

	mu    sync.Mutex
	cut   bool
	conns map[net.Conn]struct{}
}

func newLinkProxy(t *testing.T, target string) *linkProxy {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &linkProxy{lis: lis, target: target, conns: map[net.Conn]struct{}{}}
	go p.serve()
	t.Cleanup(func() { lis.Close(); p.setCut(true) })
	return p
}

func (p *linkProxy) serve() {
	for {
		c, err := p.lis.Accept()
		if err != nil {
			return
		}
		up, err := net.Dial("tcp", p.target)
		p.mu.Lock()
		if err != nil || p.cut {
			p.mu.Unlock()
			c.Close()
			if up != nil {
				up.Close()
			}
			continue
		}
		p.conns[c], p.conns[up] = struct{}{}, struct{}{}
		p.mu.Unlock()
		go p.pipe(c, up)
		go p.pipe(up, c)
	}
}

func (p *linkProxy) pipe(dst, src net.Conn) {
	io.Copy(dst, src)
	dst.Close()
	src.Close()
	p.mu.Lock()
	delete(p.conns, dst)
	delete(p.conns, src)
	p.mu.Unlock()
}

// setCut drops every open connection and refuses new ones (or stops refusing).
func (p *linkProxy) setCut(cut bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.cut = cut
	if cut {
		for c := range p.conns {
			c.Close()
		}
	}
}

type chaosNode struct {
	id, dataDir, raft, grpc, http string
	args                          []string
	cmd                           *exec.Cmd
	exited                        chan struct{} // closed once cmd has been reaped
	logPath                       string
	client                        server.WorkflowServiceClient
	conn                          *grpc.ClientConn
}

type chaosCluster struct {
	t       *testing.T
	ids     []string
	nodes   map[string]*chaosNode
	proxies map[[2]string]*linkProxy // [from, to]
	mu      sync.Mutex               // guards each node's cmd, read by worker goroutines
}

func (cc *chaosCluster) running(id string) bool {
	cc.mu.Lock()
	defer cc.mu.Unlock()
	return cc.nodes[id].cmd != nil
}

func newChaosCluster(t *testing.T) *chaosCluster {
	t.Helper()
	if testing.Short() {
		t.Skip("chaos tests start real processes; skipped with -short")
	}
	cc := &chaosCluster{t: t, ids: []string{"n1", "n2", "n3"}, nodes: map[string]*chaosNode{}, proxies: map[[2]string]*linkProxy{}}
	base := t.TempDir()
	for _, id := range cc.ids {
		cc.nodes[id] = &chaosNode{
			id: id, dataDir: filepath.Join(base, id), logPath: filepath.Join(base, id+".log"),
			raft: reservePort(t), grpc: reservePort(t), http: reservePort(t),
		}
	}
	for _, from := range cc.ids {
		for _, to := range cc.ids {
			if from != to {
				cc.proxies[[2]string{from, to}] = newLinkProxy(t, cc.nodes[to].raft)
			}
		}
	}
	for _, id := range cc.ids {
		n := cc.nodes[id]
		var peers []string
		for _, other := range cc.ids {
			addr := n.raft
			if other != id {
				addr = cc.proxies[[2]string{id, other}].lis.Addr().String()
			}
			peers = append(peers, other+"="+addr)
		}
		n.args = []string{
			"--serve", "--node-id", id, "--peers", strings.Join(peers, ","), "--raft-addr", n.raft,
			"--data-dir", n.dataDir, "--addr", n.grpc, "--http-addr", n.http, "--metrics-addr", "",
			"--raft-tick", "50ms",
		}
		conn, err := grpc.NewClient(n.grpc, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatal(err)
		}
		n.conn, n.client = conn, server.NewWorkflowServiceClient(conn)
		cc.start(id)
	}
	t.Cleanup(func() {
		if t.Failed() {
			cc.dumpStacks()
		}
		for _, n := range cc.nodes {
			cc.kill(n.id)
			n.conn.Close()
		}
		for _, id := range cc.ids {
			data, _ := os.ReadFile(cc.nodes[id].logPath)
			if bytes.Contains(data, []byte("WARNING: DATA RACE")) {
				t.Errorf("node %s reported a data race; see its log", id)
			}
			if t.Failed() {
				lines := strings.Split(strings.TrimSpace(string(data)), "\n")
				var notable []string
				for _, l := range lines {
					if strings.Contains(l, "level=WARN") || strings.Contains(l, "level=ERROR") || strings.Contains(l, "raft: status") || strings.Contains(l, "cluster:") {
						notable = append(notable, l)
					}
				}
				t.Logf("--- %s: warnings and role changes (%d) ---\n%s", id, len(notable), strings.Join(notable[max(0, len(notable)-60):], "\n"))
				t.Logf("--- %s log (last 40 lines) ---\n%s", id, strings.Join(lines[max(0, len(lines)-40):], "\n"))
			}
		}
	})
	return cc
}

// dumpStacks asks every running node to print all its goroutines (SIGQUIT),
// then logs the ones in workflowd's own packages: where a stuck or slow
// cluster is actually waiting.
func (cc *chaosCluster) dumpStacks() {
	for _, id := range cc.ids {
		cc.mu.Lock()
		n := cc.nodes[id]
		cmd, exited := n.cmd, n.exited
		n.cmd = nil
		cc.mu.Unlock()
		if cmd == nil {
			continue
		}
		_ = cmd.Process.Signal(syscall.SIGQUIT)
		select {
		case <-exited:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-exited
		}
		data, _ := os.ReadFile(n.logPath)
		_, dump, ok := strings.Cut(string(data), "SIGQUIT")
		if !ok {
			continue
		}
		var keep []string
		for _, g := range strings.Split(dump, "\n\n") {
			if strings.Contains(g, "durable-workflow-engine/internal/") && !strings.Contains(g, "chaos_test") {
				keep = append(keep, g)
			}
		}
		cc.t.Logf("--- %s goroutines in workflowd packages (%d) ---\n%s", id, len(keep), strings.Join(keep, "\n\n"))
	}
}

func (cc *chaosCluster) start(id string) {
	cc.t.Helper()
	n := cc.nodes[id]
	args, _ := json.Marshal(n.args)
	logf, err := os.OpenFile(n.logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		cc.t.Fatal(err)
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestChaosNodeHelper$")
	cmd.Env = append(os.Environ(), chaosEnvNode+"=1", chaosEnvArgs+"="+string(args))
	cmd.Stdout, cmd.Stderr = logf, logf
	if err := cmd.Start(); err != nil {
		cc.t.Fatal(err)
	}
	cc.mu.Lock()
	n.cmd, n.exited = cmd, make(chan struct{})
	cc.mu.Unlock()
	go func(exited chan struct{}) {
		cmd.Wait()
		logf.Close()
		close(exited)
	}(n.exited)
}

// kill SIGKILLs a node: no shutdown, no flush, exactly like losing the machine.
func (cc *chaosCluster) kill(id string) {
	cc.mu.Lock()
	n := cc.nodes[id]
	cmd, exited := n.cmd, n.exited
	n.cmd = nil
	cc.mu.Unlock()
	if cmd != nil {
		_ = cmd.Process.Kill()
		<-exited
	}
}

// isolate cuts every Raft link to and from id; heal restores them all.
func (cc *chaosCluster) isolate(id string) {
	for link, p := range cc.proxies {
		if link[0] == id || link[1] == id {
			p.setCut(true)
		}
	}
}

func (cc *chaosCluster) heal() {
	for _, p := range cc.proxies {
		p.setCut(false)
	}
}

type healthView struct {
	Cluster struct {
		Role    string `json:"role"`
		Term    uint64 `json:"term"`
		Leader  string `json:"leader_id"`
		Serving bool   `json:"serving"`
		Applied uint64 `json:"applied_index"`
	} `json:"cluster"`
}

func (cc *chaosCluster) health(id string) (healthView, bool) {
	var h healthView
	c := http.Client{Timeout: time.Second}
	res, err := c.Get("http://" + cc.nodes[id].http + "/api/health")
	if err != nil {
		return h, false
	}
	defer res.Body.Close()
	return h, json.NewDecoder(res.Body).Decode(&h) == nil
}

// leader waits for a node, other than the excluded ones, that serves an engine.
func (cc *chaosCluster) leader(exclude ...string) string {
	cc.t.Helper()
	if id, ok := cc.findLeader(20*time.Second, exclude...); ok {
		return id
	}
	cc.t.Fatalf("no serving leader (excluding %v)", exclude)
	return ""
}

// findLeader is leader without failing the test, safe from any goroutine.
func (cc *chaosCluster) findLeader(timeout time.Duration, exclude ...string) (string, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range cc.ids {
			if !cc.running(id) || contains(exclude, id) {
				continue
			}
			if h, ok := cc.health(id); ok && h.Cluster.Serving {
				return id, true
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	return "", false
}

func contains(ids []string, id string) bool {
	for _, x := range ids {
		if x == id {
			return true
		}
	}
	return false
}

// call runs fn against whichever node leads right now, retrying on another
// node while the cluster has no ready leader.
func (cc *chaosCluster) call(timeout time.Duration, fn func(server.WorkflowServiceClient) error) error {
	deadline := time.Now().Add(timeout)
	for {
		id, ok := cc.findLeader(time.Until(deadline))
		if !ok {
			return status.Error(codes.Unavailable, "no leader")
		}
		err := fn(cc.nodes[id].client)
		// Unavailable: that node stopped leading. DeadlineExceeded: the call
		// queued behind others too long; the write may still land, so callers
		// make retries idempotent.
		code := status.Code(err)
		if (code != codes.Unavailable && code != codes.DeadlineExceeded) || time.Now().After(deadline) {
			return err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// executions counts how many times each (workflow, step) was run by a worker.
type executions struct {
	mu sync.Mutex
	n  map[string]int
}

// work runs activity workers that always look for the current leader, until
// ctx ends. allow limits which steps they complete.
func (cc *chaosCluster) work(ctx context.Context, n int, ex *executions, allow func(step string) bool) *sync.WaitGroup {
	var wg sync.WaitGroup
	for w := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				id, ok := cc.findLeader(time.Second)
				if !ok {
					continue
				}
				client := cc.nodes[id].client
				pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				resp, err := client.PollTask(pctx, &server.PollTaskRequest{WorkerId: fmt.Sprintf("chaos-%d", w)})
				cancel()
				if err != nil || resp.GetTask() == nil {
					continue
				}
				task := resp.GetTask()
				if !allow(task.GetStepName()) {
					continue
				}
				if ex != nil {
					ex.mu.Lock()
					ex.n[task.GetWorkflowId()+"/"+task.GetStepName()]++
					ex.mu.Unlock()
				}
				cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				_, _ = client.CompleteTask(cctx, &server.CompleteTaskRequest{
					WorkflowId: task.GetWorkflowId(), StepName: task.GetStepName(), Sequence: task.GetSequence(),
					LeaseToken: task.GetLeaseToken(), Result: &server.CompleteTaskRequest_Output{Output: []byte("ok")},
				})
				cancel()
			}
		}()
	}
	return &wg
}

func (cc *chaosCluster) startWorkflow(id string) {
	cc.t.Helper()
	err := cc.call(3*time.Minute, func(c server.WorkflowServiceClient) error {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := c.StartWorkflow(ctx, &server.StartWorkflowRequest{WorkflowType: "OrderWorkflow", WorkflowId: id, Input: []byte(id)})
		if status.Code(err) == codes.AlreadyExists {
			return nil // an earlier attempt landed even though its reply was an error
		}
		return err
	})
	if err != nil {
		cc.t.Errorf("start %s: %v", id, err)
	}
}

// waitCompleted waits until the leader reports every workflow COMPLETED. On
// timeout it reports the stragglers and where each one is stuck.
func (cc *chaosCluster) waitCompleted(ids []string, timeout time.Duration) {
	cc.t.Helper()
	deadline := time.Now().Add(timeout)
	var pending []*server.WorkflowSummary
	for time.Now().Before(deadline) {
		pending = nil
		err := cc.call(10*time.Second, func(c server.WorkflowServiceClient) error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			resp, err := c.ListWorkflows(ctx, &server.ListWorkflowsRequest{})
			if err != nil {
				return err
			}
			for _, w := range resp.GetWorkflows() {
				if w.GetStatus() != server.WorkflowStatus_WORKFLOW_STATUS_COMPLETED {
					pending = append(pending, w)
				} else if !contains(ids, w.GetWorkflowId()) {
					continue
				}
			}
			if len(resp.GetWorkflows()) < len(ids) {
				pending = append(pending, &server.WorkflowSummary{WorkflowId: fmt.Sprintf("(%d not started)", len(ids)-len(resp.GetWorkflows()))})
			}
			return nil
		})
		if err == nil && len(pending) == 0 {
			return
		}
		time.Sleep(time.Second)
	}
	var report []string
	for i, w := range pending {
		if i == 5 {
			report = append(report, fmt.Sprintf("... and %d more", len(pending)-5))
			break
		}
		line := fmt.Sprintf("%s %s", w.GetWorkflowId(), w.GetStatus())
		_ = cc.call(5*time.Second, func(c server.WorkflowServiceClient) error {
			resp, err := c.GetWorkflow(context.Background(), &server.GetWorkflowRequest{WorkflowId: w.GetWorkflowId()})
			for _, st := range resp.GetSteps() {
				line += fmt.Sprintf(" [%s %s x%d %s]", st.GetStepName(), st.GetStatus(), st.GetAttempts(), st.GetError())
			}
			return err
		})
		report = append(report, line)
	}
	cc.t.Fatalf("%d workflows not completed within %s:\n  %s", len(pending), timeout, strings.Join(report, "\n  "))
}

// verifyHistories checks every node holds byte-identical history files for
// ids, and that each history is valid with every step completed exactly once.
func (cc *chaosCluster) verifyHistories(ids []string, nodes ...string) {
	cc.t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for {
		mismatch := ""
		for _, id := range ids {
			var ref []byte
			for i, n := range nodes {
				raw, err := os.ReadFile(filepath.Join(cc.nodes[n].dataDir, id+".history"))
				if err != nil {
					mismatch = fmt.Sprintf("%s missing on %s", id, n)
					break
				}
				if i > 0 && !bytes.Equal(raw, ref) {
					mismatch = fmt.Sprintf("%s differs between %s and %s", id, nodes[0], n)
					break
				}
				ref = raw
			}
			if mismatch != "" {
				break
			}
		}
		if mismatch == "" {
			break
		}
		if time.Now().After(deadline) {
			cc.t.Fatalf("histories never converged: %s", mismatch)
		}
		time.Sleep(100 * time.Millisecond)
	}
	store, err := history.NewHistoryStore(cc.nodes[nodes[0]].dataDir)
	if err != nil {
		cc.t.Fatal(err)
	}
	defer store.Close()
	for _, id := range ids {
		evs, err := store.ReadAll(id)
		if err != nil {
			cc.t.Fatalf("%s: %v", id, err)
		}
		st, err := state.Reconstruct(evs)
		if err != nil {
			cc.t.Fatalf("%s: history does not replay: %v", id, err)
		}
		if st.Status != state.StatusCompleted {
			cc.t.Fatalf("%s: status %s, want COMPLETED", id, st.Status)
		}
		seen := map[string]int{}
		for _, ev := range evs {
			if c := ev.GetStepCompleted(); c != nil {
				seen[c.GetStepName()]++
			}
		}
		for _, step := range []string{"reserve", "charge", "ship"} {
			if seen[step] != 1 {
				cc.t.Fatalf("%s: step %s completed %d times in history, want exactly 1", id, step, seen[step])
			}
		}
	}
}

// The leader is SIGKILLed with a step in flight. A new leader recovers the
// workflow from the replicated history and finishes it; nothing is lost or
// recorded twice, and the dead node catches up when it comes back.
func TestChaosKillLeaderMidWorkflow(t *testing.T) {
	cc := newChaosCluster(t)
	old := cc.leader()
	ctx, cancel := context.WithCancel(context.Background())
	wg := cc.work(ctx, 1, nil, func(step string) bool { return step == "reserve" })
	cc.startWorkflow("wf-kill")
	waitFor(t, 10*time.Second, "reserve to complete", func() bool {
		raw, _ := os.ReadFile(filepath.Join(cc.nodes[old].dataDir, "wf-kill.history"))
		return bytes.Contains(raw, []byte("charge"))
	})
	cancel()
	wg.Wait()

	cc.kill(old)
	newLead := cc.leader(old)
	t.Logf("killed leader %s; %s took over", old, newLead)
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	cc.work(ctx, 2, nil, func(string) bool { return true })
	cc.waitCompleted([]string{"wf-kill"}, 30*time.Second)

	survivors := []string{}
	for _, id := range cc.ids {
		if id != old {
			survivors = append(survivors, id)
		}
	}
	cc.verifyHistories([]string{"wf-kill"}, survivors...)
	cc.start(old)
	cc.verifyHistories([]string{"wf-kill"}, cc.ids...)
}

// A leader cut off from both followers must not commit anything: a write sent
// straight to it fails instead of succeeding, the majority elects a new leader
// and keeps going, and after the partition heals the minority's write exists
// nowhere.
func TestChaosPartitionedLeaderCannotCommit(t *testing.T) {
	cc := newChaosCluster(t)
	old := cc.leader()
	cc.isolate(old)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := cc.nodes[old].client.StartWorkflow(ctx, &server.StartWorkflowRequest{WorkflowType: "OrderWorkflow", WorkflowId: "wf-minority"})
	if err == nil {
		t.Fatal("the isolated leader acknowledged a write it could not have committed")
	}
	t.Logf("isolated leader %s answered: %v", old, err)

	newLead := cc.leader(old)
	t.Logf("majority elected %s", newLead)
	// The isolated node must have stopped serving, not linger as a second
	// "leader" handing out a stale engine.
	waitFor(t, 10*time.Second, "the isolated leader to stop serving", func() bool {
		h, ok := cc.health(old)
		return ok && !h.Cluster.Serving
	})
	wctx, wcancel := context.WithCancel(context.Background())
	defer wcancel()
	cc.work(wctx, 2, nil, func(string) bool { return true })
	cc.startWorkflow("wf-majority")
	cc.waitCompleted([]string{"wf-majority"}, 30*time.Second)

	cc.heal()
	cc.verifyHistories([]string{"wf-majority"}, cc.ids...)
	for _, id := range cc.ids {
		if _, err := os.Stat(filepath.Join(cc.nodes[id].dataDir, "wf-minority.history")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%s has a history for the write the isolated leader never committed", id)
		}
	}
	err = cc.call(10*time.Second, func(c server.WorkflowServiceClient) error {
		_, err := c.GetWorkflow(context.Background(), &server.GetWorkflowRequest{WorkflowId: "wf-minority"})
		return err
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("GetWorkflow(wf-minority) after healing: %v, want NotFound", err)
	}
}

// 100 workflows run concurrently while the leader is killed part-way. Every
// one completes, every step is recorded exactly once, and all three nodes end
// with byte-identical histories. Activities may run more than once
// (at-least-once); that is counted and reported, not hidden.
//
// Four clients submit the workflows. Every history write is a Raft commit
// under the engine's single lock, so a hundred simultaneous submissions would
// queue workers' completions behind them for longer than a task lease on one
// shared test disk; that is the documented throughput ceiling (ADR-0004), not
// what this test is about.
func TestChaos100WorkflowsAcrossLeaderChange(t *testing.T) {
	cc := newChaosCluster(t)
	first := cc.leader()
	ex := &executions{n: map[string]int{}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cc.work(ctx, 4, ex, func(string) bool { return true })

	ids := make([]string, 100)
	for i := range ids {
		ids[i] = fmt.Sprintf("wf-%03d", i)
	}
	var starts sync.WaitGroup
	startAll := func(batch []string) {
		for c := range 4 {
			starts.Add(1)
			go func() {
				defer starts.Done()
				for i := c; i < len(batch); i += 4 {
					cc.startWorkflow(batch[i])
				}
			}()
		}
	}
	startAll(ids[:50])
	// Kill the leader while the first half is in full swing.
	waitFor(t, 60*time.Second, "the first workflows to make progress", func() bool {
		h, ok := cc.health(first)
		return ok && h.Cluster.Applied > 120
	})
	cc.kill(first)
	t.Logf("killed leader %s; %s took over", first, cc.leader(first))
	startAll(ids[50:])
	starts.Wait()
	cc.waitCompleted(ids, 4*time.Minute)
	cc.start(first)
	cc.verifyHistories(ids, cc.ids...)

	var reruns []string
	for k, n := range ex.n {
		if n > 1 {
			reruns = append(reruns, fmt.Sprintf("%s×%d", k, n))
		}
	}
	sort.Strings(reruns)
	t.Logf("activities executed more than once (allowed, at-least-once): %d %v", len(reruns), reruns)
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
