package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

var leaderN2 = server.LeaderHint{ID: "n2", GRPCAddr: "127.0.0.1:7234", HTTPAddr: "127.0.0.1:8234"}

// switchSource is an EngineSource a test can flip between "serving" and
// "not the leader", the way a cluster node's leadership changes.
type switchSource struct {
	mu     sync.Mutex
	engine *server.Engine
	hint   server.LeaderHint
}

func (s *switchSource) Engine() (*server.Engine, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.engine == nil {
		return nil, &server.NotLeaderError{Leader: s.hint}
	}
	return s.engine, nil
}

func (s *switchSource) set(e *server.Engine, hint server.LeaderHint) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.engine, s.hint = e, hint
}

func dialSource(t *testing.T, src server.EngineSource) server.WorkflowServiceClient {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	server.RegisterWorkflowServiceServer(gs, server.NewGRPCServerFrom(src, slog.New(slog.NewTextHandler(io.Discard, nil))))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return server.NewWorkflowServiceClient(conn)
}

func TestGRPCFollowerAnswersWithLeaderHint(t *testing.T) {
	client := dialSource(t, &switchSource{hint: leaderN2})
	var trailer metadata.MD
	_, err := client.StartWorkflow(context.Background(), &server.StartWorkflowRequest{WorkflowType: "X"}, grpc.Trailer(&trailer))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("follower answered %v, want Unavailable", err)
	}
	for key, want := range map[string]string{
		server.TrailerLeader: "n2", server.TrailerLeaderGRPC: leaderN2.GRPCAddr, server.TrailerLeaderHTTP: leaderN2.HTTPAddr,
	} {
		if got := trailer.Get(key); len(got) != 1 || got[0] != want {
			t.Errorf("trailer %s = %v, want %q", key, got, want)
		}
	}
	// Reads are leader-only too.
	if _, err := client.ListWorkflows(context.Background(), &server.ListWorkflowsRequest{}); status.Code(err) != codes.Unavailable {
		t.Errorf("follower list: %v, want Unavailable", err)
	}
}

func TestHTTPFollowerAnswersWithLeaderHint(t *testing.T) {
	src := &switchSource{hint: leaderN2}
	api := server.NewHTTPAPI(nil, slog.New(slog.NewTextHandler(io.Discard, nil)),
		server.WithEngineSource(src),
		server.WithClusterStatus(func() map[string]any { return map[string]any{"role": "follower"} }))
	srv := httptest.NewServer(api.Wrap(api.Handler()))
	t.Cleanup(srv.Close)

	res, err := http.Get(srv.URL + "/api/workflows")
	if err != nil {
		t.Fatal(err)
	}
	var body map[string]string
	json.NewDecoder(res.Body).Decode(&body)
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable || res.Header.Get("X-Workflowd-Leader") != "n2" ||
		body["leader_http"] != leaderN2.HTTPAddr {
		t.Fatalf("follower list: %d leader=%q body=%v", res.StatusCode, res.Header.Get("X-Workflowd-Leader"), body)
	}

	// Health is answered by every node, with its cluster view.
	res, err = http.Get(srv.URL + "/api/health")
	if err != nil {
		t.Fatal(err)
	}
	var health map[string]any
	json.NewDecoder(res.Body).Decode(&health)
	res.Body.Close()
	if res.StatusCode != http.StatusOK || health["cluster"].(map[string]any)["role"] != "follower" {
		t.Fatalf("follower health: %d %v", res.StatusCode, health)
	}

	// No leader known: say when to retry instead of naming one.
	src.set(nil, server.LeaderHint{})
	res, err = http.Post(srv.URL+"/api/workflows", "application/json", strings.NewReader(`{"type":"X"}`))
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if res.StatusCode != http.StatusServiceUnavailable || res.Header.Get("Retry-After") == "" {
		t.Fatalf("no leader: %d Retry-After=%q", res.StatusCode, res.Header.Get("Retry-After"))
	}
}

// A worker long-polling a deposed leader's engine is released at once, with
// an error that sends it looking for the new leader.
func TestPollEndsWhenEngineCloses(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e := server.NewEngine(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	done := make(chan error, 1)
	go func() {
		_, err := e.PollTask(context.Background(), "w")
		done <- err
	}()
	time.Sleep(20 * time.Millisecond)
	e.Close()
	select {
	case err := <-done:
		if !errors.Is(err, server.ErrNotLeader) {
			t.Fatalf("poll on a closed engine: %v, want ErrNotLeader", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("poll still blocked after the engine closed")
	}
}

// recordingLog is a HistoryLog that writes through to the store, records each
// call, and can be told to fail the way a lost leadership does.
type recordingLog struct {
	store *history.HistoryStore
	mu    sync.Mutex
	calls []string
	fail  error
}

func (l *recordingLog) Append(id string, ev *history.HistoryEvent) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fail != nil {
		return l.fail
	}
	l.calls = append(l.calls, fmt.Sprintf("append %s #%d", id, ev.GetEventId()))
	return l.store.Append(id, ev)
}

func (l *recordingLog) NewRun(id, archive string, ev *history.HistoryEvent) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.fail != nil {
		return l.fail
	}
	l.calls = append(l.calls, "new run "+id)
	return l.store.NewRun(id, archive, ev)
}

func (l *recordingLog) Delete(id string) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.calls = append(l.calls, "delete "+id)
	return l.store.Delete(id)
}

func TestEngineWritesThroughHistoryLog(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	hl := &recordingLog{store: store}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	e := server.NewEngine(store, logger, server.WithHistoryLog(hl))
	t.Cleanup(e.Close)
	e.Register(state.WorkflowDefinition{Type: "One", Steps: state.Activities("only")})

	if _, err := e.StartWorkflow("One", nil, "wf-1"); err != nil {
		t.Fatal(err)
	}
	if want := []string{"append wf-1 #1", "append wf-1 #2"}; fmt.Sprint(hl.calls) != fmt.Sprint(want) {
		t.Fatalf("history log saw %v, want %v", hl.calls, want)
	}

	// Leadership is lost while the next write waits to commit: the caller is
	// told to go to the leader, not that the server failed.
	src := &switchSource{engine: e}
	client := dialSource(t, src)
	hl.fail = fmt.Errorf("%w: leadership lost", server.ErrNotLeader)
	src.set(e, leaderN2) // the source still hands out e, but now knows n2 leads
	var trailer metadata.MD
	_, err = client.StartWorkflow(context.Background(), &server.StartWorkflowRequest{WorkflowType: "One", WorkflowId: "wf-2"}, grpc.Trailer(&trailer))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("write that lost leadership: %v, want Unavailable", err)
	}
	src.set(nil, leaderN2)
	_, err = client.StartWorkflow(context.Background(), &server.StartWorkflowRequest{WorkflowType: "One", WorkflowId: "wf-3"}, grpc.Trailer(&trailer))
	if got := trailer.Get(server.TrailerLeader); status.Code(err) != codes.Unavailable || len(got) != 1 || got[0] != "n2" {
		t.Fatalf("after losing leadership: %v trailer %v", err, got)
	}
}

// A poller whose context ends just as a task arrives must not walk off with
// it: leasing a task to a departed worker strands it until the lease expires.
func TestPollDoesNotLeaseToDepartedPoller(t *testing.T) {
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	e := server.NewEngine(store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	t.Cleanup(e.Close)
	e.Register(state.WorkflowDefinition{Type: "One", Steps: state.Activities("only")})
	if _, err := e.StartWorkflow("One", nil, "wf-1"); err != nil {
		t.Fatal(err)
	}

	gone, cancel := context.WithCancel(context.Background())
	cancel() // the task is already queued; the poller has already left
	if _, err := e.PollTask(gone, "departed"); err == nil {
		t.Fatal("a cancelled poller was handed a task")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	task, err := e.PollTask(ctx, "live")
	if err != nil || task.GetWorkflowId() != "wf-1" {
		t.Fatalf("the next poller got %v, %v; want wf-1's task", task, err)
	}
}
