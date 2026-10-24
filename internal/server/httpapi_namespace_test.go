package server_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/olayanju-1234/durable-workflow-engine/client"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// nsHarness serves an engine with an engine token ("admin") and two
// namespaces with their own tokens, over HTTP (wrapped as runServe does) and
// gRPC (with the same authorizer).
type nsHarness struct {
	srv  *httptest.Server
	grpc string
}

func newNSHarness(t *testing.T, adminToken string) *nsHarness {
	t.Helper()
	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { store.Close() })
	set, err := server.NewNamespaceSet([]server.NamespaceConfig{
		{Name: "acme", Tokens: []string{"acme-token"}},
		{Name: "beta", Tokens: []string{"beta-token"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := server.NewEngine(store, logger, server.WithNamespaces(set))
	t.Cleanup(engine.Close)
	def := state.WorkflowDefinition{Type: "Flow", Steps: state.Activities("work")}
	for _, ns := range []string{"", "acme", "beta"} {
		engine.In(ns).RegisterWorkflow(def.Type, def)
	}

	api := server.NewHTTPAPI(engine, logger, server.WithAuthToken(adminToken), server.WithNamespaceSet(set))
	mux := api.Handler()
	mux.Handle("/", webStub{})
	srv := httptest.NewServer(api.Wrap(mux))
	t.Cleanup(srv.Close)

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(server.GRPCAuthorize(server.NewAuthorizer(adminToken, false, set))...)
	server.RegisterWorkflowServiceServer(gs, server.NewGRPCServer(engine, logger))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	return &nsHarness{srv: srv, grpc: lis.Addr().String()}
}

type nsReq struct {
	method, path, token, ns, body string
}

func (h *nsHarness) do(t *testing.T, r nsReq) (int, map[string]any) {
	t.Helper()
	req, err := http.NewRequest(r.method, h.srv.URL+r.path, strings.NewReader(r.body))
	if err != nil {
		t.Fatal(err)
	}
	if r.token != "" {
		req.Header.Set("Authorization", "Bearer "+r.token)
	}
	if r.ns != "" {
		req.Header.Set(server.HeaderNamespace, r.ns)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	var body map[string]any
	_ = json.NewDecoder(res.Body).Decode(&body)
	return res.StatusCode, body
}

func listedIDs(body map[string]any) []string {
	var ids []string
	wfs, _ := body["workflows"].([]any)
	for _, w := range wfs {
		ids = append(ids, w.(map[string]any)["workflow_id"].(string)+"="+w.(map[string]any)["status"].(string))
	}
	return ids
}

// A namespace's token reaches its namespace and nothing else; the engine
// token reaches every namespace; nothing reaches a namespace without a token.
func TestHTTPNamespaceTokens(t *testing.T) {
	h := newNSHarness(t, "admin")
	start := func(token, ns, input string) int {
		code, _ := h.do(t, nsReq{"POST", "/api/workflows", token, ns, `{"type":"Flow","workflow_id":"o1","input":"` + input + `"}`})
		return code
	}
	if code := start("acme-token", "", "acme"); code != http.StatusCreated {
		t.Fatalf("acme start: %d", code)
	}
	if code := start("beta-token", "", "beta"); code != http.StatusCreated {
		t.Fatalf("beta start with the same id: %d", code)
	}
	if code := start("admin", "", "default"); code != http.StatusCreated {
		t.Fatalf("admin start in default: %d", code)
	}

	cases := []struct {
		name string
		req  nsReq
		want int
	}{
		{"acme token in its namespace by name", nsReq{"GET", "/api/workflows", "acme-token", "acme", ""}, 200},
		{"acme token naming beta", nsReq{"GET", "/api/workflows", "acme-token", "beta", ""}, 403},
		{"acme token naming beta by query", nsReq{"GET", "/api/workflows?namespace=beta", "acme-token", "", ""}, 403},
		{"acme token naming default", nsReq{"GET", "/api/workflows", "acme-token", "default", ""}, 403},
		{"acme token starting in beta", nsReq{"POST", "/api/workflows", "acme-token", "beta", `{"type":"Flow"}`}, 403},
		{"acme token terminating beta's", nsReq{"POST", "/api/workflows/o1/terminate", "acme-token", "beta", ""}, 403},
		{"beta token, acme's key in the path", nsReq{"GET", "/api/workflows/acme%2Fo1", "beta-token", "", ""}, 404},
		{"admin token, acme's key in the path", nsReq{"GET", "/api/workflows/acme%2Fo1", "admin", "", ""}, 404},
		{"no token", nsReq{"GET", "/api/workflows", "", "", ""}, 401},
		{"no token naming acme", nsReq{"GET", "/api/workflows", "", "acme", ""}, 401},
		{"unknown token", nsReq{"GET", "/api/workflows", "nope", "", ""}, 401},
		{"header and query disagree", nsReq{"GET", "/api/workflows?namespace=beta", "admin", "acme", ""}, 400},
		{"invalid namespace", nsReq{"GET", "/api/workflows", "admin", "Acme_1", ""}, 400},
		{"admin in acme", nsReq{"GET", "/api/workflows", "admin", "acme", ""}, 200},
		{"health is open", nsReq{"GET", "/api/health", "", "", ""}, 200},
	}
	for _, c := range cases {
		if code, body := h.do(t, c.req); code != c.want {
			t.Errorf("%s: HTTP %d (%v), want %d", c.name, code, body["error"], c.want)
		}
	}

	// Each caller sees its own o1, and only it.
	for _, c := range []struct{ token, ns, input string }{
		{"acme-token", "", "acme"}, {"beta-token", "", "beta"}, {"admin", "", "default"}, {"admin", "acme", "acme"},
	} {
		code, body := h.do(t, nsReq{"GET", "/api/workflows", c.token, c.ns, ""})
		if code != 200 || len(listedIDs(body)) != 1 {
			t.Fatalf("%s/%s list: %d %v", c.token, c.ns, code, listedIDs(body))
		}
		code, body = h.do(t, nsReq{"GET", "/api/workflows/o1", c.token, c.ns, ""})
		if code != 200 || body["input"] != c.input {
			t.Fatalf("%s/%s describe o1: %d, input %v, want %q", c.token, c.ns, code, body["input"], c.input)
		}
	}

	// Workers: acme's poller, asking for any activity, gets acme's task only,
	// and beta cannot complete it.
	code, task := h.do(t, nsReq{"GET", "/api/activity-tasks?worker_id=w", "acme-token", "", ""})
	if code != 200 || task["input"] != "acme" || task["workflow_id"] != "o1" {
		t.Fatalf("acme poll: %d %v", code, task)
	}
	complete := `{"workflow_id":"o1","step_name":"work","sequence":1,"lease_token":"` + task["lease_token"].(string) + `","output":"done"}`
	if code, _ := h.do(t, nsReq{"POST", "/api/activity-tasks/complete", "beta-token", "", complete}); code != http.StatusConflict {
		t.Fatalf("beta completing acme's task: HTTP %d, want 409", code)
	}
	if code, _ := h.do(t, nsReq{"POST", "/api/activity-tasks/complete", "acme-token", "", complete}); code != 200 {
		t.Fatalf("acme completing its task: HTTP %d", code)
	}
	if _, body := h.do(t, nsReq{"GET", "/api/workflows/o1", "beta-token", "", ""}); body["status"] != "WAITING" {
		t.Fatalf("beta's o1 is %v, want WAITING", body["status"])
	}

	// Schedules belong to a namespace too.
	if code, _ := h.do(t, nsReq{"POST", "/api/schedules", "acme-token", "", `{"schedule_id":"s","cron":"@every 1h","workflow_type":"Flow"}`}); code != 201 {
		t.Fatalf("acme create schedule: %d", code)
	}
	if _, body := h.do(t, nsReq{"GET", "/api/schedules", "beta-token", "", ""}); len(body["schedules"].([]any)) != 0 {
		t.Fatalf("beta lists acme's schedule: %v", body)
	}
	if code, _ := h.do(t, nsReq{"DELETE", "/api/schedules/s", "beta-token", "", ""}); code != 404 {
		t.Fatalf("beta deleting acme's schedule: HTTP %d, want 404", code)
	}

	// Each caller is told which namespaces it can use.
	_, body := h.do(t, nsReq{"GET", "/api/namespaces", "admin", "", ""})
	if got := body["namespaces"].([]any); len(got) != 3 || body["admin"] != true {
		t.Fatalf("admin namespaces: %v", body)
	}
	_, body = h.do(t, nsReq{"GET", "/api/namespaces", "acme-token", "", ""})
	if got := body["namespaces"].([]any); len(got) != 1 || got[0] != "acme" || body["current"] != "acme" {
		t.Fatalf("acme namespaces: %v", body)
	}
}

// Without an engine token the default namespace stays open, as it was before
// namespaces, while every other namespace still needs its token.
func TestHTTPOpenDefaultNamespace(t *testing.T) {
	h := newNSHarness(t, "")
	for _, c := range []struct {
		name string
		req  nsReq
		want int
	}{
		{"anonymous start in default", nsReq{"POST", "/api/workflows", "", "", `{"type":"Flow","workflow_id":"d1"}`}, 201},
		{"anonymous list in default", nsReq{"GET", "/api/workflows", "", "default", ""}, 200},
		{"anonymous in acme", nsReq{"GET", "/api/workflows", "", "acme", ""}, 401},
		{"anonymous poll in acme", nsReq{"GET", "/api/activity-tasks", "", "acme", ""}, 401},
		{"acme token in acme", nsReq{"GET", "/api/workflows", "acme-token", "", ""}, 200},
		{"acme token in default", nsReq{"GET", "/api/workflows", "acme-token", "default", ""}, 403},
	} {
		if code, body := h.do(t, c.req); code != c.want {
			t.Errorf("%s: HTTP %d (%v), want %d", c.name, code, body["error"], c.want)
		}
	}
	_, body := h.do(t, nsReq{"GET", "/api/workflows", "acme-token", "", ""})
	if ids := listedIDs(body); len(ids) != 0 {
		t.Fatalf("acme lists default's workflows: %v", ids)
	}
}

// The gRPC API applies the same rules, through the Go client's Namespace.
func TestGRPCNamespaceTokens(t *testing.T) {
	h := newNSHarness(t, "admin")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	dial := func(token, ns string) *client.Client {
		c, err := client.Dial(client.Options{Addrs: []string{h.grpc}, AuthToken: token, Namespace: ns})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		return c
	}
	acme := dial("acme-token", "")
	if _, err := acme.StartWorkflow(ctx, "Flow", []byte("acme"), client.WithWorkflowID("g1")); err != nil {
		t.Fatalf("acme start: %v", err)
	}
	if _, err := dial("acme-token", "beta").StartWorkflow(ctx, "Flow", nil); !errors.Is(err, client.ErrForbidden) {
		t.Fatalf("acme token in beta: %v, want ErrForbidden", err)
	}
	if _, err := dial("beta-token", "").DescribeWorkflow(ctx, "g1"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("beta describing acme's g1: %v, want ErrNotFound", err)
	}
	if _, err := dial("", "acme").DescribeWorkflow(ctx, "g1"); !errors.Is(err, client.ErrUnauthenticated) {
		t.Fatalf("no token in acme: %v, want ErrUnauthenticated", err)
	}
	d, err := dial("admin", "acme").DescribeWorkflow(ctx, "g1")
	if err != nil || string(d.Input) != "acme" || d.ID != "g1" {
		t.Fatalf("admin describing acme's g1: %v, %v", d, err)
	}
	if _, err := dial("admin", "").DescribeWorkflow(ctx, "g1"); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("admin in default describing acme's g1: %v, want ErrNotFound", err)
	}
	if err := dial("beta-token", "").TerminateWorkflow(ctx, "g1", ""); !errors.Is(err, client.ErrNotFound) {
		t.Fatalf("beta terminating acme's g1: %v, want ErrNotFound", err)
	}
}
