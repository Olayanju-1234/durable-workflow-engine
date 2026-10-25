package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

func quotaEngine(t *testing.T, p Policy, cfgs ...NamespaceConfig) *Engine {
	t.Helper()
	set, err := NewNamespaceSet(cfgs)
	if err != nil {
		t.Fatal(err)
	}
	return nsEngine(t, t.TempDir(), WithNamespaces(set), WithGuardrails(p))
}

// However many starts race, a namespace never has more workflows running
// than its limit, and every refusal says so.
func TestQuotaMaxActiveUnderRace(t *testing.T) {
	e := quotaEngine(t, Policy{MaxActiveWorkflows: 5})
	var ok, refused atomic.Int32
	var wg sync.WaitGroup
	for i := range 60 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := e.Start("Flow", nil, fmt.Sprintf("w%d", i))
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrQuotaExceeded):
				refused.Add(1)
			default:
				t.Errorf("start: %v", err)
			}
		}()
	}
	wg.Wait()
	if ok.Load() != 5 || refused.Load() != 55 {
		t.Fatalf("%d started, %d refused; want 5 and 55", ok.Load(), refused.Load())
	}
	// A finished workflow frees its slot.
	sums, _ := e.ListWorkflows(WorkflowStatus_WORKFLOW_STATUS_WAITING)
	if err := e.TerminateWorkflow(sums[0].GetWorkflowId(), ""); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Start("Flow", nil, "after"); err != nil {
		t.Fatalf("start after one finished: %v", err)
	}
	if _, err := e.Start("Flow", nil, "over"); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("start over the limit again: %v", err)
	}
}

// Each namespace's own quota applies to it alone.
func TestQuotaPerNamespace(t *testing.T) {
	one := 1
	e := quotaEngine(t, Policy{}, NamespaceConfig{Name: "acme", MaxActiveWorkflows: &one})
	if _, err := e.In("acme").Start("Flow", nil, "a1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.In("acme").Start("Flow", nil, "a2"); !errors.Is(err, ErrQuotaExceeded) {
		t.Fatalf("acme over its quota: %v", err)
	}
	for i := range 5 {
		if _, err := e.In("beta").Start("Flow", nil, fmt.Sprintf("b%d", i)); err != nil {
			t.Fatalf("beta, which has no quota: %v", err)
		}
	}
}

// Starts per minute count runs that began, not refusals or existing runs,
// and a run that continues as new is not a start.
func TestQuotaStartsPerMinute(t *testing.T) {
	e := quotaEngine(t, Policy{MaxStartsPerMinute: 3})
	e.RegisterWorkflow("Loop", replay.Planner{Fn: func(ctx workflow.Context, in []byte) ([]byte, error) {
		if n := len(in); n < 5 {
			return nil, workflow.ContinueAsNew(append(in, 'x'))
		}
		return in, nil
	}})
	if _, err := e.Start("Loop", nil, "loop"); err != nil {
		t.Fatal(err)
	}
	if resp, err := e.GetWorkflow("loop"); err != nil || string(resp.GetOutput()) != "xxxxx" {
		t.Fatalf("the loop ran %v, %v; continue-as-new must not be refused", resp.GetSummary(), err)
	}
	if _, err := e.Start("Flow", nil, "s1"); err != nil {
		t.Fatal(err)
	}
	if s, err := e.Start("Flow", nil, "s1", WithIDConflictPolicy(IdConflictPolicy_ID_CONFLICT_POLICY_USE_EXISTING)); err != nil || !s.Existing {
		t.Fatalf("existing run: %v %v", s, err)
	}
	if _, err := e.Start("Flow", nil, "s1"); !errors.Is(err, ErrAlreadyExists) {
		t.Fatalf("duplicate: %v", err)
	}
	if _, err := e.Start("Flow", nil, "s2"); err != nil {
		t.Fatalf("third start in the minute: %v", err)
	}
	_, err := e.Start("Flow", nil, "s3")
	var ra *RetryAfter
	if !errors.Is(err, ErrQuotaExceeded) || !errors.As(err, &ra) || ra.After <= 0 || ra.After > time.Minute {
		t.Fatalf("fourth start: %v", err)
	}
}

func TestClientIP(t *testing.T) {
	trusted, err := ParseTrustedProxies("127.0.0.1, 10.0.0.0/8 ,::1")
	if err != nil {
		t.Fatal(err)
	}
	g := NewGuard(nil, Policy{}, trusted, nil)
	for _, c := range []struct {
		remote string
		xff    []string
		want   string
	}{
		{"127.0.0.1:5000", nil, "127.0.0.1"},
		{"127.0.0.1:5000", []string{"203.0.113.9"}, "203.0.113.9"},
		{"127.0.0.1:5000", []string{"6.6.6.6, 203.0.113.9"}, "203.0.113.9"},   // the client's own header is ignored
		{"127.0.0.1:5000", []string{"203.0.113.9, 10.1.2.3"}, "203.0.113.9"},  // through two trusted proxies
		{"127.0.0.1:5000", []string{"6.6.6.6", "203.0.113.9"}, "203.0.113.9"}, // two header lines
		{"198.51.100.7:5000", []string{"203.0.113.9"}, "198.51.100.7"},        // not from a proxy
		{"[::1]:5000", []string{"2001:db8::1"}, "2001:db8::1"},
		{"127.0.0.1:5000", []string{"10.0.0.1"}, "10.0.0.1"}, // only proxies: the left-most
	} {
		if got := g.clientIP(c.remote, c.xff); got != c.want {
			t.Errorf("clientIP(%s, %v) = %s, want %s", c.remote, c.xff, got, c.want)
		}
	}
	if NewGuard(nil, Policy{}, nil, nil).clientIP("127.0.0.1:1", []string{"9.9.9.9"}) != "127.0.0.1" {
		t.Error("X-Forwarded-For was believed with no trusted proxies")
	}
	if _, err := ParseTrustedProxies("not-an-ip"); err == nil {
		t.Error("a bad proxy address was accepted")
	}
}

// guardHarness serves the API with a guard, from behind a "proxy" (the test
// client, at 127.0.0.1, which is trusted).
type guardHarness struct {
	srv *httptest.Server
}

func newGuardHarness(t *testing.T, admin string, defaults Policy, cfgs ...NamespaceConfig) *guardHarness {
	t.Helper()
	set, err := NewNamespaceSet(cfgs)
	if err != nil {
		t.Fatal(err)
	}
	e := nsEngine(t, t.TempDir(), WithNamespaces(set), WithGuardrails(defaults))
	trusted, _ := ParseTrustedProxies("127.0.0.1,::1")
	g := NewGuard(set, defaults, trusted, e.Metrics())
	api := NewHTTPAPI(e, discardLogger(), WithAuthToken(admin), WithNamespaceSet(set), WithGuard(g))
	srv := httptest.NewServer(api.Wrap(api.Handler()))
	t.Cleanup(srv.Close)
	return &guardHarness{srv: srv}
}

func (h *guardHarness) do(t *testing.T, method, path, token, client, body string) (int, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(method, h.srv.URL+path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if client != "" {
		req.Header.Set("X-Forwarded-For", client)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	return res.StatusCode, res.Header
}

// Each client (address, or token) has its own allowance of writes; reads,
// task completions and the engine token are never limited.
func TestRateLimitPerClient(t *testing.T) {
	h := newGuardHarness(t, "", Policy{RateLimitPerMinute: 1, RateLimitBurst: 3},
		NamespaceConfig{Name: "acme", Tokens: []string{"t1", "t2"}})
	start := func(token, client string) (int, http.Header) {
		return h.do(t, "POST", "/api/workflows", token, client, `{"type":"Flow"}`)
	}
	for i := range 3 {
		if code, _ := start("", "203.0.113.1"); code != http.StatusCreated {
			t.Fatalf("write %d from a fresh client: HTTP %d", i+1, code)
		}
	}
	code, hdr := start("", "203.0.113.1")
	if code != http.StatusTooManyRequests || hdr.Get("Retry-After") == "" {
		t.Fatalf("fourth write: HTTP %d, Retry-After %q", code, hdr.Get("Retry-After"))
	}
	// A spoofed header in front of the proxy's entry changes nothing.
	if code, _ := start("", "198.51.100.66, 203.0.113.1"); code != http.StatusTooManyRequests {
		t.Fatalf("spoofed X-Forwarded-For got through: HTTP %d", code)
	}
	if code, _ := start("", "203.0.113.2"); code != http.StatusCreated {
		t.Fatalf("another client: HTTP %d", code)
	}
	if code, _ := h.do(t, "GET", "/api/workflows", "", "203.0.113.1", ""); code != http.StatusOK {
		t.Fatalf("a read from the limited client: HTTP %d", code)
	}
	if code, _ := h.do(t, "POST", "/api/activity-tasks/complete", "", "203.0.113.1",
		`{"workflow_id":"x","step_name":"work","sequence":1,"lease_token":"nope","output":""}`); code == http.StatusTooManyRequests {
		t.Fatal("a task completion was rate limited")
	}
	// Tokens are limited one by one, wherever they come from.
	for i := range 3 {
		if code, _ := start("t1", "203.0.113.1"); code != http.StatusCreated {
			t.Fatalf("token t1 write %d: HTTP %d", i+1, code)
		}
	}
	if code, _ := start("t1", "203.0.113.9"); code != http.StatusTooManyRequests {
		t.Fatalf("token t1 from another address: HTTP %d, want 429", code)
	}
	if code, _ := start("t2", "203.0.113.1"); code != http.StatusCreated {
		t.Fatalf("token t2: HTTP %d", code)
	}
}

// The engine token is never rate limited.
func TestRateLimitSparesTheEngineToken(t *testing.T) {
	h := newGuardHarness(t, "admin", Policy{RateLimitPerMinute: 1, RateLimitBurst: 1})
	for range 5 {
		if code, _ := h.do(t, "POST", "/api/workflows", "admin", "203.0.113.1", `{"type":"Flow"}`); code != http.StatusCreated {
			t.Fatalf("the engine token was limited: HTTP %d", code)
		}
	}
}

// Concurrent writes from one client never get more than its burst through.
func TestRateLimitUnderRace(t *testing.T) {
	h := newGuardHarness(t, "", Policy{RateLimitPerMinute: 1, RateLimitBurst: 4})
	var created, limited atomic.Int32
	var wg sync.WaitGroup
	for range 40 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			switch code, _ := h.do(t, "POST", "/api/workflows", "", "203.0.113.7", `{"type":"Flow"}`); code {
			case http.StatusCreated:
				created.Add(1)
			case http.StatusTooManyRequests:
				limited.Add(1)
			}
		}()
	}
	wg.Wait()
	if created.Load() != 4 || limited.Load() != 36 {
		t.Fatalf("%d created, %d limited; want 4 and 36", created.Load(), limited.Load())
	}
}

// A read-only namespace refuses client changes but keeps reads and workers;
// read-only schedules refuse only schedule changes.
func TestReadOnly(t *testing.T) {
	yes := true
	h := newGuardHarness(t, "admin", Policy{ReadOnlySchedules: true},
		NamespaceConfig{Name: "frozen", Tokens: []string{"ft"}, ReadOnly: &yes})
	for _, c := range []struct {
		name, method, path, token, body string
		want                            int
	}{
		{"start", "POST", "/api/workflows", "ft", `{"type":"Flow"}`, 403},
		{"signal", "POST", "/api/workflows/x/signal", "ft", `{"name":"s"}`, 403},
		{"cancel", "POST", "/api/workflows/x/cancel", "ft", ``, 403},
		{"terminate", "POST", "/api/workflows/x/terminate", "ft", ``, 403},
		{"reset", "POST", "/api/workflows/x/reset", "ft", ``, 403},
		{"create schedule", "POST", "/api/schedules", "ft", `{"schedule_id":"s","cron":"@hourly","workflow_type":"Flow"}`, 403},
		{"list", "GET", "/api/workflows", "ft", ``, 200},
		{"register", "POST", "/api/workflow-types", "ft", `{"workflow_types":["R"],"worker_id":"w"}`, 200},
		{"complete", "POST", "/api/activity-tasks/complete", "ft", `{"workflow_id":"x","step_name":"s","sequence":1,"lease_token":"n"}`, 409},
		{"default: create schedule", "POST", "/api/schedules", "admin", `{"schedule_id":"s","cron":"@hourly","workflow_type":"Flow"}`, 403},
		{"default: delete schedule", "DELETE", "/api/schedules/s", "admin", ``, 403},
		{"default: start", "POST", "/api/workflows", "admin", `{"type":"Flow"}`, 201},
		{"default: list schedules", "GET", "/api/schedules", "admin", ``, 200},
		{"default: pause schedule", "POST", "/api/schedules/s/pause", "admin", ``, 403},
		{"default: resume schedule", "POST", "/api/schedules/s/resume", "admin", ``, 403},
		{"default: backfill schedule", "POST", "/api/schedules/s/backfill", "admin", `{}`, 403},
		{"default: preview a spec", "POST", "/api/schedules/preview", "admin", `{"cron":"@hourly","workflow_type":"Flow"}`, 200},
		{"default: preview a missing schedule", "GET", "/api/schedules/s/preview", "admin", ``, 404},
	} {
		if code, _ := h.do(t, c.method, c.path, c.token, "", c.body); code != c.want {
			t.Errorf("%s: HTTP %d, want %d", c.name, code, c.want)
		}
	}
}

// Over gRPC: a read-only namespace answers PERMISSION_DENIED, a rate limit
// RESOURCE_EXHAUSTED, and a quota RESOURCE_EXHAUSTED too.
func TestGuardrailsOverGRPC(t *testing.T) {
	yes, one := true, 1
	set, err := NewNamespaceSet([]NamespaceConfig{
		{Name: "frozen", Tokens: []string{"ft"}, ReadOnly: &yes},
		{Name: "small", Tokens: []string{"st"}, MaxActiveWorkflows: &one},
	})
	if err != nil {
		t.Fatal(err)
	}
	defaults := Policy{RateLimitPerMinute: 1, RateLimitBurst: 2}
	e := nsEngine(t, t.TempDir(), WithNamespaces(set), WithGuardrails(defaults))
	e.In("small").RegisterWorkflow(flowDef.Type, flowDef)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(GRPCAuthorize(NewAuthorizer("", false, set), NewGuard(set, defaults, nil, nil))...)
	RegisterWorkflowServiceServer(gs, NewGRPCServer(e, discardLogger()))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	c := NewWorkflowServiceClient(conn)
	call := func(token string) error {
		ctx := context.Background()
		if token != "" {
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
		}
		_, err := c.StartWorkflow(ctx, &StartWorkflowRequest{WorkflowType: "Flow"})
		return err
	}
	if code := status.Code(call("ft")); code != codes.PermissionDenied {
		t.Fatalf("read-only namespace: %v", code)
	}
	if err := call("st"); err != nil {
		t.Fatal(err)
	}
	if code := status.Code(call("st")); code != codes.ResourceExhausted {
		t.Fatalf("over the namespace's quota: %v", code)
	}
	if code := status.Code(call("st")); code != codes.ResourceExhausted {
		t.Fatalf("over the token's rate: %v", code)
	}
}
