package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"

	"github.com/olayanju-1234/durable-workflow-engine/internal/audit"
)

type auditHarness struct {
	srv  *httptest.Server
	log  *audit.Log
	path string
	e    *Engine
	grpc WorkflowServiceClient
}

func newAuditHarness(t *testing.T) *auditHarness {
	t.Helper()
	set, err := NewNamespaceSet([]NamespaceConfig{{Name: "acme", Tokens: []string{"acme-token"}}})
	if err != nil {
		t.Fatal(err)
	}
	e := nsEngine(t, t.TempDir(), WithNamespaces(set))
	if err := e.Recover(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "audit.log")
	l, err := audit.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	authz := NewAuthorizer("admin-token", false, set)
	api := NewHTTPAPI(e, discardLogger(), WithNamespaceSet(set), WithAuthorizer(authz), WithAudit(l))
	srv := httptest.NewServer(api.Wrap(api.Handler()))
	t.Cleanup(srv.Close)

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer(append(GRPCAuthorize(authz, nil), GRPCAudit(l, func(err error) { t.Error(err) })...)...)
	RegisterWorkflowServiceServer(gs, NewGRPCServer(e, discardLogger()))
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)
	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return &auditHarness{srv: srv, log: l, path: path, e: e, grpc: NewWorkflowServiceClient(conn)}
}

func (h *auditHarness) do(t *testing.T, method, path, token, body string) (int, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, h.srv.URL+path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	raw, _ := io.ReadAll(res.Body)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return res.StatusCode, out
}

func (h *auditHarness) last(t *testing.T) audit.Entry {
	t.Helper()
	got, err := h.log.Read(nil, 1)
	if err != nil || len(got) != 1 {
		t.Fatalf("audit log: %v %v", got, err)
	}
	return got[0]
}

// Every operator action over HTTP is recorded, with who did it (a hash of
// their token, never the token), the namespace, the target, the outcome and
// its settings, and never a payload.
func TestEveryHTTPOperatorActionIsAudited(t *testing.T) {
	h := newAuditHarness(t)
	adminActor, acmeActor := "engine:"+tokenHash("admin-token"), "token:"+tokenHash("acme-token")
	steps := []struct {
		method, path, token, body string
		action, target, actor     string
		outcome, detail           string
	}{
		{"POST", "/api/workflows", "acme-token", `{"type":"Flow","workflow_id":"w1","input":"SECRET-INPUT"}`, "workflow.start", "w1", acmeActor, "ok", "type=Flow"},
		{"POST", "/api/workflows/w1/signal", "acme-token", `{"name":"go","payload":"SECRET-PAYLOAD"}`, "workflow.signal", "w1", acmeActor, "ok", "name=go"},
		{"POST", "/api/workflows/w1/cancel", "acme-token", `{"reason":"now"}`, "workflow.cancel", "w1", acmeActor, "ok", "reason=now"},
		{"POST", "/api/workflows/w1/terminate", "acme-token", `{"reason":"again"}`, "workflow.terminate", "w1", acmeActor, "error", ""},
		{"POST", "/api/workflows/w1/reset", "acme-token", `{"reason":"retry"}`, "workflow.reset", "w1", acmeActor, "ok", "reason=retry"},
		{"POST", "/api/workflows/w1/cancel", "acme-token", `{"reason":"ask nicely","graceful":true}`, "workflow.request_cancel", "w1", acmeActor, "ok", "reason=ask nicely"},
		{"POST", "/api/schedules", "admin-token", `{"schedule_id":"s1","cron":"@hourly","workflow_type":"Flow"}`, "schedule.create", "s1", adminActor, "ok", "cron=@hourly"},
		{"POST", "/api/schedules/s1/pause", "admin-token", `{"note":"maintenance"}`, "schedule.pause", "s1", adminActor, "ok", "note=maintenance"},
		{"POST", "/api/schedules/s1/resume", "admin-token", ``, "schedule.resume", "s1", adminActor, "ok", ""},
		{"POST", "/api/schedules/s1/backfill", "admin-token", `{"start_time":"2026-10-24T00:00:00Z","end_time":"2026-10-24T01:30:00Z"}`, "schedule.backfill", "s1", adminActor, "ok", ""},
		{"DELETE", "/api/schedules/s1", "admin-token", ``, "schedule.delete", "s1", adminActor, "ok", ""},
		{"GET", "/api/admin/backup", "admin-token", ``, "admin.backup", "", adminActor, "ok", ""},
	}
	for _, s := range steps {
		code, body := h.do(t, s.method, s.path, s.token, s.body)
		e := h.last(t)
		if e.Action != s.action || e.Target != s.target || e.Actor != s.actor || e.Outcome != s.outcome || e.Status != code || e.Via != "http" {
			t.Fatalf("%s %s (HTTP %d %v): entry %+v", s.method, s.path, code, body, e)
		}
		if s.detail != "" {
			k, v, _ := strings.Cut(s.detail, "=")
			if e.Details[k] != v {
				t.Fatalf("%s: details %v, want %s", s.action, e.Details, s.detail)
			}
		}
	}
	start, _ := h.log.Read(func(e audit.Entry) bool { return e.Action == "workflow.start" }, 1)
	if start[0].Namespace != "acme" || start[0].RunID == "" || start[0].Remote == "" {
		t.Fatalf("start entry %+v", start[0])
	}
	if terminate, _ := h.log.Read(func(e audit.Entry) bool { return e.Action == "workflow.terminate" }, 1); !strings.Contains(terminate[0].Error, "terminal") {
		t.Fatalf("a refused terminate records why: %+v", terminate[0])
	}
	raw, _ := os.ReadFile(h.path)
	for _, leak := range []string{"SECRET-INPUT", "SECRET-PAYLOAD", "admin-token", "acme-token"} {
		if strings.Contains(string(raw), leak) {
			t.Fatalf("the audit log holds %q", leak)
		}
	}

	// Reads are not operator actions.
	before, _ := h.log.Read(nil, 0)
	h.do(t, "GET", "/api/workflows", "admin-token", "")
	h.do(t, "GET", "/api/workflows/w1/history?namespace=acme", "admin-token", "")
	if after, _ := h.log.Read(nil, 0); len(after) != len(before) {
		t.Fatal("a read was audited")
	}

	// Reading the log: the engine token sees everything, a namespace token its
	// own namespace, and nobody without a token.
	_, all := h.do(t, "GET", "/api/audit?limit=1000", "admin-token", "")
	_, mine := h.do(t, "GET", "/api/audit?limit=1000", "acme-token", "")
	_, onlyResets := h.do(t, "GET", "/api/audit?action=workflow.reset", "admin-token", "")
	if n, m := len(all["entries"].([]any)), len(mine["entries"].([]any)); n != len(steps) || m != 6 {
		t.Fatalf("engine token sees %d, acme sees %d; want %d and 6", n, m, len(steps))
	}
	if len(onlyResets["entries"].([]any)) != 1 {
		t.Fatalf("action filter: %v", onlyResets)
	}
	if code, _ := h.do(t, "GET", "/api/audit", "", ""); code != http.StatusUnauthorized {
		t.Fatalf("without a token: %d", code)
	}
}

func TestEveryGRPCOperatorActionIsAudited(t *testing.T) {
	h := newAuditHarness(t)
	ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer admin-token")
	calls := []struct {
		action string
		call   func() error
	}{
		{"workflow.start", func() error {
			_, err := h.grpc.StartWorkflow(ctx, &StartWorkflowRequest{WorkflowType: "Flow", WorkflowId: "g1", Input: []byte("SECRET")})
			return err
		}},
		{"workflow.signal", func() error {
			_, err := h.grpc.SignalWorkflow(ctx, &SignalWorkflowRequest{WorkflowId: "g1", SignalName: "go", Payload: []byte("SECRET")})
			return err
		}},
		{"workflow.request_cancel", func() error {
			_, err := h.grpc.CancelWorkflow(ctx, &CancelWorkflowRequest{WorkflowId: "g1", Graceful: true})
			return err
		}},
		{"workflow.terminate", func() error {
			_, err := h.grpc.TerminateWorkflow(ctx, &TerminateWorkflowRequest{WorkflowId: "g1", Reason: "stop"})
			return err
		}},
		{"workflow.reset", func() error {
			_, err := h.grpc.ResetWorkflow(ctx, &ResetWorkflowRequest{WorkflowId: "g1"})
			return err
		}},
		{"workflow.cancel", func() error {
			_, err := h.grpc.CancelWorkflow(ctx, &CancelWorkflowRequest{WorkflowId: "g1", Reason: "done"})
			return err
		}},
		{"schedule.create", func() error {
			_, err := h.grpc.CreateSchedule(ctx, &CreateScheduleRequest{Schedule: &Schedule{ScheduleId: "gs", Cron: "@daily", WorkflowType: "Flow"}})
			return err
		}},
		{"schedule.pause", func() error { _, err := h.grpc.PauseSchedule(ctx, &PauseScheduleRequest{ScheduleId: "gs"}); return err }},
		{"schedule.resume", func() error {
			_, err := h.grpc.ResumeSchedule(ctx, &ResumeScheduleRequest{ScheduleId: "gs"})
			return err
		}},
		{"schedule.backfill", func() error {
			_, err := h.grpc.BackfillSchedule(ctx, &BackfillScheduleRequest{ScheduleId: "gs",
				StartTime: time.Now().Add(-49 * time.Hour).UnixNano(), EndTime: time.Now().Add(-time.Hour).UnixNano()})
			return err
		}},
		{"schedule.delete", func() error {
			_, err := h.grpc.DeleteSchedule(ctx, &DeleteScheduleRequest{ScheduleId: "gs"})
			return err
		}},
	}
	for _, c := range calls {
		err := c.call()
		e := h.last(t)
		want := "ok"
		if err != nil {
			want = "error"
		}
		if e.Action != c.action || e.Via != "grpc" || e.Actor != "engine:"+tokenHash("admin-token") || e.Outcome != want || e.Remote == "" {
			t.Fatalf("%s (%v): entry %+v", c.action, err, e)
		}
	}
	raw, _ := os.ReadFile(h.path)
	if strings.Contains(string(raw), "SECRET") {
		t.Fatal("the audit log holds a payload")
	}
	before, _ := h.log.Read(nil, 0)
	if _, err := h.grpc.ListWorkflows(ctx, &ListWorkflowsRequest{}); err != nil {
		t.Fatal(err)
	}
	if after, _ := h.log.Read(nil, 0); len(after) != len(before) {
		t.Fatal("a gRPC read was audited")
	}
}

// No client write escapes the log: every HTTP route and gRPC method the
// guard treats as a client write has an audit action, so a new one cannot be
// added without being audited.
func TestNoClientWriteIsUnaudited(t *testing.T) {
	for _, r := range []struct{ method, path string }{
		{"POST", "/api/workflows"}, {"POST", "/api/workflows/x/signal"}, {"POST", "/api/workflows/x/cancel"},
		{"POST", "/api/workflows/x/terminate"}, {"POST", "/api/workflows/x/reset"},
		{"POST", "/api/schedules"}, {"DELETE", "/api/schedules/x"}, {"POST", "/api/schedules/x/pause"},
		{"POST", "/api/schedules/x/resume"}, {"POST", "/api/schedules/x/backfill"},
	} {
		req := httptest.NewRequest(r.method, r.path, nil)
		if !httpOp(req).clientWrite() {
			t.Errorf("%s %s is not classified as a client write", r.method, r.path)
		}
		if httpAuditAction(req) == "" {
			t.Errorf("%s %s is a client write with no audit action", r.method, r.path)
		}
	}
	stop := errors.New("captured")
	for _, m := range WorkflowService_ServiceDesc.Methods {
		var req any
		_, _ = m.Handler(nil, context.Background(), func(v any) error { req = v; return stop }, nil)
		full := "/" + WorkflowService_ServiceDesc.ServiceName + "/" + m.MethodName
		action, _, _ := grpcAuditAction(req)
		if grpcOps[full].clientWrite() != (action != "") {
			t.Errorf("%s: client write %v, audit action %q", full, grpcOps[full].clientWrite(), action)
		}
	}
}

// On an open engine anyone may start workflows, but nobody without a token
// reads the audit log: it names callers and their addresses.
func TestAuditLogIsNeverAnonymous(t *testing.T) {
	e := nsEngine(t, t.TempDir())
	l, err := audit.Open(filepath.Join(t.TempDir(), "audit.log"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	api := NewHTTPAPI(e, discardLogger(), WithAudit(l))
	srv := httptest.NewServer(api.Wrap(api.Handler()))
	t.Cleanup(srv.Close)
	res, err := http.Post(srv.URL+"/api/workflows", "application/json", strings.NewReader(`{"type":"Flow"}`))
	if err != nil || res.StatusCode != http.StatusCreated {
		t.Fatalf("anonymous start: %v %v", res.StatusCode, err)
	}
	res.Body.Close()
	if got, _ := l.Read(nil, 0); len(got) != 1 || got[0].Actor != "anonymous" {
		t.Fatalf("anonymous start recorded as %+v", got)
	}
	res, err = http.Get(srv.URL + "/api/audit")
	if err != nil || res.StatusCode != http.StatusForbidden {
		t.Fatalf("anonymous audit read: %v %v", res.StatusCode, err)
	}
	res.Body.Close()
}
