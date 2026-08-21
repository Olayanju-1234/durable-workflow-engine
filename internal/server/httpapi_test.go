package server_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/web"
)

// apiHarness is an Engine behind the real HTTP mux the daemon mounts — the JSON
// API plus the embedded dashboard on "/" — exercised through httptest.
type apiHarness struct {
	srv    *httptest.Server
	store  *history.HistoryStore
	engine *server.Engine
}

func newAPIHarness(t *testing.T, defs ...state.WorkflowDefinition) *apiHarness {
	t.Helper()

	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := server.NewEngine(store, logger)
	t.Cleanup(engine.Close)
	for _, d := range defs {
		engine.Register(d)
	}

	mux := server.NewHTTPAPI(engine, logger).Handler()
	mux.Handle("/", web.Handler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &apiHarness{srv: srv, store: store, engine: engine}
}

// do issues a request and decodes the JSON response body.
func (h *apiHarness) do(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()
	var rdr io.Reader
	if body != "" {
		rdr = bytes.NewBufferString(body)
	}
	req, err := http.NewRequest(method, h.srv.URL+path, rdr)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()

	var decoded map[string]any
	raw, _ := io.ReadAll(res.Body)
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("%s %s: decode %q: %v", method, path, raw, err)
		}
	}
	return res.StatusCode, decoded
}

// TestHTTPAPIWorkflowLifecycle drives start → inspect → terminate over HTTP,
// the exact path the dashboard takes.
func TestHTTPAPIWorkflowLifecycle(t *testing.T) {
	h := newAPIHarness(t, state.WorkflowDefinition{Type: "ApiFlow", Steps: state.Activities("one", "two")})

	// Start (no worker, so it parks WAITING on step one).
	code, body := h.do(t, "POST", "/api/workflows", `{"type":"ApiFlow","input":"hello","workflow_id":"api-1"}`)
	if code != http.StatusCreated {
		t.Fatalf("start: got %d, want 201 (%v)", code, body)
	}
	if body["workflow_id"] != "api-1" {
		t.Errorf("start: workflow_id = %v", body["workflow_id"])
	}

	// List.
	code, body = h.do(t, "GET", "/api/workflows", "")
	if code != http.StatusOK {
		t.Fatalf("list: got %d", code)
	}
	list, _ := body["workflows"].([]any)
	if len(list) != 1 {
		t.Fatalf("list: got %d workflows, want 1", len(list))
	}
	row := list[0].(map[string]any)
	if row["status"] != "WAITING" || row["workflow_type"] != "ApiFlow" {
		t.Errorf("list row = %v", row)
	}
	// Timestamps are RFC3339 strings, not raw nanos.
	if _, err := time.Parse(time.RFC3339Nano, row["started_at"].(string)); err != nil {
		t.Errorf("started_at %q is not RFC3339: %v", row["started_at"], err)
	}

	// Filter round-trips the bare status names the API emits.
	code, body = h.do(t, "GET", "/api/workflows?status=WAITING", "")
	if list, _ := body["workflows"].([]any); code != http.StatusOK || len(list) != 1 {
		t.Errorf("filter WAITING: code %d, %d rows", code, len(list))
	}
	code, body = h.do(t, "GET", "/api/workflows?status=COMPLETED", "")
	if list, _ := body["workflows"].([]any); code != http.StatusOK || len(list) != 0 {
		t.Errorf("filter COMPLETED: code %d, %d rows", code, len(list))
	}
	if code, _ := h.do(t, "GET", "/api/workflows?status=BOGUS", ""); code != http.StatusBadRequest {
		t.Errorf("bad filter: got %d, want 400", code)
	}

	// Detail carries the step breakdown.
	code, body = h.do(t, "GET", "/api/workflows/api-1", "")
	if code != http.StatusOK {
		t.Fatalf("get: got %d", code)
	}
	if body["input"] != "hello" {
		t.Errorf("input = %v, want hello", body["input"])
	}
	steps, _ := body["steps"].([]any)
	if len(steps) != 1 {
		t.Fatalf("steps: got %d, want 1", len(steps))
	}
	step := steps[0].(map[string]any)
	if step["step_name"] != "one" || step["kind"] != "ACTIVITY" || step["status"] != "SCHEDULED" {
		t.Errorf("step = %v", step)
	}

	// Terminate, then confirm the durable effect and the conflict on re-kill.
	if code, body := h.do(t, "POST", "/api/workflows/api-1/terminate", `{"reason":"via http"}`); code != http.StatusOK {
		t.Fatalf("terminate: got %d (%v)", code, body)
	}
	st := waitForStatus(t, h.store, "api-1", state.StatusFailed)
	if !st.Terminated {
		t.Error("state.Terminated = false, want true")
	}
	code, body = h.do(t, "GET", "/api/workflows/api-1", "")
	if body["status"] != "TERMINATED" {
		t.Errorf("status = %v, want TERMINATED", body["status"])
	}
	if code, _ := h.do(t, "POST", "/api/workflows/api-1/terminate", `{"reason":"again"}`); code != http.StatusConflict {
		t.Errorf("double terminate: got %d, want 409", code)
	}
}

func TestHTTPAPIErrors(t *testing.T) {
	h := newAPIHarness(t, state.WorkflowDefinition{Type: "ApiFlow", Steps: state.Activities("one")})

	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{"unknown workflow", "GET", "/api/workflows/nope", "", http.StatusNotFound},
		{"terminate unknown", "POST", "/api/workflows/nope/terminate", `{}`, http.StatusNotFound},
		{"unknown type", "POST", "/api/workflows", `{"type":"Nope"}`, http.StatusBadRequest},
		{"malformed json", "POST", "/api/workflows", `{"type":`, http.StatusBadRequest},
		{"unknown field", "POST", "/api/workflows", `{"bogus":1}`, http.StatusBadRequest},
		{"signal unknown", "POST", "/api/workflows/nope/signal", `{"name":"x"}`, http.StatusNotFound},
		{"signal empty name", "POST", "/api/workflows/api-1/signal", `{"name":""}`, http.StatusBadRequest},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			code, body := h.do(t, tc.method, tc.path, tc.body)
			if code != tc.want {
				t.Errorf("got %d, want %d (%v)", code, tc.want, body)
			}
			if body["error"] == nil {
				t.Errorf("no error message in response: %v", body)
			}
		})
	}
}

// TestHTTPAPISignal parks a workflow on a signal wait and releases it over HTTP.
func TestHTTPAPISignal(t *testing.T) {
	h := newAPIHarness(t, state.WorkflowDefinition{
		Type:  "Gated",
		Steps: []state.StepDef{state.AwaitSignal("gate", "approval")},
	})

	if code, body := h.do(t, "POST", "/api/workflows", `{"type":"Gated","workflow_id":"gate-1"}`); code != http.StatusCreated {
		t.Fatalf("start: %d (%v)", code, body)
	}
	if code, body := h.do(t, "POST", "/api/workflows/gate-1/signal", `{"name":"approval","payload":"yes"}`); code != http.StatusOK {
		t.Fatalf("signal: %d (%v)", code, body)
	}
	// The signal satisfies the only step, so the workflow completes.
	st := waitForStatus(t, h.store, "gate-1", state.StatusCompleted)
	if string(st.Output) != "yes" {
		t.Errorf("output = %q, want yes", st.Output)
	}
}

// TestDashboardServed proves the embedded assets are actually compiled in and
// that unknown paths fall back to the SPA shell instead of 404ing.
func TestDashboardServed(t *testing.T) {
	h := newAPIHarness(t)

	for _, path := range []string{"/", "/styles.css", "/app.js", "/some/deep/route"} {
		res, err := http.Get(h.srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		body, _ := io.ReadAll(res.Body)
		res.Body.Close()
		if res.StatusCode != http.StatusOK {
			t.Errorf("GET %s: got %d, want 200", path, res.StatusCode)
		}
		if len(body) == 0 {
			t.Errorf("GET %s: empty body", path)
		}
	}

	// The API must still win over the dashboard's catch-all route.
	code, body := h.do(t, "GET", "/api/health", "")
	if code != http.StatusOK || body["status"] != "ok" {
		t.Errorf("health: %d %v", code, body)
	}
}
