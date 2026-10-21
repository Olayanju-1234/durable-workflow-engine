package server_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// authHarness is an API harness with bearer-token auth enabled, wrapped exactly
// as runServe assembles it (Handler + dashboard + Wrap).
type authHarness struct {
	srv   *httptest.Server
	store *history.HistoryStore
}

func newAuthHarness(t *testing.T, token string, extra ...server.HTTPOption) *authHarness {
	t.Helper()

	store, err := history.NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := server.NewEngine(store, logger)
	t.Cleanup(engine.Close)
	engine.Register(state.WorkflowDefinition{Type: "Flow", Steps: state.Activities("one")})

	api := server.NewHTTPAPI(engine, logger, append([]server.HTTPOption{server.WithAuthToken(token)}, extra...)...)
	mux := api.Handler()
	mux.Handle("/", webStub{})
	srv := httptest.NewServer(api.Wrap(mux))
	t.Cleanup(srv.Close)
	return &authHarness{srv: srv, store: store}
}

// webStub stands in for the embedded dashboard: any non-API path serves 200 HTML.
type webStub struct{}

func (webStub) ServeHTTP(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	_, _ = w.Write([]byte("<html>dashboard</html>"))
}

func (h *authHarness) do(t *testing.T, method, path, token, body string) int {
	t.Helper()
	req, err := http.NewRequest(method, h.srv.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer res.Body.Close()
	return res.StatusCode
}

func TestHTTPAPIAuth(t *testing.T) {
	h := newAuthHarness(t, "sekrit")

	tests := []struct {
		name, method, path, token, body string
		want                            int
	}{
		{"health is open", "GET", "/api/health", "", "", http.StatusOK},
		{"dashboard is open", "GET", "/", "", "", http.StatusOK},
		{"start without token", "POST", "/api/workflows", "", `{"type":"Flow"}`, http.StatusUnauthorized},
		{"start with wrong token", "POST", "/api/workflows", "nope", `{"type":"Flow"}`, http.StatusUnauthorized},
		{"list without token", "GET", "/api/workflows", "", "", http.StatusUnauthorized},
		{"start with token", "POST", "/api/workflows", "sekrit", `{"type":"Flow","workflow_id":"a-1"}`, http.StatusCreated},
		{"signal with token", "POST", "/api/workflows/a-1/signal", "sekrit", `{"name":"x"}`, http.StatusOK},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := h.do(t, tc.method, tc.path, tc.token, tc.body); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

// With public reads, anyone can list and describe workflows; everything that
// changes state or leases work still needs the token.
func TestHTTPAPIAuthPublicReads(t *testing.T) {
	h := newAuthHarness(t, "sekrit", server.WithPublicReads())
	if got := h.do(t, "POST", "/api/workflows", "sekrit", `{"type":"Flow","workflow_id":"p-1"}`); got != http.StatusCreated {
		t.Fatalf("start with token: got %d", got)
	}
	tests := []struct {
		name, method, path, body string
		want                     int
	}{
		{"list is open", "GET", "/api/workflows", "", http.StatusOK},
		{"describe is open", "GET", "/api/workflows/p-1", "", http.StatusOK},
		{"start needs the token", "POST", "/api/workflows", `{"type":"Flow"}`, http.StatusUnauthorized},
		{"signal needs the token", "POST", "/api/workflows/p-1/signal", `{"name":"x"}`, http.StatusUnauthorized},
		{"terminate needs the token", "POST", "/api/workflows/p-1/terminate", `{}`, http.StatusUnauthorized},
		{"workflow-task poll needs the token", "GET", "/api/workflow-tasks?types=Flow", "", http.StatusUnauthorized},
		{"activity poll needs the token", "GET", "/api/activity-tasks", "", http.StatusUnauthorized},
		{"type registration needs the token", "POST", "/api/workflow-types", `{"workflow_types":["X"]}`, http.StatusUnauthorized},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := h.do(t, tc.method, tc.path, "", tc.body); got != tc.want {
				t.Errorf("got %d, want %d", got, tc.want)
			}
		})
	}
}

func TestBearerAuthGuardsMetrics(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("workflowd_up 1")) })
	open := httptest.NewServer(server.BearerAuth("", ok))
	t.Cleanup(open.Close)
	guarded := httptest.NewServer(server.BearerAuth("scrape", ok))
	t.Cleanup(guarded.Close)

	get := func(url, token string) int {
		t.Helper()
		req, _ := http.NewRequest("GET", url+"/metrics", nil)
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		res.Body.Close()
		return res.StatusCode
	}
	if got := get(open.URL, ""); got != http.StatusOK {
		t.Errorf("no token configured: got %d, want 200", got)
	}
	if got := get(guarded.URL, ""); got != http.StatusUnauthorized {
		t.Errorf("guarded, no token: got %d, want 401", got)
	}
	if got := get(guarded.URL, "wrong"); got != http.StatusUnauthorized {
		t.Errorf("guarded, wrong token: got %d, want 401", got)
	}
	if got := get(guarded.URL, "scrape"); got != http.StatusOK {
		t.Errorf("guarded, right token: got %d, want 200", got)
	}
}

func TestHTTPAPIAuthDisabled(t *testing.T) {
	h := newAuthHarness(t, "")
	if got := h.do(t, "POST", "/api/workflows", "", `{"type":"Flow","workflow_id":"b-1"}`); got != http.StatusCreated {
		t.Errorf("no token configured: got %d, want 201", got)
	}
}

func TestHTTPAPIStartDuplicate(t *testing.T) {
	h := newAuthHarness(t, "")
	body := `{"type":"Flow","workflow_id":"dup-1"}`
	if got := h.do(t, "POST", "/api/workflows", "", body); got != http.StatusCreated {
		t.Fatalf("first start: got %d, want 201", got)
	}
	if got := h.do(t, "POST", "/api/workflows", "", body); got != http.StatusConflict {
		t.Errorf("duplicate start: got %d, want 409", got)
	}
}

func TestHTTPAPIListPaginationAndSearch(t *testing.T) {
	h := newAuthHarness(t, "")

	start := func(id string) {
		t.Helper()
		if got := h.do(t, "POST", "/api/workflows", "", `{"type":"Flow","workflow_id":"`+id+`"}`); got != http.StatusCreated {
			t.Fatalf("start %s: got %d, want 201", id, got)
		}
	}
	start("alpha-1")
	start("alpha-2")
	start("beta-1")

	list := func(path string) map[string]any {
		t.Helper()
		req, err := http.NewRequest("GET", h.srv.URL+path, nil)
		if err != nil {
			t.Fatalf("new request: %v", err)
		}
		res, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer res.Body.Close()
		var body map[string]any
		if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		return body
	}

	all := list("/api/workflows")
	if all["total"].(float64) != 3 {
		t.Errorf("total = %v, want 3", all["total"])
	}

	page := list("/api/workflows?limit=2&offset=2")
	wfs := page["workflows"].([]any)
	if len(wfs) != 1 || page["total"].(float64) != 3 {
		t.Errorf("page = %v (n=%d), want 1 row with total 3", page, len(wfs))
	}

	hit := list("/api/workflows?q=beta")
	wfs = hit["workflows"].([]any)
	if hit["total"].(float64) != 1 || wfs[0].(map[string]any)["workflow_id"] != "beta-1" {
		t.Errorf("q=beta matched %v, want only beta-1", hit)
	}

	none := list("/api/workflows?q=zzz")
	if none["total"].(float64) != 0 || len(none["workflows"].([]any)) != 0 {
		t.Errorf("q=zzz matched %v, want nothing", none)
	}
}

func TestHTTPAPIHealthShape(t *testing.T) {
	h := newAuthHarness(t, "")
	req, _ := http.NewRequest("GET", h.srv.URL+"/api/health", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("health: %v", err)
	}
	defer res.Body.Close()
	var body map[string]any
	if err := json.NewDecoder(res.Body).Decode(&body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	for _, key := range []string{"status", "version", "uptime_seconds", "workflows_active", "task_queue_depth"} {
		if _, ok := body[key]; !ok {
			t.Errorf("health response missing %q: %v", key, body)
		}
	}
	if body["demo_worker"] != false {
		t.Errorf("demo_worker = %v, want false by default", body["demo_worker"])
	}
}
