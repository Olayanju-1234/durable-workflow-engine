package server_test

import (
	"net/http"
	"testing"

	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

func TestHTTPResetWorkflow(t *testing.T) {
	h := newAPIHarness(t, state.WorkflowDefinition{Type: "Flow", Steps: state.Activities("one", "two")})
	if code, body := h.do(t, "POST", "/api/workflows", `{"type":"Flow","workflow_id":"r-1"}`); code != http.StatusCreated {
		t.Fatalf("start: %d %v", code, body)
	}
	if code, _ := h.do(t, "POST", "/api/workflows/r-1/reset", ``); code != http.StatusConflict {
		t.Fatalf("reset of a live run: %d, want 409", code)
	}
	if code, _ := h.do(t, "POST", "/api/workflows/nobody/reset", ``); code != http.StatusNotFound {
		t.Fatalf("reset of an unknown id: %d, want 404", code)
	}
	h.do(t, "POST", "/api/workflows/r-1/terminate", `{"reason":"stuck"}`)
	_, body := h.do(t, "GET", "/api/workflows/r-1", "")
	point, _ := body["reset_point"].(map[string]any)
	if point["event_id"] != float64(2) || point["step_name"] != "one" || point["sequence"] != float64(1) {
		t.Fatalf("reset_point = %v, want step one at event 2", body["reset_point"])
	}
	oldRun := body["run_id"]
	if code, _ := h.do(t, "POST", "/api/workflows/r-1/reset", `{"event_id":1}`); code != http.StatusBadRequest {
		t.Fatalf("reset to event 1: %d, want 400", code)
	}
	code, body := h.do(t, "POST", "/api/workflows/r-1/reset", `{"reason":"try again"}`)
	if code != http.StatusOK || body["previous_run_id"] != oldRun || body["reset_event_id"] != float64(2) || body["run_id"] == oldRun {
		t.Fatalf("reset: %d %v", code, body)
	}
	_, body = h.do(t, "GET", "/api/workflows/r-1", "")
	if body["status"] != "WAITING" || body["reset_from_run_id"] != oldRun || body["reset_point"] != nil {
		t.Fatalf("after reset: %v", body)
	}
	_, body = h.do(t, "GET", "/api/workflows/r-1/runs", "")
	if runs := body["runs"].([]any); len(runs) != 2 {
		t.Fatalf("runs = %v", runs)
	}
}

func TestHTTPMetricsSummary(t *testing.T) {
	h := newAPIHarness(t, state.WorkflowDefinition{Type: "Flow", Steps: state.Activities("one")})
	h.do(t, "POST", "/api/workflows", `{"type":"Flow"}`)
	code, body := h.do(t, "GET", "/api/metrics?window=5m", "")
	if code != http.StatusOK || body["namespace"] != "default" || body["window_seconds"] != float64(300) {
		t.Fatalf("metrics: %d %v", code, body)
	}
	if now := body["now"].(map[string]any); now["activity_queue"] != float64(1) {
		t.Fatalf("now = %v, want the started workflow's queued task", now)
	}
	for _, bad := range []string{"30s", "2h", "soon"} {
		if code, _ := h.do(t, "GET", "/api/metrics?window="+bad, ""); code != http.StatusBadRequest {
			t.Errorf("window=%s: %d, want 400", bad, code)
		}
	}
}
