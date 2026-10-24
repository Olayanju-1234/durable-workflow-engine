package server_test

import (
	"context"
	"net/http"
	"testing"
	"time"
)

// Over HTTP: a start names its queue, workers poll by queue, and describe
// and the task say which queue they are on.
func TestHTTPTaskQueues(t *testing.T) {
	h := newNSHarness(t, "")
	if code, body := h.do(t, nsReq{"POST", "/api/workflows", "", "", `{"type":"Flow","workflow_id":"q1","task_queue":"billing"}`}); code != http.StatusCreated {
		t.Fatalf("start: %d %v", code, body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, "GET", h.srv.URL+"/api/activity-tasks?worker_id=w", nil)
	if res, err := http.DefaultClient.Do(req); err == nil {
		res.Body.Close()
		t.Fatalf("a default-queue poll answered HTTP %d with billing's task waiting", res.StatusCode)
	}
	code, task := h.do(t, nsReq{"GET", "/api/activity-tasks?task_queue=billing&activities=work", "", "", ""})
	if code != 200 || task["workflow_id"] != "q1" || task["task_queue"] != "billing" {
		t.Fatalf("billing poll: %d %v", code, task)
	}
	_, wf := h.do(t, nsReq{"GET", "/api/workflows/q1", "", "", ""})
	steps, _ := wf["steps"].([]any)
	if wf["task_queue"] != "billing" || len(steps) != 1 || steps[0].(map[string]any)["task_queue"] != "billing" {
		t.Fatalf("describe: %v", wf)
	}
	if code, _ := h.do(t, nsReq{"GET", "/api/activity-tasks?task_queue=bad%20queue", "", "", ""}); code != http.StatusBadRequest {
		t.Fatalf("poll on an invalid queue: HTTP %d, want 400", code)
	}
}
