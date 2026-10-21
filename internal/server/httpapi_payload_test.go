package server_test

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"testing"
)

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// TestHTTPProtocolBinaryPayloadsMarkersRetries drives one workflow through the
// HTTP workflow-task protocol the way an SDK does: binary payloads in both
// directions, a version marker, an SDK-chosen retry policy and a heartbeat.
func TestHTTPProtocolBinaryPayloadsMarkersRetries(t *testing.T) {
	h := newAPIHarness(t)
	mustOK := func(code int, body map[string]any) map[string]any {
		t.Helper()
		if code != http.StatusOK && code != http.StatusCreated {
			t.Fatalf("HTTP %d %v", code, body)
		}
		return body
	}
	mustOK(h.do(t, "POST", "/api/workflow-types", `{"workflow_types":["RemoteBin"],"worker_id":"w"}`))
	start := mustOK(h.do(t, "POST", "/api/workflows",
		fmt.Sprintf(`{"type":"RemoteBin","input_base64":%q}`, b64("\xff\x00in"))))
	id := start["workflow_id"].(string)

	task := mustOK(h.do(t, "GET", "/api/workflow-tasks?types=RemoteBin", ""))
	if got := task["input_base64"]; got != b64("\xff\x00in") {
		t.Fatalf("task input_base64 = %v, want the exact input bytes", got)
	}
	complete := func(task map[string]any, cmd string) (int, map[string]any) {
		return h.do(t, "POST", "/api/workflow-tasks/complete",
			fmt.Sprintf(`{"workflow_id":%q,"task_token":%q,"command":%s}`, id, task["task_token"], cmd))
	}
	mustOK(complete(task, `{"type":"record_marker","step_name":"change-1","sequence":1,"marker":"version","data":"2"}`))

	task = mustOK(h.do(t, "GET", "/api/workflow-tasks?types=RemoteBin", ""))
	step := task["steps"].([]any)[0].(map[string]any)
	if step["kind"] != "MARKER" || step["marker"] != "version" || step["output"] != "2" {
		t.Fatalf("marker step = %v, want MARKER/version/2", step)
	}
	if code, body := complete(task, `{"type":"schedule_activity","step_name":"bin","sequence":2,"input_base64":"","retry_policy":{"max_attempts":0}}`); code != http.StatusBadRequest {
		t.Fatalf("invalid retry policy: HTTP %d %v, want 400", code, body)
	}
	mustOK(complete(task, fmt.Sprintf(`{"type":"schedule_activity","step_name":"bin","sequence":2,"input_base64":%q,"retry_policy":{"max_attempts":2,"initial_backoff_ms":1}}`, b64("\xfe"))))

	act := mustOK(h.do(t, "GET", "/api/activity-tasks?activities=bin", ""))
	if act["input_base64"] != b64("\xfe") {
		t.Fatalf("activity input_base64 = %v", act["input_base64"])
	}
	beat := fmt.Sprintf(`{"workflow_id":%q,"lease_token":%q,"worker_id":"w"}`, id, act["lease_token"])
	mustOK(h.do(t, "POST", "/api/activity-tasks/heartbeat", beat))
	mustOK(h.do(t, "POST", "/api/activity-tasks/complete", fmt.Sprintf(
		`{"workflow_id":%q,"step_name":"bin","sequence":2,"lease_token":%q,"error":"boom"}`, id, act["lease_token"])))

	act2 := mustOK(h.do(t, "GET", "/api/activity-tasks?activities=bin", ""))
	if code, _ := h.do(t, "POST", "/api/activity-tasks/heartbeat", beat); code != http.StatusConflict {
		t.Errorf("heartbeat with a spent lease: HTTP %d, want 409", code)
	}
	mustOK(h.do(t, "POST", "/api/activity-tasks/complete", fmt.Sprintf(
		`{"workflow_id":%q,"step_name":"bin","sequence":2,"lease_token":%q,"output_base64":%q}`, id, act2["lease_token"], b64("\x80out"))))

	task = mustOK(h.do(t, "GET", "/api/workflow-tasks?types=RemoteBin", ""))
	step = task["steps"].([]any)[1].(map[string]any)
	if step["status"] != "COMPLETED" || step["attempts"] != float64(2) || step["output_base64"] != b64("\x80out") {
		t.Fatalf("activity step = %v, want COMPLETED after 2 attempts with the exact output", step)
	}
	mustOK(complete(task, fmt.Sprintf(`{"type":"complete_workflow","output_base64":%q}`, b64("\x81done"))))

	_, wf := h.do(t, "GET", "/api/workflows/"+id, "")
	if wf["status"] != "COMPLETED" || wf["output_base64"] != b64("\x81done") || wf["input_base64"] != b64("\xff\x00in") {
		t.Fatalf("workflow = %v", wf)
	}
	if code, _ := h.do(t, "POST", "/api/workflows", `{"type":"RemoteBin","input_base64":"!!"}`); code != http.StatusBadRequest {
		t.Errorf("bad base64: HTTP %d, want 400", code)
	}
}
