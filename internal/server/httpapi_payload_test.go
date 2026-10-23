package server_test

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
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

func TestHTTPSchedules(t *testing.T) {
	h := newAPIHarness(t)
	if code, body := h.do(t, "POST", "/api/schedules", `{"schedule_id":"s","cron":"nonsense","workflow_type":"T"}`); code != http.StatusBadRequest {
		t.Fatalf("bad cron: HTTP %d %v", code, body)
	}
	if code, body := h.do(t, "POST", "/api/schedules", `{"schedule_id":"s","cron":"0 9 * * MON-FRI","timezone":"Europe/London","workflow_type":"T","input":"x"}`); code != http.StatusCreated {
		t.Fatalf("create: HTTP %d %v", code, body)
	}
	_, body := h.do(t, "GET", "/api/schedules", "")
	list := body["schedules"].([]any)
	if len(list) != 1 || list[0].(map[string]any)["next_fire_at"] == "" || list[0].(map[string]any)["timezone"] != "Europe/London" {
		t.Fatalf("list: %v", body)
	}
	if code, _ := h.do(t, "DELETE", "/api/schedules/s", ""); code != http.StatusOK {
		t.Fatalf("delete: HTTP %d", code)
	}
	if code, _ := h.do(t, "DELETE", "/api/schedules/s", ""); code != http.StatusNotFound {
		t.Fatalf("delete again: HTTP %d, want 404", code)
	}
}

// TestHTTPBatchCommands: an SDK returns several commands at once and every
// activity is dispatched before any completes.
func TestHTTPBatchCommands(t *testing.T) {
	h := newAPIHarness(t)
	h.do(t, "POST", "/api/workflow-types", `{"workflow_types":["Fan"],"worker_id":"w"}`)
	_, start := h.do(t, "POST", "/api/workflows", `{"type":"Fan"}`)
	id := start["workflow_id"].(string)
	_, task := h.do(t, "GET", "/api/workflow-tasks?types=Fan", "")
	both := fmt.Sprintf(`{"workflow_id":%q,"task_token":%q,"command":{"type":"noop"},"commands":[{"type":"noop"}]}`, id, task["task_token"])
	if code, _ := h.do(t, "POST", "/api/workflow-tasks/complete", both); code != http.StatusBadRequest {
		t.Fatalf("command and commands together: HTTP %d, want 400", code)
	}
	batch := fmt.Sprintf(`{"workflow_id":%q,"task_token":%q,"commands":[
		{"type":"schedule_activity","step_name":"x","sequence":1,"input":"1"},
		{"type":"schedule_activity","step_name":"y","sequence":2,"input":"2"}]}`, id, task["task_token"])
	if code, body := h.do(t, "POST", "/api/workflow-tasks/complete", batch); code != http.StatusOK {
		t.Fatalf("batch: HTTP %d %v", code, body)
	}
	_, x := h.do(t, "GET", "/api/activity-tasks?activities=x", "")
	_, y := h.do(t, "GET", "/api/activity-tasks?activities=y", "")
	if x["input"] != "1" || y["input"] != "2" {
		t.Fatalf("activity tasks %v %v; want both dispatched at once", x, y)
	}
	h.do(t, "POST", "/api/activity-tasks/complete", fmt.Sprintf(`{"workflow_id":%q,"step_name":"y","sequence":2,"lease_token":%q,"output":"Y"}`, id, y["lease_token"]))
	_, task = h.do(t, "GET", "/api/workflow-tasks?types=Fan", "")
	steps := task["steps"].([]any)
	if steps[0].(map[string]any)["status"] != "SCHEDULED" || steps[1].(map[string]any)["resolved_event_id"] == nil {
		t.Fatalf("steps %v: x still in flight, y resolved with its event id", steps)
	}
}

func TestHTTPSignalRequestID(t *testing.T) {
	h := newAPIHarness(t, state.WorkflowDefinition{Type: "Wait", Steps: []state.StepDef{state.AwaitSignal("w", "never")}})
	_, start := h.do(t, "POST", "/api/workflows", `{"type":"Wait"}`)
	id := start["workflow_id"].(string)
	for range 3 {
		if code, body := h.do(t, "POST", "/api/workflows/"+id+"/signal", `{"name":"noise","request_id":"r1"}`); code != http.StatusOK {
			t.Fatalf("HTTP %d %v", code, body)
		}
	}
	evs, _ := h.store.ReadAll(id)
	n := 0
	for _, ev := range evs {
		if ev.GetSignalReceived() != nil {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("%d signals recorded for one request id, want 1", n)
	}
}

// TestHTTPTimeouts: execution_timeout_ms on start, activity timeouts on
// schedule_activity, and the attempt's limit on the activity task.
func TestHTTPTimeouts(t *testing.T) {
	h := newAPIHarness(t)
	if code, body := h.do(t, "POST", "/api/workflow-types", `{"workflow_types":["RemoteT"],"worker_id":"w"}`); code != http.StatusOK {
		t.Fatalf("register: HTTP %d %v", code, body)
	}
	if code, _ := h.do(t, "POST", "/api/workflows", `{"type":"RemoteT","execution_timeout_ms":-5}`); code != http.StatusBadRequest {
		t.Fatalf("negative execution timeout: HTTP %d, want 400", code)
	}
	code, start := h.do(t, "POST", "/api/workflows", `{"type":"RemoteT","execution_timeout_ms":400}`)
	if code != http.StatusCreated {
		t.Fatalf("start: HTTP %d %v", code, start)
	}
	id := start["workflow_id"].(string)
	_, task := h.do(t, "GET", "/api/workflow-tasks?types=RemoteT", "")
	code, body := h.do(t, "POST", "/api/workflow-tasks/complete", fmt.Sprintf(
		`{"workflow_id":%q,"task_token":%q,"command":{"type":"schedule_activity","step_name":"slow","sequence":1,"start_to_close_timeout_ms":5000,"schedule_to_close_timeout_ms":60000}}`,
		id, task["task_token"]))
	if code != http.StatusOK {
		t.Fatalf("complete: HTTP %d %v", code, body)
	}
	_, act := h.do(t, "GET", "/api/activity-tasks?activities=slow", "")
	if act["start_to_close_timeout_ms"] != float64(5000) {
		t.Fatalf("activity task %v, want start_to_close_timeout_ms 5000", act)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		_, wf := h.do(t, "GET", "/api/workflows/"+id, "")
		if wf["status"] == "TIMED_OUT" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("workflow %v, want TIMED_OUT after 400ms", wf)
		}
		time.Sleep(20 * time.Millisecond)
	}
	beat := fmt.Sprintf(`{"workflow_id":%q,"lease_token":%q,"worker_id":"w"}`, id, act["lease_token"])
	if code, _ := h.do(t, "POST", "/api/activity-tasks/heartbeat", beat); code != http.StatusConflict {
		t.Fatalf("heartbeat after the workflow timed out: HTTP %d, want 409", code)
	}
}

// TestHTTPCancellation: POST /cancel ends a workflow as CANCELLED once; a
// cancel_step command closes a step; a worker turned away for a cancelled
// step gets a 409 that says "cancelled".
func TestHTTPCancellation(t *testing.T) {
	h := newAPIHarness(t)
	if code, body := h.do(t, "POST", "/api/workflow-types", `{"workflow_types":["RemoteC"],"worker_id":"w"}`); code != http.StatusOK {
		t.Fatalf("register: HTTP %d %v", code, body)
	}
	start := func() string {
		code, body := h.do(t, "POST", "/api/workflows", `{"type":"RemoteC"}`)
		if code != http.StatusCreated {
			t.Fatalf("start: HTTP %d %v", code, body)
		}
		return body["workflow_id"].(string)
	}
	complete := func(id string, cmds string) {
		t.Helper()
		_, task := h.do(t, "GET", "/api/workflow-tasks?types=RemoteC", "")
		if code, body := h.do(t, "POST", "/api/workflow-tasks/complete",
			fmt.Sprintf(`{"workflow_id":%q,"task_token":%q,"commands":%s}`, id, task["task_token"], cmds)); code != http.StatusOK {
			t.Fatalf("complete %s: HTTP %d %v", cmds, code, body)
		}
	}

	id := start()
	complete(id, `[{"type":"schedule_activity","step_name":"work","sequence":1},{"type":"await_signal","step_name":"gate","sequence":2,"signal_name":"go"}]`)
	_, act := h.do(t, "GET", "/api/activity-tasks?activities=work", "")
	if code, body := h.do(t, "POST", "/api/workflows/"+id+"/signal", `{"name":"go"}`); code != http.StatusOK {
		t.Fatalf("signal: HTTP %d %v", code, body)
	}
	complete(id, `[{"type":"cancel_step","step_name":"work","sequence":1,"reason":"lost the race"},{"type":"start_timer","step_name":"t","sequence":3,"delay_ms":60000}]`)
	beat := fmt.Sprintf(`{"workflow_id":%q,"lease_token":%q,"worker_id":"w"}`, id, act["lease_token"])
	if code, body := h.do(t, "POST", "/api/activity-tasks/heartbeat", beat); code != http.StatusConflict || body["cancelled"] != true {
		t.Fatalf("heartbeat for a cancelled step: HTTP %d %v, want 409 cancelled", code, body)
	}
	if code, body := h.do(t, "POST", "/api/workflows/"+id+"/cancel", `{"reason":"not needed"}`); code != http.StatusOK || body["status"] != "cancelled" {
		t.Fatalf("cancel: HTTP %d %v", code, body)
	}
	_, wf := h.do(t, "GET", "/api/workflows/"+id, "")
	steps := wf["steps"].([]any)
	if wf["status"] != "CANCELLED" || wf["error"] != "cancelled: not needed" ||
		steps[0].(map[string]any)["status"] != "CANCELLED" || steps[0].(map[string]any)["error"] != "cancelled: lost the race" ||
		steps[2].(map[string]any)["status"] != "CANCELLED" {
		t.Fatalf("workflow %v", wf)
	}
	if code, _ := h.do(t, "POST", "/api/workflows/"+id+"/cancel", ""); code != http.StatusConflict {
		t.Fatalf("second cancel: HTTP %d, want 409", code)
	}
	if code, body := h.do(t, "GET", "/api/workflows?status=CANCELLED", ""); code != http.StatusOK || body["total"] != float64(1) {
		t.Fatalf("list CANCELLED: HTTP %d %v", code, body)
	}

	// A worker whose lease merely expired is told so, without "cancelled".
	other := start()
	complete(other, `[{"type":"schedule_activity","step_name":"work","sequence":1}]`)
	lost := fmt.Sprintf(`{"workflow_id":%q,"lease_token":"not-a-token","worker_id":"w"}`, other)
	if code, body := h.do(t, "POST", "/api/activity-tasks/heartbeat", lost); code != http.StatusConflict || body["cancelled"] != false {
		t.Fatalf("heartbeat with an unknown token: HTTP %d %v, want 409 not cancelled", code, body)
	}
}

// TestHTTPNonRetryable: a failure's error_type and non_retryable reach the
// retry decision, the policy's non_retryable_error_types is honoured, and the
// step shows its error type.
func TestHTTPNonRetryable(t *testing.T) {
	h := newAPIHarness(t)
	if code, body := h.do(t, "POST", "/api/workflow-types", `{"workflow_types":["RemoteN"],"worker_id":"w"}`); code != http.StatusOK {
		t.Fatalf("register: HTTP %d %v", code, body)
	}
	_, start := h.do(t, "POST", "/api/workflows", `{"type":"RemoteN"}`)
	id := start["workflow_id"].(string)
	_, task := h.do(t, "GET", "/api/workflow-tasks?types=RemoteN", "")
	code, body := h.do(t, "POST", "/api/workflow-tasks/complete", fmt.Sprintf(
		`{"workflow_id":%q,"task_token":%q,"commands":[`+
			`{"type":"schedule_activity","step_name":"a","sequence":1,"retry_policy":{"max_attempts":5,"non_retryable_error_types":["BadRequest"]}},`+
			`{"type":"schedule_activity","step_name":"b","sequence":2,"retry_policy":{"max_attempts":5}}]}`,
		id, task["task_token"]))
	if code != http.StatusOK {
		t.Fatalf("complete: HTTP %d %v", code, body)
	}
	fail := func(step, extra string) {
		t.Helper()
		_, act := h.do(t, "GET", "/api/activity-tasks?activities="+step, "")
		if code, body := h.do(t, "POST", "/api/activity-tasks/complete", fmt.Sprintf(
			`{"workflow_id":%q,"step_name":%q,"sequence":%v,"lease_token":%q,"error":"nope"%s}`,
			id, step, act["sequence"], act["lease_token"], extra)); code != http.StatusOK {
			t.Fatalf("fail %s: HTTP %d %v", step, code, body)
		}
	}
	fail("a", `,"error_type":"BadRequest"`)
	fail("b", `,"error_type":"Whatever","non_retryable":true`)
	_, task = h.do(t, "GET", "/api/workflow-tasks?types=RemoteN", "")
	for _, s := range task["steps"].([]any) {
		step := s.(map[string]any)
		if step["status"] != "FAILED" || step["attempts"] != float64(1) || step["error_type"] == nil {
			t.Fatalf("step %v, want FAILED after 1 attempt with its error type", step)
		}
	}
	_, wf := h.do(t, "GET", "/api/workflows/"+id, "")
	if steps := wf["steps"].([]any); steps[0].(map[string]any)["error_type"] != "BadRequest" {
		t.Fatalf("GET workflow steps %v, want error_type BadRequest", steps)
	}
}
