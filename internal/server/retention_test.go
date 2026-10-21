package server

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// TestPurgeTerminated proves retention deletes finished workflows past the
// cutoff (completed and terminated alike) and never touches a live one.
func TestPurgeTerminated(t *testing.T) {
	e := newTestEngine(t, t.TempDir(),
		state.WorkflowDefinition{Type: "Gated", Steps: []state.StepDef{state.AwaitSignal("gate", "go")}},
		state.WorkflowDefinition{Type: "Work", Steps: state.Activities("do")},
	)
	mustStart := func(typ, id string) {
		t.Helper()
		if _, err := e.StartWorkflow(typ, nil, id); err != nil {
			t.Fatalf("start %s: %v", id, err)
		}
	}
	mustStart("Gated", "done")
	if err := e.SignalWorkflow("done", "go", []byte("ok")); err != nil {
		t.Fatal(err)
	}
	mustStart("Work", "killed")
	if err := e.TerminateWorkflow("killed", "test"); err != nil {
		t.Fatal(err)
	}
	mustStart("Work", "live") // WAITING on "do" with no worker

	// Nothing is older than an hour ago.
	if n, err := e.PurgeTerminated(time.Now().Add(-time.Hour)); err != nil || n != 0 {
		t.Fatalf("purge with past cutoff = %d, %v; want 0", n, err)
	}

	n, err := e.PurgeTerminated(time.Now().Add(time.Hour))
	if err != nil || n != 2 {
		t.Fatalf("purge with future cutoff = %d, %v; want 2", n, err)
	}
	for _, id := range []string{"done", "killed"} {
		if _, err := e.GetWorkflow(id); !errors.Is(err, ErrNotFound) {
			t.Errorf("GetWorkflow(%s) after purge: err = %v, want ErrNotFound", id, err)
		}
	}
	resp, err := e.GetWorkflow("live")
	if err != nil || resp.GetSummary().GetStatus() != WorkflowStatus_WORKFLOW_STATUS_WAITING {
		t.Fatalf("live workflow after purge = %v, %v; want WAITING", resp.GetSummary().GetStatus(), err)
	}

	if list, err := e.ListWorkflows(WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED); err != nil || len(list) != 1 {
		t.Errorf("ListWorkflows after purge = %d rows, %v; want only the live one", len(list), err)
	}

	var metricsOut strings.Builder
	e.Metrics().WriteText(&metricsOut)
	if !strings.Contains(metricsOut.String(), "workflowd_workflows_purged_total 2") {
		t.Errorf("purge counter not at 2:\n%s", metricsOut.String())
	}
}
