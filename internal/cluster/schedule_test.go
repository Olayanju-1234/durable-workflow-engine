package cluster

import (
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// TestClusterSchedulesFireOnTheLeaderAcrossFailover: a schedule is replicated
// like any history, only the leader fires it, and after the leader dies the
// next one keeps firing without repeating a fire.
func TestClusterSchedulesFireOnTheLeaderAcrossFailover(t *testing.T) {
	tc := newTestCluster(t, 3, 0)
	old, e := tc.leader()
	if err := e.CreateSchedule(&server.Schedule{ScheduleId: "tick", Cron: "@every 1s", WorkflowType: "Three"}); err != nil {
		t.Fatal(err)
	}
	runs := func(e *server.Engine) map[string]bool {
		all, err := e.ListWorkflows(0)
		if err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, s := range all {
			if strings.HasPrefix(s.GetWorkflowId(), "tick-") {
				out[s.GetWorkflowId()] = true
			}
		}
		return out
	}
	deadline := time.Now().Add(5 * time.Second)
	for len(runs(e)) < 2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	before := runs(e)
	if len(before) < 2 {
		t.Fatalf("leader started %d runs, want 2", len(before))
	}
	tc.waitIdentical("__workflowd-schedules", 2) // started + the upsert, on every node

	tc.stop(old.id)
	_, e2 := tc.leader()
	deadline = time.Now().Add(6 * time.Second)
	for len(runs(e2)) < len(before)+2 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	after := runs(e2)
	if len(after) < len(before)+2 {
		t.Fatalf("after failover: %d runs, had %d before; the new leader is not firing", len(after), len(before))
	}
	for id := range before {
		if !after[id] {
			t.Fatalf("run %s from the old leader is missing", id)
		}
	}
	scheds, err := e2.ListSchedules()
	if err != nil || len(scheds) != 1 {
		t.Fatalf("new leader's schedules = %v, %v", scheds, err)
	}
}
