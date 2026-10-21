package cluster

import "testing"

// TestClusterSignalRequestIDSurvivesLeaderChange: a signal's idempotency key
// is replicated with it, so the next leader recognises a retry the old leader
// had already recorded.
func TestClusterSignalRequestIDSurvivesLeaderChange(t *testing.T) {
	tc := newTestCluster(t, 3, 0)
	old, e := tc.leader()
	if _, err := e.StartWorkflow("Three", nil, "wf-sig"); err != nil {
		t.Fatal(err)
	}
	if err := e.SignalWorkflowOnce("wf-sig", "nudge", "once-1", []byte("x")); err != nil {
		t.Fatal(err)
	}
	tc.waitIdentical("wf-sig", 3) // started, a scheduled, the signal
	tc.stop(old.id)

	_, e2 := tc.leader()
	if err := e2.SignalWorkflowOnce("wf-sig", "nudge", "once-1", []byte("x")); err != nil {
		t.Fatalf("retry on the new leader: %v", err)
	}
	tc.start(old.id)
	evs := tc.waitIdentical("wf-sig", 3)
	signals := 0
	for _, ev := range evs {
		if ev.GetSignalReceived() != nil {
			signals++
		}
	}
	if signals != 1 {
		t.Fatalf("%d signals in the replicated history, want 1", signals)
	}
}
