package raft

import (
	"slices"
	"testing"
)

func installSnapshot(index, term uint64) *Envelope {
	return &Envelope{From: "n2", To: "n1", Body: &Envelope_InstallSnapshot{InstallSnapshot: &InstallSnapshotRequest{
		Term: 3, LeaderId: "n2",
		Meta: &SnapshotMeta{LastIncludedIndex: index, LastIncludedTerm: term},
		Data: []byte("state"),
	}}}
}

// drain plays a trivial driver: hand out every Ready and acknowledge it.
func drain(c *core) {
	for c.hasReady() {
		c.advance(c.ready())
	}
}

// A duplicated InstallSnapshot can arrive long after the follower moved past
// it. Installing it again would throw away entries the follower already
// acknowledged, which the leader may have counted towards a commit.
func TestStaleSnapshotIgnored(t *testing.T) {
	c := newTestCore(t, "n1", 3)
	c.step(installSnapshot(5, 2))
	drain(c)
	c.step(&Envelope{From: "n2", To: "n1", Body: &Envelope_AppendEntries{AppendEntries: &AppendEntriesRequest{
		Term: 3, LeaderId: "n2", PrevLogIndex: 5, PrevLogTerm: 2, LeaderCommit: 8,
		Entries: []*LogEntry{{Index: 6, Term: 3}, {Index: 7, Term: 3}, {Index: 8, Term: 3}},
	}}})
	drain(c)

	c.step(installSnapshot(5, 2)) // the duplicate
	if c.pendingInstall != nil || c.log.lastIndex() != 8 || c.log.committed != 8 {
		t.Fatalf("stale snapshot reinstalled: last %d, commit %d, pending %v", c.log.lastIndex(), c.log.committed, c.pendingInstall)
	}
}

// A follower that already holds the snapshot's last entry keeps the entries
// after it and just learns the new commit point (paper Figure 13, step 6).
func TestSnapshotKeepsMatchingSuffix(t *testing.T) {
	c := newTestCore(t, "n1", 3, logFromTerms("", []uint64{1, 1, 1, 1, 1, 1, 1, 1})...)
	c.step(installSnapshot(5, 1))
	if c.pendingInstall != nil || c.log.lastIndex() != 8 || c.log.committed != 5 {
		t.Fatalf("matching snapshot: last %d, commit %d, pending %v; want 8, 5, none", c.log.lastIndex(), c.log.committed, c.pendingInstall)
	}
}

// A follower whose log disagrees with the snapshot discards all of it.
func TestSnapshotReplacesConflictingLog(t *testing.T) {
	c := newTestCore(t, "n1", 3, logFromTerms("", []uint64{1, 1, 1, 1, 1, 1, 1, 1})...)
	c.step(installSnapshot(6, 2))
	rd := c.ready()
	if rd.Snapshot == nil || rd.Snapshot.Index != 6 {
		t.Fatalf("Ready.Snapshot = %v, want the leader's snapshot at 6", rd.Snapshot)
	}
	if c.log.lastIndex() != 6 || len(c.log.entries) != 0 || c.log.committed != 6 {
		t.Fatalf("after install: last %d, %d entries, commit %d; want an empty log after 6", c.log.lastIndex(), len(c.log.entries), c.log.committed)
	}
	if len(rd.CommittedEntries) != 0 {
		t.Fatalf("entries the snapshot replaces were also handed out to apply: %v", rd.CommittedEntries)
	}
}

// A follower that was down while the leader compacted its log can only catch
// up from the leader's snapshot. It must end with exactly the leader's state.
func TestLaggingFollowerCatchesUpFromSnapshot(t *testing.T) {
	s := newSimCluster(t, 3, 40)
	lead := electLeader(t, s)
	lagger := s.others(lead)[0]
	s.crash(lagger)

	want := payloads("cmd", 50)
	for _, p := range want[:40] {
		s.propose(lead, p)
	}
	s.run(5)
	s.snapshot(lead)
	if s.cores[lead].log.offset == 0 {
		t.Fatal("setup: the leader did not compact its log")
	}
	for _, p := range want[40:] {
		s.propose(lead, p)
	}

	s.restart(lagger)
	s.runUntil(100, "the lagging follower to catch up", func() bool { return s.allApplied(want) })
	if s.disk[lagger].snapshot == nil {
		t.Fatal("the follower caught up without installing a snapshot")
	}
	s.checker.assertClean(t)
}

// A node restarts from its own snapshot: the state machine comes back from the
// snapshot, and only entries after it are applied again.
func TestRestartFromOwnSnapshot(t *testing.T) {
	s := newSimCluster(t, 3, 41)
	lead := electLeader(t, s)
	want := payloads("cmd", 20)
	for _, p := range want {
		s.propose(lead, p)
	}
	s.runUntil(50, "every node to apply", func() bool { return s.allApplied(want) })
	node := s.others(lead)[0]
	s.snapshot(node)
	snapAt := s.disk[node].snapshot.Index

	s.crash(node)
	s.restart(node)
	if got := s.appliedData(node); !slices.Equal(got, want) {
		t.Fatalf("after restart from a snapshot at %d: applied %v, want %v", snapAt, got, want)
	}
	if c := s.cores[node]; c.log.offset != snapAt || c.log.committed != snapAt {
		t.Fatalf("restarted log starts at %d with commit %d, want both %d", c.log.offset, c.log.committed, snapAt)
	}
	s.propose(lead, "after")
	s.runUntil(50, "the restarted node to apply new entries", func() bool { return s.allApplied(append(want, "after")) })
	s.checker.assertClean(t)
}
