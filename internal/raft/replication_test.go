package raft

import (
	"errors"
	"fmt"
	"slices"
	"testing"
)

func electLeader(t *testing.T, s *simCluster) string {
	t.Helper()
	s.runUntil(200, "a stable leader", func() bool { return s.stableLeader() != "" })
	return s.stableLeader()
}

// allApplied reports whether every up node applied exactly want, in order.
func (s *simCluster) allApplied(want []string) bool {
	for _, id := range s.ids {
		if !s.down[id] && !slices.Equal(s.appliedData(id), want) {
			return false
		}
	}
	return true
}

func payloads(prefix string, n int) []string {
	out := make([]string, n)
	for i := range out {
		out[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return out
}

func TestReplicatesAndCommits(t *testing.T) {
	s := newSimCluster(t, 3, 10)
	lead := electLeader(t, s)
	want := payloads("cmd", 10)
	for _, p := range want {
		s.propose(lead, p)
	}
	s.runUntil(50, "every node to apply all 10 entries", func() bool { return s.allApplied(want) })
	s.checker.assertClean(t)
}

// The leader's first act is an empty entry in its own term; committing it is
// how a new leader learns where the commit point is.
func TestLeaderOpensTermWithEmptyEntry(t *testing.T) {
	s := newSimCluster(t, 3, 11)
	lead := electLeader(t, s)
	c := s.cores[lead]
	first, _ := c.log.term(1)
	if c.log.lastIndex() != 1 || first != c.term || len(c.log.entries[0].GetData()) != 0 {
		t.Fatalf("leader log after election: %v, want one empty entry in term %d", c.log.entries, c.term)
	}
	s.runUntil(20, "the opening entry to commit", func() bool { return s.cores[lead].log.committed == 1 })
	if !s.cores[lead].status().LeaderReady() {
		t.Fatal("leader with its opening entry applied should report LeaderReady")
	}
}

func TestProposeOnlyOnLeaderOfThatTerm(t *testing.T) {
	s := newSimCluster(t, 3, 12)
	lead := electLeader(t, s)
	follower := s.others(lead)[0]
	if _, err := s.cores[follower].propose(s.cores[follower].term, []byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("propose on follower: %v, want ErrNotLeader", err)
	}
	if _, err := s.cores[lead].propose(s.cores[lead].term-1, []byte("x")); !errors.Is(err, ErrNotLeader) {
		t.Fatalf("propose for an older term: %v, want ErrNotLeader", err)
	}
}

// One follower down still leaves a majority (2 of 3), so writes commit; the
// follower then catches up, even across more entries than fit in one message.
func TestCommitWithFollowerDownThenCatchUp(t *testing.T) {
	s := newSimCluster(t, 3, 13)
	lead := electLeader(t, s)
	lagger := s.others(lead)[0]
	s.crash(lagger)

	want := payloads("cmd", 3*maxEntriesPerMsg+7)
	for _, p := range want {
		s.propose(lead, p)
	}
	s.runUntil(50, "commits on the remaining majority", func() bool {
		return s.cores[lead].log.committed == s.cores[lead].log.lastIndex()
	})

	s.restart(lagger)
	s.runUntil(100, "the restarted follower to catch up", func() bool { return s.allApplied(want) })
	s.checker.assertClean(t)
}

// A leader in the minority can append but never commit; once the partition
// heals, its uncommitted entries are overwritten by the majority's history.
func TestMinorityLeaderCannotCommit(t *testing.T) {
	s := newSimCluster(t, 5, 14)
	old := electLeader(t, s)
	s.runUntil(20, "the opening entry to commit", func() bool { return s.allApplied(nil) && s.cores[old].log.committed > 0 })
	others := s.others(old)
	minority := []string{old, others[0]}
	majority := others[1:]
	s.partition(minority, majority)

	committedBefore := s.cores[old].log.committed
	s.propose(old, "lost-write")
	s.run(3)
	if c := s.cores[old].log.committed; c != committedBefore {
		t.Fatalf("minority leader advanced commit %d -> %d", committedBefore, c)
	}

	s.runUntil(200, "a leader in the majority", func() bool { return s.stableLeader(majority...) != "" })
	lead := s.stableLeader(majority...)
	s.propose(lead, "kept-write")

	s.heal()
	s.runUntil(200, "convergence on the majority's history", func() bool {
		return s.stableLeader() != "" && s.allApplied([]string{"kept-write"})
	})
	for _, id := range s.ids {
		if slices.Contains(s.appliedData(id), "lost-write") {
			t.Fatalf("%s applied the minority leader's uncommitted write", id)
		}
	}
	s.checker.assertClean(t)
}

// figure7 rebuilds the logs of paper Figure 7: a leader about to start term 8
// and six followers whose logs are missing entries, hold extra uncommitted
// entries, or both.
var figure7 = map[string][]uint64{
	"leader": {1, 1, 1, 4, 4, 5, 5, 6, 6, 6},
	"a":      {1, 1, 1, 4, 4, 5, 5, 6, 6},
	"b":      {1, 1, 1, 4},
	"c":      {1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6},
	"d":      {1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7},
	"e":      {1, 1, 1, 4, 4, 4, 4},
	"f":      {1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3},
}

func logFromTerms(id string, terms []uint64) []*LogEntry {
	ents := make([]*LogEntry, len(terms))
	for i, t := range terms {
		// Same (index, term) means the same command, as Log Matching promises,
		// so payloads are derived from the pair, not from the node.
		ents[i] = &LogEntry{Index: uint64(i + 1), Term: t, Data: fmt.Appendf(nil, "%d@%d", t, i+1)}
	}
	return ents
}

func TestFigure7FollowersConverge(t *testing.T) {
	s := newSimCluster(t, 0, 15)
	for id := range figure7 {
		s.ids = append(s.ids, id)
	}
	slices.Sort(s.ids)
	for _, id := range s.ids {
		s.disk[id] = &simDisk{hardState: &HardState{CurrentTerm: 7}, entries: logFromTerms(id, figure7[id])}
		s.start(id)
	}

	// The leader campaigns for term 8. c and d refuse (their logs are more up
	// to date), but a, b, e and f make a majority of seven.
	s.cores["leader"].campaign()
	s.flush("leader")
	s.deliverAll()
	if s.cores["leader"].role != Leader || s.cores["leader"].term != 8 {
		t.Fatalf("setup: leader did not win term 8\n%s", s.dump())
	}
	s.deliverAll() // no ticks: replication and repair run on responses alone

	want := append(slices.Clone(figure7["leader"]), 8)
	for _, id := range s.ids {
		var got []uint64
		for _, e := range s.cores[id].log.entries {
			got = append(got, e.GetTerm())
		}
		if !slices.Equal(got, want) {
			t.Errorf("%s log terms %v, want %v", id, got, want)
		}
	}
	if c := s.cores["leader"].log.committed; c != 11 {
		t.Errorf("leader commit %d, want 11 (its term-8 entry)", c)
	}
	s.checker.assertClean(t)
}

// A delayed AppendEntries carrying a prefix the follower already has must not
// truncate the newer entries it received since.
func TestStaleAppendDoesNotTruncate(t *testing.T) {
	c := newTestCore(t, "n1", 2)
	ents := logFromTerms("", []uint64{2, 2, 2, 2, 2})
	appendReq := func(prev uint64, es []*LogEntry) *Envelope {
		return &Envelope{From: "n2", To: "n1", Body: &Envelope_AppendEntries{AppendEntries: &AppendEntriesRequest{
			Term: 2, LeaderId: "n2", PrevLogIndex: prev, PrevLogTerm: 2 * min(prev, 1), Entries: es,
		}}}
	}
	c.step(appendReq(0, ents))
	c.step(appendReq(0, ents[:3])) // the older, shorter request arrives late
	if got := c.log.lastIndex(); got != 5 {
		t.Fatalf("follower log shrank to %d entries after a stale request", got)
	}
}

func TestConflictHintSkipsWholeTerm(t *testing.T) {
	l := newRaftLog(logFromTerms("", figure7["f"]))
	tests := []struct {
		prev, want uint64
	}{
		{prev: 10, want: 7},  // term 3 runs 7..11: retry from its first index
		{prev: 5, want: 4},   // term 2 runs 4..6
		{prev: 20, want: 12}, // past our end: one after our last entry
	}
	for _, tt := range tests {
		if got := l.conflictHint(tt.prev); got != tt.want {
			t.Errorf("conflictHint(%d) = %d, want %d", tt.prev, got, tt.want)
		}
	}
}

// Committed entries live in every node's log on disk. After the whole cluster
// restarts, each node rebuilds its state machine by applying them again.
func TestFullRestartReplaysCommittedEntries(t *testing.T) {
	s := newSimCluster(t, 3, 16)
	lead := electLeader(t, s)
	want := payloads("cmd", 20)
	for _, p := range want {
		s.propose(lead, p)
	}
	s.runUntil(50, "every node to apply", func() bool { return s.allApplied(want) })

	for _, id := range s.ids {
		s.crash(id)
	}
	for _, id := range s.ids {
		s.restart(id)
		if got := s.appliedData(id); len(got) != 0 {
			t.Fatalf("%s applied %v before learning the commit index", id, got)
		}
	}
	s.runUntil(200, "every node to re-apply after restart", func() bool { return s.allApplied(want) })
	s.checker.assertClean(t)
}

// forgetLeader makes a follower behave as if its leader had been silent for the
// minimum election timeout, so it will consider vote requests again.
func (s *simCluster) forgetLeader(ids ...string) {
	for _, id := range ids {
		s.cores[id].electionElapsed = s.electionTick
	}
}

// elect runs an election for id with exactly the given voters, one message at
// a time, then throws away everything else in flight. It retries in a new term
// if the voters had already voted in the first one.
func (s *simCluster) elect(id string, voters ...string) {
	s.t.Helper()
	for range 3 {
		s.forgetLeader(voters...)
		s.cores[id].campaign()
		s.flush(id)
		for _, v := range voters {
			s.deliverOne(id, v)
			s.deliverOne(v, id)
		}
		s.inflight = nil
		if s.cores[id].role == Leader {
			return
		}
	}
	s.t.Fatalf("%s could not win an election with %v\n%s", id, voters, s.dump())
}

// Paper Figure 8: an entry from an older term can sit on a majority and still
// be overwritten by a later leader that never saw it. So a leader must not
// count replicas to commit an older-term entry. Here n1 gets its term-1 entry
// onto a majority while leading term 3; committing it then would let n5 erase
// an applied entry in term 4.
func TestFigure8OldTermEntryNotCommittedByCount(t *testing.T) {
	s := newSimCluster(t, 5, 17)
	n1, n2, n3, n4, n5 := s.ids[0], s.ids[1], s.ids[2], s.ids[3], s.ids[4]

	// (a) n1 leads term 1; its opening entry (index 1) reaches everyone. Then
	// it gets entry X (index 2) onto n2 only. X is over the per-message byte
	// limit, so later X travels alone in its message.
	s.elect(n1, n2, n3, n4, n5)
	s.cores[n1].broadcastAppend()
	s.flush(n1)
	s.deliverAll()
	s.cores[n1].propose(s.cores[n1].term, make([]byte, maxBytesPerMsg+1))
	s.flush(n1)
	s.deliverOne(n1, n2)
	s.inflight = nil

	// (b) n1 crashes; n5 wins term 2 with n3 and n4, appends its own index 2,
	// and crashes before replicating it.
	s.crash(n1)
	s.elect(n5, n3, n4)
	s.crash(n5)

	// (c) n1 restarts and wins term 3 with n2, n3, n4. It replicates its
	// term-3 entry to n2, and X (term 1) to n3: X is now on n1, n2 and n3.
	s.restart(n1)
	s.elect(n1, n2, n3, n4)
	s.cores[n1].sendAppend(n2)
	s.flush(n1)
	s.deliverOne(n1, n2) // n2 takes index 3
	s.deliverOne(n2, n1)
	s.cores[n1].progress[n3].next = 2
	s.cores[n1].sendAppend(n3)
	s.flush(n1)
	s.deliverOne(n1, n3) // X alone: n3 now holds [1, X]
	s.deliverOne(n3, n1)
	if c := s.cores[n1].log.committed; c >= 2 {
		t.Errorf("n1 committed index %d by counting replicas of a term-1 entry", c)
	}

	// (d) n1 crashes; n5 restarts and wins term 4 with n3 and n4 (its last
	// term, 2, beats theirs), then overwrites X everywhere it can reach.
	s.inflight = nil
	s.crash(n1)
	s.restart(n5)
	s.elect(n5, n3, n4)
	s.forgetLeader(n2, n3, n4)
	s.run(50)
	s.checker.assertClean(t)
}

// A follower may hold stale entries past what the leader just sent. The
// leader's commit index only vouches for entries the follower has verified
// against the leader's log, so commit is capped at the last new entry.
func TestCommitCappedAtLastNewEntry(t *testing.T) {
	// Follower log: index 4 and 5 from term 2, which the leader doesn't have.
	c := newTestCore(t, "n1", 3, logFromTerms("", []uint64{1, 1, 1, 2, 2})...)
	c.step(&Envelope{From: "n2", To: "n1", Body: &Envelope_AppendEntries{AppendEntries: &AppendEntriesRequest{
		Term: 3, LeaderId: "n2", PrevLogIndex: 3, PrevLogTerm: 1, LeaderCommit: 5,
	}}})
	if got := c.log.committed; got != 3 {
		t.Fatalf("follower committed up to %d, want 3: entries 4-5 were never checked", got)
	}
}
