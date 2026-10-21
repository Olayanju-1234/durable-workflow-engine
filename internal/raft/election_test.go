package raft

import (
	"io"
	"log/slog"
	"math/rand/v2"
	"testing"
)

func TestElectsOneLeader(t *testing.T) {
	for _, n := range []int{1, 3, 5} {
		for seed := range uint64(20) {
			s := newSimCluster(t, n, seed)
			s.runUntil(100, "a stable leader", func() bool { return s.stableLeader() != "" })
			s.checker.assertClean(t)
		}
	}
}

// With heartbeats flowing, nobody's election timer fires: the term and the
// leader stay put however long the cluster runs.
func TestLeaderStaysStable(t *testing.T) {
	s := newSimCluster(t, 3, 1)
	s.runUntil(100, "a stable leader", func() bool { return s.stableLeader() != "" })
	lead, term := s.stableLeader(), s.cores[s.stableLeader()].term
	s.run(500)
	if got := s.stableLeader(); got != lead || s.cores[got].term != term {
		t.Fatalf("leadership moved from %s/term %d to %q/term %d", lead, term, got, s.cores[lead].term)
	}
}

func TestReelectionAfterLeaderCrash(t *testing.T) {
	s := newSimCluster(t, 3, 2)
	s.runUntil(100, "a stable leader", func() bool { return s.stableLeader() != "" })
	old := s.stableLeader()
	oldTerm := s.cores[old].term

	s.crash(old)
	s.runUntil(100, "a new leader", func() bool { return s.stableLeader(s.others(old)...) != "" })
	lead := s.stableLeader(s.others(old)...)
	if term := s.cores[lead].term; term <= oldTerm {
		t.Fatalf("new leader %s has term %d, want > %d", lead, term, oldTerm)
	}

	// The old leader comes back with its old term from disk, hears the new
	// leader, and falls in line instead of fighting it.
	s.restart(old)
	s.runUntil(50, "the old leader to follow", func() bool { return s.stableLeader() == lead })
	s.checker.assertClean(t)
}

// A leader cut off from the majority steps down on its own (CheckQuorum), the
// majority elects a new leader, and the isolated node can never win: it can't
// collect a majority of votes from its side of the partition.
func TestPartitionedLeaderStepsDown(t *testing.T) {
	s := newSimCluster(t, 3, 3)
	s.runUntil(100, "a stable leader", func() bool { return s.stableLeader() != "" })
	old := s.stableLeader()
	majority := s.others(old)

	s.partition([]string{old}, majority)
	s.runUntil(4*s.electionTick, "the isolated leader to step down", func() bool { return s.cores[old].role != Leader })
	s.runUntil(100, "a leader in the majority", func() bool { return s.stableLeader(majority...) != "" })
	s.run(200)
	if s.cores[old].role == Leader {
		t.Fatalf("isolated node %s became leader without a majority", old)
	}

	s.heal()
	s.runUntil(100, "one leader after healing", func() bool { return s.stableLeader() != "" })
	s.checker.assertClean(t)
}

// Three candidates in the same term split the vote three ways. Randomized
// timeouts break the tie on a later term.
func TestSplitVoteResolves(t *testing.T) {
	s := newSimCluster(t, 3, 4)
	for _, id := range s.ids {
		s.cores[id].campaign()
		s.flush(id)
	}
	s.deliverAll()
	if got := s.leaders(); len(got) != 0 {
		t.Fatalf("split vote produced leaders %v", got)
	}
	s.runUntil(200, "a leader after the split", func() bool { return s.stableLeader() != "" })
	if term := s.cores[s.stableLeader()].term; term < 2 {
		t.Fatalf("leader elected in term %d, want a later term than the split", term)
	}
	s.checker.assertClean(t)
}

// The checker must catch a broken cluster, or a clean run proves nothing.
// A node that forgets its vote across a restart can vote twice in one term,
// and the second vote elects a second leader for that term.
func TestForgottenVoteElectsTwoLeaders(t *testing.T) {
	scenario := func(forget bool) *simCluster {
		s := newSimCluster(t, 3, 5)
		s.forgetVotes = forget
		a, b, c := s.ids[0], s.ids[1], s.ids[2]
		s.cut[[2]string{a, b}] = true // a and b never hear each other
		s.cut[[2]string{b, a}] = true

		s.cores[a].campaign() // both campaign in term 1
		s.flush(a)
		s.cores[b].campaign()
		s.flush(b)
		s.deliverOne(a, c) // c votes for a first...
		s.deliverOne(c, a) // ...and a wins term 1 with {a, c}
		if s.cores[a].role != Leader {
			t.Fatalf("setup: %s should lead term 1\n%s", a, s.dump())
		}
		// Lose everything else in flight, including a's first AppendEntries: c's
		// log stays empty, so only its vote record stands in b's way.
		s.inflight = nil

		s.crash(c)
		s.restart(c)
		s.send(b, c, &Envelope_RequestVote{RequestVote: &RequestVoteRequest{Term: 1, CandidateId: b}})
		s.deliverAll()
		return s
	}

	scenario(false).checker.assertClean(t)

	broken := scenario(true)
	if len(broken.checker.violations) == 0 {
		t.Fatal("checker missed two leaders in one term")
	}
}

// deliverOne delivers the oldest in-flight message from one node to another.
func (s *simCluster) deliverOne(from, to string) {
	s.t.Helper()
	for i, m := range s.inflight {
		if m.GetFrom() == from && m.GetTo() == to {
			s.inflight = append(s.inflight[:i], s.inflight[i+1:]...)
			s.deliver(m)
			return
		}
	}
	s.t.Fatalf("no message in flight from %s to %s", from, to)
}

func (s *simCluster) send(from, to string, body isEnvelope_Body) {
	s.inflight = append(s.inflight, &Envelope{From: from, To: to, Body: body})
}

// newTestCore builds a lone core for unit-testing individual rules.
func newTestCore(t *testing.T, id string, term uint64, entries ...*LogEntry) *core {
	t.Helper()
	c, err := newCore(coreConfig{
		id:            id,
		members:       []string{"n1", "n2", "n3"},
		electionTick:  10,
		heartbeatTick: 1,
		rand:          rand.New(rand.NewPCG(1, 2)),
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, persistedState{hardState: &HardState{CurrentTerm: term}, entries: entries})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func voteRequest(from string, term, lastIndex, lastTerm uint64) *Envelope {
	return &Envelope{From: from, To: "n1", Body: &Envelope_RequestVote{RequestVote: &RequestVoteRequest{
		Term: term, CandidateId: from, LastLogIndex: lastIndex, LastLogTerm: lastTerm,
	}}}
}

// lastVoteResponse returns the most recent vote answer the core produced.
func lastVoteResponse(t *testing.T, c *core) *RequestVoteResponse {
	t.Helper()
	for i := len(c.msgs) - 1; i >= 0; i-- {
		if r, ok := c.msgs[i].GetBody().(*Envelope_RequestVoteResponse); ok {
			return r.RequestVoteResponse
		}
	}
	t.Fatal("no vote response sent")
	return nil
}

// §5.4.1: a voter grants only a candidate whose log is at least as up to date
// as its own. Last term decides first; equal terms fall back to length.
func TestVoteRequiresUpToDateLog(t *testing.T) {
	voterLog := []*LogEntry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}}
	tests := []struct {
		name                string
		lastIndex, lastTerm uint64
		want                bool
	}{
		{"older last term, longer log", 9, 1, false},
		{"same last term, shorter log", 2, 2, false},
		{"same last term, same length", 3, 2, true},
		{"same last term, longer log", 4, 2, true},
		{"newer last term, shorter log", 1, 3, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestCore(t, "n1", 5, voterLog...)
			c.step(voteRequest("n2", 6, tt.lastIndex, tt.lastTerm))
			if got := lastVoteResponse(t, c).GetVoteGranted(); got != tt.want {
				t.Fatalf("vote granted = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOneVotePerTerm(t *testing.T) {
	c := newTestCore(t, "n1", 1)
	c.step(voteRequest("n2", 2, 0, 0))
	if !lastVoteResponse(t, c).GetVoteGranted() {
		t.Fatal("first candidate in term 2 should get the vote")
	}
	c.step(voteRequest("n3", 2, 0, 0))
	if lastVoteResponse(t, c).GetVoteGranted() {
		t.Fatal("second candidate in term 2 got a vote too")
	}
	c.step(voteRequest("n2", 2, 0, 0))
	if !lastVoteResponse(t, c).GetVoteGranted() {
		t.Fatal("a retransmitted request from the same candidate should be granted again")
	}
	if c.hardState().GetVotedFor() != "n2" || c.hardState().GetCurrentTerm() != 2 {
		t.Fatalf("hard state %v, want term 2 vote n2", c.hardState())
	}
}

// §6: a follower that heard from its leader within the minimum election
// timeout ignores vote requests entirely, even from a higher term.
func TestLiveLeaderSuppressesVotes(t *testing.T) {
	c := newTestCore(t, "n1", 3)
	c.step(&Envelope{From: "n2", To: "n1", Body: &Envelope_AppendEntries{AppendEntries: &AppendEntriesRequest{Term: 3, LeaderId: "n2"}}})
	c.msgs = nil
	c.step(voteRequest("n3", 9, 0, 0))
	if c.term != 3 || len(c.msgs) != 0 {
		t.Fatalf("follower in lease reacted to a vote request: term %d, msgs %v", c.term, c.msgs)
	}

	// Once the leader has been silent for the minimum election timeout, the
	// same request is considered.
	for range c.electionTick {
		c.electionElapsed++
	}
	c.step(voteRequest("n3", 9, 0, 0))
	if c.term != 9 || !lastVoteResponse(t, c).GetVoteGranted() {
		t.Fatalf("follower out of lease: term %d, want 9 with vote granted", c.term)
	}
}

// Any message from a newer term demotes the receiver; any message from an
// older term is answered with the newer term so its sender demotes itself.
func TestTermRules(t *testing.T) {
	leader := newTestCore(t, "n1", 4)
	leader.becomeCandidate()
	leader.becomeLeader()
	leader.step(&Envelope{From: "n2", To: "n1", Body: &Envelope_AppendEntriesResponse{AppendEntriesResponse: &AppendEntriesResponse{Term: 7}}})
	if leader.role != Follower || leader.term != 7 {
		t.Fatalf("leader saw term 7: role %s term %d, want follower in 7", leader.role, leader.term)
	}

	f := newTestCore(t, "n1", 5)
	f.step(&Envelope{From: "n2", To: "n1", Body: &Envelope_AppendEntries{AppendEntries: &AppendEntriesRequest{Term: 3, LeaderId: "n2"}}})
	resp := f.msgs[len(f.msgs)-1].GetAppendEntriesResponse()
	if resp == nil || resp.GetSuccess() || resp.GetTerm() != 5 {
		t.Fatalf("stale AppendEntries answered with %v, want failure carrying term 5", resp)
	}
	if f.lead != none {
		t.Fatalf("follower accepted stale leader %q", f.lead)
	}
}
