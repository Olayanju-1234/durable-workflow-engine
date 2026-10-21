package raft

import (
	"strings"
	"testing"
)

// The randomized test only means something if its checker can fail. Each test
// here switches off one Raft rule (a testing knob) and demands that the
// checker reports a violation. A broken core may also trip an internal
// invariant and panic; that seed is skipped, since the point is the checker.

// runBrokenChaos runs random schedules with knobs set until the checker
// reports a violation, and returns its first message ("" if none in budget).
func runBrokenChaos(t *testing.T, knobs testingKnobs, seeds uint64) string {
	t.Helper()
	for seed := range seeds {
		if v := brokenSeed(t, knobs, seed); v != "" {
			return v
		}
	}
	return ""
}

func brokenSeed(t *testing.T, knobs testingKnobs, seed uint64) (violation string) {
	defer func() {
		if r := recover(); r != nil {
			violation = "" // an internal invariant fired first; try the next seed
		}
	}()
	ch := newChaos(t, seed)
	ch.s.knobs = knobs
	for _, id := range ch.s.ids {
		ch.s.cores[id].knobs = knobs
	}
	for i := range 3000 {
		ch.step()
		if i%10 == 0 {
			ch.s.checker.checkLogMatching(ch.s)
		}
		if v := ch.s.checker.violations; len(v) > 0 {
			return v[0]
		}
	}
	return ""
}

// Without the up-to-date check a node with a stale log can win an election
// and overwrite committed entries: Leader Completeness breaks.
func TestCheckerCatchesStaleVotes(t *testing.T) {
	v := runBrokenChaos(t, testingKnobs{grantStaleVotes: true}, 50)
	if v == "" {
		t.Fatal("no violation found with the up-to-date vote check disabled")
	}
	t.Logf("caught: %s", v)
}

// Checking only prevLogIndex lets a follower graft entries onto a log that
// diverged earlier: Log Matching breaks.
func TestCheckerCatchesSkippedTermCheck(t *testing.T) {
	v := runBrokenChaos(t, testingKnobs{skipPrevTermCheck: true}, 50)
	if v == "" {
		t.Fatal("no violation found with the prevLogTerm check disabled")
	}
	t.Logf("caught: %s", v)
}

// Counting replicas of an older-term entry commits X in the Figure 8 schedule,
// and the next leader then overwrites an entry n1 already applied.
func TestCheckerCatchesFigure8Commit(t *testing.T) {
	s := figure8(t, testingKnobs{countOldTerms: true})
	var applied bool
	for _, v := range s.checker.violations {
		applied = applied || strings.HasPrefix(v, "state machine safety")
	}
	if !applied {
		t.Fatalf("want n1's applied entry to be overwritten (state machine safety), got %q", s.checker.violations)
	}
	t.Logf("caught: %s", s.checker.violations[0])
}

// No knob makes a leader rewrite its own log, so this checks the checker
// directly: a leader whose entry changes under it is reported.
func TestCheckerCatchesLeaderRewrite(t *testing.T) {
	s := newSimCluster(t, 3, 30)
	lead := electLeader(t, s)
	s.propose(lead, "original")
	c := s.cores[lead]
	c.log.entries[len(c.log.entries)-1] = &LogEntry{Index: c.log.lastIndex(), Term: c.term, Data: []byte("rewritten")}
	s.checker.observe(s)
	if len(s.checker.violations) == 0 {
		t.Fatal("checker missed a leader rewriting its own entry")
	}
	t.Logf("caught: %s", s.checker.violations[0])
}
