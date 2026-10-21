package raft

import (
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"
)

// simCluster is a deterministic, in-memory Raft cluster. There is no clock, no
// goroutine and no socket: a test decides when each node ticks and which
// in-flight messages are delivered, dropped or held back. With a fixed seed
// every run is identical, so a failing schedule can be replayed exactly.
type simCluster struct {
	t        testing.TB
	ids      []string
	cores    map[string]*core
	disk     map[string]*simDisk // what each node has made durable
	down     map[string]bool
	cut      map[[2]string]bool // directed links that drop everything
	inflight []*Envelope
	rand     *rand.Rand
	logger   *slog.Logger

	electionTick, heartbeatTick int

	// forgetVotes simulates a node that does not persist its vote: on restart
	// it comes back with its term but no record of who it voted for. Only the
	// teeth tests set it.
	forgetVotes bool

	checker *safetyChecker
}

// simDisk is one node's durable state. It survives crash/restart; everything
// else in the core does not.
type simDisk struct {
	hardState *HardState
}

func newSimCluster(t testing.TB, n int, seed uint64) *simCluster {
	t.Helper()
	return newSimClusterTicks(t, n, seed, 10)
}

// newSimClusterTicks is newSimCluster with a custom election timeout, in ticks.
func newSimClusterTicks(t testing.TB, n int, seed uint64, electionTick int) *simCluster {
	t.Helper()
	s := &simCluster{
		t:             t,
		cores:         make(map[string]*core),
		disk:          make(map[string]*simDisk),
		down:          make(map[string]bool),
		cut:           make(map[[2]string]bool),
		rand:          rand.New(rand.NewPCG(seed, seed^0x9e3779b97f4a7c15)),
		logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
		electionTick:  electionTick,
		heartbeatTick: 1,
		checker:       newSafetyChecker(),
	}
	for i := 1; i <= n; i++ {
		s.ids = append(s.ids, fmt.Sprintf("n%d", i))
	}
	for _, id := range s.ids {
		s.disk[id] = &simDisk{hardState: &HardState{}}
		s.start(id)
	}
	return s
}

func (s *simCluster) start(id string) {
	s.t.Helper()
	d := s.disk[id]
	hs := proto.Clone(d.hardState).(*HardState)
	if s.forgetVotes {
		hs.VotedFor = none
	}
	c, err := newCore(coreConfig{
		id:            id,
		members:       s.ids,
		electionTick:  s.electionTick,
		heartbeatTick: s.heartbeatTick,
		rand:          rand.New(rand.NewPCG(s.rand.Uint64(), s.rand.Uint64())),
		logger:        s.logger,
	}, persistedState{hardState: hs})
	if err != nil {
		s.t.Fatalf("start %s: %v", id, err)
	}
	s.cores[id] = c
	delete(s.down, id)
}

// flush plays the driver for one node: persist the Ready, then put its
// messages on the wire, then advance.
func (s *simCluster) flush(id string) {
	c := s.cores[id]
	for c.hasReady() {
		rd := c.ready()
		if rd.HardState != nil {
			s.disk[id].hardState = proto.Clone(rd.HardState).(*HardState)
		}
		s.inflight = append(s.inflight, rd.Messages...)
		c.advance(rd)
	}
	s.checker.observe(s)
}

func (s *simCluster) tick(id string) {
	if s.down[id] {
		return
	}
	s.cores[id].tick()
	s.flush(id)
}

func (s *simCluster) tickAll() {
	for _, id := range s.ids {
		s.tick(id)
	}
}

// deliver hands one message to its destination, unless the destination is
// down or the link is cut, in which case the message is lost. A message that
// was already on the wire still arrives after its sender crashes.
func (s *simCluster) deliver(m *Envelope) {
	if s.down[m.GetTo()] || s.cut[[2]string{m.GetFrom(), m.GetTo()}] {
		return
	}
	s.cores[m.GetTo()].step(m)
	s.flush(m.GetTo())
}

// deliverAll delivers every in-flight message, including the ones those
// deliveries produce, until the network is quiet.
func (s *simCluster) deliverAll() {
	for len(s.inflight) > 0 {
		m := s.inflight[0]
		s.inflight = s.inflight[1:]
		s.deliver(m)
	}
}

// run advances time by n ticks, letting the network settle after each.
func (s *simCluster) run(n int) {
	for range n {
		s.tickAll()
		s.deliverAll()
	}
}

// runUntil runs until cond holds, failing the test after limit ticks.
func (s *simCluster) runUntil(limit int, what string, cond func() bool) {
	s.t.Helper()
	for range limit {
		if cond() {
			return
		}
		s.tickAll()
		s.deliverAll()
	}
	if !cond() {
		s.t.Fatalf("after %d ticks: %s never happened\n%s", limit, what, s.dump())
	}
}

func (s *simCluster) crash(id string) {
	s.down[id] = true
}

func (s *simCluster) restart(id string) {
	s.start(id)
	s.flush(id)
}

// partition splits the cluster: nodes in different groups cannot talk.
func (s *simCluster) partition(groups ...[]string) {
	s.cut = make(map[[2]string]bool)
	group := make(map[string]int)
	for i, g := range groups {
		for _, id := range g {
			group[id] = i
		}
	}
	for _, a := range s.ids {
		for _, b := range s.ids {
			if group[a] != group[b] {
				s.cut[[2]string{a, b}] = true
			}
		}
	}
}

func (s *simCluster) heal() {
	s.cut = make(map[[2]string]bool)
}

// leaders returns the up nodes that currently believe they lead.
func (s *simCluster) leaders() []string {
	var out []string
	for _, id := range s.ids {
		if !s.down[id] && s.cores[id].role == Leader {
			out = append(out, id)
		}
	}
	return out
}

// stableLeader returns the leader when exactly one up node leads and every up
// node in its group agrees on it and on its term; "" otherwise.
func (s *simCluster) stableLeader(among ...string) string {
	if len(among) == 0 {
		among = s.ids
	}
	var lead string
	for _, id := range among {
		if !s.down[id] && s.cores[id].role == Leader {
			if lead != "" {
				return ""
			}
			lead = id
		}
	}
	if lead == "" {
		return ""
	}
	for _, id := range among {
		if s.down[id] {
			continue
		}
		if c := s.cores[id]; c.lead != lead || c.term != s.cores[lead].term {
			return ""
		}
	}
	return lead
}

func (s *simCluster) others(id string) []string {
	return slices.DeleteFunc(slices.Clone(s.ids), func(x string) bool { return x == id })
}

func (s *simCluster) dump() string {
	out := ""
	for _, id := range s.ids {
		c := s.cores[id]
		out += fmt.Sprintf("  %s down=%v role=%s term=%d lead=%q vote=%q\n", id, s.down[id], c.role, c.term, c.lead, c.vote)
	}
	return out
}

// safetyChecker watches a simCluster and records every violation of a Raft
// safety property it can see. It records rather than failing at once so the
// teeth tests can assert that a deliberately broken cluster IS caught.
type safetyChecker struct {
	leaderOf   map[uint64]string // term -> the node that led it
	violations []string
}

func newSafetyChecker() *safetyChecker {
	return &safetyChecker{leaderOf: make(map[uint64]string)}
}

func (k *safetyChecker) violate(format string, args ...any) {
	k.violations = append(k.violations, fmt.Sprintf(format, args...))
}

func (k *safetyChecker) observe(s *simCluster) {
	// Election Safety: at most one leader can be elected in a given term.
	for _, id := range s.ids {
		c := s.cores[id]
		if s.down[id] || c.role != Leader {
			continue
		}
		if prev, ok := k.leaderOf[c.term]; ok && prev != id {
			k.violate("election safety: %s and %s both led term %d", prev, id, c.term)
		}
		k.leaderOf[c.term] = id
	}
}

func (k *safetyChecker) assertClean(t testing.TB) {
	t.Helper()
	if len(k.violations) > 0 {
		t.Fatalf("%d safety violations, first: %s", len(k.violations), k.violations[0])
	}
}
