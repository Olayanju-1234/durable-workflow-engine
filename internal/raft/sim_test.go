package raft

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"slices"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
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
	applied  map[string][]*LogEntry // what each node's state machine applied since it last started
	rand     *rand.Rand
	logger   *slog.Logger

	electionTick, heartbeatTick int

	// forgetVotes simulates a node that does not persist its vote: on restart
	// it comes back with its term but no record of who it voted for. Only the
	// teeth tests set it, as they do knobs.
	forgetVotes bool
	knobs       testingKnobs

	checker *safetyChecker
}

// simDisk is one node's durable state. It survives crash/restart; everything
// else in the core does not.
type simDisk struct {
	hardState *HardState
	snapshot  *Snapshot
	entries   []*LogEntry // the entries after snapshot
}

func (d *simDisk) base() uint64 {
	if d.snapshot == nil {
		return 0
	}
	return d.snapshot.Index
}

// encodeApplied serializes a simulated state machine (the list of entries it
// applied) as snapshot data, using the same record framing as everything else.
func encodeApplied(ents []*LogEntry) []byte {
	var out []byte
	for _, e := range ents {
		payload, _ := proto.Marshal(e)
		rec, _ := record.Encode(payload)
		out = append(out, rec...)
	}
	return out
}

func decodeApplied(data []byte) []*LogEntry {
	var out []*LogEntry
	_, err := record.Scan(bufio.NewReader(bytes.NewReader(data)), func(p []byte) error {
		e := &LogEntry{}
		if err := proto.Unmarshal(p, e); err != nil {
			return err
		}
		out = append(out, e)
		return nil
	})
	if err != nil {
		panic(err)
	}
	return out
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
		applied:       make(map[string][]*LogEntry),
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
		knobs:         s.knobs,
	}, persistedState{hardState: hs, snapshot: d.snapshot, entries: slices.Clone(d.entries)})
	if err != nil {
		s.t.Fatalf("start %s: %v", id, err)
	}
	s.cores[id] = c
	s.applied[id] = nil // rebuilt from the snapshot, then by re-applying the log
	if d.snapshot != nil {
		s.restoreSM(id, d.snapshot)
	}
	s.checker.restarted(id, c)
	delete(s.down, id)
}

// flush plays the driver for one node: persist the Ready, then put its
// messages on the wire, then apply what committed, then advance.
func (s *simCluster) flush(id string) {
	c := s.cores[id]
	for c.hasReady() {
		rd := c.ready()
		d := s.disk[id]
		if rd.HardState != nil {
			d.hardState = proto.Clone(rd.HardState).(*HardState)
		}
		if rd.Snapshot != nil {
			d.snapshot, d.entries = rd.Snapshot, nil
		}
		if len(rd.Entries) > 0 {
			first := rd.Entries[0].GetIndex()
			d.entries = append(d.entries[:first-d.base()-1], rd.Entries...)
		}
		s.inflight = append(s.inflight, rd.Messages...)
		if rd.Snapshot != nil {
			s.restoreSM(id, rd.Snapshot)
		}
		for _, e := range rd.CommittedEntries {
			s.applied[id] = append(s.applied[id], e)
			s.checker.applied(id, e)
		}
		c.advance(rd)
	}
	s.checker.observe(s)
}

// restoreSM replaces a node's state machine with a snapshot's contents. Every
// entry in it must agree with what other nodes applied (State Machine Safety).
func (s *simCluster) restoreSM(id string, snap *Snapshot) {
	s.applied[id] = decodeApplied(snap.Data)
	for _, e := range s.applied[id] {
		s.checker.applied(id, e)
	}
}

// snapshot has a node snapshot its state machine at everything applied so
// far and compact its log, as Node does once enough entries pile up.
func (s *simCluster) snapshot(id string) {
	c, d := s.cores[id], s.disk[id]
	idx := c.log.handedOut
	if s.down[id] || idx <= c.log.offset {
		return
	}
	term, _ := c.log.term(idx)
	snap := &Snapshot{Index: idx, Term: term, Data: encodeApplied(s.applied[id])}
	d.entries = slices.Clone(d.entries[idx-d.base():])
	d.snapshot = snap
	c.compact(snap)
}

// propose submits data on the current leader and returns its index.
func (s *simCluster) propose(lead string, data string) uint64 {
	s.t.Helper()
	c := s.cores[lead]
	idx, err := c.propose(c.term, []byte(data))
	if err != nil {
		s.t.Fatalf("propose on %s: %v", lead, err)
	}
	s.flush(lead)
	return idx
}

// appliedData lists the payloads a node applied, skipping leaders' empty
// term-opening entries.
func (s *simCluster) appliedData(id string) []string {
	var out []string
	for _, e := range s.applied[id] {
		if len(e.GetData()) > 0 {
			out = append(out, string(e.GetData()))
		}
	}
	return out
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
// deliveries produce, until the network is quiet. A network that never goes
// quiet is a livelock (two nodes answering each other forever) and fails fast.
func (s *simCluster) deliverAll() {
	for n := 0; len(s.inflight) > 0; n++ {
		if n > 100_000 {
			s.t.Fatalf("network never went quiet: livelock\n%s", s.dump())
		}
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
		out += fmt.Sprintf("  %s down=%v role=%s term=%d lead=%q vote=%q last=%d commit=%d\n",
			id, s.down[id], c.role, c.term, c.lead, c.vote, c.log.lastIndex(), c.log.committed)
	}
	return out
}

// safetyChecker watches a simCluster and records every violation it sees of
// the five properties Raft guarantees (paper Figure 3). It records rather than
// failing at once, so the teeth tests can assert that a deliberately broken
// core IS caught.
type safetyChecker struct {
	leaderOf    map[uint64]string     // term -> the node that led it
	appliedAt   map[uint64]*LogEntry  // index -> what the first node applied there
	committedAt map[uint64]*LogEntry  // index -> what the first node committed there
	seenCommit  map[string]uint64     // node -> commit index already recorded
	leaderLog   map[string]leaderView // node -> its log as last seen while leading
	violations  []string
}

// leaderView is a copy of a leader's log, taken while it led term.
type leaderView struct {
	term    uint64
	entries []*LogEntry
}

func newSafetyChecker() *safetyChecker {
	return &safetyChecker{
		leaderOf:    make(map[uint64]string),
		appliedAt:   make(map[uint64]*LogEntry),
		committedAt: make(map[uint64]*LogEntry),
		seenCommit:  make(map[string]uint64),
		leaderLog:   make(map[string]leaderView),
	}
}

func (k *safetyChecker) violate(format string, args ...any) {
	for i, a := range args {
		if e, ok := a.(*LogEntry); ok {
			args[i] = describe(e)
		}
	}
	k.violations = append(k.violations, fmt.Sprintf(format, args...))
}

// describe prints an entry compactly: a payload can be megabytes.
func describe(e *LogEntry) string {
	if e == nil {
		return "<none>"
	}
	data := e.GetData()
	if len(data) > 16 {
		return fmt.Sprintf("{%d@t%d %q… (%d bytes)}", e.GetIndex(), e.GetTerm(), data[:16], len(data))
	}
	return fmt.Sprintf("{%d@t%d %q}", e.GetIndex(), e.GetTerm(), data)
}

// restarted forgets a node's volatile history: its commit index starts over.
func (k *safetyChecker) restarted(id string, c *core) {
	k.seenCommit[id] = c.log.committed
	delete(k.leaderLog, id)
}

// entryAt returns the entry a core's log holds at index i, or nil.
func entryAt(c *core, i uint64) *LogEntry {
	if i <= c.log.offset || i > c.log.lastIndex() {
		return nil
	}
	return c.log.entries[i-c.log.offset-1]
}

func (k *safetyChecker) observe(s *simCluster) {
	for _, id := range s.ids {
		if s.down[id] {
			continue
		}
		c := s.cores[id]
		k.recordCommits(id, c)
		if c.role != Leader {
			delete(k.leaderLog, id)
			continue
		}

		// Election Safety: at most one leader can be elected in a given term.
		if prev, ok := k.leaderOf[c.term]; ok && prev != id {
			k.violate("election safety: %s and %s both led term %d", prev, id, c.term)
		}
		k.leaderOf[c.term] = id

		view, ok := k.leaderLog[id]
		if ok && view.term == c.term {
			// Leader Append-Only: a leader never overwrites or deletes entries in
			// its log; it only appends.
			for _, e := range view.entries {
				if e.GetIndex() > c.log.offset && !proto.Equal(entryAt(c, e.GetIndex()), e) {
					k.violate("leader append-only: leader %s of term %d changed index %d from %v to %v",
						id, c.term, e.GetIndex(), e, entryAt(c, e.GetIndex()))
					break
				}
			}
		} else {
			// Leader Completeness: a leader holds every entry committed before
			// its term. Checked the moment it is first seen leading.
			for i, e := range k.committedAt {
				if i > c.log.offset && !proto.Equal(entryAt(c, i), e) {
					k.violate("leader completeness: %s leads term %d without committed entry %v (has %v)",
						id, c.term, e, entryAt(c, i))
					break
				}
			}
		}
		k.leaderLog[id] = leaderView{term: c.term, entries: slices.Clone(c.log.entries)}
	}
}

// recordCommits notes every entry a node newly counts as committed. Two nodes
// committing different entries at one index is already a safety failure.
func (k *safetyChecker) recordCommits(id string, c *core) {
	for i := max(k.seenCommit[id], c.log.offset) + 1; i <= c.log.committed; i++ {
		e := entryAt(c, i)
		if prev, ok := k.committedAt[i]; ok && !proto.Equal(prev, e) {
			k.violate("state machine safety: %s committed %v at index %d, another node committed %v", id, e, i, prev)
			continue
		}
		k.committedAt[i] = e
	}
	k.seenCommit[id] = c.log.committed
}

// applied checks State Machine Safety: once any node applies an entry at an
// index, no node ever applies a different entry at that index.
func (k *safetyChecker) applied(id string, e *LogEntry) {
	if prev, ok := k.appliedAt[e.GetIndex()]; ok && !proto.Equal(prev, e) {
		k.violate("state machine safety: %s applied %v at index %d, another node applied %v",
			id, e, e.GetIndex(), prev)
		return
	}
	k.appliedAt[e.GetIndex()] = e
}

// checkLogMatching verifies Log Matching across every pair of running nodes:
// if two logs hold an entry with the same index and term, they are identical
// in every entry up to that index. It is O(nodes² × log), so callers run it
// periodically rather than on every message.
func (k *safetyChecker) checkLogMatching(s *simCluster) {
	for i, a := range s.ids {
		for _, b := range s.ids[i+1:] {
			if s.down[a] || s.down[b] {
				continue
			}
			ca, cb := s.cores[a], s.cores[b]
			lo := max(ca.log.offset, cb.log.offset) + 1
			hi := min(ca.log.lastIndex(), cb.log.lastIndex())
			top := uint64(0) // highest index where both hold the same term
			for j := hi; j >= lo && j > 0; j-- {
				if entryAt(ca, j).GetTerm() == entryAt(cb, j).GetTerm() {
					top = j
					break
				}
			}
			for j := lo; j <= top; j++ {
				if !proto.Equal(entryAt(ca, j), entryAt(cb, j)) {
					k.violate("log matching: %s and %s agree at index %d but differ at %d (%v vs %v)",
						a, b, top, j, entryAt(ca, j), entryAt(cb, j))
					return
				}
			}
		}
	}
}

func (k *safetyChecker) assertClean(t testing.TB) {
	t.Helper()
	if len(k.violations) > 0 {
		t.Fatalf("%d safety violations, first: %s", len(k.violations), k.violations[0])
	}
}
