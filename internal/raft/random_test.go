package raft

import (
	"fmt"
	"slices"
	"testing"
)

// chaos drives a simCluster through a random schedule: ticks, reordered,
// dropped and duplicated messages, crashes, restarts and partitions. The
// safety checker watches every step. Each seed is a different, reproducible
// schedule; a failure message names the seed so it can be replayed.
type chaos struct {
	s        *simCluster
	proposed *int
}

func (ch chaos) step() {
	s := ch.s
	r := s.rand
	switch p := r.IntN(100); {
	case p < 5:
		ch.proposeAnywhere()
	case p < 30:
		s.tick(s.ids[r.IntN(len(s.ids))])
	case p < 85:
		ch.deliverRandom(false)
	case p < 88:
		ch.deliverRandom(true) // duplicate: deliver and keep a copy on the wire
	case p < 92:
		if len(s.inflight) > 0 { // drop
			i := r.IntN(len(s.inflight))
			s.inflight = append(s.inflight[:i], s.inflight[i+1:]...)
		}
	case p < 95:
		id := s.ids[r.IntN(len(s.ids))]
		if s.down[id] {
			s.restart(id)
		} else {
			s.crash(id)
		}
	case p < 98:
		ch.randomPartition()
	default:
		s.heal()
	}
}

// proposeAnywhere submits a command to every node that thinks it leads; stale
// leaders accept proposals too, and those are exactly the ones that must
// never be applied.
func (ch chaos) proposeAnywhere() {
	for _, id := range ch.s.leaders() {
		c := ch.s.cores[id]
		*ch.proposed++
		if _, err := c.propose(c.term, fmt.Appendf(nil, "cmd-%d", *ch.proposed)); err != nil {
			panic(err) // leaders() only returns leaders
		}
		ch.s.flush(id)
	}
}

func (ch chaos) deliverRandom(keep bool) {
	s := ch.s
	if len(s.inflight) == 0 {
		return
	}
	i := s.rand.IntN(len(s.inflight))
	m := s.inflight[i]
	if !keep {
		s.inflight = append(s.inflight[:i], s.inflight[i+1:]...)
	}
	s.deliver(m)
}

func (ch chaos) randomPartition() {
	s := ch.s
	var a, b []string
	for _, id := range s.ids {
		if s.rand.IntN(2) == 0 {
			a = append(a, id)
		} else {
			b = append(b, id)
		}
	}
	s.partition(a, b)
}

// settle ends the chaos: everyone up, network whole. Liveness then demands a
// leader that everybody agrees on, and that a fresh command reaches every
// state machine, by which point all of them must have applied the same log.
func (ch chaos) settle(t *testing.T, seed uint64) {
	t.Helper()
	s := ch.s
	s.heal()
	for _, id := range s.ids {
		if s.down[id] {
			s.restart(id)
		}
	}
	lead := ""
	for range 300 {
		if lead = s.stableLeader(); lead != "" {
			break
		}
		s.tickAll()
		s.deliverAll()
	}
	if lead == "" {
		t.Fatalf("seed %d: no stable leader after the chaos settled\n%s", seed, s.dump())
	}
	s.propose(lead, "final")
	for range 300 {
		done := true
		for _, id := range s.ids {
			if d := s.appliedData(id); len(d) == 0 || d[len(d)-1] != "final" {
				done = false
			}
		}
		if done {
			break
		}
		s.tickAll()
		s.deliverAll()
	}
	want := s.appliedData(lead)
	for _, id := range s.ids {
		if got := s.appliedData(id); !slices.Equal(got, want) {
			t.Fatalf("seed %d: %s applied %d commands, leader %s applied %d\n%s", seed, id, len(got), lead, len(want), s.dump())
		}
	}
}

// newChaos builds a cluster for a random schedule. Odd seeds get five nodes,
// even seeds three, and a short election timeout so a run sees many elections.
func newChaos(t *testing.T, seed uint64) chaos {
	return chaos{s: newSimClusterTicks(t, 3+2*int(seed%2), seed, 3), proposed: new(int)}
}

func TestRandomizedSafety(t *testing.T) {
	seeds, steps := uint64(200), 3000
	if testing.Short() {
		seeds = 20
	}
	for seed := range seeds {
		ch := newChaos(t, seed)
		for range steps {
			ch.step()
		}
		ch.settle(t, seed)
		if v := ch.s.checker.violations; len(v) > 0 {
			t.Fatalf("seed %d: %s", seed, v[0])
		}
	}
}
