package raft

import "fmt"

// raftLog is the core's in-memory view of its log, plus three cursors into it:
//
//	1 ............ handedOut ....... committed ........ stabled ....... lastIndex
//	   (sent to the state machine)  (safe on a majority) (on this disk)
//
// stabled and committed are independent: a follower may know an entry is
// committed before it has written it, and a leader writes entries long before
// they commit. Entries are contiguous, so entries[i] holds index offset+i+1.
type raftLog struct {
	// offset/offsetTerm describe the entry just before entries[0]: index 0 and
	// term 0 for a log that starts at the beginning.
	offset, offsetTerm uint64
	entries            []*LogEntry

	stabled   uint64 // highest index known to be durable on this node
	committed uint64 // highest index known to be on a majority
	handedOut uint64 // highest index handed to the driver to apply
}

func newRaftLog(entries []*LogEntry) *raftLog {
	l := &raftLog{entries: entries}
	l.stabled = l.lastIndex() // whatever we loaded came from disk
	return l
}

func (l *raftLog) lastIndex() uint64 {
	return l.offset + uint64(len(l.entries))
}

func (l *raftLog) lastTerm() uint64 {
	t, _ := l.term(l.lastIndex())
	return t
}

// term returns the term of the entry at index i, and false if this log has no
// entry there.
func (l *raftLog) term(i uint64) (uint64, bool) {
	switch {
	case i == l.offset:
		return l.offsetTerm, true
	case i < l.offset || i > l.lastIndex():
		return 0, false
	default:
		return l.entries[i-l.offset-1].GetTerm(), true
	}
}

func (l *raftLog) matchTerm(i, term uint64) bool {
	t, ok := l.term(i)
	return ok && t == term
}

// isUpToDate reports whether a log ending at (lastIndex, lastTerm) is at least
// as up to date as this one (§5.4.1): the later last term wins, and with equal
// last terms the longer log wins.
func (l *raftLog) isUpToDate(lastIndex, lastTerm uint64) bool {
	if lastTerm != l.lastTerm() {
		return lastTerm > l.lastTerm()
	}
	return lastIndex >= l.lastIndex()
}

// slice returns entries [lo, hi), capped at maxBytes of payload but always at
// least one entry so a single large entry can still make progress.
func (l *raftLog) slice(lo, hi uint64, maxBytes int) []*LogEntry {
	if lo >= hi {
		return nil
	}
	ents := l.entries[lo-l.offset-1 : hi-l.offset-1]
	size := 0
	for i, e := range ents {
		size += len(e.GetData())
		if i > 0 && size > maxBytes {
			return ents[:i]
		}
	}
	return ents
}

// append adds entries at the end. Only a leader appends to its own log this
// way, and a leader never overwrites (Leader Append-Only).
func (l *raftLog) append(ents ...*LogEntry) {
	l.entries = append(l.entries, ents...)
}

// truncateAndAppend writes ents into the log starting at ents[0].Index,
// discarding anything already at or after that index. The caller has checked
// that ents[0] really conflicts, or that it lands right after the last entry.
func (l *raftLog) truncateAndAppend(ents []*LogEntry) {
	first := ents[0].GetIndex()
	if first <= l.offset || first > l.lastIndex()+1 {
		panic(fmt.Sprintf("raft: truncateAndAppend at %d outside (%d, %d]", first, l.offset, l.lastIndex()+1))
	}
	l.entries = append(l.entries[:first-l.offset-1], ents...)
	// The replaced entries may already be on disk; the new ones are not.
	l.stabled = min(l.stabled, first-1)
}

// findConflict returns the index of the first entry in ents whose term differs
// from what this log holds at that index (or that this log lacks entirely), or
// 0 if every entry is already present.
func (l *raftLog) findConflict(ents []*LogEntry) uint64 {
	for _, e := range ents {
		if !l.matchTerm(e.GetIndex(), e.GetTerm()) {
			return e.GetIndex()
		}
	}
	return 0
}

// conflictHint tells a leader where to retry after this log failed the
// consistency check at prevIndex: one past our end if we're simply too short,
// otherwise the first index of our term at prevIndex, so the leader skips the
// whole conflicting term in one round trip instead of one entry per round.
func (l *raftLog) conflictHint(prevIndex uint64) uint64 {
	if prevIndex > l.lastIndex() {
		return l.lastIndex() + 1
	}
	t, _ := l.term(prevIndex)
	i := prevIndex
	for i > l.offset+1 && l.matchTerm(i-1, t) {
		i--
	}
	return i
}

func (l *raftLog) unstable() []*LogEntry {
	return l.entries[l.stabled-l.offset:]
}

func (l *raftLog) nextCommitted() []*LogEntry {
	if l.committed <= l.handedOut {
		return nil
	}
	return l.entries[l.handedOut-l.offset : l.committed-l.offset]
}
