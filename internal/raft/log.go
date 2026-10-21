package raft

// raftLog is the core's in-memory view of its log. Entries are 1-indexed and
// contiguous, so entries[i] holds index i+1.
type raftLog struct {
	entries []*LogEntry
}

func newRaftLog(entries []*LogEntry) *raftLog {
	return &raftLog{entries: entries}
}

func (l *raftLog) lastIndex() uint64 {
	return uint64(len(l.entries))
}

func (l *raftLog) lastTerm() uint64 {
	if len(l.entries) == 0 {
		return 0
	}
	return l.entries[len(l.entries)-1].GetTerm()
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
