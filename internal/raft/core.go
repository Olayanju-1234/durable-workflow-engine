package raft

import (
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"

	"google.golang.org/protobuf/proto"
)

// ErrNotLeader rejects a proposal made to a node that does not lead the term
// the proposer asked for. Nothing was appended; the proposer should find the
// current leader.
var ErrNotLeader = errors.New("raft: not the leader")

// Limits on one AppendEntries message, so catching up a far-behind follower is
// a stream of bounded messages rather than one enormous one.
const (
	maxEntriesPerMsg = 256
	maxBytesPerMsg   = 1 << 20
)

// Role is a node's position in the cluster at a moment in time.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return fmt.Sprintf("role(%d)", int(r))
	}
}

// none is the empty node id: no leader known, or no vote cast this term.
const none = ""

// coreConfig is everything the core needs at construction. Time is measured in
// ticks, not durations: the driver decides how long a tick is, and tests just
// call tick() as often as they like.
type coreConfig struct {
	id string
	// members is every node in the cluster, this one included.
	members []string
	// electionTick is the minimum election timeout. Each node draws its actual
	// timeout from [electionTick, 2*electionTick) so that, after a leader dies,
	// one follower usually times out well before the others and wins alone.
	electionTick int
	// heartbeatTick is how often a leader asserts itself. It must be well under
	// electionTick, or followers would start elections against a live leader.
	heartbeatTick int
	rand          *rand.Rand
	logger        *slog.Logger
}

// persistedState is what a node reloads from disk when it (re)starts.
type persistedState struct {
	hardState *HardState
	entries   []*LogEntry
}

// Ready is one batch of work the core hands its driver, to be handled in this
// order: persist HardState and Entries, then send Messages, then apply
// CommittedEntries. Nothing may leave the node, and nothing may be applied,
// until the state it depends on would survive a crash.
type Ready struct {
	// HardState is non-nil when the term or the vote changed.
	HardState *HardState
	// Entries must be written to the log, replacing anything already stored at
	// or after Entries[0].Index (a follower repairing a conflict).
	Entries  []*LogEntry
	Messages []*Envelope
	// CommittedEntries are safe on a majority and ready for the state machine.
	CommittedEntries []*LogEntry
}

// Status is a read-only snapshot of a node's volatile state.
type Status struct {
	ID     string
	Term   uint64
	Role   Role
	Leader string // empty when unknown
	// Commit is the highest log index this node knows to be committed.
	Commit uint64
	// Applied is the highest index this node's state machine has applied.
	Applied uint64
	// termStart is the index of the entry a leader appends when it takes
	// office. Once that is applied, so is everything committed before it.
	termStart uint64
}

// LeaderReady reports whether this node leads and has applied every entry
// committed before its term began, so its state machine is fully current.
// Serving reads or deciding new writes any earlier would act on stale state.
func (s Status) LeaderReady() bool {
	return s.Role == Leader && s.termStart > 0 && s.Applied >= s.termStart
}

// core is the Raft algorithm as a deterministic state machine. It is not safe
// for concurrent use: exactly one goroutine (the driver, or a test) owns it.
type core struct {
	id     string
	peers  []string // every member except this node, sorted for determinism
	quorum int      // votes or acknowledgements needed for a majority

	// Persistent state (Figure 2). Changes are emitted in Ready.HardState.
	term uint64
	vote string
	log  *raftLog

	role Role
	lead string

	votes     map[string]bool      // candidate: the answer from each voter so far
	progress  map[string]*progress // leader: what it knows about each follower
	termStart uint64               // leader: index of the entry that opened its term

	// Elapsed ticks since the last reset. For a follower or candidate the
	// election timer; a leader reuses it to time its quorum check.
	electionElapsed  int
	heartbeatElapsed int

	electionTick              int
	heartbeatTick             int
	randomizedElectionTimeout int
	rand                      *rand.Rand

	msgs     []*Envelope
	lastHard *HardState // the HardState most recently handed out in a Ready

	logger *slog.Logger
}

// progress is the leader's view of one follower.
type progress struct {
	// match is the highest index known to be replicated on the follower; next
	// is the index the leader will send it next. match < next always holds.
	match, next uint64
	// recentActive is set whenever the follower answers, and cleared at every
	// quorum check. A leader that cannot hear a majority steps down.
	recentActive bool
}

func newCore(cfg coreConfig, st persistedState) (*core, error) {
	if !slices.Contains(cfg.members, cfg.id) {
		return nil, fmt.Errorf("raft: node %q is not in the member list %v", cfg.id, cfg.members)
	}
	if cfg.heartbeatTick <= 0 || cfg.electionTick <= cfg.heartbeatTick {
		return nil, fmt.Errorf("raft: need 0 < heartbeat tick (%d) < election tick (%d)", cfg.heartbeatTick, cfg.electionTick)
	}
	peers := slices.DeleteFunc(slices.Clone(cfg.members), func(m string) bool { return m == cfg.id })
	slices.Sort(peers)
	hs := st.hardState
	if hs == nil {
		hs = &HardState{}
	}
	c := &core{
		id:            cfg.id,
		peers:         peers,
		quorum:        len(cfg.members)/2 + 1,
		term:          hs.GetCurrentTerm(),
		vote:          hs.GetVotedFor(),
		log:           newRaftLog(st.entries),
		electionTick:  cfg.electionTick,
		heartbeatTick: cfg.heartbeatTick,
		rand:          cfg.rand,
		lastHard:      proto.Clone(hs).(*HardState),
		logger:        cfg.logger.With("node_id", cfg.id),
	}
	c.becomeFollower(c.term, none)
	return c, nil
}

// tick advances the core's notion of time by one unit.
func (c *core) tick() {
	if c.role == Leader {
		c.tickLeader()
		return
	}
	c.electionElapsed++
	if c.electionElapsed >= c.randomizedElectionTimeout {
		c.campaign()
	}
}

func (c *core) tickLeader() {
	c.electionElapsed++
	if c.electionElapsed >= c.electionTick {
		c.electionElapsed = 0
		if !c.quorumActive() {
			// We may be on the minority side of a partition. Stepping down stops
			// this node from accepting writes it can never commit, and sends its
			// clients looking for the real leader.
			c.logger.Warn("raft: lost contact with a majority; stepping down", "term", c.term)
			c.becomeFollower(c.term, none)
			return
		}
	}
	c.heartbeatElapsed++
	if c.heartbeatElapsed >= c.heartbeatTick {
		c.heartbeatElapsed = 0
		// A heartbeat carries whatever each follower is still missing, so it is
		// also how lost messages get re-sent.
		c.broadcastAppend()
	}
}

// quorumActive reports whether a majority (counting this node) answered since
// the last check, and resets the tally for the next interval.
func (c *core) quorumActive() bool {
	active := 1
	for _, id := range c.peers {
		if c.progress[id].recentActive {
			active++
		}
		c.progress[id].recentActive = false
	}
	return active >= c.quorum
}

// campaign starts an election: a new term, a vote for ourselves, and a vote
// request to everyone else.
func (c *core) campaign() {
	c.becomeCandidate()
	if c.quorum == 1 {
		c.becomeLeader() // a one-node cluster is its own majority
		return
	}
	for _, id := range c.peers {
		c.send(id, &Envelope_RequestVote{RequestVote: &RequestVoteRequest{
			Term:         c.term,
			CandidateId:  c.id,
			LastLogIndex: c.log.lastIndex(),
			LastLogTerm:  c.log.lastTerm(),
		}})
	}
}

// reset moves to term, forgetting the vote only if the term actually changed:
// within one term a vote is final.
func (c *core) reset(term uint64) {
	if c.term != term {
		c.term = term
		c.vote = none
	}
	c.lead = none
	c.electionElapsed = 0
	c.heartbeatElapsed = 0
	c.randomizedElectionTimeout = c.electionTick + c.rand.IntN(c.electionTick)
	c.votes = nil
	c.progress = nil
	c.termStart = 0
}

func (c *core) becomeFollower(term uint64, lead string) {
	c.reset(term)
	c.role = Follower
	c.lead = lead
}

func (c *core) becomeCandidate() {
	c.reset(c.term + 1)
	c.role = Candidate
	c.vote = c.id
	c.votes = map[string]bool{c.id: true}
	c.logger.Info("raft: election timeout; starting election", "term", c.term)
}

func (c *core) becomeLeader() {
	c.reset(c.term)
	c.role = Leader
	c.lead = c.id
	c.progress = make(map[string]*progress, len(c.peers))
	for _, id := range c.peers {
		// Optimistic: assume followers are up to date and back off on rejection.
		c.progress[id] = &progress{next: c.log.lastIndex() + 1}
	}
	// A new leader can't count replicas of older terms' entries to commit them
	// (§5.4.2). Appending an empty entry in its own term and committing that
	// commits everything before it too, and tells the leader where the commit
	// point really is (§8).
	c.termStart = c.log.lastIndex() + 1
	c.log.append(&LogEntry{Index: c.termStart, Term: c.term})
	c.logger.Info("raft: won election; now leader", "term", c.term, "term_start", c.termStart)
	// Announce at once rather than waiting a heartbeat interval, so the other
	// candidates in this term stand down before their own timers fire.
	c.broadcastAppend()
}

// propose appends data as new entries in the leader's log and starts
// replicating them, returning the index of the first. term pins the proposal
// to one term: a caller acting on behalf of an older leadership is refused,
// even if this node has since been re-elected.
func (c *core) propose(term uint64, data ...[]byte) (uint64, error) {
	if c.role != Leader {
		return 0, fmt.Errorf("%w: node %s is a %s in term %d (leader %q)", ErrNotLeader, c.id, c.role, c.term, c.lead)
	}
	if term != c.term {
		return 0, fmt.Errorf("%w: node %s leads term %d, not term %d", ErrNotLeader, c.id, c.term, term)
	}
	first := c.log.lastIndex() + 1
	for i, d := range data {
		c.log.append(&LogEntry{Index: first + uint64(i), Term: c.term, Data: d})
	}
	c.broadcastAppend()
	return first, nil
}

func (c *core) broadcastAppend() {
	for _, id := range c.peers {
		c.sendAppend(id)
	}
}

// sendAppend sends one follower the entries from its next index onward (none,
// for a follower that is up to date: then it is a pure heartbeat).
func (c *core) sendAppend(to string) {
	pr := c.progress[to]
	prev := pr.next - 1
	prevTerm, ok := c.log.term(prev)
	if !ok {
		panic(fmt.Sprintf("raft: leader %s has no term for index %d (log %d..%d)", c.id, prev, c.log.offset, c.log.lastIndex()))
	}
	hi := min(c.log.lastIndex()+1, pr.next+maxEntriesPerMsg)
	c.send(to, &Envelope_AppendEntries{AppendEntries: &AppendEntriesRequest{
		Term:         c.term,
		LeaderId:     c.id,
		PrevLogIndex: prev,
		PrevLogTerm:  prevTerm,
		// A copy: the log's backing array may be overwritten after a later
		// conflict while this message still waits in a transport queue.
		Entries:      slices.Clone(c.log.slice(pr.next, hi, maxBytesPerMsg)),
		LeaderCommit: c.log.committed,
	}})
}

// maybeCommit advances the commit index to the highest entry stored on a
// majority, counting this leader only for entries already on its own disk.
func (c *core) maybeCommit() {
	matches := []uint64{c.log.stabled}
	for _, id := range c.peers {
		matches = append(matches, c.progress[id].match)
	}
	slices.Sort(matches)
	n := matches[len(matches)-c.quorum] // a majority holds n or more
	if n <= c.log.committed {
		return
	}
	// Only an entry from the current term is committed by counting replicas.
	// An older-term entry on a majority can still be overwritten by a leader
	// that never saw it (paper Figure 8); it commits indirectly once an entry
	// of ours after it does.
	if t, _ := c.log.term(n); t != c.term {
		return
	}
	c.log.committed = n
}

// inLease reports whether this node heard from a live leader within the
// minimum election timeout. Such a node ignores vote requests, even ones with
// a higher term (§6): a server that was partitioned away and keeps timing out
// would otherwise depose a perfectly healthy leader every time it reconnects.
func (c *core) inLease() bool {
	return c.lead != none && c.electionElapsed < c.electionTick
}

// step processes one message from a peer.
func (c *core) step(m *Envelope) {
	term := messageTerm(m)
	switch {
	case term > c.term:
		if _, ok := m.GetBody().(*Envelope_RequestVote); ok && c.inLease() {
			c.logger.Debug("raft: ignoring vote request while a leader is live",
				"from", m.GetFrom(), "their_term", term, "term", c.term)
			return
		}
		// Any message from a newer term means our term is over: whatever role we
		// held, we are now a follower in theirs. This single rule is what makes a
		// deposed leader harmless.
		lead := none
		if _, ok := m.GetBody().(*Envelope_AppendEntries); ok {
			lead = m.GetFrom()
		}
		c.becomeFollower(term, lead)
	case term < c.term:
		// A stale leader or candidate. Answer with our term so it learns it has
		// fallen behind and steps down; stale responses are simply dropped.
		switch m.GetBody().(type) {
		case *Envelope_AppendEntries:
			c.send(m.GetFrom(), &Envelope_AppendEntriesResponse{AppendEntriesResponse: &AppendEntriesResponse{Term: c.term}})
		case *Envelope_RequestVote:
			c.send(m.GetFrom(), &Envelope_RequestVoteResponse{RequestVoteResponse: &RequestVoteResponse{Term: c.term}})
		}
		return
	}

	switch b := m.GetBody().(type) {
	case *Envelope_RequestVote:
		c.handleRequestVote(m.GetFrom(), b.RequestVote)
	case *Envelope_RequestVoteResponse:
		c.handleVoteResponse(m.GetFrom(), b.RequestVoteResponse)
	case *Envelope_AppendEntries:
		c.handleAppendEntries(m.GetFrom(), b.AppendEntries)
	case *Envelope_AppendEntriesResponse:
		c.handleAppendResponse(m.GetFrom(), b.AppendEntriesResponse)
	}
}

func (c *core) handleRequestVote(from string, req *RequestVoteRequest) {
	// One vote per term: we can vote for this candidate if we haven't voted (and
	// don't already know a leader for this term), or if we already voted for it
	// and are seeing a retransmitted request.
	canVote := c.vote == from || (c.vote == none && c.lead == none)
	grant := canVote && c.log.isUpToDate(req.GetLastLogIndex(), req.GetLastLogTerm())
	if grant {
		c.vote = from
		c.electionElapsed = 0 // granting a vote resets the election timer (Figure 2)
	}
	c.logger.Debug("raft: vote request", "from", from, "term", c.term, "granted", grant)
	c.send(from, &Envelope_RequestVoteResponse{RequestVoteResponse: &RequestVoteResponse{Term: c.term, VoteGranted: grant}})
}

func (c *core) handleVoteResponse(from string, resp *RequestVoteResponse) {
	if c.role != Candidate {
		return // late answer to an election that is already decided
	}
	c.votes[from] = resp.GetVoteGranted()
	granted, rejected := 0, 0
	for _, ok := range c.votes {
		if ok {
			granted++
		} else {
			rejected++
		}
	}
	switch {
	case granted >= c.quorum:
		c.becomeLeader()
	case rejected >= c.quorum:
		// This election is lost. Wait for the next timeout rather than retrying
		// at once, which gives a better-placed candidate the chance to win.
		c.becomeFollower(c.term, none)
	}
}

func (c *core) handleAppendEntries(from string, req *AppendEntriesRequest) {
	// Only a leader sends AppendEntries, and there is one leader per term, so a
	// candidate in this term has lost and a follower now knows who leads.
	if c.role == Leader {
		c.logger.Error("raft: two leaders in one term; ignoring", "other", from, "term", c.term)
		return
	}
	if c.role == Candidate {
		c.becomeFollower(c.term, from)
	}
	c.lead = from
	c.electionElapsed = 0

	prev, prevTerm, ents := req.GetPrevLogIndex(), req.GetPrevLogTerm(), req.GetEntries()
	if prev < c.log.committed {
		// Our committed prefix is identical to the leader's (Log Matching plus
		// Leader Completeness), so a delayed or duplicated request overlapping
		// it is checked from our commit point instead.
		skip := min(c.log.committed-prev, uint64(len(ents)))
		ents = ents[skip:]
		prev = c.log.committed
		prevTerm, _ = c.log.term(prev)
	}
	if !c.log.matchTerm(prev, prevTerm) {
		c.send(from, &Envelope_AppendEntriesResponse{AppendEntriesResponse: &AppendEntriesResponse{
			Term: c.term, ConflictIndex: c.log.conflictHint(prev),
		}})
		return
	}
	// Entries we already hold are skipped, not rewritten: a reordered, older
	// request must never truncate entries a newer one already delivered.
	if ci := c.log.findConflict(ents); ci != 0 {
		if ci <= c.log.committed {
			c.logger.Error("raft: refusing to overwrite a committed entry", "index", ci, "committed", c.log.committed, "leader", from)
			return
		}
		c.log.truncateAndAppend(ents[ci-ents[0].GetIndex():])
	}
	lastNew := prev + uint64(len(ents))
	if commit := min(req.GetLeaderCommit(), lastNew); commit > c.log.committed {
		c.log.committed = commit
	}
	c.send(from, &Envelope_AppendEntriesResponse{AppendEntriesResponse: &AppendEntriesResponse{
		Term: c.term, Success: true, MatchIndex: lastNew,
	}})
}

func (c *core) handleAppendResponse(from string, resp *AppendEntriesResponse) {
	if c.role != Leader {
		return
	}
	pr, ok := c.progress[from]
	if !ok {
		return
	}
	pr.recentActive = true
	if resp.GetSuccess() {
		if resp.GetMatchIndex() > pr.match {
			pr.match = resp.GetMatchIndex()
			c.maybeCommit()
		}
		pr.next = max(pr.next, pr.match+1)
		if pr.next <= c.log.lastIndex() {
			c.sendAppend(from) // keep streaming to a follower that is catching up
		}
		return
	}
	// Rejected: back up to the follower's hint (never below what it is known to
	// hold) and try again straight away.
	hint := resp.GetConflictIndex()
	if hint == 0 || hint >= pr.next {
		hint = pr.next - 1 // a stale or hint-less rejection: step back by one
	}
	pr.next = max(pr.match+1, hint)
	c.sendAppend(from)
}

func (c *core) send(to string, body isEnvelope_Body) {
	c.msgs = append(c.msgs, &Envelope{From: c.id, To: to, Body: body})
}

func (c *core) hardState() *HardState {
	return &HardState{CurrentTerm: c.term, VotedFor: c.vote}
}

func (c *core) hasReady() bool {
	return len(c.msgs) > 0 || !proto.Equal(c.hardState(), c.lastHard) ||
		len(c.log.unstable()) > 0 || len(c.log.nextCommitted()) > 0
}

// ready collects the work produced since the last advance.
func (c *core) ready() Ready {
	rd := Ready{
		Entries:          c.log.unstable(),
		Messages:         c.msgs,
		CommittedEntries: c.log.nextCommitted(),
	}
	if hs := c.hardState(); !proto.Equal(hs, c.lastHard) {
		rd.HardState = hs
	}
	return rd
}

// advance tells the core the driver has handled everything in rd.
func (c *core) advance(rd Ready) {
	if rd.HardState != nil {
		c.lastHard = rd.HardState
	}
	if n := len(rd.Entries); n > 0 {
		c.log.stabled = rd.Entries[n-1].GetIndex()
		if c.role == Leader {
			c.maybeCommit() // our own copy now counts towards the majority
		}
	}
	if n := len(rd.CommittedEntries); n > 0 {
		c.log.handedOut = rd.CommittedEntries[n-1].GetIndex()
	}
	c.msgs = c.msgs[len(rd.Messages):]
}

// status reports Applied as what the core has handed out, which is exact for
// a driver that applies synchronously; Node replaces it with what its apply
// goroutine has actually finished.
func (c *core) status() Status {
	return Status{
		ID: c.id, Term: c.term, Role: c.role, Leader: c.lead,
		Commit: c.log.committed, Applied: c.log.handedOut, termStart: c.termStart,
	}
}

// messageTerm extracts the sender's term, which every Raft message carries.
func messageTerm(m *Envelope) uint64 {
	switch b := m.GetBody().(type) {
	case *Envelope_RequestVote:
		return b.RequestVote.GetTerm()
	case *Envelope_RequestVoteResponse:
		return b.RequestVoteResponse.GetTerm()
	case *Envelope_AppendEntries:
		return b.AppendEntries.GetTerm()
	case *Envelope_AppendEntriesResponse:
		return b.AppendEntriesResponse.GetTerm()
	default:
		return 0
	}
}
