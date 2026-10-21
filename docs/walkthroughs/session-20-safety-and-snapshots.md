# Session 20: Raft, part 3. Proving it's safe, and snapshots

*Plain-English walkthrough for a Node/TS developer who is new to Go and new to
distributed consensus. Two halves today: first, turning Raft's five safety
promises into checks that run against thousands of random failure schedules;
second, snapshots, so the log stops growing forever.*

## The five promises

The Raft paper (Figure 3) lists five properties that hold no matter what the
network or the machines do (short of losing a majority of disks):

1. **Election Safety**: at most one leader per term.
2. **Leader Append-Only**: a leader never rewrites or deletes its own log; it
   only appends.
3. **Log Matching**: if two logs have an entry with the same index and term,
   everything up to that entry is identical.
4. **Leader Completeness**: once an entry is committed, every later leader has
   it.
5. **State Machine Safety**: if one node applies an entry at index 7, no node
   ever applies a different entry at index 7.

Number 5 is the one users care about ("all copies agree"). The other four are
the scaffolding that makes it true.

## Turning them into a checker

The simulated cluster from Session 18 (no clock, no goroutines, seeded
randomness) already had a `safetyChecker` that watched for two leaders in one
term. It now watches all five:

- **Election Safety**: remember which node led each term.
- **Leader Append-Only**: while a node leads a term, keep a copy of its log;
  at every later step, every entry in that copy must still be there,
  unchanged (unless it was folded into a snapshot).
- **Log Matching**: for every pair of nodes, find the highest index where both
  logs have the same term, then check every entry below it is identical. This
  one is expensive, so the random test runs it every 10 steps.
- **Leader Completeness**: every time an entry becomes committed on any node,
  record it. The moment a node is first seen leading a term, it must hold
  every recorded entry.
- **State Machine Safety**: record what each index applied to; any different
  entry applied at the same index anywhere is a violation. Restoring a
  snapshot counts as applying everything in it.

Then the randomized test does what it did before (random ticks, dropped,
duplicated and reordered messages, crashes, restarts, partitions, proposals),
plus random snapshots, and the checker watches every step of 200 schedules.

Node analogy: this is property-based testing, what `fast-check` does for JS. You
don't write examples; you write the invariant and let a generator hunt for a
counterexample.

## Proving the checker can fail

A checker that has never caught anything might simply be broken. So the core
now has three **testing knobs**, each switching off exactly one rule:

```go
type testingKnobs struct {
    grantStaleVotes   bool // vote without the up-to-date log check
    skipPrevTermCheck bool // check prevLogIndex but not its term
    countOldTerms     bool // commit older-term entries by counting replicas
}
```

Production never sets them (the zero value is the correct algorithm), and each
has a permanent test asserting the checker notices:

| Knob | What the checker reports |
|---|---|
| `grantStaleVotes` | Leader Completeness: a leader missing a committed entry |
| `skipPrevTermCheck` | Log Matching: two logs that agree at index 36 but differ at 32 |
| `countOldTerms` (Figure 8 schedule) | Leader Completeness, then State Machine Safety: an applied entry overwritten |
| a leader rewriting its own entry (done to the state directly) | Leader Append-Only |
| a node forgetting its vote (Session 18) | Election Safety |

So each of the five properties has at least one test where it fires on a
broken implementation and stays silent on the real one. That's the difference
between "the tests pass" and "the tests would notice".

One honest detail: `countOldTerms` never showed up in random schedules (0 out of
200). The empty entry every new leader appends makes the dangerous Figure 8
situation very rare by chance, so its teeth test replays the exact Figure 8
schedule from Session 19 instead. Random testing finds a lot, not everything.

## Snapshots: why the log can't grow forever

Every history write in Session 21 will be a log entry. Without a limit, the log
grows forever, a restarted node replays it all from entry 1, and a node that was
down for a week needs a week of entries sent to it one batch at a time.

A **snapshot** is the state machine's whole state, serialized, as of some log
index. Once it's safely on disk, every log entry up to that index can be
deleted: the snapshot replaces them.

Node analogy: it's what Redis does with RDB dumps next to its append-only file,
or a Redux store where you save `getState()` and then throw away the actions
that produced it.

### Taking one

The `Node` counts applied entries. Every `SnapshotEvery` entries (default 8,192)
the apply goroutine asks the state machine for `Snapshot()`, and hands the bytes
to the run loop, which:

1. writes the snapshot file (temp file, fsync, rename, fsync the directory);
2. rewrites the log file without the covered entries (the same atomic dance);
3. tells the core to drop those entries from memory.

The order matters. If the machine dies between 1 and 2, the log still has
entries the snapshot already covers, and loading simply skips them. If it were
the other way round, a crash could leave entries deleted with no snapshot to
replace them.

### Sending one: InstallSnapshot

A follower that comes back after the leader compacted its log is missing
entries the leader no longer has. So the leader sends its snapshot instead, in
an `InstallSnapshot` message (paper section 7), and then carries on with normal
`AppendEntries` from the entry after it.

The follower has two cases:

- **It already has the snapshot's last entry** (same index, same term). Then it
  keeps its own log and just learns that everything up to there is committed.
- **It doesn't**. Then its log is useless: it throws all of it away, writes the
  snapshot, empties its log file, and restores its state machine from the
  snapshot before applying anything newer.

### A bug the mutation run found

Breaking one rule on purpose ("install a snapshot even if we're already past
it") failed no test at all. That looked harmless until I thought about message
duplication: a follower installs a snapshot at index 5, receives and
acknowledges entries 6 to 8 (the leader counts them towards a commit), and then
a delayed duplicate of the same snapshot arrives. Installing it again throws
away entries 6 to 8, which the leader may already have counted. That can lose a
committed entry. `TestStaleSnapshotIgnored` now replays exactly that sequence,
and fails if the rule is removed.

### Two lessons from the real network test

The gRPC tests now include a follower catching up from a snapshot over real
sockets, and a 5 MB snapshot (gRPC refuses messages over 4 MB unless you raise
the limit; the test fails if you don't). Getting them stable taught me two
things:

1. **A leader can step down because a follower is slow, not dead.** On this
   Mac every fsync takes several milliseconds, and the follower was doing one
   fsync per message. With the test's 100 ms quorum window, its answers came
   back too late, and the leader decided it had lost its majority. The fix is a
   real improvement, not a test hack: the run loop now takes every message
   already waiting before it writes to disk, so a burst costs one fsync.
2. **Measure before you optimise.** The 5 MB test took 30 seconds. My first
   guess (the leader re-sending the snapshot too often) was a real flaw and got
   fixed, but it wasn't the cause: the test took 1.5 seconds without `-race`.
   The race detector makes copying megabytes about 20 times slower. One 5 MB
   entry instead of twelve 512 KB ones proves the same thing in 2.5 seconds.

## ADR-0004: how the engine will use all this

Before wiring Raft into the engine (Session 21), the design is written down in
`docs/adr/0004-replicated-history-leader-only-engine.md`. The short version:

- The log carries **history events**, the engine's outputs, not client
  requests. Followers never run workflow logic.
- Every node's **history directory is the state machine**. Each applied event
  remembers the Raft index that committed it, so re-applying after a restart
  is safe.
- **Only the leader runs an engine**, a brand-new one per term, started only
  after the leader has applied everything committed before its term. To the
  engine, a leader change looks exactly like a restart, which it has handled
  since the Kill Test in Phase 1.
- **Followers reject client calls with a hint** naming the leader.
- **At-least-once still holds**: activities can run twice, a step is never
  recorded twice, and now a client can get an error for a write that did land,
  so retries should carry a workflow id.

## Go things that showed up

- **`slices.Clone` to free memory.** Compacting the log does
  `slices.Clone(entries[n:])` instead of just `entries[n:]`. Re-slicing keeps
  the whole old array alive (the slice still points into it), so the dropped
  entries would never be garbage-collected.
- **`os.File.ReadAt`** reads from an offset without moving the file position,
  which the log uses to copy out the entries it keeps during compaction.
- **gRPC options**: `grpc.MaxRecvMsgSize` on the server and
  `grpc.MaxCallSendMsgSize` on the client, exposed as `raft.ServerOptions()` so
  every server that carries Raft traffic gets them.

## What's next

Session 21: the engine moves onto Raft. History writes go through the log,
only the leader runs the engine, followers hint, and chaos tests kill and
partition real processes while 100 workflows run.
