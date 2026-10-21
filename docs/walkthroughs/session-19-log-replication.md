# Session 19: Raft, part 2. Copying the log

*Plain-English walkthrough for a Node/TS developer who is new to Go and new to
distributed consensus. Session 18 gave the cluster a leader. This session gives
the leader something to lead: a log that every node ends up holding, entry for
entry, in the same order.*

## The log is the whole point

A Raft log is an append-only array of commands, numbered from 1. Each entry
records the command, its index, and the **term** of the leader that created
it:

```
index:  1    2    3    4    5
term:   1    1    2    2    2
data:   ∅   "a"  ∅   "b"  "c"
```

If you've used Redux, it's the action log. If you've used Kafka, it's one
partition. The promise Raft makes is: every node applies the same entries in
the same order, so every node's state ends up the same. In Session 21 those
commands become workflow history events, and "every node's state" becomes
"every node's copy of every workflow's history".

Only the leader adds entries. Followers copy them.

## Copying: AppendEntries with a safety check

The leader sends each follower the entries it's missing, in an
`AppendEntries` message. The important part is what comes *with* them:

```
prevLogIndex = 3, prevLogTerm = 2, entries = [4: "b", 5: "c"]
```

"Here are entries 4 and 5. They go right after an entry at index 3 from term
2." The follower only accepts if its own log has exactly that at index 3.
Otherwise it says no, and the leader backs up and tries an earlier starting
point.

Git is the best analogy. The follower's log is a branch, the leader's log is
`main`. A fast-forward only works if the follower's HEAD is the parent of the
new commits. If it isn't, the follower has commits `main` doesn't have, and
those get thrown away (a reset to the common ancestor, then a fast-forward).

That one check, applied every time, gives you the **Log Matching** property: if
two logs have an entry with the same index and term, they are identical up to
that point. Proof by induction, and the check is the induction step.

## When does an entry count? A majority on disk

An entry is **committed** once the leader knows a majority of nodes have it on
disk. Committed entries are never lost and never changed, so only committed
entries are handed to the state machine.

If you've used MongoDB replica sets, you already know this: it's
`writeConcern: { w: "majority" }`. The write isn't acknowledged until most
nodes have it, because then any future primary (elected by a majority) must
overlap with one node that has it.

The leader tracks two numbers per follower:

- `match`: the highest index known to be on that follower.
- `next`: the index it will send next.

Sort all the `match` values (plus the leader's own durable last index), pick
the one a majority has reached, and that's the candidate commit index. The
code is short:

```go
matches := []uint64{c.log.stabled}            // the leader counts only what's on its own disk
for _, id := range c.peers {
    matches = append(matches, c.progress[id].match)
}
slices.Sort(matches)
n := matches[len(matches)-c.quorum]            // a majority holds n or more
```

## The subtle rule: only your own term's entries commit by counting

This is the part of Raft that trips everyone up (it's Figure 8 in the paper),
so it gets a test of its own.

The rule: a leader may only commit an entry **from its own term** by counting
replicas. Entries from older terms commit indirectly, when a later entry from
the current term commits.

Why? Here is the story `TestFigure8OldTermEntryNotCommittedByCount` replays:

1. n1 leads term 1 and gets entry X onto n2, but no further.
2. n1 crashes. n5 wins term 2 (votes from n3 and n4), writes its own entry at
   the same index, and crashes before sending it.
3. n1 comes back and wins term 3. It copies X to n3. X is now on n1, n2 and
   n3: a majority.
4. If n1 committed X now, it would apply it. But then n1 crashes, n5 comes
   back, and n5 can win term 4: its last entry is from term 2, which beats
   the term-1 X that n3 and n4 hold. n5 overwrites X everywhere.

A committed, applied entry just disappeared. That's the bug the rule prevents.

The practical fix is something every Raft leader does: **the moment it wins,
it appends an empty entry in its own term**. Once that empty entry commits,
everything before it commits too. It also tells a new leader where the commit
point really is, which matters again in Session 21 (the engine only starts on
a new leader after that empty entry has been applied).

## Repairing followers that went off track

`TestFigure7FollowersConverge` rebuilds Figure 7 from the paper: a new leader
and six followers whose logs are missing entries, have extra entries from
terms that never committed, or both. The leader then just does its normal job
(send, get rejected, back up, send again) with no special repair mode, and
every follower ends up with the leader's exact log.

Backing up one entry at a time would be slow, so a follower that rejects also
sends a **hint**: "my log has a different term at that index; that term starts
at index 7, retry from there." The leader then skips a whole term per round
trip instead of a single entry.

Two details that are easy to get wrong:

- **A follower never deletes entries just because a message didn't mention
  them.** Messages can arrive out of order. An old, delayed `AppendEntries`
  carrying entries 1 to 3 must not cut off entries 4 and 5 that a newer
  message already delivered. The follower only truncates at a real conflict
  (same index, different term). `TestStaleAppendDoesNotTruncate` pins this.
- **The follower's commit index is capped at the last entry it just checked.**
  The leader says "I've committed up to 5". If the follower only verified
  entries up to 3 in this message, its own entries 4 and 5 might be stale
  leftovers that don't match the leader. Committing them would apply the
  wrong thing. `TestCommitCappedAtLastNewEntry` pins this.

## The log on disk

The Raft log is a file in `<data-dir>/raft/log`, using the same
`[length][CRC32C][payload]` records as the workflow histories (the
`internal/record` package from Session 18 now has three users). The storage
keeps the byte offset of every entry in memory, so when a follower has to
throw away a conflicting suffix, it truncates the file at that entry's offset
and appends. A torn last record from a crash is cut off on open, exactly like a
history file. A bad checksum in the middle refuses to start the node.

Each batch of entries is **one write and one fsync**. That matters, because
fsync is the expensive part (milliseconds, much more on a Mac). If ten
proposals arrive while the node is busy, the driver hands all ten to the core
at once, and they share one disk write and one round of messages.

The persist-then-send rule from Session 18 extends to entries: a follower writes
the entries to disk before it answers "got them", because the leader counts
that answer towards a majority.

## Applying, on a separate goroutine

Committed entries go to the **state machine**, whatever is being replicated.
For now that's a test object that records payloads. Applying can be slow
(Session 21's state machine fsyncs a history file per event), and a slow apply
must not delay heartbeats, or followers would think the leader died. So the
`Node` runs two goroutines:

- the **run loop** owns the core, persists, sends, and pushes committed
  entries onto a queue;
- the **apply loop** pops entries and applies them in order.

Node analogy: it's a request handler that enqueues a job and a worker that
processes the queue, so the HTTP thread stays responsive.

## Proposing, and the three ways it can fail

```go
result, err := node.Propose(ctx, term, data)
```

`Propose` waits until **this node** has applied the entry, then returns what
the state machine returned. The `term` argument is a fencing token: you're
saying "I'm acting for the leadership of term 7". If the node no longer leads
term 7 (even if it has since won term 9), the proposal is refused. Session 21
needs this: each leadership gets a fresh engine, and an engine left over from
an older term must never write.

The errors are deliberately different, because they mean different things:

| Error | Meaning | Did it happen? |
|---|---|---|
| `ErrNotLeader` | Refused up front. | No, definitely. |
| `ErrProposalDropped` | A new leader overwrote the entry. | No, definitely. |
| `ErrLeadershipLost` | Leadership ended while waiting. | **Unknown.** |

The third one is the honest answer to a hard question. The entry might have
reached a majority just before the leader lost its job, in which case the next
leader commits it. Or it might not have. It's like a payment API timing out:
you don't know whether the card was charged, so you need an idempotency key
before you retry. Workflowd's version of that key is the workflow id plus the
event id, and Session 21 explains how the engine uses it.

## Tests, and three lessons about tests

The simulated cluster now persists entries and applies committed ones, and the
safety checker gained **State Machine Safety**: if any node applies an entry at
index 7, no node may ever apply a different entry at index 7. The randomized
chaos run now proposes commands too (to every node that thinks it leads,
including stale leaders, whose commands must never apply), then stops the
chaos, proposes one last command and demands that every node applied exactly
the same sequence.

On top of that, real 3-node gRPC tests: 200 concurrent proposals, a follower
that misses 50 entries and catches up, a leader killed mid-stream with the new
leader carrying on, and a full cluster restart where every node rebuilds its
state machine from its own log.

Mutation results (break the rule on purpose, see which test catches it):

| Broken rule | Caught by |
|---|---|
| Skip the prevLogIndex/prevLogTerm check | the randomized test (panics on a log gap) |
| Truncate even when entries match | `TestStaleAppendDoesNotTruncate`, plus a livelock in the randomized test |
| Follower commit not capped at last new entry | `TestCommitCappedAtLastNewEntry` |
| Commit old-term entries by counting | `TestFigure8OldTermEntryNotCommittedByCount` |
| Vote without the up-to-date log check | the randomized test (State Machine Safety) |

The lessons:

1. **My first Figure 8 test passed for the wrong reason.** It forgot that a
   leader's first act is the empty entry, so every index was off by one and
   the dangerous entry never reached the right nodes. With the bug put back
   in, the test still passed. Only the mutation run showed it was testing
   nothing. Always break the code once and watch the test go red.
2. **A broken harness can hang instead of failing.** The "truncate even when
   matching" bug made a leader and follower answer each other forever.
   `deliverAll` now gives up after 100,000 messages and reports a livelock, so
   that failure takes 0.1 seconds instead of the 10-minute test timeout.
3. **Coverage of a randomized test is something you measure.** In Session 18
   the schedule produced fewer than 2 elections per run until I measured it.
   Now each run averages about 8.5 elections and 22 applied commands.

## Go things that showed up

- **Labelled `break`.** `break drain` exits the outer `for` loop from inside a
  `select`, where a plain `break` would only leave the `select`.
- **`sync.WaitGroup`** to know when both goroutines have exited before
  closing `done`, like `Promise.all` over two workers.
- **Slice aliasing.** A Go slice is a view onto an array, and two slices can
  share the same array. Building an outgoing message straight from a slice of
  the log would share the log's array, so a later in-place overwrite (a
  conflict repair after the node steps down) could change a message still
  waiting in the transport queue. So the message gets its own copy,
  `slices.Clone`. JS arrays don't have this trap, which is exactly why it's
  worth knowing about.

## Try it

`make cluster` still runs the 3 nodes, and `/api/health` now shows
`commit_index` and `applied_index`. Each election commits the new leader's empty
entry, so killing the leader and watching those numbers move is the visible
part. Cluster mode still doesn't serve the workflow API; that's Session 21.

## What's next

Session 20: proving all five of Raft's safety properties hold under random
failure, plus snapshots, so the log doesn't grow forever and a node that was
down for a long time can catch up from a snapshot instead of from entry 1.
