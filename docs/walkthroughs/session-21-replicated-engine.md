# Session 21: Raft, part 4. The engine goes replicated

*Plain-English walkthrough for a Node/TS developer who is new to Go and new to
distributed consensus. Three sessions built Raft on its own. This one puts the
workflow engine on top of it, then tries hard to break the result: killing the
leader mid-workflow, cutting it off from the network, and running 100 workflows
through a leader change.*

## What you can do now

```bash
make cluster
```

starts three nodes on your laptop. Each runs the full `workflowd`: gRPC,
dashboard, JSON API, metrics. One of them is the leader. Start an OrderSaga on
the leader and watch it run (the leader also runs the demo worker). Ask a
follower for anything and it answers `503` with the leader's address:

```
GET http://127.0.0.1:8234/api/workflows
503  X-Workflowd-Leader: n3
{"error":"server: not the leader; leader is n3 ...","leader_http":"127.0.0.1:8235", ...}
```

Now kill the leader's process in the middle of a workflow. In my run, another
node was serving 1.2 seconds later, picked the workflow up exactly where it was
(halfway through a durable timer), and finished it 5.8 seconds after the kill.
The two surviving nodes' history files for that workflow were byte-for-byte
identical.

Without `--peers`, nothing changed: single-node mode is the same code path it
was, every existing test passes untouched, and production still runs single-node.

## Decision 1: replicate results, not requests

The engine is not deterministic. It stamps wall-clock times, picks random ids
and lease tokens, and computes timer deadlines from `time.Now()`. If every node
ran the engine on the same requests, they'd write different histories.

So only the leader runs the engine, and what goes into the Raft log is what the
engine *decided*: one history event per log entry (`AppendEvent{workflow_id,
event}`), plus deletes for retention. Followers never run workflow logic; they
just store events.

You know this pattern from MongoDB. A replica set's oplog doesn't record
`{$inc: {count: 1}}`; it records the resulting `{$set: {count: 6}}`, so
secondaries apply results and can re-apply them safely. Same idea here.

## Decision 2: the history folder is the state machine

Session 19's `StateMachine` interface gets a real implementation in
`internal/cluster/statemachine.go`. Every node applies each committed event to
its own history store, so every node ends with the same `.history` files.

Two rules make that safe:

- **Every applied event is stamped with its Raft index** (a new optional field,
  `raft_index`, that old binaries simply ignore). After a restart, a node
  re-applies its log from the last snapshot; the stamp on a file's last event
  says which entries are already in it, so nothing is written twice.
- **An event only goes in at the next position.** If an event says it is event 5
  and the history has 4 events, it's appended; otherwise every node rejects it,
  identically. It's the same idea as the polyglot `history_length` fence from
  Session 13, and as an optimistic-lock version check.

## Decision 3: a brand-new engine every time a node becomes leader

When a node wins an election, it does **not** start serving immediately. First
it waits until it has applied the empty entry it opened its term with
(`LeaderReady`, from Session 19). At that moment it has applied every entry
committed before its term, so its history folder is complete. Then it builds a
fresh `Engine` and runs `Recover()`, the same crash recovery the engine has had
since the Kill Test in Phase 1.

To the engine, a leader change is just a restart on a different machine. That
reuse is the nicest thing about this design: no new failover logic, just the
recovery path that's already been tested for months.

When a node stops being leader, its engine is closed: its timers stop, its long
polls end with "not the leader", and any write it was waiting on fails.

Every engine is also **fenced by its term**. Its writes say "I'm acting for term
7", and Raft refuses them once the node no longer leads term 7, even if it has
since won term 9. An engine left over from an older term can never write.

## The write path

The engine used to call `store.Append` directly. Now it calls an interface:

```go
type HistoryLog interface {
    Append(workflowID string, ev *history.HistoryEvent) error
    Delete(workflowID string) error
}
```

Single-node mode passes the store itself (nothing changes). Cluster mode passes
a `replicatedLog` that proposes the event to Raft and returns once this node has
applied it, so the engine's next read sees its own write. If leadership changes
while it waits, the write fails with "not the leader", and the client gets
`Unavailable` / `503` with a hint, not a 500.

In Express terms: the routes didn't change, only the repository behind them.

## Routing: followers point at the leader

Both API layers now ask an `EngineSource` which engine should serve each call. In
single-node mode that's always the one engine. In a cluster it's the current
term's engine on the leader, and a `NotLeaderError{LeaderHint}` everywhere else.

- gRPC answers `Unavailable` with the leader in trailers (`workflowd-leader`,
  `workflowd-leader-grpc`, `workflowd-leader-http`).
- HTTP answers `503` with the leader in a header and the JSON body, or
  `Retry-After: 1` while there's no leader yet.
- `/api/health` is answered by every node and shows its cluster view.

How does a follower know the leader's *client* address, when `--peers` only lists
Raft addresses? Every Raft message carries the sender's client addresses in gRPC
metadata, added by a client interceptor and read by a server interceptor. If
you've written Express middleware that adds a header, that's an interceptor.

## The part that took longest: it was slow

The first 100-workflow run didn't finish in 4 minutes. Instead of guessing, I
measured:

- One fsync on this Mac takes **21 ms**. (Go's `Sync()` on macOS is
  `F_FULLFSYNC`, which flushes the drive's own cache.)
- Each commit was doing **six** fsyncs: the Raft log and the history file, on
  three nodes that all share one laptop disk. That's ~117 ms per commit.

Two fixes, both standard practice:

1. **The leader replicates while it writes** (paper section 10.2.1). It sends the
   new entry to followers at the same time as writing its own copy, and only
   counts itself towards a majority once its write is done.
2. **History files stop fsyncing on every event in cluster mode.** The Raft log
   is already the durable write-ahead log; the history files are a view of it.
   They're fsynced before each snapshot (which is when the log entries behind
   them get deleted), and anything newer is simply re-applied from the log after
   a crash; the `raft_index` stamps already make that safe. etcd's storage
   works the same way.

A test that disables the Raft log's fsync then showed the rest: 100 workflows
in 6.3 seconds instead of minutes. All three nodes fsyncing to one drive is
what made it slow, and on real servers each node has its own disk.

That's also why the chaos tests switch the Raft log's fsync off through
`raft.UnsafeDisableFsyncForTests()`. Those tests kill *processes*, never
machines, and a killed process's writes still reach the disk from the page
cache, so fsync protects nothing they measure. Production never calls it, and
real fsync is still exercised by the storage and in-process cluster tests.

## Chaos tests: real processes, real partitions

`cmd/workflowd/chaos_test.go` starts three real `workflowd` processes. The trick
(the same one as Phase 1's Kill Test) is that the test binary re-runs itself with
an environment variable, and that copy calls `main()` with node flags.

For partitions, every Raft link between two nodes goes through a small TCP proxy
the test owns (one per direction, six in all). "Cut the network between n1 and
the others" means closing those proxies' connections and refusing new ones. No
root, no firewall rules, and the nodes have no idea they're being tested.

The three tests:

| Test | What it proves |
|---|---|
| Kill the leader mid-workflow | SIGKILL with a step in flight; the new leader recovers and finishes it; each step recorded once; the dead node catches up byte-for-byte when restarted |
| Partitioned leader | A write sent straight to an isolated leader fails instead of succeeding; the majority elects a new leader and carries on; after healing, the isolated leader's write exists on no node |
| 100 workflows across a leader change | All 100 complete, every step recorded exactly once, all three nodes byte-identical |

The 100-workflow run reported, for example, that `wf-007/charge` and two
`reserve` steps were *executed* twice. That's at-least-once doing its job: the
worker finished the activity, but its result hadn't committed before the
leader died, so the new leader ran it again. In the history, each step still
appears exactly once. Activities must be idempotent, in a cluster just as on one
node.

## The bug only CI could see: a message snowball

With all of that green locally, the pull request's CI run failed: after the
leader was killed, the new leader committed **one entry per second**, every
time, for four minutes. It never happened on my laptop, even with CPU limits.

I added warnings for slow commits (with each follower's replication progress),
slow disk rounds and slow peer sends, and had the chaos test dump every node's
goroutine stacks when it fails. The next failing run said: commits take about
960 ms, the follower is fully caught up, the leader's Raft goroutines are idle,
and no single send is slow.

The number gave it away: 960 ms is almost exactly **1024 × 0.94 ms**. The queue
of messages waiting for that follower (1024 slots) was full, and every new
message waited behind a thousand older ones.

Why was it full? Two behaviours that are each reasonable on their own:

1. Every heartbeat re-sent all the entries the follower hadn't acknowledged yet
   (that's how lost messages used to get re-sent).
2. Every acknowledgement that still showed the follower behind triggered
   another send.

On a busy CI runner, acknowledgements started arriving later than one
heartbeat. Then each heartbeat sent the same entries again, each copy drew its
own acknowledgement, each acknowledgement drew another send... a snowball,
until the queue was full and stayed full.

The fix is what production Raft implementations do, called **pipelining with
flow control**:

- After sending entries to a follower, the leader optimistically moves that
  follower's `next` past them, so heartbeats stop re-sending them.
- If a message is lost, the follower notices on the *next* message (its
  consistency check fails, because the previous entry is missing) and rejects
  it, and the leader backs up. Messages to one peer arrive in order, so a
  rejection can't be one message overtaking another.
- A window (1024 entries) caps how far ahead of a follower's acknowledgements
  the leader may get, and the per-peer queue shrank to 256 so a slow peer adds
  less latency.

`TestHeartbeatsDoNotResendInflightEntries` pins it: one proposal and ten
heartbeats must send the new entry to each follower exactly once. Before the
fix, it was sent 11 times.

Node analogy: a retry loop without backoff. Every timeout fires another
request; a slow server makes more timeouts; more requests make it slower.

## A bug the chaos test found that had nothing to do with Raft

Running the full test suite (every package in parallel, so the machine is busy),
the 100-workflow test failed now and then: one workflow `FAILED` with
`lease expired: worker "chaos-0" missed its deadline` on a `reserve` step.

The cause was older than Phase 4. A worker long-polls for a task with a
deadline. If that deadline expires at the exact moment a task arrives, the
engine had already taken the task off the queue and leased it to the worker,
but the worker had stopped listening, so the reply went nowhere. The task then
sat leased to nobody for 30 seconds, the lease expired, and that counted as a
failed attempt. `reserve` has no retries in the demo workflow, so the whole
workflow failed.

It's like a restaurant handing your order to a delivery rider who already left:
the food exists, it's marked "out for delivery", and nobody notices for half an
hour.

The fix, in `Engine.PollTask` and `PollWorkflowTask`: after taking a task off
the queue, check (under the engine lock, right before granting the lease)
whether the poller is still there. If it has gone, put the task back for the
next poller. A delivery over a network can still be lost after that check;
that's what leases are for. But the window went from "as long as the engine
lock is busy" to microseconds, and six full-suite runs in a row passed.
`TestPollDoesNotLeaseToDepartedPoller` pins it, and fails without the fix.

## What clustering guarantees, and what it doesn't

It does guarantee:

- **No committed history is lost while a majority survives.** One of three nodes,
  or two of five, can die or be cut off.
- **One history.** All nodes end with identical history files; two leaders can
  never both extend a workflow (Raft's election safety plus the event-id fence).
- **A step is recorded at most once.** Duplicates are rejected on every node.
- **Failover without a human.** A new leader takes over in about one to two
  election timeouts (1–2 seconds by default) and recovers every workflow.

It doesn't:

- **Make activities exactly-once.** They run at least once, as before.
- **Hide a lost leader from clients.** A write in flight during a leader change
  can come back as an error even though it succeeded. Retry it with the same
  `workflow_id`, so a duplicate start gets `AlreadyExists`.
- **Scale writes.** Every history write is a Raft commit under the engine's one
  lock. That's the same ceiling as single-node, now with a network round trip
  added.
- **Survive losing a majority.** Two of three nodes down means no progress
  (nothing committed is lost, though).
- **Secure the Raft port.** It has no TLS or auth; keep it on a private network.
  Membership is fixed by `--peers`; there's no PreVote yet (a reconnecting node
  can cost one extra election); the TypeScript and Python SDKs don't follow
  leader hints yet.

## Go things that showed up

- **gRPC interceptors**: middleware for RPCs, on the client
  (`grpc.WithUnaryInterceptor`) and on the server (`grpc.UnaryInterceptor`).
- **`sync/atomic.Bool`**: a flag that's safe to read from any goroutine without a
  mutex.
- **`context.WithCancel` plus a goroutine** to end a long poll when *either* the
  caller gives up *or* the engine closes, like `Promise.race` over two signals.
- **`io.Copy`** in both directions is a whole TCP proxy in two lines.
- **The race detector found two data races, both in my test harnesses** (worker
  goroutines reading a node handle while the test restarted that node), and none
  in the product code. Run tests with `-race`.

## What's next

Phase 4 is done. The roadmap's Phase 5 is the developer experience: a clean
public Go SDK, safe versioning of workflow code, and a quickstart. The
clustering follow-ups (PreVote, SDKs following leader hints, per-workflow
locking for write throughput, follower reads, TLS on the Raft port) are in the
session log.
