# ADR-0004: Replicate history through Raft; run the engine on the leader only

- **Status:** Accepted
- **Date:** 2026-10-21

## Context

A single workflowd node is durable but not available: if its machine dies,
every workflow stops until it comes back. Weeks 11–13 built a hand-rolled Raft
(`internal/raft`): elections, a replicated log with majority commit, snapshots,
and a `StateMachine` interface that every node applies committed entries to.
This ADR decides how the engine sits on top of it.

Forces:

- **The engine is not deterministic.** It stamps wall-clock timestamps, draws
  random workflow ids and lease tokens, computes timer deadlines from
  `time.Now()`, and keeps in-memory runtime state (task queues, leases, armed
  timers, pending SDK decisions). Two nodes fed the same requests would not
  produce the same history.
- **History is already the source of truth.** Everything durable about a
  workflow is its event history; everything else is rebuilt by `Recover()` after
  a restart (Phase 1, the Kill Test).
- **Single-node mode must not change**: same flags, same on-disk format, every
  existing test unchanged, and production stays single-node on a 1-vCPU VPS.
- ADR-0003: no external services, so consensus is embedded.

## Decision

### 1. Replicate history mutations, not requests

The Raft log carries the engine's *outputs*: one entry per history mutation.

```proto
message Command {                 // package cluster.v1
  oneof command {
    AppendEvent append = 1;       // { workflow_id, history.v1.HistoryEvent event }
    DeleteWorkflow delete = 2;    // { workflow_id }  (retention)
  }
}
```

The leader's engine decides everything (event ids, timestamps, timer deadlines)
exactly as it does today; followers only store the results. Replicating client
requests instead would require a deterministic engine on every node, which it
is not and should not need to be.

### 2. The history directory is the state machine

Every node applies committed `Command`s to its own `HistoryStore`, so every
node's data directory converges on the same `<id>.history` files. Only
committed entries are ever applied, so a history file never contains an event
that could later be rolled back.

Applying must be idempotent, because after a restart a node re-applies every
entry since its last snapshot. Each applied event is stamped with the Raft
index that committed it, in a new optional field `HistoryEvent.raft_index`.
Older binaries ignore the field, and single-node mode never sets it, so the file
format stays readable both ways. On apply:

- an `AppendEvent` whose index is at or below the workflow's last stamp is
  already in the file and is skipped;
- otherwise it is appended only if `event.event_id == events_so_far + 1`, a
  deterministic fence (the same idea as the polyglot `history_length` fence).
  Two engines can never both extend a workflow at the same position;
- a `DeleteWorkflow` is skipped if the file's last stamp is newer than the
  delete (the id was reused after the purge).

Because the outcome of every apply depends only on the file and the entry, a
snapshot taken while a restarted node is still catching up ("fuzzy", in
ZooKeeper's terms) still converges once the log is replayed over it.

### 3. The write path

`Engine.appendLocked` and retention's delete go through a small interface:

```go
type HistoryLog interface {
    Append(workflowID string, ev *history.HistoryEvent) error
    Delete(workflowID string) error
}
```

Single-node mode passes the `HistoryStore` itself: no behaviour change. Cluster
mode proposes the `Command` with `Node.Propose(ctx, term, data)` and returns
once this node has applied it, so the engine's next `ReadAll` sees its own
write. The engine keeps holding its mutex across that wait; that keeps event ids
contiguous with no other change to the engine, at the cost of one commit round
trip per write (see Consequences).

### 4. Only the leader runs an engine, and a new one per term

Followers run Raft and apply entries; nothing else. The leader runs the whole
engine: task queue, timers, leases, polyglot decisions, the demo worker and
retention.

Each leadership term gets a **fresh `Engine`**, built only after the leader has
applied the empty entry that opened its term (`Status.LeaderReady()`). At that
point every entry committed before the term is in the history directory, so
`Recover()` sees the complete history and rebuilds runtime state from it. A
leader change is, to the engine, exactly a restart on another machine, which
the engine has handled since Phase 1.

The engine's proposals carry its term. Raft refuses them once the node no longer
leads that term, even if it has since been re-elected, so an engine left over
from an older term can never write. When leadership is lost, proposals still
waiting fail (outcome unknown, `ErrLeadershipLost`), the engine is closed, and
its long polls end.

### 5. Client routing: followers answer with a leader hint

Every workflow API call (gRPC `WorkflowService` and the HTTP `/api` routes,
reads included) is served by the ready leader only. A follower, or a leader
whose term has not yet applied its opening entry, rejects with:

- gRPC: `codes.Unavailable`, with the leader's id and client addresses in the
  `workflowd-leader` / `workflowd-leader-grpc` / `workflowd-leader-http` trailers.
- HTTP: `503 Service Unavailable` with `{"error", "leader_id", "leader_grpc",
  "leader_http"}` and an `X-Workflowd-Leader` header; `Retry-After: 1` while no
  leader is known.

`/api/health` is answered by every node and reports role, term, leader, commit
and applied indexes. Leaders learn each peer's client addresses from metadata on
Raft `Send` calls, so `--peers` only needs Raft addresses.

There are no follower reads. A leader cut off by a partition keeps serving reads
until CheckQuorum makes it step down (one election timeout); writes it accepts in
that window never commit.

### 6. Snapshot format

The history state machine's snapshot is a sequence of `internal/record` frames:
a header (format version, workflow count), then one frame per workflow holding
its id and the **raw bytes of its history file**. Restoring writes each file
through a temp file and rename, removes files the snapshot doesn't list, then
records the snapshot's index in a marker file (`raft/applied-snapshot`) so a
restart can skip restoring a snapshot the directory already reflects. A
snapshot travels in one `InstallSnapshot` message (capped at 64 MB by the record
format); retention keeps it small in practice.

### 7. Bootstrapping

Cluster mode refuses to start on a data directory that holds history files but
no Raft state: those workflows exist on this node only, and the others would
never learn of them. Moving a single-node deployment into a cluster is future
work (an import through the leader).

### 8. What "at-least-once" means in a cluster

Unchanged from single-node:

- **Activities run at least once.** A worker can finish an activity whose
  `CompleteTask` never commits (the leader died first, or the lease expired);
  the next leader's `Recover()` sees the step still scheduled and dispatches it
  again. Activities must be idempotent, exactly as today.
- **A step is recorded at most once.** The event-id fence, applied identically
  on every node, rejects a second completion for the same position, and a stale
  worker's `CompleteTask` on a new leader fails the lease check (leases are not
  replicated).

New in a cluster:

- **A client can get an error for a write that did happen.** If leadership
  changes while a write waits for commit, the caller gets `Unavailable`
  (outcome unknown) even though the entry may commit under the next leader.
  Clients retry against the new leader; a supplied `workflow_id` makes
  `StartWorkflow` safe to retry (a duplicate gets `AlreadyExists`).
- **Committed history survives the loss of any minority of nodes** (one of
  three, two of five). Losing a majority stops progress; it does not lose
  committed history unless the disks themselves are lost.

## Consequences

- **Pro:** the engine keeps one code path. Single-node is untouched, and a
  cluster reuses `Recover()`, the Kill-Test-proven recovery, as its failover.
  Followers run no workflow logic, so there is nothing to keep deterministic
  beyond what already is.
- **Con, throughput:** each history write is a Raft commit (leader fsync,
  follower fsync, local apply fsync) under the engine's global mutex. Writes are
  serialised cluster-wide. The fix (per-workflow locking and pipelined
  proposals) is the same backlog item as the single-node O(n²) ceiling.
- **Con, double write:** every event is written to the Raft log and to its
  history file, each fsync'd.
- **Limits kept on purpose:** static membership (`--peers`); no PreVote (a
  rejoining partitioned node costs one election); no TLS or auth on the Raft
  port (bind it to a private network); leader-only reads; the TypeScript and
  Python SDKs don't follow leader hints yet; production stays single-node.

## Alternatives considered

- **Replicate requests and run the engine on every node.** Requires a
  deterministic engine (time, randomness, lease tokens, timers, side effects).
  Rejected: large rewrite for no gain.
- **Ship history files between nodes (rsync, streaming).** No ordering or
  agreement, so split brain is still possible. Rejected.
- **External consensus (etcd, Consul) or hashicorp/raft.** Breaks ADR-0003's
  no-external-service rule (etcd, Consul) or the project's learning goal and
  dependency budget (hashicorp/raft). Rejected.
- **Follower reads via ReadIndex or leases.** Worth it once read load matters;
  deferred.
- **Partition workflows across several Raft groups (sharding).** The way past
  the single-leader write ceiling; far beyond this phase.
