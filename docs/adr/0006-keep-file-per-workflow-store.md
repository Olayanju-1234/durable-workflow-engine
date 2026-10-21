# ADR-0006: Keep the file-per-workflow history store; no LSM engine

- **Status:** Accepted
- **Date:** 2026-10-21
- **Revisits:** the storage follow-up in [ADR-0001](0001-record-format.md)

## Context

ADR-0001 chose one append-only, CRC-framed file per workflow and named a way
out: "if file count or read amplification becomes a problem, migrate to an
LSM-backed store (BadgerDB) behind the same `Append`/`ReadAll` interface". The
roadmap carried that as "storage upgrade path to LSM/BadgerDB if needed".

Two of the costs that motivated it were in the engine, not the format:

- every operation read and replayed the workflow's whole file (`ReadAll` +
  `Reconstruct`), under one global lock, so a step cost O(history) and nothing
  ran in parallel;
- `ListWorkflows` read and replayed every file in the data directory.

Both are now fixed without touching the format: per-workflow locks and an
in-memory state cache (each appended event is folded in), and a visibility index
built during recovery and kept current on every append and purge.

BadgerDB is a third-party module. ADR-0003 allows gRPC and protobuf only.

## Measurements

Intel Mac (i9-9980HK, macOS, APFS), `internal/server` benchmarks and a one-off
probe over seeded data directories (12-event, five-step histories, one in ten
still running):

| What | Number |
|---|---|
| History size | 369 B per 12-event workflow |
| Recovery (startup) | 0.83 s at 10k workflows, 5.9 s at 50k |
| List running workflows, 10k on disk | 734 ms before the index, 0.75 ms after |
| List all, 50k (index, cloned and sorted) | 40 ms |
| Step cost, 1000-step workflow | 2.25 ms before the cache, 49 µs after (flat in history length) |
| Durable append (write + `F_FULLFSYNC`) | 20 ms |
| Buffered append (no fsync) | 10.6 µs |

Production today: 5 workflows, 24 KB, `--retention 168h`.

## Decision

Keep the file-per-workflow store. Do not adopt BadgerDB or another LSM engine.

What the numbers say:

- **Read amplification is gone.** A live workflow's history is read once, then
  kept in memory; listing never opens a file. An LSM would not make either
  faster.
- **File count is not a problem at the scale this project runs.** Retention
  bounds finished workflows; recovery is a few seconds at 50k histories.
- **The real cost is durability, and it is one fsync per event.** A buffered
  append is 2000x cheaper than a durable one. That is what an LSM's shared
  write-ahead log would buy: group commit, many events per fsync. It is also
  something this store can do itself, without a dependency, if it is ever
  needed (see Consequences). Cluster mode already pays one fsync per Raft batch,
  not per event.

## Consequences

- The on-disk format, recovery, the Kill Test and the cluster snapshot format
  (raw history files) stay exactly as they are. Nothing to migrate.
- Revisit when one of these is measured, not before:
  - recovery past ~15 s, or a data directory past ~100k histories that
    retention cannot bound;
  - sustained durable write rates where fsync per event is the limit. The first
    step then is group commit in `HistoryStore` (a shared write-ahead log, or
    batching concurrent appends into one fsync), still behind
    `Append`/`ReadAll`, before any new engine.
- The visibility index and state cache are memory-only. Memory grows with the
  number of indexed workflows (one summary each) and cached live workflows
  (bounded by `WithStateCacheSize`).

## Alternatives considered

- **BadgerDB (LSM).** Group commit and compaction, but a large dependency (and
  its own), breaks ADR-0003, and solves the two read problems the engine has
  already solved. Rejected.
- **SQLite via a pure-Go driver.** Same dependency problem; turns history into
  rows the engine would immediately fold back into the same structures.
  Rejected.
- **A hand-written LSM.** Possible (the project already hand-writes Raft), but
  all it would add over today's store is group commit, which a shared WAL gives
  for far less code. Deferred until the write rate needs it.
