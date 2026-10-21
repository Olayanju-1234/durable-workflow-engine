# ADR-0001: Append-only, CRC-framed record format for the history store

- **Status:** Accepted
- **Date:** 2026-06-21

## Context

The history store is the source of truth for all workflow state. It must survive process
crashes: a successfully-recorded event must never be lost, and a crash mid-write must never
corrupt earlier events or be silently read back as valid. We also need to read a workflow's
full event sequence cheaply on every decision-loop iteration and on recovery.

## Decision

Each workflow execution gets its own append-only file `<dataDir>/<workflowID>.history`.
Records are length-prefixed and checksummed:

```
[4B big-endian length N][4B big-endian CRC32C of payload][N-byte protobuf HistoryEvent]
```

- Append a record, then `fsync` before returning success. An event is "committed" only once
  durable on disk.
- Write each record with a single `write()` of one pre-built buffer to minimize the torn-write
  window.
- On read: a CRC mismatch on an otherwise-complete record is a hard error; a short read
  (torn header or payload from a crash mid-append) is treated as the end of the usable log
  and silently truncated. The append-only design means the only possible torn record is the
  last one.
- Reject workflow IDs containing path separators to prevent directory traversal.

## Consequences

- **Pro:** dead simple, debuggable, crash-safe, integrity-checked. One file per workflow keeps
  locking trivial and isolates corruption blast radius.
- **Con:** `fsync` per append is slow (correctness over performance for now). Many tiny files
  at very high workflow counts; no compaction; no secondary indexes.
- **Follow-ups:** Phase 4 batches writes to amortize fsync. If file count or read amplification
  becomes a problem, migrate to an LSM-backed store (BadgerDB) behind the same
  `Append`/`ReadAll` interface — the format is an implementation detail, not API.
  (Revisited with measurements in [ADR-0006](0006-keep-file-per-workflow-store.md): not needed.)

## Alternatives considered

- **JSON lines:** human-readable but no integrity check, larger, and no schema evolution story.
  Rejected — the blueprint explicitly forbids JSON on disk.
- **Single shared LSM tree (BadgerDB) from day one:** more capable but heavier and obscures the
  storage-engine fundamentals this project exists to teach. Deferred as an upgrade path.
- **CRC32 (IEEE) vs CRC32C (Castagnoli):** chose Castagnoli — hardware-accelerated on modern
  CPUs and the de-facto choice for storage systems.
