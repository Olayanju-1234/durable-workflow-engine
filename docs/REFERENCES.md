# References & Reading List

Curated from the blueprint. Read these as the corresponding phases approach — not all up front.

## Papers
- **In Search of an Understandable Consensus Algorithm** — Ongaro & Ousterhout (the Raft paper, *extended* version). Phase 4. Read three times: problem → sub-problems → map to our engine.
- **Sagas** — Garcia-Molina & Salem, 1987 (the original compensation paper). Phase 2.
- **The Log: What every software engineer should know about real-time data's unifying abstraction** — Jay Kreps. Phase 1 (event sourcing intuition).

## Books
- **Designing Data-Intensive Applications** — Kleppmann (chs. 5–9: replication, partitioning, transactions, consistency, consensus).
- **The Go Programming Language** — Donovan & Kernighan.
- **Database Internals** — Petrov (storage engines, WAL, B-trees vs LSM).

## Codebases to study
- **etcd/raft** (`github.com/etcd-io/raft`) — reference Go Raft. Study *after* implementing our own.
- **temporalio/temporal** — history service & matching service for architectural patterns.
- **hatchet-dev/hatchet** — newer, simpler Go workflow engine; API-design inspiration.
- **dgraph-io/badger** — LSM KV store; reference if we upgrade storage.

## Interactive / blogs
- **Raft visualization** — thesecretlivesofdata.com/raft
- **How Temporal Works** — temporal.io docs, concepts section
- **Building a Distributed Log from Scratch** — travisjeffery.com

## What this project teaches (talk about in any infra/platform interview)
Storage-engine design (append-only, WAL, fsync trade-offs, compaction) · consensus (Raft
election, replication, safety, snapshots) · failure modes (partitions, split brain, crash
vs Byzantine) · gRPC/protocol design (schema evolution, streaming) · deterministic execution
(replay, side-effect isolation, non-determinism detection) · task scheduling (priority queues,
affinity, heartbeat liveness, lease tokens) · Saga (distributed transactions, compensation) ·
observability (structured logs, Prometheus) · SDK/API design (DX, testing frameworks,
versioning) · Go concurrency · binary serialization (protobuf, CRC) · operations (graceful
shutdown, rolling upgrades, zero-downtime).
