# ADR-0003: Single-binary, zero-dependency, embeddable engine

- **Status:** Accepted
- **Date:** 2026-06-21

## Context

The market has two extremes: Temporal/Cadence (powerful but require Cassandra/MySQL, multiple
services, Kubernetes, and a DevOps team) and ad-hoc Redis-queue/cron hacks (fragile, lose
state, no replay). The opportunity is the middle: durable execution a developer can run in
under five minutes with no external infrastructure.

## Decision

Ship the engine as a **single Go binary with zero external runtime dependencies**:

- Persistence is local disk (the history store), not an external database.
- The web dashboard (Phase 3) is bundled via `go:embed` — no separate frontend deployment.
- High availability (Phase 4) is built in via an embedded Raft implementation, not an
  external coordination service.
- The product is also **embeddable** as a Go library (the `sdk` package), not only a server.

Open-core business model: the engine is Apache-2.0 OSS for adoption; a managed/hosted product
(dashboard, multi-tenancy, RBAC, audit, team collaboration) is the future commercial layer.

## Consequences

- **Pro:** trivial to adopt (`download → run`), the core differentiator vs Temporal. Self-contained
  binary is easy to operate, test, and demo. Aligns every technical decision toward "no external
  deps."
- **Con:** we must implement things Temporal delegates to mature databases (storage durability,
  replication, snapshotting) ourselves — more work, and we own those failure modes. Single-node
  throughput is bounded by local disk fsync until batching/Raft.
- **Constraint going forward:** reject any design that introduces a mandatory external service.
  Optional integrations (e.g. external object storage for snapshots) are fine; mandatory ones are not.

## Alternatives considered

- **Require Postgres/SQLite:** SQLite would keep "single dependency" but cedes the storage-engine
  learning and couples us to its model. Postgres breaks the zero-dependency promise. Rejected.
- **Library-only (no server):** loses the language-agnostic worker story and the managed-product
  path. We do both: embeddable library *and* standalone server.
