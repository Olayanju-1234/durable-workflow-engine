# durable-workflow-engine

> A single-binary, zero-dependency durable workflow engine for Go. Write ordinary
> functions; the engine makes them crash-proof, resumable, and observable — no Kubernetes,
> no external database, no DevOps team.

**Status:** 🚧 Pre-alpha. Phase 1 (single-node persistence) in progress. Not yet usable as a
product — see [`docs/ROADMAP.md`](docs/ROADMAP.md).

## Why

Every backend team eventually builds a broken version of durable execution — payment flows,
onboarding sequences, billing cycles, approval chains — with Redis queues, cron jobs, and DB
status flags. Those lose state on crashes, can't replay, and breed idempotency bugs.

The robust answer (Temporal, Cadence) is the opposite problem: Cassandra/MySQL, multiple
services, Kubernetes, a dedicated ops team. Most teams can't justify it.

**This fills the gap:** durable execution you can run in under five minutes. One binary. A Go
SDK. Your workflow code runs, crashes, restarts, and picks up exactly where it left off.

## How it works (one idea)

Every workflow is an append-only sequence of events. The engine loops: **read history →
replay to find the current position → schedule the next step → record the result.** Kill the
process at any point; on restart it re-reads the on-disk history, replays, and resumes. No
duplicate execution, no lost state. Details in [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md).

## Quickstart (developer / contributor)

Requires Go 1.26+ and (for proto regeneration) protoc 35+.

```sh
git clone git@github.com:olayanju-1234/durable-workflow-engine.git
cd durable-workflow-engine
make test          # run the suite (history store: durability, CRC, restart survival)
make build         # build all packages
go run ./cmd/workflowd --demo   # smoke test: append + read back from a real on-disk store
```

> End-user quickstart (define a workflow, start it via gRPC, run a worker) lands with Phase 1
> Weeks 3–4. Today the history store and its restart-survival guarantee are real and tested.

## Project layout

```
cmd/workflowd/      engine daemon (entrypoint)
internal/history/   append-only event store — the source of truth   ✅
internal/state/     workflow state machine (decision loop)          ⬜ Wk2
internal/server/    gRPC API + task scheduler                       ⬜ Wk3
internal/timer/     timers, signals, cron                           ⬜ Phase 2
internal/raft/      consensus / replication                         ⬜ Phase 4
sdk/                public Go SDK developers import                 ⬜ Phase 5
proto/              protobuf definitions
docs/               architecture, roadmap, ADRs, references
```

## Roadmap

5 phases / 17 weeks: persistence → timers & replay → multi-worker & observability → Raft HA →
SDK & DX. Track progress in [`docs/ROADMAP.md`](docs/ROADMAP.md).

## Contributing

See [`CONTRIBUTING.md`](CONTRIBUTING.md). Conventions and architecture for AI-assisted work
live in [`CLAUDE.md`](CLAUDE.md).

## License

[Apache-2.0](LICENSE). The engine is open source; a future managed/hosted product is the
commercial layer (open-core).
