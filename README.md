# durable-workflow-engine

> A single-binary, zero-dependency durable workflow engine. Write ordinary functions — in
> Go, TypeScript or Python — and the engine makes them crash-proof, resumable, and
> observable. No Kubernetes, no external database, no DevOps team.

![workflowd's embedded dashboard: nine OrderSaga workflows completed and two TypeScript-SDK workflows waiting on a signal](docs/assets/dashboard.png)

**Status:** 🚧 Pre-alpha — weeks 1–14 of 17 complete. Phases 1–4 are done: durable history,
deterministic replay of real Go workflows, timers, signals, retries, Saga compensation, worker
leases, Prometheus metrics, a visibility API, an embedded dashboard, and opt-in high
availability through a hand-rolled Raft cluster. Next up: SDK polish (Phase 5). Not
production-ready — see [`docs/ROADMAP.md`](docs/ROADMAP.md).

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

## What a workflow looks like

```go
// Ordinary Go. Kill the process at any line — on restart it resumes exactly
// here, with no completed work repeated.
func OrderWorkflow(ctx sdk.Context, input []byte) ([]byte, error) {
	reserved, err := ctx.ExecuteActivity("reserve", input)
	if err != nil {
		return nil, err
	}

	ctx.Sleep("settle", 2*time.Second) // durable: survives restarts

	if _, err := ctx.ExecuteActivity("charge", reserved); err != nil {
		ctx.ExecuteActivity("release", reserved) // compensate (Saga)
		return nil, fmt.Errorf("charge failed, reservation released: %w", err)
	}
	return ctx.ExecuteActivity("ship", reserved)
}
```

## Quickstart

Requires Go 1.26+ and (for proto regeneration) protoc 35+.

```sh
git clone git@github.com:olayanju-1234/durable-workflow-engine.git
cd durable-workflow-engine
make test                        # full suite under -race
go build -o workflowd ./cmd/workflowd
./workflowd --serve
```

One process, three endpoints:

| Address | Protocol | Purpose |
|---------|----------|---------|
| `:7233` | gRPC | workflow API — start, poll, complete, signal, visibility |
| `:8233` | HTTP | embedded dashboard + JSON API |
| `:9090` | HTTP | `/metrics` for Prometheus |

```sh
curl -X POST http://localhost:8233/api/workflows \
  -H 'Content-Type: application/json' \
  -d '{"type":"OrderSaga","input":"order-42"}'
```

Then open <http://localhost:8233> to watch it. Full guide: [`web/site/docs.html`](web/site/docs.html).

## Running a cluster (optional)

By default `workflowd` is a single node. Give it peers and it becomes one member of a Raft
cluster: every history write is replicated to a majority before it counts, only the leader
runs the engine, and if the leader dies another node takes over in a second or two and
carries on every workflow from the replicated history.

```sh
make cluster    # three local nodes; see scripts/cluster.sh for the ports
```

or by hand, one command per node:

```sh
./workflowd --serve --node-id n1 --peers n1=10.0.0.1:7333,n2=10.0.0.2:7333,n3=10.0.0.3:7333 \
  --data-dir data/n1 --addr :7233 --http-addr :8233
```

Any node answers `/api/health`; followers answer workflow calls with `503` (HTTP) or
`Unavailable` (gRPC) plus the leader's address. What a cluster guarantees, and what it
doesn't, is in [ADR-0004](docs/adr/0004-replicated-history-leader-only-engine.md). Keep the
Raft port (`--raft-addr`) on a private network: it has no TLS or auth.

## Workflows in other languages

Workflow code doesn't have to be Go. The engine owns durability; an SDK in your language owns
the orchestration logic. The non-Go SDKs are **dependency-free** — no npm install, no pip
install, no protobuf toolchain.

```js
// sdks/typescript — run with `node example.mjs`
worker.workflow('OrderWorkflow', async (ctx) => {
  const reserved = await ctx.activity('reserve', ctx.input);
  await ctx.sleep('settle', 2000);                  // durable
  try {
    await ctx.activity('charge', reserved);
  } catch (err) {
    await ctx.activity('release', reserved);        // compensate
    throw new WorkflowFailure(`charge failed: ${err.message}`);
  }
  return await ctx.activity('ship', reserved);
});
```

| SDK | Status |
|-----|--------|
| Go (`sdk/`) | in-process, compiled into the engine |
| TypeScript (`sdks/typescript/`) | verified end-to-end |
| Python (`sdks/python/`) | verified end-to-end |
| Java (`sdks/java/`) | written, not yet compiled — reference implementation |

See [`sdks/README.md`](sdks/README.md) for the protocol (three HTTP endpoints) if you want to
add a language.

## Project layout

```
cmd/workflowd/      engine daemon (entrypoint)                       ✅
internal/history/   append-only event store — the source of truth    ✅
internal/state/     workflow state machine (decision loop)           ✅
internal/server/    gRPC API, task queue, leases, visibility, HTTP   ✅
internal/timer/     durable timers (min-heap)                        ✅
internal/metrics/   zero-dependency Prometheus registry              ✅
sdk/                workflows as Go functions (replay engine)        ✅ core
sdks/               TypeScript · Python · Java SDKs (dependency-free) ✅
web/dashboard/      embedded operational UI (go:embed)               ✅
web/site/           landing page + developer documentation           ✅
internal/raft/      consensus: elections, replication, snapshots     ✅
internal/cluster/   replicated history + leader-only engine          ✅
internal/record/    shared CRC-framed record format                  ✅
proto/              protobuf definitions
docs/               architecture, roadmap, ADRs, session walkthroughs
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
