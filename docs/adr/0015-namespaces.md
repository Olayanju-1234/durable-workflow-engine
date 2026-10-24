# ADR-0015: Namespaces

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

One engine has served one tenant. liftoff's webhooks and custom steps could not share the public
demo engine without showing up on its dashboard and sharing its token, so liftoff got a second
engine: a second unit, binary copy, data directory, port triple, token and deploy step. Every
further app would need the same. The demo's own workers and the SDK examples also share workflow
types and activity names with anything else on the engine.

Forces:

- **Everything that exists keeps working.** Histories, the unauthenticated demo, clients and
  SDKs that know nothing of namespaces, every URL and every in-process caller.
- **A namespace's token must not see or touch another namespace**, through any route: listing,
  describing, starting, signalling, ending, polling tasks, completing them with a stolen lease
  token, schedules, workflow types.
- **One process.** The VPS has one vCPU; a second engine per tenant is what we are removing.
- **The format contract (ADR-0011)** must say what an older binary does with a namespace.

## Decision

1. **A namespace is a name**: 1 to 63 lowercase letters, digits and hyphens (a DNS label, safe
   as a directory name on case-insensitive file systems too). `default` always exists and is
   everything that existed before.
2. **Store keys.** The engine names a history by its key: the bare workflow id in `default`,
   `<namespace>/<id>` elsewhere. Workflow ids cannot contain `/`, so a key has one reading.
   `default` stays at the root of the data directory; every other namespace's histories, earlier
   runs and schedule history live in `namespaces/<namespace>/`. History content holds local ids
   only, so workflow code sees its own id.
3. **One engine, scoped by key.** Workflow types, engine retry policies, activity and workflow
   task routing, schedules, the visibility index and retention are all per namespace. A poller
   only ever receives its own namespace's tasks, whatever names it asks for. `Engine.In(ns)`
   returns a `Scope`, the only way the HTTP and gRPC layers reach the engine: it checks every
   incoming id is a plain id (never a key) and names every outgoing workflow by its local id.
   The engine's own methods are the default namespace's, so in-process callers are unchanged.
4. **Format 6.** A binary before 0.4.0 lists only the root of the data directory and would
   silently not run a namespace's workflows. The store raises the directory's format to 6
   before it creates a namespace's directory or a history in it; `--check-format` counts any
   namespaced history as format 6.
5. **Tokens and selection.** `--namespaces FILE` (JSON) lists namespaces with their tokens
   (`token_file`, or `token_sha256` so the file holds no secret) and settings (retention here;
   quotas and read-only in ADR-0017). A call names its namespace with the `X-Workflowd-Namespace`
   header, `?namespace=`, or gRPC metadata `workflowd-namespace`. One `Authorizer` decides for
   both transports:
   - the engine token (`--auth-token`) reaches every namespace;
   - a namespace token reaches its namespace, which is also where a call naming none goes, and
     gets 403 / `PERMISSION_DENIED` anywhere else;
   - without a token, only `default`, and only when it is open (no engine token, no default
     tokens) or, with `--auth-public-reads`, for reads. Every other namespace needs a token: 401.
   A token grants one namespace; `GET /api/namespaces` says which ones a caller can use.
6. **Clients.** Go `client.Options.Namespace`, the SDKs' `namespace` option, the dashboard's
   namespace picker (and `?namespace=` links). A namespace token needs no namespace option.
7. **Clusters.** Keys travel in the Raft commands and snapshots like ids did; a snapshot install
   recreates namespace directories (and raises the format) on the follower.

## Consequences

- liftoff can move into a namespace of a shared engine (its own token, its own dashboard view)
  instead of running a second one. That move is liftoff's choice; the private engine keeps
  working as it is.
- Namespaces are configured, not created through the API: tokens are secrets that belong in
  files, and a restart takes under a second. Adding one is a config change and a restart.
- Isolation is logical. Namespaces share the process, the disk and the CPU, so a busy one can
  slow the others; ADR-0017's quotas bound that. Metrics stay engine-wide.
- Error messages for a namespaced workflow may show its key (`acme/order-1`): only to callers of
  that namespace, since lookups never cross namespaces.
- Rolling back below 0.4.0 needs every namespaced history gone (finished and purged, then
  `--rescan-format`), or a backup from before.

## Alternatives considered

- **An engine per namespace in one process.** Isolation for free, but N sets of timers and
  queues, and a cluster would have to replicate N engines' state through one Raft log and
  build N engines per term. One engine with scoped keys reuses everything already replicated.
- **A namespace field in every request message.** The Temporal shape. It touches every RPC and
  SDK call for a value that is per connection in practice; a header and metadata key leave the
  messages alone, and older clients keep working unchanged in `default`.
- **A prefix inside the workflow id, in the root directory.** Ambiguous: any existing id might
  already look like `acme~order-1`. A directory per namespace cannot collide with an id.
- **Creating namespaces through the API, stored in history.** Puts token hashes in replicated
  history and needs an admin API before there is RBAC; a config file is simpler and auditable.
