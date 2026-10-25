# ADR-0023: Reloading secrets and auditing operator actions

- **Status:** Accepted
- **Date:** 2026-10-25

## Context

Two operational gaps showed up running liftoff on the private engine:

- **Rotating a token meant a restart.** The engine token, a namespace's token, the metrics token
  and (with ADR-0022) the encryption keys were read once at startup. A restart is quick, but it
  drops every worker's connection and lease, and on a shared engine it interrupts every app to
  rotate one app's token.
- **Nothing recorded who did what.** The request log says a `POST .../terminate` happened; it
  does not say with which token, and it is not kept. With several apps on one engine (ADR-0015)
  and an operator's command line (ADR-0017), "who terminated this, and when" needs an answer that
  survives log rotation.

## Decision

1. **The engine re-reads its secret files without a restart**: `--auth-token-file`,
   `--metrics-auth-token-file`, `--namespaces` (tokens, retention, quotas, read-only flags) and
   `--encryption-keys`. On `SIGHUP`, and on its own when a file's content changes (checked every
   `--reload-interval`, 10 s by default; 0 leaves only the signal). TLS certificates already
   reloaded themselves.
2. **All or nothing.** A reload reads every file first; if any fails to parse or validate, nothing
   changes, the engine keeps what it had, and the log and the audit log say why. So a half-edited
   file never leaves the engine open, or locked out.
3. **One shared state.** The HTTP API and the gRPC server use one `Authorizer`, whose engine token
   can be swapped, and the engine, the guard and the authorizer share one `NamespaceSet`, replaced
   in place. The guard already rebuilt a namespace's rate limiter when its numbers changed, and
   quotas and retention already read the policy on every use, so new settings apply to the next
   call. An old token stops working on the next call after the reload.
4. **An audit log** records every operator action: workflow start, signal, cancel (and graceful
   cancel request), terminate and reset; schedule create, delete, pause, resume and backfill;
   backups; and reloads (`config.reload`, with what changed: which namespaces were added, removed
   or changed, whether the engine or metrics token was replaced, which keys were added, removed or
   made active). Each entry has the time, the actor, the namespace, the action, its target (a
   workflow or schedule id, and the run a start or reset made), the outcome and status, the
   caller's address, how it came (`http`, `grpc`, `signal`, `file`) and the request's settings (a
   reason, a signal's name, a workflow type). **Never a payload or an input, and never a token**:
   the actor is `engine:<hash>` or `token:<hash>`, the first 16 hex digits of the token's SHA-256,
   which an operator can compute from a token file (`tr -d '\n' < file | sha256sum | cut -c1-16`)
   or match against a namespaces file's `token_sha256`.
5. **Recorded at the transport edge, by the guard's classification.** An HTTP route or gRPC method
   the guard counts as a client write is audited; a test checks that every one of them has an audit
   action, so a new write cannot be added unaudited. Failed attempts by an authorized caller are
   recorded too (with the error); calls refused before that (no token, rate limited) are in the
   request log, not here. Engine-internal work (schedule fires, retention) is not an operator
   action.
6. **Where.** JSON lines in `--audit-log` (default `<data-dir>/audit/audit.log`; `off` disables),
   written and fsync'd before the answer goes out, rotated at 16 MiB keeping four earlier files.
   `GET /api/audit` (newest first; `action`, `target`, `limit`): the engine token sees every
   namespace and the reloads, a namespace token its own namespace, and nobody without a token,
   since entries name callers and their addresses. `workflowd ctl audit`.

## Consequences

- Rotating liftoff's token is: generate it, add its hash to the namespaces file next to the old
  one, wait for the reload (or `systemctl reload`), switch the app, drop the old hash. No engine
  restart, no dropped leases.
- `SIGHUP` no longer stops the engine (it used to, as Go's default).
- An audited action costs one more fsync. Writing the audit entry cannot fail the action (it
  already happened); a failed write is logged as an error.
- The audit log is per node: in a cluster, the node that served the call records it. It is not in
  backups, and retention does not touch it.
- Secrets given as flags or environment variables cannot be reloaded; the startup log says what a
  reload covers.
- Removing an encryption key that still seals events makes those events unreadable; the reload
  warns, and `--check-format` says which keys are in use first.

## Alternatives considered

- **inotify / kqueue file watching.** Immediate, but platform-specific and a dependency (or a lot of
  syscall code); a content check every few seconds costs a read of four small files and works on
  every platform and through symlinked secret mounts.
- **Auditing in the engine** (a Scope that knows its caller). Closer to the action, but the engine
  does not know tokens or addresses, and every API method would need a caller passed through; the
  transport edge already has both, and the guard's classification makes completeness checkable.
- **Audit entries in history.** Durable and replicated, but they would change every workflow's
  history (and its format) for something workflow code must never see, and reloads belong to no
  workflow.
- **Recording payloads.** Useful for forensics, but it would copy tenant data out of encrypted
  histories into a plaintext file.
