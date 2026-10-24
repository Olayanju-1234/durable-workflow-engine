# ADR-0017: `workflowd ctl`, the operator's command line

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

Every operation on the VPS engines so far has been `curl` with hand-written JSON: starting a
smoke-test workflow, reading a history while debugging liftoff, checking health after a deploy,
taking a backup (a root-only tar of the data directory, by a systemd timer). Curl knows nothing
of tokens in files, namespaces or the leader of a cluster, and the JSON API had no way to see a
history as recorded, only describe's replayed view.

Forces: no new dependencies (no CLI framework); one binary to ship, as for the engine; the same
token, namespace and TLS rules as every other client; output a person can read and a script can
parse; and no command that can damage a data directory by accident.

## Decision

1. **A subcommand of the same binary**, `workflowd ctl <command>`, in `internal/ctl`, built on
   the standard library's `flag` and `text/tabwriter`. It is a client of the JSON API only: it
   never needs to run on the engine's host, and it works the same against a cluster (it follows
   a follower's leader hint).
2. **Connection settings from flags or the environment**: `--addr` (`WORKFLOWD_ADDR`),
   `--token-file` (`WORKFLOWD_TOKEN_FILE`, else the `WORKFLOWD_TOKEN` variable), `--namespace` /
   `-n` (`WORKFLOWD_NAMESPACE`), `--ca-file` (`WORKFLOWD_CA_FILE`). There is no token flag, so a
   token never lands in shell history or the process list.
3. **Commands**: `health`, `namespaces`, `list` (status, type, text, paging, `--all`),
   `describe`, `history`, `runs`, `start`, `signal`, `cancel` (`--graceful`), `terminate`,
   `schedule list|create|delete`, `backup`, `restore`. Flags may come before or after the
   arguments. Tables by default; `--json` prints the engine's JSON unchanged. Exit status 0 on
   success, 1 when the engine refuses or fails, 2 for a usage mistake.
4. **Two endpoints for it**, in the JSON API like everything else:
   - `GET /api/workflows/{id}/history[?run_id=]`: a run's events exactly as recorded, in the
     protobuf JSON mapping (proto field names). `ctl history` prints them one per line.
   - `GET /api/admin/backup`: a gzipped tar of every history, laid out as in the data directory,
     `FORMAT` last (it only rises, so the backup records at least the format of every history in
     it). It covers every namespace, so it needs the engine token: a namespace token, or anyone
     on an open engine, gets 403. Histories are append-only, so a backup taken while the engine
     runs holds a whole-record prefix of each one.
5. **Restore is offline and never overwrites.** `ctl restore --data-dir NEW file` unpacks into a
   sibling directory, checks every history (whole records, checksums) and every path (only
   history files and `FORMAT`), fsyncs everything, then renames it into place. The target must
   not exist or be empty. `ctl backup --data-dir D` reads a data directory directly, for a host
   where the engine is stopped or has no token.

## Consequences

- Deploy smoke tests, liftoff debugging and backups become `workflowd ctl ...` commands, with the
  token in a file.
- `--json` passes the engine's JSON through, so its shape is the HTTP API's, documented once.
- A backup through the API holds the whole data directory in one HTTP response; very large
  directories are better backed up from the host (`--data-dir`, or the filesystem).
- Restoring is a deliberate two-step (restore into a new directory, then point `--data-dir` at it
  or swap directories), which is slower to type and impossible to do to a live engine.

## Alternatives considered

- **A separate `workflowctl` binary.** Two artifacts to build, version and copy to the VPS for
  no gain; a subcommand dispatched before flag parsing costs nothing.
- **gRPC instead of HTTP.** The Go client exists, but the HTTP API is the one every other tool
  uses, it already carries the namespace and auth rules, and its JSON is what `--json` should
  print.
- **Online restore through the API.** Replacing a live engine's histories under it would need
  the engine to stop serving, re-run recovery and fence every worker; doing it offline with the
  engine stopped is simple and safe.
