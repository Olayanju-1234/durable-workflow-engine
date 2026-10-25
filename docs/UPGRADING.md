# Upgrading and rolling back

workflowd keeps all its state in history files (`--data-dir`). Upgrading the binary is
always safe for them. Rolling back is safe until the new binary writes something the old one
cannot read, and from 0.3.0 on the binary checks that for you (ADR-0011).

## The contract

1. **Every binary reads every history an older binary wrote.** Upgrading never needs a
   migration step.
2. **History has numbered formats.** A format names a change an older binary would refuse or
   misread. A binary reads every format up to its own (`workflowd --version` prints it).
3. **A data directory's format rises only when a feature that needs it is used,** the first
   time it is used. Upgrading and changing nothing else keeps the old format, so the rollback
   stays open.
4. **From 0.3.0, a binary refuses to start on a directory in a newer format** than it reads,
   before it reads a single history, and says which binary it needs. It refuses a single
   history in a newer format the same way during recovery, naming the workflow.

The directory's format is recorded in `<data-dir>/FORMAT`, raised before the first event that
needs it is written. Binaries before 0.3.0 neither write nor read that file.

## Formats

| Format | What a history can hold | First binary that reads it | What an older binary does |
|-------:|-------------------------|----------------------------|---------------------------|
| 1 | Activities, timers, signals, retries, terminate (Phases 1–4) | every binary | — |
| 2 | Version and side-effect markers (`GetVersion`, `SideEffect`, `getVersion`, `sideEffect`) | 0.1.0 (any build from `14a1fa0`) | refuses the history (unknown event) |
| 3 | Two steps in flight at once (parallel activities, `waitAny` races) | 0.1.0 (any build from `f7c18c1`) | refuses the history |
| 4 | A cancelled step, or a step closed between retries by a timeout or a cancel | 0.2.0 | refuses a history with a step closed between retries; reads a cancelled step as failed |
| 5 | Continue-as-new and graceful cancellation | 0.3.0 | reads a run that continued as new as completed (the chain stops); ignores a cancel request |
| 6 | A namespace other than `default`, a named task queue, or a paused, jittered or `BUFFER_ONE` schedule | 0.4.0 | does not see the namespace's workflows at all (they stop running); hands a named queue's tasks to any worker polling the default queue; fires a paused schedule, without jitter, and overlapping a held fire |
| 7 | An event encrypted at rest (`--encryption-keys`) | 0.5.0 | refuses the history (an event with no contents); `--decrypt-all` brings a directory back |

Statuses added as flags (`TIMED_OUT`, `CANCELLED`, `TERMINATED`) need no format: an older
binary shows them as `FAILED`, which is harmless. A reset run (0.5.0, ADR-0021) needs none
either: an older binary reads it as an ordinary run and the run it was reset from as an
ordinary earlier run. In a cluster, reset only once every node runs 0.5.0.

## Before you upgrade

1. Take a backup of the data directory (or of the VPS's `workflowd-backup` service). It is the
   only way back from a format you cannot roll back across.
2. Check the SDK table below: new SDK features need a new engine, never the other way round.
3. In a cluster, upgrade every node before any workflow uses a new feature. A node too old for
   an event stops instead of diverging.

## Rolling back

With the engine stopped, ask the **new** binary what the directory needs:

```sh
workflowd --check-format --data-dir /opt/durable/data
```

```
data directory:   /opt/durable/data
recorded format:  4 (needs workflowd 0.2.0 or later)
histories need:   4 (needs workflowd 0.2.0 or later; 12 histories scanned)
this binary:      reads up to format 5
OK: this binary can serve this data directory.
```

Then compare *histories need* with the table:

- **The target binary is 0.3.0 or later:** just install it. If it cannot read the directory it
  will not start, and its log says why; nothing is touched.
- **The target binary is older than 0.3.0:** it does not check. Roll back only if the format
  the histories need is one it reads (the table's third column).
- **It needs a newer format than the target reads:** either restore the backup you took before
  the upgrade (losing what ran since), or wait until the workflows that use the new feature
  have finished and retention has purged them, then run `--rescan-format` (below) and check
  again.

`--rescan-format` records the format the histories need now, which lowers it once the
histories that needed more are gone. Run it with the engine stopped.

`/api/health` reports `history_format` (the directory's) and `reads_history_format` (the
binary's), so a deploy can be checked from outside.

## SDKs and engines

An SDK works with every engine from the release that introduced the features it uses. Older
SDKs keep working against newer engines.

| SDK | Needs an engine from | Notes |
|-----|----------------------|-------|
| 0.1.x | 0.1.0 | |
| 0.2.0 | 0.2.0 for timeouts, cancellation, error types, `ctx.now()` | TS `ctx.now()` needs `started_at` on workflow tasks |
| 0.3.0 | 0.3.0 for continue-as-new, id reuse policies, graceful cancellation | |
| 0.5.0 | 0.5.0 for trace context on tasks (`ctx.traceparent`, an activity's `traceparent`) | Against an older engine the start's `traceparent` headers are ignored and tasks carry none, so the values are empty; nothing else changes |
| 0.4.0 | 0.4.0 for namespaces and task queues | An engine before 0.4.0 ignores the namespace header and the poll's `task_queue`, so a namespaced or queued worker would work in `default` there; it is refused instead only if it uses a namespace token. Starts and commands that name a queue get `400 unknown field` |

When an SDK sends a field an older engine does not know, the engine refuses the request
(`400 unknown field`). SDKs fall back where the result would otherwise be lost (activity error
types); otherwise the feature is simply unavailable until the engine is upgraded.

## Release notes on rollback

- **0.5.0:** encryption at rest (`--encryption-keys`) raises the directory to format 7 at the
  first sealed event; `--decrypt-all` (engine stopped) rewrites everything as plaintext and lowers
  it again, the way back to an older binary. Nothing else in 0.5.0 changes a format: a reset run
  and a traced run read as ordinary runs to an older binary, and the audit log
  (`<data-dir>/audit/`) is a file older binaries never look at. In a cluster, every node must run
  0.5.0 before a workflow is reset.

- **0.4.0:** the first history in a namespace other than `default`, the first event naming a
  task queue other than the default one, or the first schedule paused, jittered or set to
  `BUFFER_ONE`, raises the directory to format 6 (namespaced histories
  live under `namespaces/`, which older binaries do not read; older binaries route by step name
  alone). Upgrading without using either keeps the format, and the rollback, where they were.
- **0.3.0:** adds formats (this document). A workflow that continues as new raises the
  directory to format 5 and moves its finished runs to `__workflowd-run~...` history files,
  which older binaries ignore. The new limits apply to existing workflows as soon as you
  upgrade: one already past 51,200 events (or 50 MiB) fails at its next event, so raise
  `--max-history-events` / `--max-history-bytes` first if you have any that long
  (`history_length` in `GET /api/workflows`). Payloads over 2 MiB are refused from now on.
- **0.2.0** (format 4): older binaries read timed-out and cancelled workflows as `FAILED`,
  ignore timeouts and non-retryable flags, and refuse a history where a step between retries
  was closed by a deadline or a cancel.
- **Parallel steps** (`f7c18c1`, format 3): older binaries refuse a history with two steps in
  flight.
- **Markers** (`14a1fa0`, format 2): older binaries refuse a history with a marker.
