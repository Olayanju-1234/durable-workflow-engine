# ADR-0019: Archiving finished histories

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

Retention (`--retention`) deletes a finished workflow's history a while after its last event,
so the data directory does not grow forever. The demo has run with 168 hours since Session 15.
Deleting is final: once retention runs, there is no way to answer "what did order 7 do last
month?". The daily VPS backups are the only copy, kept 14 days, on the same disk.

Forces: no new dependencies, so no AWS SDK; R2 (S3-compatible, cheap egress) and a plain
directory (another disk, a mounted volume) should both work; a history must never be deleted
unless the archive really has it; archived histories must stay readable by namespace, through
the same API and token rules as everything else.

## Decision

1. **Archive before delete.** With `--archive`, retention exports a finished run's history to
   the archive store and deletes it only after the store confirmed it. If the export fails, the
   history stays on disk and is tried again at the next pass (`workflowd_archive_failures_total`);
   if the delete fails after an export, the next pass exports again (same key, same bytes) and
   deletes. Earlier runs (continue-as-new) are archived the same way.
2. **What is archived** is the history file's bytes, unchanged, under
   `<namespace>/<workflow id>/<closed at, 16 hex>~<run id>.history`: names sort by when the run
   closed, and any binary that reads history files reads an archived one.
3. **Two stores** behind one interface (`Put` durable and intact, `Get`, `List`):
   - a **directory** (`--archive /path` or `file:///path`): temp file, fsync, rename, fsync the
     directory; keys are checked so none can leave it;
   - **S3-compatible** (`--archive s3://bucket/prefix?endpoint=...&region=...`): path-style
     requests signed with a hand-written AWS Signature Version 4, tested against AWS's own
     test-suite vectors. Every PUT carries `Content-MD5` and the payload's SHA-256, so the store
     refuses a body that arrived damaged, and the ETag is checked against the MD5. The region
     defaults to `auto` for an R2 endpoint. Credentials come from `--archive-credentials-file` or
     the environment (`WORKFLOWD_ARCHIVE_*`, else `AWS_*`), never a flag's value.
4. **Read path**, in the caller's namespace: `GET /api/archive/workflows[?prefix=]`,
   `GET /api/archive/workflows/{id}[?run_id=]` (described like a live run) and
   `.../history`; `workflowd ctl archive list|describe|history`. Without an archive they answer
   501. `/api/health` says whether archiving is on (`archiving`), not where.

## Consequences

- Retention can be short without losing records: the data directory stays small, the archive
  keeps everything.
- A misconfigured archive (bad credentials, a full disk) stops retention from deleting, which
  is loud in the logs and the metric, and never loses data. Disk use grows until it is fixed.
- Uploads run under the finished workflow's lock, one at a time; a slow store slows retention,
  not live workflows.
- Archived runs are not restored into the data directory: they are read where they are. A
  history file from the archive can be copied back by hand if one ever needs to run again.
- An S3 store with millions of runs lists slowly by prefix; the API pages by workflow-id prefix
  and a limit.

## Alternatives considered

- **An AWS SDK.** Correct and featureful, and a large dependency tree for three requests.
  SigV4 for PUT, GET and ListObjectsV2 is about a hundred lines and testable against AWS's vectors.
- **Archive at completion instead of at deletion.** Doubles the writes of every workflow and
  needs the same failure handling; at deletion is the only point where losing it matters.
- **A compressed bundle per pass.** Fewer objects, but reading one run means fetching a bundle,
  and a failed upload holds back every run in it.
