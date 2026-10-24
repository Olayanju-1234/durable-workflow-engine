# ADR-0020: Schedule controls

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

Schedules (Session 24) could be created, listed and deleted, and could skip a fire while an
earlier run was going. Running them for real asked for more: stop one during a migration
without losing its definition; run the fire that came while a long run was going, once it is
done, rather than drop it; spread schedules that all fire at the top of the hour; re-run the
fires a broken deploy missed last night; and see when a cron expression actually fires (time
zones and daylight saving make that hard to do in one's head).

Forces: schedules are stored as spec upserts in a reserved history (ADR-0004's replication,
ADR-0011's formats); a fire's workflow id is `<schedule id>-<fire time>`, which is what makes
firing idempotent and must stay so; and a binary that predates a control must not fire wrongly.

## Decision

1. **Every control is a field of the spec**, recorded by the same upsert event as before, so it
   replicates in a cluster and survives restarts with no new event types:
   - `paused` and `note`: a paused schedule arms no timer; fires due while it is paused are not
     started later, and recovery catches nothing up for it. `PauseSchedule` / `ResumeSchedule`
     record the spec with the flag changed.
   - `overlap_policy`: `SKIP` (the old `skip_if_running`, which is still accepted and is recorded
     alongside, so an older binary skips too), `BUFFER_ONE` (hold the fire that came while a run was
     going and start it as soon as no run is going; one held at a time, later ones skipped, and a
     held fire goes before any newer one), `ALLOW_ALL` (start it anyway; the old default).
   - `jitter_ms`: each run starts at its fire time plus an offset in `[0, jitter)` fixed by a hash
     of the schedule and the fire time, so a restart or the preview picks the same offset. The
     run's id keeps the fire time.
2. **Backfill** starts the runs a schedule would have started for its fire times in a past range
   (after the start, up to the end), under the same ids a fire uses: a fire that already ran, or a
   repeated backfill, is counted, not started again. `ALLOW_ALL` (the default) or `SKIP`; at most
   1,000 fires per call. It works on a paused schedule, and every start goes through the
   namespace's quotas (ADR-0018).
3. **Preview** returns the next fire times of a schedule, or of a spec not yet created, with each
   run's jittered start, so a cron expression can be checked before it is saved.
4. **Formats.** A paused schedule, jitter or `BUFFER_ONE` would be ignored by an older binary,
   which would then fire wrongly; those specs are stamped format 6. `SKIP` and `ALLOW_ALL` are not:
   older binaries read them the same way.
5. **Everywhere.** gRPC (`PauseSchedule`, `ResumeSchedule`, `BackfillSchedule`,
   `PreviewSchedule`), HTTP (`/api/schedules/{id}/pause|resume|backfill|preview`,
   `POST /api/schedules/preview`), the Go client, `workflowd ctl schedule ...`, and the dashboard's
   schedules panel. Pause, resume and backfill are schedule changes for the guardrails (read-only
   schedules refuse them); preview is a read.

## Consequences

- A held fire lives in memory. After a restart, recovery's catch-up takes the latest fire missed in
  the last day instead, and holds it if a run is still going: the fire that runs may be a later one
  than the fire that was held, which is what holding the latest fire means anyway.
- A paused schedule's spec is still there to resume; deleting it is a separate, explicit call.
- Creating a schedule again (an upsert) resumes it unless the new spec says paused.
- Jitter larger than the interval between fires can start runs out of fire order; the docs say
  to keep it smaller.

## Alternatives considered

- **New event types for pause and resume.** Clearer in the raw history, but older binaries would
  refuse the schedule history outright; a field on the upsert they already read, plus a format
  stamp where they would misread it, keeps rollbacks open for schedules that use none of it.
- **Random jitter per fire.** Simpler, but a restart would re-roll it and the preview could not
  show it.
- **A backfill that ignores the fire ids** (always new runs). Not idempotent: retrying a request
  after a timeout would double every run.
