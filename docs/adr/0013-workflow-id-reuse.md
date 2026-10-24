# ADR-0013: Workflow id reuse and conflict policies

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

A caller-chosen workflow id is how clients make starts idempotent: liftoff starts a delivery
under an id derived from the delivery, so a retried request cannot start a second one. Before
this ADR the only answer to "this id has history" was `409 already exists`, whatever that
history was. Two common needs had no expression:

- **"Start it, or give me the one already there."** A client retrying a start after a lost
  reply, or two services racing to start the same entity, want the run that holds the id, not
  an error to special-case. The SDKs swallowed 409s only for ids they generated themselves.
- **"Start it again."** A nightly job, or a tenant re-provisioned after a failure, wants to reuse
  a business id once the earlier run is over: always, or only if it failed.

Forces: the default must stay what every engine has done, since callers rely on the 409 for
idempotency; two starts racing for one id must never both begin a run; and the earlier run
must not be lost, because it is the record of what happened.

## Decision

1. **Two start options**, in `StartWorkflowRequest` and every SDK:
   - `id_reuse_policy`, for an id whose run has finished: `REJECT_DUPLICATE` (the default, as
     before), `ALLOW_DUPLICATE`, or `ALLOW_DUPLICATE_FAILED_ONLY` (failed, timed out, cancelled
     or terminated, not completed).
   - `id_conflict_policy`, for an id that is taken (a run still going, or a finished one the
     reuse policy does not let this start replace): `FAIL` (the default, `ALREADY_EXISTS` /
     409) or `USE_EXISTING`, which returns that run instead.
2. **Starts say what they did:** `StartWorkflowResponse.run_id` and `started` (false when an
   existing run was returned). HTTP answers 201 for a new run and 200 for an existing one.
3. **Reuse is a new run** (ADR-0012): the finished run is kept as an earlier run of the id,
   through the same `NewRun` store operation as continue-as-new, and is listed by
   `ListWorkflowRuns` until retention removes it. A running workflow is never replaced.
4. **The check and the start happen under the workflow's lock**, so of any number of starts
   racing for one id exactly one begins a run; the others see it running. `NewRun` also refuses
   to replace a run that has not finished, so a mistake elsewhere cannot archive a live run.
   In a cluster the leader's engine does both, and the `NewRun` command is refused on every
   node unless the history ends with a terminal event.

## Consequences

- liftoff-style idempotent starts can use `USE_EXISTING` instead of catching 409s, and get the
  run id either way.
- Reusing an id changes what "the workflow" means to readers: describing the id shows the new
  run; earlier runs need `run_id`. Signals go to the current run.
- Schedules keep their own rule (a fire's id is unique per fire time) and use the defaults.
- Engines before 0.3.0 refuse the new fields (`400 unknown field`); the SDKs send them only
  when set. No history format change: reuse writes what continue-as-new writes.

## Alternatives considered

- **Reuse by deleting the finished history.** Simpler, but loses the record of the earlier run,
  which is usually why someone is looking.
- **A separate "get or start" RPC.** Same effect, but a second call shape for every SDK; an
  option on the existing start keeps one.
- **Terminate-and-replace a running workflow** (Temporal's `TERMINATE_EXISTING`). Destructive
  enough to deserve its own explicit call; terminate then start does it today.
