# ADR-0010: Non-retryable failures and error types

- **Status:** Accepted
- **Date:** 2026-10-23

## Context

A retry policy retried every failure the same way. liftoff delivers webhooks with six
attempts over about two and a half minutes, which is right for a timeout or a 503 and wrong
for a 404: the receiver will answer the same way every time. Unable to say so, liftoff's
activity returned a "settled, not delivered" result instead of throwing, so the workflow's
own status said "completed" for a delivery that failed.

## Decision

1. **A worker can say a failure is not worth retrying.** `CompleteTask` (HTTP activity
   completion) takes `non_retryable` with an error. The engine records it on `StepFailed` and
   the failure is the step's result on that attempt, whatever the policy allows.
2. **Failures have a type.** `CompleteTask` takes `error_type`, recorded on `StepFailed`.
   SDKs fill it from the error itself: the class name in TypeScript (`err.name`, unless plain
   `Error`), Python and Java; Go has no class names, so `worker.Typed(type, err)` sets it. The
   engine types its own failures: `LeaseExpired`, `StartToCloseTimeout`,
   `ScheduleToCloseTimeout`.
3. **A retry policy can list types it does not retry** (`RetryPolicy.non_retryable_error_types`,
   recorded with the policy on `StepScheduled`). The decision lives in one place,
   `retryAfter`, next to the attempt count and the schedule-to-close deadline, and it reads
   the type and flag from history, so recovery makes the same decision after a crash.
4. **Workflow code sees both**: Go `*workflow.ActivityError{Type, NonRetryable}`, TypeScript,
   Python and Java `ActivityFailure.type` / `nonRetryable`. The error text is unchanged.
5. **Compatibility with older engines.** New SDKs send `error_type` on most failures, and an
   older engine's strict JSON decoding refuses unknown fields. Rather than lose the failure
   (the lease would expire and the attempt would count as a dead worker), the SDKs resend the
   completion without the two fields when an engine answers `400 unknown field`. The policy
   list is sent only when non-empty.

## Consequences

- liftoff can throw `NonRetryableError` for a 4xx and let the workflow fail (or compensate)
  instead of encoding the outcome in a result.
- `testsuite` applies the same rules in virtual time.
- **Rollback:** an older binary ignores the flag and the policy list and retries such failures
  until attempts run out; histories stay readable both ways.

## Alternatives considered

- **Only a policy list, no worker flag.** Forces every caller to know every error type an
  activity may raise; the activity knows best that a 404 is final.
- **Only the worker flag.** The workflow author could not override an activity shared by many
  workflows, nor stop retrying the engine's own failures (a start-to-close timeout).
- **Type from the Go error's dynamic type** (`%T`). Gives `*errors.errorString`; meaningless.
