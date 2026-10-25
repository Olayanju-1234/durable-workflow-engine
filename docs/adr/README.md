# Architecture Decision Records

Each ADR captures one significant, hard-to-reverse decision: the context, the choice, and
its consequences. They are immutable once accepted — to change a decision, write a new ADR
that supersedes the old one.

| # | Title | Status |
|---|-------|--------|
| [0001](0001-record-format.md) | Append-only, CRC-framed record format for the history store | Accepted |
| [0002](0002-protobuf-events.md) | Protocol Buffers for events and the gRPC API | Accepted |
| [0003](0003-single-binary-embedded.md) | Single-binary, zero-dependency, embeddable engine | Accepted |
| [0004](0004-replicated-history-leader-only-engine.md) | Replicate history through Raft; run the engine on the leader only | Accepted |
| [0005](0005-public-go-sdk.md) | The public Go SDK: workflow, client, worker, embedded | Accepted |
| [0006](0006-keep-file-per-workflow-store.md) | Keep the file-per-workflow history store; no LSM engine | Accepted |
| [0007](0007-parallel-steps.md) | Parallel steps and batched commands | Accepted |
| [0008](0008-timeouts.md) | Workflow and activity timeouts | Accepted |
| [0009](0009-cancellation.md) | Cancelling workflows and steps | Accepted |
| [0010](0010-non-retryable-failures.md) | Non-retryable failures and error types | Accepted |
| [0011](0011-history-format-contract.md) | History formats and the upgrade/rollback contract | Accepted |
| [0012](0012-continue-as-new-and-limits.md) | Continue-as-new, runs, and history and payload limits | Accepted |
| [0013](0013-workflow-id-reuse.md) | Workflow id reuse and conflict policies | Accepted |
| [0014](0014-graceful-cancellation.md) | Graceful cancellation | Accepted |
| [0015](0015-namespaces.md) | Namespaces | Accepted |
| [0016](0016-task-queues.md) | Named task queues | Accepted |
| [0017](0017-ctl-command-line.md) | `workflowd ctl`, the operator's command line | Accepted |
| [0018](0018-guardrails.md) | Engine guardrails: quotas, rate limits, read-only | Accepted |
| [0019](0019-archiving.md) | Archiving finished histories | Accepted |
| [0020](0020-schedule-controls.md) | Schedule controls | Accepted |
| [0021](0021-reset.md) | Resetting a finished run | Accepted |

## Template

```markdown
# ADR-NNNN: Title

- **Status:** Proposed | Accepted | Superseded by ADR-XXXX
- **Date:** YYYY-MM-DD

## Context
What forces are at play? What problem are we solving?

## Decision
What we decided, stated plainly.

## Consequences
Trade-offs, what becomes easier/harder, follow-ups.

## Alternatives considered
What else we looked at and why we passed.
```
