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
