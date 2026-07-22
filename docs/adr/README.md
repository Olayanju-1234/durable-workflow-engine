# Architecture Decision Records

Each ADR captures one significant, hard-to-reverse decision: the context, the choice, and
its consequences. They are immutable once accepted — to change a decision, write a new ADR
that supersedes the old one.

| # | Title | Status |
|---|-------|--------|
| [0001](0001-record-format.md) | Append-only, CRC-framed record format for the history store | Accepted |
| [0002](0002-protobuf-events.md) | Protocol Buffers for events and the gRPC API | Accepted |
| [0003](0003-single-binary-embedded.md) | Single-binary, zero-dependency, embeddable engine | Accepted |

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
