# ADR-0002: Protocol Buffers for events and the gRPC API

- **Status:** Accepted
- **Date:** 2026-06-21

## Context

We need a serialization format for (a) events written to the on-disk history and (b) the
public gRPC API workers and clients use. The blueprint allows either hand-rolled
`encoding/binary` or Protocol Buffers for the on-disk payload, but the gRPC API (Week 3)
mandates protobuf regardless. We also need schema evolution: new event types will be added
over the project's life without breaking already-written history.

## Decision

Use **Protocol Buffers** for event payloads and reuse the same message types for the gRPC
API. Events are modeled as a single `HistoryEvent` wrapper containing a `oneof attributes`,
Temporal-style:

```proto
message HistoryEvent {
  int64 event_id = 1;     // monotonic per workflow
  int64 timestamp = 2;    // unix nanos
  oneof attributes {
    WorkflowStartedEvent workflow_started = 10;
    StepScheduledEvent   step_scheduled   = 11;
    // ... one field per event type; new types get new field numbers
  }
}
```

The **record framing** around the protobuf payload (length + CRC32C) is still hand-rolled with
`encoding/binary` — so we get the raw-serialization learning *and* protobuf's schema evolution.

## Consequences

- **Pro:** one schema for storage and wire; adding an event type = adding a `oneof` field
  (old logs still parse); efficient binary encoding; strong typing; generated Go.
- **Con:** requires `protoc` + `protoc-gen-go` in the toolchain; generated code is committed
  (so CI needn't run protoc). A `oneof` wrapper is slightly more verbose to construct in Go.
- **Follow-up:** adopt **`buf`** (replaces raw `protoc`) for proto linting and automated
  breaking-change detection — directly enforces the schema-evolution discipline. Tracked in
  the roadmap backlog.

## Alternatives considered

- **`encoding/binary` hand-rolled events:** teaches serialization deeply but would be thrown
  away the moment gRPC arrives, and reinvents schema-evolution poorly. Rejected for events;
  kept for the framing layer.
- **Flat message per event type (no wrapper):** simpler messages but no uniform envelope for
  `event_id`/`timestamp` and a clumsier read path. Rejected.
- **JSON / MessagePack:** no schema/codegen story for gRPC; rejected.
