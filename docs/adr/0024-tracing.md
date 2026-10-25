# ADR-0024: Distributed tracing with W3C Trace Context and OTLP

- **Status:** Accepted
- **Date:** 2026-10-25

## Context

A request into liftoff that starts a workflow disappears from its trace at the engine: the
workflow runs, its activities call webhooks, and nothing ties those calls back to the request
that caused them. Teams that run OpenTelemetry expect any component to carry the trace through
(W3C Trace Context, the `traceparent` header) and to report its own spans to their collector.

Forces:

- **Zero new dependencies.** The OpenTelemetry Go SDK is a large tree of modules; the engine's
  only dependencies are gRPC and protobuf.
- **Durable and deterministic.** A run lasts across restarts, failovers and continue-as-new; its
  activities must keep the same parent whenever they are dispatched, and workflow code that reads
  its trace context must read the same value on every replay.
- **Every SDK**, through the same protocol, without requiring OpenTelemetry in them either.
- **Off costs nothing**: an engine without a collector, and a start without a traceparent, record
  and send nothing extra.

## Decision

1. **A run is a span.** A start that carries a W3C traceparent (the `traceparent`/`tracestate`
   HTTP headers, the `StartWorkflowRequest.traceparent` field, or `traceparent` gRPC metadata as
   OpenTelemetry's gRPC instrumentation sends it) makes the run a child of the caller's span. The
   run's context (trace id, its own span id, the parent's, the sampled flag and tracestate) is
   recorded on WorkflowStarted (`trace`), so it survives restarts and failovers. Without a
   traceparent, a run is traced only if the engine exports traces (a new root); otherwise nothing is
   recorded. A malformed traceparent is ignored, as the specification asks.
2. **The trace is carried, not just recorded.** Workflow tasks carry the run's span as a
   traceparent (the same on every task, so replay can read it: `ctx.traceparent` in every SDK,
   `workflow.Info.TraceParent` in Go). Activity tasks carry a new span per attempt, a child of the
   run's: `task.traceparent` (TypeScript), `info["traceparent"]` (Python),
   `ActivityContext.traceparent()` (Java), `worker.ActivityInfo.TraceParent` (Go), so an activity can
   put it on the requests it makes and they join the same trace. Continue-as-new and reset start a
   new span in the same trace, a child of the run before. Schedule fires begin their own traces.
3. **Optional export over OTLP/HTTP**, `--otlp-endpoint URL` (with `--otlp-headers-file` for API
   keys and `--trace-service-name`): the engine sends a span per run (`workflow <type>`, from start
   to end, kind SERVER under a caller and INTERNAL as a root, status ERROR for a failure or timeout),
   per activity attempt (`activity <step>`, dispatch to result, with the attempt, worker, task queue
   and outcome: completed, failed, timed out, lease expired, abandoned or cancelled), and per SDK
   workflow task (`workflow task <type>`, dispatch to answer). Only sampled spans are sent.
4. **A hand-rolled encoder.** The OTLP `ExportTraceServiceRequest` is written field by field with
   `protowire` (part of the protobuf module the engine already uses), from the field numbers of the
   OpenTelemetry protocol; it was checked by decoding its output with `protoc` against the
   upstream `.proto` files. Spans go out in batches (256, or every 2 seconds), from a bounded queue:
   exporting never blocks the engine, a full queue drops spans, a 429 or 5xx is retried once.
   `workflowd_trace_spans_exported_total`, `..._dropped_total` and `workflowd_trace_export_failures_total`
   count what happened.

## Consequences

- No new history format: older binaries ignore `WorkflowStarted.trace` and read the run as before.
  New task fields are ignored by older SDKs; the SDKs send the context as the standard headers,
  which older engines ignore.
- A run's span is exported only when it ends, so a long-running workflow appears in the trace
  backend when it finishes; its activity attempts appear as they finish.
- Spans in flight when the engine stops are lost (an attempt's span lives in memory with its
  lease); the run's span is not, since its context is in history and it is sent at its end.
- Workflow code should treat `ctx.traceparent` as context for logs and calls made by activities,
  not as data: a reset run has a different span.
- The engine does not sample: it follows the caller's sampled flag and samples every root. A
  collector-side sampler can thin them.

## Alternatives considered

- **The OpenTelemetry SDK.** Spans, propagation and exporters for free, at the cost of dozens of
  modules and their churn in a single-binary engine; the protocol is small enough to write.
- **OTLP/JSON.** Simpler to produce with `encoding/json`, but protobuf is the default every
  collector accepts and is a third of the size; `protowire` makes it as easy.
- **A span per workflow event** (each timer, signal and marker). Detailed, but noisy and costly for
  long workflows; history already has that detail, and the dashboard shows it.
- **Propagating through SDK clients automatically** (an activity's context flowing into a client
  call made inside it). Useful, but it needs async-context machinery in each language; the explicit
  `traceparent` option on start is the first step.
