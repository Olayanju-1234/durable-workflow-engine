package server

import (
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/internal/tracing"
)

// Tracing (ADR-0024). A run is a span: a child of the caller's span when its
// start carried a W3C traceparent, a root of its own when the engine exports
// traces and it did not. The run's trace is recorded on WorkflowStarted, so
// it survives restarts and every task of the run carries the same parent:
// workflow tasks carry the run's span (replay can read it), activity tasks a
// span of their own per attempt, a child of the run's. With an exporter the
// engine sends the runs, the attempts and SDK workflow tasks as spans over
// OTLP/HTTP; without one it still passes the context on.

// WithTracer exports the engine's spans with x, and gives every run a trace
// even when its start carried none.
func WithTracer(x *tracing.Exporter) Option {
	return func(e *Engine) { e.tracer = x }
}

// WithTraceParent makes the run a child of the caller's span (W3C
// traceparent and tracestate). A malformed traceparent is ignored.
func WithTraceParent(traceparent, tracestate string) StartOption {
	return func(o *startOptions) { o.traceparent, o.tracestate = traceparent, tracestate }
}

// runTrace is the trace a new run records: a child of the caller's span, a
// new root (when exporting), or none.
func (e *Engine) runTrace(traceparent, tracestate string) *history.TraceContext {
	if c, ok := tracing.Parse(traceparent, tracestate); ok {
		span := tracing.NewSpanID()
		return &history.TraceContext{TraceId: c.TraceID[:], SpanId: span[:], ParentSpanId: c.SpanID[:],
			Sampled: c.Sampled, TraceState: c.State}
	}
	if e.tracer == nil {
		return nil
	}
	trace, span := tracing.NewTraceID(), tracing.NewSpanID()
	return &history.TraceContext{TraceId: trace[:], SpanId: span[:], Sampled: true}
}

// childTrace is the trace of a run that follows tc's (continue-as-new, a
// reset): the same trace, a new span, tc's span its parent.
func childTrace(tc *history.TraceContext) *history.TraceContext {
	if tc == nil {
		return nil
	}
	span := tracing.NewSpanID()
	return &history.TraceContext{TraceId: tc.GetTraceId(), SpanId: span[:], ParentSpanId: tc.GetSpanId(),
		Sampled: tc.GetSampled(), TraceState: tc.GetTraceState()}
}

// spanContext is tc's own span as a tracing.Context (invalid for nil).
func spanContext(tc *history.TraceContext) tracing.Context {
	return tracing.FromIDs(tc.GetTraceId(), tc.GetSpanId(), tc.GetSampled(), tc.GetTraceState())
}

// runTraceparent is the run's span as a traceparent ("" when untraced).
func runTraceparent(tc *history.TraceContext) (traceparent, tracestate string) {
	c := spanContext(tc)
	return c.Traceparent(), c.State
}

// attemptEnd says how an activity attempt ended, for its span.
type attemptEnd struct {
	status    tracing.StatusCode
	message   string
	errorType string
	outcome   string
}

// startAttemptSpan gives a dispatched task its own span, a child of the
// run's (which the task carried until now), and keeps it on the lease.
func startAttemptSpan(t *Task, le *lease) {
	parent, ok := tracing.Parse(t.GetTraceparent(), t.GetTracestate())
	if !ok {
		return
	}
	le.parent = parent.SpanID
	le.span = tracing.Context{TraceID: parent.TraceID, SpanID: tracing.NewSpanID(), Sampled: parent.Sampled, State: parent.State}
	le.attempt, le.queue = t.GetAttempt(), t.GetTaskQueue()
	t.Traceparent, t.Tracestate = le.span.Traceparent(), le.span.State
}

// exportAttempt sends an attempt's span, if it has one and is sampled.
func (e *Engine) exportAttempt(le *lease, how attemptEnd) {
	if e.tracer == nil || le == nil || !le.span.Valid() {
		return
	}
	attrs := []tracing.Attr{
		{Key: "workflowd.namespace", Value: nsOf(le.workflowID)},
		{Key: "workflowd.workflow_id", Value: localID(le.workflowID)},
		{Key: "workflowd.step", Value: le.stepName},
		{Key: "workflowd.sequence", Value: le.sequence},
		{Key: "workflowd.attempt", Value: int(le.attempt)},
		{Key: "workflowd.task_queue", Value: le.queue},
		{Key: "workflowd.worker_id", Value: le.workerID},
		{Key: "workflowd.outcome", Value: how.outcome},
	}
	if how.errorType != "" {
		attrs = append(attrs, tracing.Attr{Key: "workflowd.error_type", Value: how.errorType})
	}
	e.tracer.Export(tracing.Span{Context: le.span, Parent: le.parent, Name: "activity " + le.stepName,
		Kind: tracing.KindInternal, Start: le.claimedAt, End: time.Now(), Attrs: attrs,
		Status: how.status, StatusMessage: how.message})
}

// exportRunSpan sends a finished run's span: from its start to its end.
func (e *Engine) exportRunSpan(workflowID string, events []*history.HistoryEvent, st *state.State) {
	if e.tracer == nil || len(events) == 0 {
		return
	}
	c := spanContext(st.Trace)
	if !c.Valid() {
		return
	}
	var parent tracing.SpanID
	copy(parent[:], st.Trace.GetParentSpanId())
	kind := tracing.KindInternal
	if !parent.IsZero() {
		kind = tracing.KindServer
	}
	status, msg := tracing.StatusOK, ""
	switch st.Status {
	case state.StatusFailed, state.StatusTimedOut:
		if !st.Terminated {
			status, msg = tracing.StatusError, st.Error
		} else {
			status = tracing.StatusUnset
		}
	case state.StatusCancelled:
		status = tracing.StatusUnset
	}
	e.tracer.Export(tracing.Span{Context: c, Parent: parent, Name: "workflow " + st.WorkflowType, Kind: kind,
		Start: time.Unix(0, events[0].GetTimestamp()), End: time.Unix(0, events[len(events)-1].GetTimestamp()),
		Status: status, StatusMessage: msg, Attrs: []tracing.Attr{
			{Key: "workflowd.namespace", Value: nsOf(workflowID)},
			{Key: "workflowd.workflow_id", Value: localID(workflowID)},
			{Key: "workflowd.workflow_type", Value: st.WorkflowType},
			{Key: "workflowd.run_id", Value: st.RunID},
			{Key: "workflowd.task_queue", Value: state.QueueName(st.TaskQueue)},
			{Key: "workflowd.status", Value: protoStatusName(st)},
			{Key: "workflowd.history_length", Value: len(events)},
		}})
}

// exportDecision sends an SDK workflow task's span: dispatch to answer.
func (e *Engine) exportDecision(workflowID string, st *state.State, p *decisionTask, commands int) {
	if e.tracer == nil || p == nil {
		return
	}
	run := spanContext(st.Trace)
	if !run.Valid() {
		return
	}
	span := run
	span.SpanID = tracing.NewSpanID()
	e.tracer.Export(tracing.Span{Context: span, Parent: run.SpanID, Name: "workflow task " + st.WorkflowType,
		Kind: tracing.KindInternal, Start: p.dispatchedAt, End: time.Now(), Attrs: []tracing.Attr{
			{Key: "workflowd.namespace", Value: nsOf(workflowID)},
			{Key: "workflowd.workflow_id", Value: localID(workflowID)},
			{Key: "workflowd.history_length", Value: p.historyLength},
			{Key: "workflowd.commands", Value: commands},
		}})
}

// registerTraceCounters reports the exporter's counts on /metrics.
func (e *Engine) registerTraceCounters() {
	stat := func(i int) func() float64 {
		return func() float64 {
			exported, dropped, failed := e.tracer.Stats()
			return float64([]uint64{exported, dropped, failed}[i])
		}
	}
	e.registry.CounterFunc("workflowd_trace_spans_exported_total", "Spans sent to the OTLP collector.", stat(0))
	e.registry.CounterFunc("workflowd_trace_spans_dropped_total",
		"Spans not sent: the export queue was full, or the collector refused or could not be reached.", stat(1))
	e.registry.CounterFunc("workflowd_trace_export_failures_total", "OTLP export requests that failed after a retry.", stat(2))
}
