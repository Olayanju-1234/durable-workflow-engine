package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/tracing"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

const callerTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

type fakeCollector struct {
	mu    sync.Mutex
	spans []tracing.Span
}

func (c *fakeCollector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_, spans, err := tracing.DecodeExport(body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	c.spans = append(c.spans, spans...)
	c.mu.Unlock()
}

func (c *fakeCollector) named(prefix string) []tracing.Span {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []tracing.Span
	for _, s := range c.spans {
		if strings.HasPrefix(s.Name, prefix) {
			out = append(out, s)
		}
	}
	return out
}

func tracedEngine(t *testing.T, fns map[string]workflow.Func, opts ...Option) (*Engine, *fakeCollector, *tracing.Exporter) {
	t.Helper()
	col := &fakeCollector{}
	srv := httptest.NewServer(col)
	t.Cleanup(srv.Close)
	x := tracing.NewExporter(tracing.ExporterConfig{Endpoint: srv.URL, FlushInterval: 20 * time.Millisecond})
	t.Cleanup(x.Close)
	e := timeoutEngine(t, newStore(t, t.TempDir()), fns, append([]Option{WithTracer(x)}, opts...)...)
	return e, col, x
}

// A start that carries a caller's traceparent makes the run a child span of
// it; every activity attempt gets a span of its own under the run; and the
// engine exports them all, linked, when the run ends.
func TestTraceFromStartThroughActivities(t *testing.T) {
	e, col, x := tracedEngine(t, map[string]workflow.Func{"Pipeline": pipeline})
	broken := true
	w := newStepWorker(func(step string) bool { return step == "b" && broken })
	if _, err := e.Start("Pipeline", []byte("in"), "tr-1", WithTraceParent(callerTP, "vendor=x")); err != nil {
		t.Fatal(err)
	}
	caller, _ := tracing.Parse(callerTP, "")
	evs, _ := e.store.ReadAll("tr-1")
	run := evs[0].GetWorkflowStarted().GetTrace()
	runCtx := spanContext(run)
	if runCtx.TraceID != caller.TraceID || string(run.GetParentSpanId()) != string(caller.SpanID[:]) ||
		runCtx.SpanID == caller.SpanID || !run.GetSampled() || run.GetTraceState() != "vendor=x" {
		t.Fatalf("run trace %v", run)
	}

	task := mustPoll(t, e, "w", time.Second)
	attempt, ok := tracing.Parse(task.GetTraceparent(), task.GetTracestate())
	if !ok || attempt.TraceID != caller.TraceID || attempt.SpanID == runCtx.SpanID || task.GetTracestate() != "vendor=x" {
		t.Fatalf("activity task traceparent %q", task.GetTraceparent())
	}
	if err := e.CompleteTask(task.GetWorkflowId(), task.GetStepName(), task.GetSequence(), task.GetLeaseToken(), []byte("a"), "", false); err != nil {
		t.Fatal(err)
	}
	w.runOne(t, e) // b fails for good, so the run fails
	waitFinished(t, e, "tr-1", 3*time.Second)
	x.Close()

	runs := col.named("workflow Pipeline")
	if len(runs) != 1 {
		t.Fatalf("%d run spans", len(runs))
	}
	r := runs[0]
	if r.TraceID != caller.TraceID || r.SpanID != runCtx.SpanID || r.Parent != caller.SpanID || r.Kind != tracing.KindServer ||
		r.Status != tracing.StatusError || r.Attr("workflowd.workflow_id") != "tr-1" || r.Attr("workflowd.status") != "FAILED" ||
		!r.End.After(r.Start) {
		t.Fatalf("run span %+v", r)
	}
	acts := col.named("activity ")
	if len(acts) != 2 {
		t.Fatalf("%d activity spans, want a and b", len(acts))
	}
	for _, a := range acts {
		if a.TraceID != caller.TraceID || a.Parent != runCtx.SpanID || a.Attr("workflowd.attempt") != int64(1) {
			t.Fatalf("activity span %+v", a)
		}
		switch a.Name {
		case "activity a":
			if a.SpanID != attempt.SpanID || a.Status != tracing.StatusOK {
				t.Fatalf("a's span %+v (the task carried %s)", a, attempt.SpanID)
			}
		case "activity b":
			if a.Status != tracing.StatusError || a.StatusMessage != "b broke" || a.Attr("workflowd.outcome") != "failed" {
				t.Fatalf("b's span %+v", a)
			}
		}
	}
}

// Without an exporter the context is still carried; without a traceparent
// and without an exporter, nothing is recorded at all.
func TestTraceContextWithoutExport(t *testing.T) {
	e := timeoutEngine(t, newStore(t, t.TempDir()), map[string]workflow.Func{"Pipeline": pipeline})
	if _, err := e.Start("Pipeline", nil, "plain-1"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Start("Pipeline", nil, "bad-tp", WithTraceParent("00-garbage", "")); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"plain-1", "bad-tp"} {
		evs, _ := e.store.ReadAll(id)
		if evs[0].GetWorkflowStarted().GetTrace() != nil {
			t.Fatalf("%s recorded a trace with no traceparent and no exporter", id)
		}
	}
	unsampled := strings.TrimSuffix(callerTP, "01") + "00"
	if _, err := e.Start("Pipeline", nil, "carried", WithTraceParent(unsampled, "")); err != nil {
		t.Fatal(err)
	}
	evs, _ := e.store.ReadAll("carried")
	if tr := evs[0].GetWorkflowStarted().GetTrace(); tr == nil || tr.GetSampled() {
		t.Fatalf("an unsampled caller's context: %v", tr)
	}
	for range 3 {
		task := mustPoll(t, e, "w", time.Second)
		if task.GetWorkflowId() == "carried" && !strings.HasPrefix(task.GetTraceparent(), "00-4bf92f3577b34da6a3ce929d0e0e4736-") {
			t.Fatalf("activity of a traced run: %q", task.GetTraceparent())
		}
		if task.GetWorkflowId() == "plain-1" && task.GetTraceparent() != "" {
			t.Fatalf("activity of an untraced run: %q", task.GetTraceparent())
		}
	}
}

// With an exporter, a start without a traceparent begins a trace of its own;
// continue-as-new keeps the trace with a new span under the last run's.
func TestTraceRootsAndContinueAsNew(t *testing.T) {
	e, col, x := tracedEngine(t, map[string]workflow.Func{"Countdown": countdown})
	runWorker(t, e)
	if _, err := e.Start("Countdown", []byte("1"), "cd-1"); err != nil {
		t.Fatal(err)
	}
	waitFinished(t, e, "cd-1", 5*time.Second)
	x.Close()
	runs := col.named("workflow Countdown")
	if len(runs) != 2 {
		t.Fatalf("%d run spans, want two runs", len(runs))
	}
	first, second := runs[0], runs[1]
	if first.Start.After(second.Start) {
		first, second = second, first
	}
	if !first.Parent.IsZero() || first.Kind != tracing.KindInternal || first.TraceID != second.TraceID || second.Parent != first.SpanID {
		t.Fatalf("first %+v\nsecond %+v", first, second)
	}
}

// An SDK's workflow tasks carry the run's span (the same on every task, so
// replay reads one value), and each decision is exported as a span.
func TestWorkflowTasksCarryTheRunSpan(t *testing.T) {
	e, col, x := tracedEngine(t, nil)
	if err := e.RegisterWorkflowTypes([]string{"Remote"}, "sdk", "test"); err != nil {
		t.Fatal(err)
	}
	if _, err := e.Start("Remote", nil, "r-1", WithTraceParent(callerTP, "")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	wt, err := e.PollWorkflowTask(ctx, "sdk", []string{"Remote"})
	if err != nil {
		t.Fatal(err)
	}
	evs, _ := e.store.ReadAll("r-1")
	want, _ := runTraceparent(evs[0].GetWorkflowStarted().GetTrace())
	if wt.GetTraceparent() == "" || wt.GetTraceparent() != want {
		t.Fatalf("workflow task traceparent %q, want the run's %q", wt.GetTraceparent(), want)
	}
	done := &WorkflowCommand{Command: &WorkflowCommand_CompleteWorkflow{CompleteWorkflow: &CompleteWorkflowCommand{}}}
	if err := e.CompleteWorkflowTask("r-1", wt.GetTaskToken(), []*WorkflowCommand{done}, ""); err != nil {
		t.Fatal(err)
	}
	x.Close()
	tasks := col.named("workflow task Remote")
	run, _ := tracing.Parse(want, "")
	if len(tasks) != 1 || tasks[0].Parent != run.SpanID || tasks[0].Attr("workflowd.commands") != int64(1) {
		t.Fatalf("decision spans %+v", tasks)
	}
}

// A dead worker's attempt is exported as an error, and the retry gets a new
// span under the same run.
func TestExpiredLeaseEndsTheAttemptSpan(t *testing.T) {
	e, col, x := tracedEngine(t, map[string]workflow.Func{"Pipeline": pipeline}, WithLeaseDuration(100*time.Millisecond))
	e.SetRetryPolicy("a", RetryPolicy{MaxAttempts: 2})
	if _, err := e.Start("Pipeline", nil, "dead-1", WithTraceParent(callerTP, "")); err != nil {
		t.Fatal(err)
	}
	first := mustPoll(t, e, "w", time.Second) // and never answered
	second := mustPoll(t, e, "w", 3*time.Second)
	if first.GetTraceparent() == second.GetTraceparent() || second.GetAttempt() != 2 {
		t.Fatalf("retry carries %q (attempt %d), first %q", second.GetTraceparent(), second.GetAttempt(), first.GetTraceparent())
	}
	x.Close()
	acts := col.named("activity a")
	if len(acts) != 1 || acts[0].Status != tracing.StatusError || acts[0].Attr("workflowd.outcome") != "lease expired" {
		t.Fatalf("expired attempt spans %+v", acts)
	}
}
