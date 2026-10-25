package server

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/metrics"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

func TestQuantileInterpolatesInsideABucket(t *testing.T) {
	h := metrics.HistogramSnapshot{Bounds: []float64{0.1, 1, 10}, Counts: []uint64{10, 30, 40}, Count: 40}
	for _, c := range []struct {
		q    float64
		want float64
	}{
		{0.25, 100},  // rank 10: the top of the first bucket
		{0.5, 550},   // rank 20: halfway through (0.1, 1]
		{0.95, 8200}, // rank 38: 8/10 through (1, 10]
	} {
		if got := quantileMs(h, c.q); got == nil || *got != c.want {
			t.Errorf("q%.2f = %v, want %v ms", c.q, got, c.want)
		}
	}
	// Observations past the last bound: the quantile is that bound.
	over := metrics.HistogramSnapshot{Bounds: []float64{0.1}, Counts: []uint64{1}, Count: 10}
	if got := quantileMs(over, 0.99); got == nil || *got != 100 {
		t.Errorf("past the last bound: %v", got)
	}
	if quantileMs(metrics.HistogramSnapshot{Bounds: []float64{1}, Counts: []uint64{0}}, 0.5) != nil {
		t.Error("an empty histogram has no quantile")
	}
}

func TestPointBetweenSamples(t *testing.T) {
	a := &nsSample{started: 5, finished: map[string]uint64{"completed": 3, "failed": 1},
		results: map[string]uint64{"completed": 10, "failed": 2},
		steps:   metrics.HistogramSnapshot{Bounds: []float64{1}, Counts: []uint64{10}, Count: 12}}
	b := &nsSample{started: 9, finished: map[string]uint64{"completed": 5, "failed": 2, "timed_out": 1, "terminated": 1, "continued_as_new": 4},
		results: map[string]uint64{"completed": 14, "failed": 3},
		steps:   metrics.HistogramSnapshot{Bounds: []float64{1}, Counts: []uint64{15}, Count: 17}, activityQueue: 2, active: 3}
	p := pointBetween(a, b, time.Now())
	if p.Started != 4 || p.Completed != 2 || p.Failed != 2 || p.Other != 1 || p.Steps != 5 || p.StepsFailed != 1 ||
		p.ActivityQueue != 2 || p.Active != 3 || p.StepP50Ms == nil {
		t.Fatalf("point %+v", p)
	}
	// A restart resets the counters: what is there now is all that happened.
	restarted := &nsSample{started: 2, finished: map[string]uint64{"completed": 1}, results: map[string]uint64{}}
	if p := pointBetween(b, restarted, time.Now()); p.Started != 2 || p.Completed != 1 {
		t.Fatalf("after a restart: %+v", p)
	}
}

// The panel's numbers are the namespace's own: two namespaces running the
// same type are summarised apart.
func TestMetricsSummaryPerNamespace(t *testing.T) {
	e := NewEngine(newStore(t, t.TempDir()), discardLogger(), WithPulseInterval(20*time.Millisecond))
	t.Cleanup(e.Close)
	flow := state.WorkflowDefinition{Type: "Flow", Steps: state.Activities("one")}
	e.Register(flow)
	e.In("acme").RegisterWorkflow("Flow", flow)
	if err := e.Recover(); err != nil { // builds the index, as at every start
		t.Fatal(err)
	}

	run := func(ns string, fail bool) {
		t.Helper()
		if _, err := e.In(ns).Start("Flow", nil, ""); err != nil {
			t.Fatal(err)
		}
		task, err := pollNS(e, ns)
		if err != nil {
			t.Fatal(err)
		}
		if fail {
			err = e.In(ns).FailTask(task.GetWorkflowId(), "one", task.GetSequence(), task.GetLeaseToken(), TaskFailure{Message: "no", NonRetryable: true})
		} else {
			err = e.In(ns).CompleteTask(task.GetWorkflowId(), "one", task.GetSequence(), task.GetLeaseToken(), nil)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	for range 3 {
		run("acme", false)
	}
	run("acme", true)
	run("", false)
	if _, err := e.In("acme").Start("Flow", nil, "left-waiting"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(80 * time.Millisecond) // a few samples

	acme := e.In("acme").MetricsSummary(time.Minute)
	tot := acme.Totals
	if tot.Started != 5 || tot.Completed != 3 || tot.Failed != 1 || tot.Steps != 4 || tot.StepsFailed != 1 {
		t.Fatalf("acme totals %+v", tot)
	}
	if tot.FailureRate == nil || *tot.FailureRate != 0.25 || tot.StepP95Ms == nil || tot.PerMinute <= 0 {
		t.Fatalf("acme rates %+v", tot)
	}
	if acme.Now.ActivityQueue != 1 || acme.Now.Active != 1 {
		t.Fatalf("acme now %+v, want the waiting workflow's one queued task", acme.Now)
	}
	var sum uint64
	for _, p := range acme.Points {
		sum += p.Completed
	}
	if sum != 3 || len(acme.Points) == 0 || len(acme.Points) > pulsePoints {
		t.Fatalf("%d points adding up to %d completions", len(acme.Points), sum)
	}
	def := e.In("").MetricsSummary(time.Minute).Totals
	if def.Completed != 1 || def.Failed != 0 {
		t.Fatalf("default totals %+v", def)
	}

	var scrape strings.Builder
	e.Metrics().WriteText(&scrape)
	for _, want := range []string{
		`workflowd_namespace_workflows_finished_total{namespace="acme",status="completed"} 3`,
		`workflowd_namespace_workflows_finished_total{namespace="default",status="completed"} 1`,
		`workflowd_namespace_task_queue_depth{namespace="acme"} 1`,
		`workflowd_namespace_workflows_active{namespace="acme"} 1`,
		`workflowd_namespace_step_duration_seconds_count{namespace="acme"} 4`,
		"workflowd_workflows_started_total 6", // the unlabelled family is unchanged
	} {
		if !strings.Contains(scrape.String(), want) {
			t.Errorf("/metrics lacks %s", want)
		}
	}
}

func pollNS(e *Engine, ns string) (*Task, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	return e.In(ns).PollTask(ctx, "w", "")
}

// The engine keeps an hour of samples, no more.
func TestPulseKeepsAnHour(t *testing.T) {
	p := &pulse{interval: time.Minute}
	for i := range 200 {
		p.add(pulseSample{at: time.Unix(int64(i*60), 0)})
	}
	if len(p.samples) != 61 || !p.samples[0].at.Equal(time.Unix(139*60, 0)) {
		t.Fatalf("%d samples from %v", len(p.samples), p.samples[0].at)
	}
}
