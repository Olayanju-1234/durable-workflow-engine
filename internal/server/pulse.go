package server

import (
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/metrics"
)

// The pulse: the dashboard's metrics panel without a Prometheus server. The
// engine samples its own per-namespace metric families every sample interval
// and keeps the last hour; a summary turns the samples into rates (finished
// workflows per interval, failure rate, step latency quantiles from histogram
// bucket deltas) and depths, the way PromQL's rate() and histogram_quantile()
// would over the same families.

const (
	defaultPulseInterval = 10 * time.Second
	pulseKeep            = time.Hour
	// pulsePoints is the most points a summary returns: samples are grouped
	// into that many intervals at most.
	pulsePoints = 60
)

// WithPulseInterval sets how often the engine samples its metrics for the
// dashboard's metrics panel (default 10s; tests shorten it).
func WithPulseInterval(d time.Duration) Option {
	return func(e *Engine) { e.pulse.interval = d }
}

type pulse struct {
	interval time.Duration // immutable after NewEngine

	mu      sync.Mutex
	samples []pulseSample // oldest first, at most pulseKeep / interval + 1
}

// pulseSample is every namespace's counters and gauges at one moment.
type pulseSample struct {
	at time.Time
	ns map[string]*nsSample
}

type nsSample struct {
	started       uint64
	finished      map[string]uint64 // by status
	results       map[string]uint64 // completed | failed
	steps         metrics.HistogramSnapshot
	activityQueue int
	workflowQueue int
	active        int
}

func (p *pulse) add(s pulseSample) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.samples = append(p.samples, s)
	if max := int(pulseKeep/p.interval) + 1; len(p.samples) > max {
		p.samples = append(p.samples[:0:0], p.samples[len(p.samples)-max:]...)
	}
}

// runPulse samples until the engine closes.
func (e *Engine) runPulse() {
	e.pulse.add(e.samplePulse(time.Now()))
	t := time.NewTicker(e.pulse.interval)
	defer t.Stop()
	for {
		select {
		case <-e.done:
			return
		case now := <-t.C:
			e.pulse.add(e.samplePulse(now))
		}
	}
}

// samplePulse reads the per-namespace families as they are now.
func (e *Engine) samplePulse(now time.Time) pulseSample {
	s := pulseSample{at: now, ns: map[string]*nsSample{}}
	get := func(ns string) *nsSample {
		n, ok := s.ns[ns]
		if !ok {
			n = &nsSample{finished: map[string]uint64{}, results: map[string]uint64{}}
			s.ns[ns] = n
		}
		return n
	}
	for k, v := range e.metrics.nsStarted.Values() {
		get(k).started = v
	}
	for k, v := range e.metrics.nsFinished.Values() {
		ns, status, _ := strings.Cut(k, "\x00")
		get(ns).finished[status] = v
	}
	for k, v := range e.metrics.nsResults.Values() {
		ns, result, _ := strings.Cut(k, "\x00")
		get(ns).results[result] = v
	}
	for ns, h := range e.metrics.nsStepDuration.Snapshots() {
		get(ns).steps = h
	}
	for ns, n := range e.queue.depthBy(taskNamespace) {
		get(ns).activityQueue = n
	}
	for ns, n := range e.workflowQueue.depthBy(workflowTaskNamespace) {
		get(ns).workflowQueue = n
	}
	for ns, n := range e.activeByNamespace() {
		get(ns).active = n
	}
	return s
}

func taskNamespace(t *Task) string                 { return nsOf(t.GetWorkflowId()) }
func workflowTaskNamespace(t *WorkflowTask) string { return nsOf(t.GetWorkflowId()) }

// activeByNamespace counts running and waiting workflows per namespace, from
// the visibility index (empty until it is built).
func (e *Engine) activeByNamespace() map[string]int {
	e.idx.mu.Lock()
	defer e.idx.mu.Unlock()
	out := make(map[string]int, len(e.idx.active))
	for ns, n := range e.idx.active {
		if n > 0 {
			out[ns] = n
		}
	}
	return out
}

// registerNamespaceGauges adds the per-namespace gauges to /metrics.
func (e *Engine) registerNamespaceGauges() {
	toFloats := func(m map[string]int) map[string]float64 {
		out := make(map[string]float64, len(m))
		for k, v := range m {
			out[k] = float64(v)
		}
		return out
	}
	e.registry.GaugeVecFunc("workflowd_namespace_task_queue_depth",
		"Activity tasks waiting for a worker, by namespace.", []string{"namespace"},
		func() map[string]float64 { return toFloats(e.queue.depthBy(taskNamespace)) })
	e.registry.GaugeVecFunc("workflowd_namespace_workflow_task_queue_depth",
		"Workflow decisions waiting for an SDK worker, by namespace.", []string{"namespace"},
		func() map[string]float64 { return toFloats(e.workflowQueue.depthBy(workflowTaskNamespace)) })
	e.registry.GaugeVecFunc("workflowd_namespace_workflows_active",
		"Workflows running or waiting, by namespace.", []string{"namespace"},
		func() map[string]float64 { return toFloats(e.activeByNamespace()) })
}

// MetricsSummary is a namespace's pulse over a window: a point per interval
// (what happened in it, and the depths at its end), totals for the window,
// and the gauges right now.
type MetricsSummary struct {
	Namespace     string         `json:"namespace"`
	WindowSeconds int64          `json:"window_seconds"`
	StepSeconds   int64          `json:"step_seconds"`
	Since         string         `json:"since,omitempty"` // the oldest sample: the engine keeps no more
	Points        []MetricsPoint `json:"points"`
	Totals        MetricsTotals  `json:"totals"`
	Now           MetricsNow     `json:"now"`
}

// MetricsPoint is one interval. Failed counts failures and timeouts; Other
// counts cancellations and terminations (continue-as-new is neither: the
// workflow goes on). Latencies are in milliseconds, nil without steps.
type MetricsPoint struct {
	At            string   `json:"at"`
	Started       uint64   `json:"started"`
	Completed     uint64   `json:"completed"`
	Failed        uint64   `json:"failed"`
	Other         uint64   `json:"other"`
	Steps         uint64   `json:"steps"`
	StepsFailed   uint64   `json:"steps_failed"`
	StepP50Ms     *float64 `json:"step_p50_ms"`
	StepP95Ms     *float64 `json:"step_p95_ms"`
	ActivityQueue int      `json:"activity_queue"`
	WorkflowQueue int      `json:"workflow_queue"`
	Active        int      `json:"active"`
}

// MetricsTotals is the whole window. FailureRate is failed over completed
// plus failed (nil when none finished either way).
type MetricsTotals struct {
	Started     uint64   `json:"started"`
	Completed   uint64   `json:"completed"`
	Failed      uint64   `json:"failed"`
	Other       uint64   `json:"other"`
	FailureRate *float64 `json:"failure_rate"`
	PerMinute   float64  `json:"finished_per_minute"`
	Steps       uint64   `json:"steps"`
	StepsFailed uint64   `json:"steps_failed"`
	StepP50Ms   *float64 `json:"step_p50_ms"`
	StepP95Ms   *float64 `json:"step_p95_ms"`
	StepP99Ms   *float64 `json:"step_p99_ms"`
}

// MetricsNow is the namespace's gauges as the summary is made.
type MetricsNow struct {
	ActivityQueue int `json:"activity_queue"`
	WorkflowQueue int `json:"workflow_queue"`
	Active        int `json:"active"`
}

// metricsSummary summarises ns over the last window.
func (e *Engine) metricsSummary(ns string, window time.Duration) MetricsSummary {
	now := time.Now()
	cur := e.samplePulse(now)
	e.pulse.mu.Lock()
	samples := append([]pulseSample(nil), e.pulse.samples...)
	e.pulse.mu.Unlock()

	out := MetricsSummary{Namespace: ns, WindowSeconds: int64(window / time.Second), Points: []MetricsPoint{}}
	if c := cur.ns[ns]; c != nil {
		out.Now = MetricsNow{ActivityQueue: c.activityQueue, WorkflowQueue: c.workflowQueue, Active: c.active}
	}
	// The baseline is the last sample at or before the window's start (or the
	// oldest there is); then every sample since, and the live one as the end.
	from := now.Add(-window)
	start := 0
	for i, s := range samples {
		if !s.at.After(from) {
			start = i
		}
	}
	samples = append(samples[start:], cur)
	if len(samples) > 0 {
		out.Since = samples[0].at.UTC().Format(time.RFC3339Nano)
	}
	if len(samples) < 2 {
		return out
	}
	n := len(samples) - 1 // intervals
	group := (n + pulsePoints - 1) / pulsePoints
	out.StepSeconds = int64((e.pulse.interval * time.Duration(group)).Seconds())
	for end := n % group; end <= n; end += group {
		if end == 0 {
			continue
		}
		begin := max(0, end-group)
		out.Points = append(out.Points, pointBetween(samples[begin].ns[ns], samples[end].ns[ns], samples[end].at))
	}
	first, last := samples[0].ns[ns], samples[n].ns[ns]
	t := pointBetween(first, last, now)
	out.Totals = MetricsTotals{
		Started: t.Started, Completed: t.Completed, Failed: t.Failed, Other: t.Other,
		Steps: t.Steps, StepsFailed: t.StepsFailed, StepP50Ms: t.StepP50Ms, StepP95Ms: t.StepP95Ms,
	}
	if done := t.Completed + t.Failed; done > 0 {
		r := float64(t.Failed) / float64(done)
		out.Totals.FailureRate = &r
	}
	if span := samples[n].at.Sub(samples[0].at); span > 0 {
		out.Totals.PerMinute = float64(t.Completed+t.Failed+t.Other) / span.Minutes()
	}
	if h := histDelta(first, last); h.Count > 0 {
		out.Totals.StepP99Ms = quantileMs(h, 0.99)
	}
	return out
}

// pointBetween is what changed from a to b (either may be nil: nothing yet),
// with b's depths. A counter that went down means the engine restarted, so
// b's value is all there is.
func pointBetween(a, b *nsSample, at time.Time) MetricsPoint {
	p := MetricsPoint{At: at.UTC().Format(time.RFC3339Nano)}
	if b == nil {
		return p
	}
	if a == nil {
		a = &nsSample{}
	}
	p.Started = delta(a.started, b.started)
	fin := func(status string) uint64 { return delta(a.finished[status], b.finished[status]) }
	p.Completed = fin("completed")
	p.Failed = fin("failed") + fin("timed_out")
	p.Other = fin("cancelled") + fin("terminated")
	p.Steps = delta(a.results["completed"], b.results["completed"]) + delta(a.results["failed"], b.results["failed"])
	p.StepsFailed = delta(a.results["failed"], b.results["failed"])
	if h := histDelta(a, b); h.Count > 0 {
		p.StepP50Ms, p.StepP95Ms = quantileMs(h, 0.5), quantileMs(h, 0.95)
	}
	p.ActivityQueue, p.WorkflowQueue, p.Active = b.activityQueue, b.workflowQueue, b.active
	return p
}

func delta(a, b uint64) uint64 {
	if b < a {
		return b
	}
	return b - a
}

// histDelta is the observations made between two samples' histograms.
func histDelta(a, b *nsSample) metrics.HistogramSnapshot {
	if b == nil || b.steps.Count == 0 {
		return metrics.HistogramSnapshot{}
	}
	if a == nil || a.steps.Count == 0 || a.steps.Count > b.steps.Count {
		return b.steps
	}
	d := metrics.HistogramSnapshot{Bounds: b.steps.Bounds, Counts: make([]uint64, len(b.steps.Counts)),
		Count: b.steps.Count - a.steps.Count, Sum: b.steps.Sum - a.steps.Sum}
	for i := range d.Counts {
		d.Counts[i] = b.steps.Counts[i] - a.steps.Counts[i]
	}
	return d
}

// quantileMs estimates the q-quantile of a histogram in milliseconds, by
// linear interpolation inside the bucket that holds it, as Prometheus's
// histogram_quantile does. Past the last bound it is that bound.
func quantileMs(h metrics.HistogramSnapshot, q float64) *float64 {
	if h.Count == 0 || len(h.Bounds) == 0 {
		return nil
	}
	rank := q * float64(h.Count)
	i := sort.Search(len(h.Counts), func(i int) bool { return float64(h.Counts[i]) >= rank })
	var v float64
	switch {
	case i == len(h.Counts):
		v = h.Bounds[len(h.Bounds)-1]
	default:
		lower, below := 0.0, uint64(0)
		if i > 0 {
			lower, below = h.Bounds[i-1], h.Counts[i-1]
		}
		in := h.Counts[i] - below
		v = h.Bounds[i]
		if in > 0 {
			v = lower + (h.Bounds[i]-lower)*(rank-float64(below))/float64(in)
		}
	}
	ms := math.Round(v*1e6) / 1e3
	return &ms
}

// MetricsSummary summarises the namespace's pulse over the last window
// (see Engine.metricsSummary).
func (s *Scope) MetricsSummary(window time.Duration) MetricsSummary {
	return s.e.metricsSummary(s.ns, window)
}
