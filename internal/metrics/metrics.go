// Package metrics is a minimal, dependency-free metrics registry that renders
// the Prometheus text exposition format (version 0.0.4). It exists so the
// engine can expose /metrics while keeping the module's only dependencies
// gRPC and protobuf — the same zero-dependency stance as the hand-rolled WAL.
//
// It implements just what the engine needs: counters (optionally labelled),
// gauges (stored or computed at scrape time), and one histogram shape. It is
// not a general client_golang replacement.
package metrics

import (
	"fmt"
	"io"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// DefBuckets are the default histogram bucket upper bounds, in seconds. They
// follow the Prometheus client defaults, extended to 60s because workflow
// activities (unlike HTTP handlers) routinely run for tens of seconds.
var DefBuckets = []float64{.005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}

// collector is one named metric family that can render itself.
type collector interface {
	name() string
	write(w io.Writer)
}

// Registry holds metric families and renders them all on scrape. Families are
// rendered in registration order, so output is deterministic. Registration is
// expected at construction time; scraping is safe concurrently with updates.
type Registry struct {
	mu    sync.RWMutex
	names map[string]bool
	fams  []collector
}

func NewRegistry() *Registry {
	return &Registry{names: make(map[string]bool)}
}

// register panics on a duplicate name: two owners of one metric is a
// programmer error worth failing loudly on, at startup, not at scrape time.
func (r *Registry) register(c collector) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.names[c.name()] {
		panic(fmt.Sprintf("metrics: duplicate registration of %q", c.name()))
	}
	r.names[c.name()] = true
	r.fams = append(r.fams, c)
}

// WriteText renders every registered family in the Prometheus text format.
func (r *Registry) WriteText(w io.Writer) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, f := range r.fams {
		f.write(w)
	}
}

// Handler serves the registry over HTTP — mount it at /metrics.
func (r *Registry) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		r.WriteText(w)
	})
}

// Counter is a monotonically increasing count. Lock-free: updates are a single
// atomic add, cheap enough to sit inside the engine's hot paths.
type Counter struct {
	meta
	v atomic.Uint64
}

func (r *Registry) Counter(name, help string) *Counter {
	c := &Counter{meta: meta{fqName: name, help: help, typ: "counter"}}
	r.register(c)
	return c
}

func (c *Counter) Inc()          { c.v.Add(1) }
func (c *Counter) Value() uint64 { return c.v.Load() }

func (c *Counter) write(w io.Writer) {
	c.writeHeader(w)
	fmt.Fprintf(w, "%s %d\n", c.fqName, c.v.Load())
}

// CounterVec is a family of counters split by label values (e.g. workflows
// finished by status). Children are created lazily and live forever — label
// values must be low-cardinality (statuses, not workflow ids).
type CounterVec struct {
	meta
	labels   []string
	mu       sync.RWMutex
	children map[string]*Counter // key: joined label values
}

func (r *Registry) CounterVec(name, help string, labels ...string) *CounterVec {
	v := &CounterVec{
		meta:     meta{fqName: name, help: help, typ: "counter"},
		labels:   labels,
		children: make(map[string]*Counter),
	}
	r.register(v)
	return v
}

// With returns the child counter for the given label values (one per label
// name, in order), creating it on first use.
func (v *CounterVec) With(values ...string) *Counter {
	if len(values) != len(v.labels) {
		panic(fmt.Sprintf("metrics: %s expects %d label values, got %d", v.fqName, len(v.labels), len(values)))
	}
	key := strings.Join(values, "\x00")
	v.mu.RLock()
	c, ok := v.children[key]
	v.mu.RUnlock()
	if ok {
		return c
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if c, ok = v.children[key]; ok {
		return c
	}
	c = &Counter{meta: meta{fqName: v.fqName + renderLabels(v.labels, values)}}
	v.children[key] = c
	return c
}

// Values returns each child's count, keyed by its label values joined with
// "\x00" (in label order): a snapshot for code that reads its own metrics.
func (v *CounterVec) Values() map[string]uint64 {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make(map[string]uint64, len(v.children))
	for k, c := range v.children {
		out[k] = c.v.Load()
	}
	return out
}

func (v *CounterVec) write(w io.Writer) {
	v.writeHeader(w)
	v.mu.RLock()
	defer v.mu.RUnlock()
	keys := make([]string, 0, len(v.children))
	for k := range v.children {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic scrape output
	for _, k := range keys {
		c := v.children[k]
		fmt.Fprintf(w, "%s %d\n", c.fqName, c.v.Load())
	}
}

// Gauge is a value that goes up and down (integer-valued; the engine gauges
// are all counts of things currently alive).
type Gauge struct {
	meta
	v atomic.Int64
}

func (r *Registry) Gauge(name, help string) *Gauge {
	g := &Gauge{meta: meta{fqName: name, help: help, typ: "gauge"}}
	r.register(g)
	return g
}

func (g *Gauge) Inc()         { g.v.Add(1) }
func (g *Gauge) Dec()         { g.v.Add(-1) }
func (g *Gauge) Add(n int64)  { g.v.Add(n) }
func (g *Gauge) Set(n int64)  { g.v.Store(n) }
func (g *Gauge) Value() int64 { return g.v.Load() }

func (g *Gauge) write(w io.Writer) {
	g.writeHeader(w)
	fmt.Fprintf(w, "%s %d\n", g.fqName, g.v.Load())
}

// GaugeFunc reads its value at scrape time — for values something else already
// owns (queue depth is len(channel); no bookkeeping to keep in sync).
type GaugeFunc struct {
	meta
	fn func() float64
}

func (r *Registry) GaugeFunc(name, help string, fn func() float64) {
	r.register(&GaugeFunc{meta: meta{fqName: name, help: help, typ: "gauge"}, fn: fn})
}

func (g *GaugeFunc) write(w io.Writer) {
	g.writeHeader(w)
	fmt.Fprintf(w, "%s %s\n", g.fqName, formatFloat(g.fn()))
}

// CounterFunc is a counter something else keeps, read at scrape time.
type CounterFunc struct {
	meta
	fn func() float64
}

func (r *Registry) CounterFunc(name, help string, fn func() float64) {
	r.register(&CounterFunc{meta: meta{fqName: name, help: help, typ: "counter"}, fn: fn})
}

func (c *CounterFunc) write(w io.Writer) {
	c.writeHeader(w)
	fmt.Fprintf(w, "%s %s\n", c.fqName, formatFloat(c.fn()))
}

// GaugeVecFunc is a labelled gauge family read at scrape time: fn returns
// each child's value keyed by its label values joined with "\x00".
type GaugeVecFunc struct {
	meta
	labels []string
	fn     func() map[string]float64
}

func (r *Registry) GaugeVecFunc(name, help string, labels []string, fn func() map[string]float64) {
	r.register(&GaugeVecFunc{meta: meta{fqName: name, help: help, typ: "gauge"}, labels: labels, fn: fn})
}

func (g *GaugeVecFunc) write(w io.Writer) {
	g.writeHeader(w)
	vals := g.fn()
	keys := make([]string, 0, len(vals))
	for k := range vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "%s%s %s\n", g.fqName, renderLabels(g.labels, strings.Split(k, "\x00")), formatFloat(vals[k]))
	}
}

// Histogram counts observations into cumulative buckets, Prometheus-style:
// each bucket counts observations ≤ its upper bound, plus a +Inf catch-all,
// a running sum, and a total count. Quantiles are computed by the Prometheus
// server from these, not here.
type Histogram struct {
	meta
	labels string // a HistogramVec child's rendered labels, without braces
	bounds []float64
	mu     sync.Mutex
	counts []uint64
	sum    float64
	count  uint64
}

// HistogramSnapshot is a histogram's state at one moment: cumulative counts
// per upper bound (Counts[i] observations <= Bounds[i]), and the total.
type HistogramSnapshot struct {
	Bounds []float64
	Counts []uint64
	Count  uint64
	Sum    float64
}

func (r *Registry) Histogram(name, help string, buckets []float64) *Histogram {
	if len(buckets) == 0 {
		buckets = DefBuckets
	}
	if !sort.Float64sAreSorted(buckets) {
		panic(fmt.Sprintf("metrics: %s: histogram buckets must be sorted", name))
	}
	h := &Histogram{
		meta:   meta{fqName: name, help: help, typ: "histogram"},
		bounds: buckets,
		counts: make([]uint64, len(buckets)),
	}
	r.register(h)
	return h
}

func (h *Histogram) Observe(v float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, b := range h.bounds {
		if v <= b {
			h.counts[i]++
			break
		}
	}
	h.sum += v
	h.count++
}

// Snapshot copies the histogram's buckets, cumulative, with its count and sum.
func (h *Histogram) Snapshot() HistogramSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	s := HistogramSnapshot{Bounds: h.bounds, Counts: make([]uint64, len(h.bounds)), Count: h.count, Sum: h.sum}
	var cum uint64
	for i := range h.bounds {
		cum += h.counts[i]
		s.Counts[i] = cum
	}
	return s
}

func (h *Histogram) write(w io.Writer) {
	h.writeHeader(w)
	h.writeSamples(w)
}

func (h *Histogram) writeSamples(w io.Writer) {
	s := h.Snapshot()
	sep, plain := "", ""
	if h.labels != "" {
		sep, plain = h.labels+",", "{"+h.labels+"}"
	}
	for i, b := range s.Bounds {
		fmt.Fprintf(w, "%s_bucket{%sle=%q} %d\n", h.fqName, sep, formatFloat(b), s.Counts[i])
	}
	fmt.Fprintf(w, "%s_bucket{%sle=\"+Inf\"} %d\n", h.fqName, sep, s.Count)
	fmt.Fprintf(w, "%s_sum%s %s\n", h.fqName, plain, formatFloat(s.Sum))
	fmt.Fprintf(w, "%s_count%s %d\n", h.fqName, plain, s.Count)
}

// HistogramVec is a family of histograms split by label values, all with the
// same buckets. Like CounterVec, children live forever: low-cardinality labels.
type HistogramVec struct {
	meta
	labels   []string
	bounds   []float64
	mu       sync.RWMutex
	children map[string]*Histogram
}

func (r *Registry) HistogramVec(name, help string, buckets []float64, labels ...string) *HistogramVec {
	if len(buckets) == 0 {
		buckets = DefBuckets
	}
	if !sort.Float64sAreSorted(buckets) {
		panic(fmt.Sprintf("metrics: %s: histogram buckets must be sorted", name))
	}
	v := &HistogramVec{meta: meta{fqName: name, help: help, typ: "histogram"}, labels: labels, bounds: buckets,
		children: make(map[string]*Histogram)}
	r.register(v)
	return v
}

// With returns the child histogram for the given label values, creating it
// on first use.
func (v *HistogramVec) With(values ...string) *Histogram {
	if len(values) != len(v.labels) {
		panic(fmt.Sprintf("metrics: %s expects %d label values, got %d", v.fqName, len(v.labels), len(values)))
	}
	key := strings.Join(values, "\x00")
	v.mu.RLock()
	h, ok := v.children[key]
	v.mu.RUnlock()
	if ok {
		return h
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if h, ok = v.children[key]; ok {
		return h
	}
	rendered := renderLabels(v.labels, values)
	h = &Histogram{meta: meta{fqName: v.fqName}, labels: rendered[1 : len(rendered)-1], bounds: v.bounds,
		counts: make([]uint64, len(v.bounds))}
	v.children[key] = h
	return h
}

// Snapshots returns each child's snapshot, keyed like CounterVec.Values.
func (v *HistogramVec) Snapshots() map[string]HistogramSnapshot {
	v.mu.RLock()
	defer v.mu.RUnlock()
	out := make(map[string]HistogramSnapshot, len(v.children))
	for k, h := range v.children {
		out[k] = h.Snapshot()
	}
	return out
}

func (v *HistogramVec) write(w io.Writer) {
	v.writeHeader(w)
	v.mu.RLock()
	defer v.mu.RUnlock()
	keys := make([]string, 0, len(v.children))
	for k := range v.children {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v.children[k].writeSamples(w)
	}
}

// meta is the shared identity of a metric family: its name, help text, and
// type line. Vec children reuse it with the label set baked into fqName.
type meta struct {
	fqName string
	help   string
	typ    string
}

func (m meta) name() string { return m.fqName }

func (m meta) writeHeader(w io.Writer) {
	fmt.Fprintf(w, "# HELP %s %s\n", m.fqName, m.help)
	fmt.Fprintf(w, "# TYPE %s %s\n", m.fqName, m.typ)
}

// renderLabels builds the `{k="v",...}` suffix, escaping values per the
// exposition format (backslash, double quote, newline).
func renderLabels(names, values []string) string {
	var b strings.Builder
	b.WriteByte('{')
	for i, n := range names {
		if i > 0 {
			b.WriteByte(',')
		}
		v := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`).Replace(values[i])
		fmt.Fprintf(&b, `%s="%s"`, n, v)
	}
	b.WriteByte('}')
	return b.String()
}

func formatFloat(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}
