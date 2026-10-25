package metrics

import (
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func scrape(r *Registry) string {
	var b strings.Builder
	r.WriteText(&b)
	return b.String()
}

func TestCounterRendersPrometheusText(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("wf_started_total", "Workflows started.")
	c.Inc()
	c.Inc()

	want := "# HELP wf_started_total Workflows started.\n" +
		"# TYPE wf_started_total counter\n" +
		"wf_started_total 2\n"
	if got := scrape(r); got != want {
		t.Errorf("scrape:\n got %q\nwant %q", got, want)
	}
}

func TestCounterVecChildrenSortedAndEscaped(t *testing.T) {
	r := NewRegistry()
	v := r.CounterVec("wf_finished_total", "Workflows finished.", "status")
	v.With("failed").Inc()
	v.With("completed").Inc()
	v.With("completed").Inc()
	v.With(`we"ird\value`).Inc() // escaping, not a real status

	got := scrape(r)
	want := "# HELP wf_finished_total Workflows finished.\n" +
		"# TYPE wf_finished_total counter\n" +
		"wf_finished_total{status=\"completed\"} 2\n" +
		"wf_finished_total{status=\"failed\"} 1\n" +
		"wf_finished_total{status=\"we\\\"ird\\\\value\"} 1\n"
	if got != want {
		t.Errorf("scrape:\n got %q\nwant %q", got, want)
	}
}

func TestGaugeAndGaugeFunc(t *testing.T) {
	r := NewRegistry()
	g := r.Gauge("wf_active", "Active workflows.")
	g.Add(3)
	g.Dec()
	depth := 7
	r.GaugeFunc("queue_depth", "Tasks queued.", func() float64 { return float64(depth) })

	got := scrape(r)
	for _, want := range []string{"wf_active 2\n", "queue_depth 7\n", "# TYPE queue_depth gauge\n"} {
		if !strings.Contains(got, want) {
			t.Errorf("scrape missing %q:\n%s", want, got)
		}
	}
	depth = 9 // GaugeFunc reads at scrape time, not registration time
	if got := scrape(r); !strings.Contains(got, "queue_depth 9\n") {
		t.Errorf("GaugeFunc not re-read at scrape:\n%s", got)
	}
}

func TestHistogramCumulativeBuckets(t *testing.T) {
	r := NewRegistry()
	h := r.Histogram("step_seconds", "Step duration.", []float64{0.1, 1, 10})
	for _, v := range []float64{0.05, 0.5, 0.7, 5, 100} {
		h.Observe(v)
	}

	got := scrape(r)
	want := "# HELP step_seconds Step duration.\n" +
		"# TYPE step_seconds histogram\n" +
		"step_seconds_bucket{le=\"0.1\"} 1\n" +
		"step_seconds_bucket{le=\"1\"} 3\n" + // cumulative: ≤1 includes ≤0.1
		"step_seconds_bucket{le=\"10\"} 4\n" +
		"step_seconds_bucket{le=\"+Inf\"} 5\n" + // 100 lands only in +Inf
		"step_seconds_sum 106.25\n" +
		"step_seconds_count 5\n"
	if got != want {
		t.Errorf("scrape:\n got %q\nwant %q", got, want)
	}
}

func TestDuplicateRegistrationPanics(t *testing.T) {
	r := NewRegistry()
	r.Counter("dup", "first")
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on duplicate metric name")
		}
	}()
	r.Gauge("dup", "second")
}

func TestHandlerServesTextFormat(t *testing.T) {
	r := NewRegistry()
	r.Counter("c_total", "help").Inc()

	rec := httptest.NewRecorder()
	r.Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))

	if ct := rec.Header().Get("Content-Type"); ct != "text/plain; version=0.0.4; charset=utf-8" {
		t.Errorf("content type = %q", ct)
	}
	if body := rec.Body.String(); !strings.Contains(body, "c_total 1\n") {
		t.Errorf("body missing counter:\n%s", body)
	}
}

// Concurrent updates and scrapes must be data-race-free (run with -race).
func TestConcurrentUpdatesAndScrapes(t *testing.T) {
	r := NewRegistry()
	c := r.Counter("c_total", "")
	v := r.CounterVec("v_total", "", "s")
	g := r.Gauge("g", "")
	h := r.Histogram("h_seconds", "", nil)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				c.Inc()
				v.With("a").Inc()
				v.With("b").Inc()
				g.Inc()
				h.Observe(float64(j) / 100)
				_ = scrape(r)
			}
		}()
	}
	wg.Wait()

	if got := c.Value(); got != 4000 {
		t.Errorf("counter = %d, want 4000", got)
	}
	if got := v.With("a").Value(); got != 4000 {
		t.Errorf("vec child a = %d, want 4000", got)
	}
}

func TestHistogramVecRendersLabelsBeforeLe(t *testing.T) {
	r := NewRegistry()
	v := r.HistogramVec("step_seconds", "Step latency.", []float64{0.1, 1}, "namespace")
	v.With("b").Observe(0.05)
	v.With("a").Observe(0.5)
	v.With("a").Observe(5)

	want := "# HELP step_seconds Step latency.\n" +
		"# TYPE step_seconds histogram\n" +
		"step_seconds_bucket{namespace=\"a\",le=\"0.1\"} 0\n" +
		"step_seconds_bucket{namespace=\"a\",le=\"1\"} 1\n" +
		"step_seconds_bucket{namespace=\"a\",le=\"+Inf\"} 2\n" +
		"step_seconds_sum{namespace=\"a\"} 5.5\n" +
		"step_seconds_count{namespace=\"a\"} 2\n" +
		"step_seconds_bucket{namespace=\"b\",le=\"0.1\"} 1\n" +
		"step_seconds_bucket{namespace=\"b\",le=\"1\"} 1\n" +
		"step_seconds_bucket{namespace=\"b\",le=\"+Inf\"} 1\n" +
		"step_seconds_sum{namespace=\"b\"} 0.05\n" +
		"step_seconds_count{namespace=\"b\"} 1\n"
	if got := scrape(r); got != want {
		t.Errorf("scrape:\n got %q\nwant %q", got, want)
	}
	snap := v.Snapshots()["a"]
	if snap.Count != 2 || snap.Counts[0] != 0 || snap.Counts[1] != 1 || snap.Sum != 5.5 {
		t.Errorf("snapshot %+v", snap)
	}
}

func TestGaugeVecFuncAndCounterValues(t *testing.T) {
	r := NewRegistry()
	r.GaugeVecFunc("depth", "Queued.", []string{"namespace", "kind"}, func() map[string]float64 {
		return map[string]float64{"b\x00task": 2, "a\x00task": 1}
	})
	got := scrape(r)
	if !strings.Contains(got, "depth{namespace=\"a\",kind=\"task\"} 1\ndepth{namespace=\"b\",kind=\"task\"} 2\n") {
		t.Errorf("scrape:\n%s", got)
	}
	c := r.CounterVec("done_total", "Done.", "namespace", "status")
	c.With("a", "ok").Inc()
	if v := c.Values()["a\x00ok"]; v != 1 {
		t.Errorf("Values = %v", c.Values())
	}
}
