package tracing

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestParseTraceparent(t *testing.T) {
	const tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	c, ok := Parse(tp, " congo=t61rcWkgMzE ")
	if !ok || c.TraceID.String() != "4bf92f3577b34da6a3ce929d0e0e4736" || c.SpanID.String() != "00f067aa0ba902b7" ||
		!c.Sampled || c.State != "congo=t61rcWkgMzE" || c.Traceparent() != tp {
		t.Fatalf("parse %q = %+v %v", tp, c, ok)
	}
	if c, ok := Parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00", ""); !ok || c.Sampled {
		t.Fatalf("unsampled: %+v %v", c, ok)
	}
	// A later version may append fields; its first four are read as 00's.
	if _, ok := Parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-what-comes-next", ""); !ok {
		t.Fatal("a future version was refused")
	}
	for _, bad := range []string{
		"", "garbage",
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",                 // no flags
		"00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",              // uppercase
		"00-00000000000000000000000000000000-00f067aa0ba902b7-01",              // zero trace id
		"00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",              // zero span id
		"00-4bf92f3577b34da6a3ce929d0e0e47-00f067aa0ba902b7-01",                // short trace id
		"ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",              // version ff
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra",        // 00 has 4 fields
		"zz-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",              // version not hex
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-1",               // flags too short
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902g7-01",              // not hex
		"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\n00-xx-yy-zz", // two values
	} {
		if c, ok := Parse(bad, ""); ok {
			t.Errorf("Parse(%q) = %+v, want refused", bad, c)
		}
	}
	if NewTraceID().IsZero() || NewSpanID().IsZero() || NewSpanID() == NewSpanID() {
		t.Fatal("new ids must be random and non-zero")
	}
}

func sampleSpan() Span {
	parent, _ := Parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", "a=b")
	return Span{Context: Context{TraceID: parent.TraceID, SpanID: NewSpanID(), Sampled: true, State: "a=b"}, Parent: parent.SpanID,
		Name: "workflow Order", Kind: KindServer, Start: time.Unix(1700000000, 7), End: time.Unix(1700000001, 9),
		Attrs:  []Attr{{"workflowd.workflow_id", "o-1"}, {"workflowd.attempt", 2}, {"workflowd.retried", true}, {"ratio", 0.25}},
		Status: StatusError, StatusMessage: "declined"}
}

// What EncodeExport writes decodes back to the same span (and protoc decodes
// it against the OTLP schema too: see the PR that added it).
func TestEncodeExportRoundTrip(t *testing.T) {
	s := sampleSpan()
	res, spans, err := DecodeExport(EncodeExport([]Attr{{"service.name", "workflowd"}}, "workflowd", "test", []Span{s, s}))
	if err != nil || len(spans) != 2 || len(res) != 1 || res[0].Value != "workflowd" {
		t.Fatalf("decode: %v %v %v", res, spans, err)
	}
	got := spans[0]
	if got.TraceID != s.TraceID || got.SpanID != s.SpanID || got.Parent != s.Parent || got.Name != s.Name || got.Kind != s.Kind ||
		!got.Start.Equal(s.Start) || !got.End.Equal(s.End) || got.Status != s.Status || got.StatusMessage != s.StatusMessage ||
		!got.Sampled || got.State != "a=b" {
		t.Fatalf("decoded %+v\nwant    %+v", got, s)
	}
	if got.Attr("workflowd.workflow_id") != "o-1" || got.Attr("workflowd.attempt") != int64(2) || got.Attr("workflowd.retried") != true ||
		got.Attr("ratio") != 0.25 {
		t.Fatalf("attributes %v", got.Attrs)
	}
}

type collector struct {
	mu      sync.Mutex
	spans   []Span
	headers []http.Header
	fail    atomic.Int32 // answer this status this many times... (set by tests)
	status  atomic.Int32
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	c.mu.Lock()
	c.headers = append(c.headers, r.Header.Clone())
	c.mu.Unlock()
	if c.fail.Load() > 0 {
		c.fail.Add(-1)
		w.WriteHeader(int(c.status.Load()))
		return
	}
	_, spans, err := DecodeExport(body)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	c.spans = append(c.spans, spans...)
	c.mu.Unlock()
}

func (c *collector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.spans)
}

func TestExporterBatchesAndFlushesOnClose(t *testing.T) {
	col := &collector{}
	srv := httptest.NewServer(col)
	defer srv.Close()
	x := NewExporter(ExporterConfig{Endpoint: srv.URL, Headers: map[string]string{"X-Api-Key": "k"}, BatchSize: 3, FlushInterval: time.Hour})
	for range 7 {
		x.Export(sampleSpan())
	}
	unsampled := sampleSpan()
	unsampled.Sampled = false
	x.Export(unsampled)
	deadline := time.Now().Add(3 * time.Second)
	for col.count() < 6 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if col.count() != 6 {
		t.Fatalf("%d spans before Close, want two full batches of 3", col.count())
	}
	x.Close()
	if exported, dropped, _ := x.Stats(); col.count() != 7 || exported != 7 || dropped != 0 {
		t.Fatalf("after Close: collector %d, exported %d, dropped %d", col.count(), exported, dropped)
	}
	if h := col.headers[0]; h.Get("Content-Type") != "application/x-protobuf" || h.Get("X-Api-Key") != "k" {
		t.Fatalf("headers %v", h)
	}
}

func TestExporterRetriesOnceThenDrops(t *testing.T) {
	col := &collector{}
	srv := httptest.NewServer(col)
	defer srv.Close()
	col.status.Store(http.StatusServiceUnavailable)
	col.fail.Store(1)
	x := NewExporter(ExporterConfig{Endpoint: srv.URL, BatchSize: 1, FlushInterval: time.Hour})
	x.Export(sampleSpan())
	x.Close()
	if exported, dropped, failed := x.Stats(); exported != 1 || dropped != 0 || failed != 0 || col.count() != 1 {
		t.Fatalf("a 503 then a 200: exported %d dropped %d failed %d", exported, dropped, failed)
	}

	col.status.Store(http.StatusBadRequest)
	col.fail.Store(5)
	x = NewExporter(ExporterConfig{Endpoint: srv.URL, BatchSize: 1, FlushInterval: time.Hour})
	x.Export(sampleSpan())
	x.Close()
	if exported, dropped, failed := x.Stats(); exported != 0 || dropped != 1 || failed != 1 || col.fail.Load() != 4 {
		t.Fatalf("a 400 is not retried: exported %d dropped %d failed %d, tries %d", exported, dropped, failed, 5-col.fail.Load())
	}
}

func TestExporterNeverBlocks(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-block }))
	defer srv.Close()
	defer close(block)
	x := NewExporter(ExporterConfig{Endpoint: srv.URL, BatchSize: 1, QueueSize: 2, FlushInterval: time.Hour,
		Client: &http.Client{Timeout: 50 * time.Millisecond}})
	start := time.Now()
	for range 100 {
		x.Export(sampleSpan())
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("Export blocked on a stuck collector")
	}
	if _, dropped, _ := x.Stats(); dropped < 90 {
		t.Fatalf("dropped %d of 100 with a queue of 2", dropped)
	}
	var nilX *Exporter
	nilX.Export(sampleSpan())
	nilX.Close()
}
