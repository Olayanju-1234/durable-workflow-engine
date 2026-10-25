package tracing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// SpanKind and StatusCode are OTLP's enums.
type (
	SpanKind   int32
	StatusCode int32
)

const (
	KindInternal SpanKind = 1
	KindServer   SpanKind = 2
	KindConsumer SpanKind = 5

	StatusUnset StatusCode = 0
	StatusOK    StatusCode = 1
	StatusError StatusCode = 2
)

// Attr is a span or resource attribute: a string, bool, int, int64 or float64.
type Attr struct {
	Key   string
	Value any
}

// Span is a finished span, ready to export.
type Span struct {
	Context
	Parent        SpanID // zero for a root span
	Name          string
	Kind          SpanKind
	Start, End    time.Time
	Attrs         []Attr
	Status        StatusCode
	StatusMessage string
}

// Field numbers of the OTLP protobuf messages (opentelemetry-proto
// collector/trace/v1/trace_service.proto, trace/v1/trace.proto,
// common/v1/common.proto, resource/v1/resource.proto).
const (
	fExportResourceSpans = 1 // ExportTraceServiceRequest.resource_spans

	fResourceSpansResource   = 1
	fResourceSpansScopeSpans = 2
	fResourceAttributes      = 1
	fScopeSpansScope         = 1
	fScopeSpansSpans         = 2
	fScopeName               = 1
	fScopeVersion            = 2

	fSpanTraceID      = 1
	fSpanSpanID       = 2
	fSpanTraceState   = 3
	fSpanParentSpanID = 4
	fSpanName         = 5
	fSpanKind         = 6
	fSpanStart        = 7
	fSpanEnd          = 8
	fSpanAttributes   = 9
	fSpanStatus       = 15
	fSpanFlags        = 16
	fStatusMessage    = 2
	fStatusCode       = 3

	fKeyValueKey   = 1
	fKeyValueValue = 2
	fAnyString     = 1
	fAnyBool       = 2
	fAnyInt        = 3
	fAnyDouble     = 4
)

func appendMessage(b []byte, num protowire.Number, msg []byte) []byte {
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendBytes(b, msg)
}

func appendString(b []byte, num protowire.Number, s string) []byte {
	if s == "" {
		return b
	}
	b = protowire.AppendTag(b, num, protowire.BytesType)
	return protowire.AppendString(b, s)
}

func appendAttrs(b []byte, num protowire.Number, attrs []Attr) []byte {
	for _, a := range attrs {
		var v []byte
		switch x := a.Value.(type) {
		case string:
			v = protowire.AppendTag(v, fAnyString, protowire.BytesType)
			v = protowire.AppendString(v, x)
		case bool:
			v = protowire.AppendTag(v, fAnyBool, protowire.VarintType)
			v = protowire.AppendVarint(v, protowire.EncodeBool(x))
		case int:
			v = protowire.AppendTag(v, fAnyInt, protowire.VarintType)
			v = protowire.AppendVarint(v, uint64(int64(x)))
		case int64:
			v = protowire.AppendTag(v, fAnyInt, protowire.VarintType)
			v = protowire.AppendVarint(v, uint64(x))
		case float64:
			v = protowire.AppendTag(v, fAnyDouble, protowire.Fixed64Type)
			v = protowire.AppendFixed64(v, math.Float64bits(x))
		default:
			v = protowire.AppendTag(v, fAnyString, protowire.BytesType)
			v = protowire.AppendString(v, fmt.Sprint(x))
		}
		var kv []byte
		kv = appendString(kv, fKeyValueKey, a.Key)
		kv = appendMessage(kv, fKeyValueValue, v)
		b = appendMessage(b, num, kv)
	}
	return b
}

func encodeSpan(s Span) []byte {
	var b []byte
	b = appendMessage(b, fSpanTraceID, s.TraceID[:])
	b = appendMessage(b, fSpanSpanID, s.SpanID[:])
	b = appendString(b, fSpanTraceState, s.State)
	if !s.Parent.IsZero() {
		b = appendMessage(b, fSpanParentSpanID, s.Parent[:])
	}
	b = appendString(b, fSpanName, s.Name)
	if s.Kind != 0 {
		b = protowire.AppendTag(b, fSpanKind, protowire.VarintType)
		b = protowire.AppendVarint(b, uint64(s.Kind))
	}
	b = protowire.AppendTag(b, fSpanStart, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, uint64(s.Start.UnixNano()))
	b = protowire.AppendTag(b, fSpanEnd, protowire.Fixed64Type)
	b = protowire.AppendFixed64(b, uint64(s.End.UnixNano()))
	b = appendAttrs(b, fSpanAttributes, s.Attrs)
	if s.Status != StatusUnset || s.StatusMessage != "" {
		var st []byte
		st = appendString(st, fStatusMessage, s.StatusMessage)
		if s.Status != StatusUnset {
			st = protowire.AppendTag(st, fStatusCode, protowire.VarintType)
			st = protowire.AppendVarint(st, uint64(s.Status))
		}
		b = appendMessage(b, fSpanStatus, st)
	}
	var flags uint32
	if s.Sampled {
		flags = 1
	}
	b = protowire.AppendTag(b, fSpanFlags, protowire.Fixed32Type)
	return protowire.AppendFixed32(b, flags)
}

// EncodeExport is an OTLP ExportTraceServiceRequest in protobuf: one
// resource (the engine), one instrumentation scope, and the spans.
func EncodeExport(resource []Attr, scopeName, scopeVersion string, spans []Span) []byte {
	var scope []byte
	scope = appendString(scope, fScopeName, scopeName)
	scope = appendString(scope, fScopeVersion, scopeVersion)
	var ss []byte
	ss = appendMessage(ss, fScopeSpansScope, scope)
	for _, s := range spans {
		ss = appendMessage(ss, fScopeSpansSpans, encodeSpan(s))
	}
	var res []byte
	res = appendAttrs(res, fResourceAttributes, resource)
	var rs []byte
	rs = appendMessage(rs, fResourceSpansResource, res)
	rs = appendMessage(rs, fResourceSpansScopeSpans, ss)
	return appendMessage(nil, fExportResourceSpans, rs)
}

// ExporterConfig configures an OTLP/HTTP exporter.
type ExporterConfig struct {
	// Endpoint is the collector's traces URL, e.g. http://localhost:4318/v1/traces.
	Endpoint string
	// Headers go on every request (an API key, say).
	Headers map[string]string
	// Resource describes the process (service.name, service.version, ...).
	Resource     []Attr
	ScopeName    string
	ScopeVersion string
	Logger       *slog.Logger
	// BatchSize spans, or every FlushInterval, whichever comes first.
	BatchSize     int
	FlushInterval time.Duration
	// QueueSize spans may wait; past it new spans are dropped (and counted).
	QueueSize int
	Client    *http.Client
}

// Exporter sends finished spans to an OTLP/HTTP collector in batches, off
// the engine's path: Export never blocks, and a collector that is down only
// costs the spans that do not fit in the queue.
type Exporter struct {
	cfg  ExporterConfig
	ch   chan Span
	done chan struct{}
	wg   sync.WaitGroup
	once sync.Once

	exported, dropped, failed atomic.Uint64
}

// NewExporter starts an exporter; Close flushes and stops it.
func NewExporter(cfg ExporterConfig) *Exporter {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 256
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = 2 * time.Second
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 4096
	}
	if cfg.Client == nil {
		cfg.Client = &http.Client{Timeout: 10 * time.Second}
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	x := &Exporter{cfg: cfg, ch: make(chan Span, cfg.QueueSize), done: make(chan struct{})}
	x.wg.Add(1)
	go x.run()
	return x
}

// Export queues a span (if it is sampled). A nil exporter exports nothing.
func (x *Exporter) Export(s Span) {
	if x == nil || !s.Sampled || !s.Valid() {
		return
	}
	select {
	case x.ch <- s:
	default:
		x.dropped.Add(1)
	}
}

// Stats counts spans sent, dropped (the queue was full, or the collector
// refused them) and failed export requests.
func (x *Exporter) Stats() (exported, dropped, failed uint64) {
	if x == nil {
		return 0, 0, 0
	}
	return x.exported.Load(), x.dropped.Load(), x.failed.Load()
}

// Close sends what is queued and stops. Safe to call more than once.
func (x *Exporter) Close() {
	if x == nil {
		return
	}
	x.once.Do(func() { close(x.done) })
	x.wg.Wait()
}

func (x *Exporter) run() {
	defer x.wg.Done()
	t := time.NewTicker(x.cfg.FlushInterval)
	defer t.Stop()
	var batch []Span
	flush := func() {
		if len(batch) > 0 {
			x.send(batch)
			batch = nil
		}
	}
	for {
		select {
		case s := <-x.ch:
			if batch = append(batch, s); len(batch) >= x.cfg.BatchSize {
				flush()
			}
		case <-t.C:
			flush()
		case <-x.done:
			for {
				select {
				case s := <-x.ch:
					batch = append(batch, s)
				default:
					flush()
					return
				}
			}
		}
	}
}

// send posts one batch, retrying once after a refusal a retry may cure.
func (x *Exporter) send(batch []Span) {
	body := EncodeExport(x.cfg.Resource, x.cfg.ScopeName, x.cfg.ScopeVersion, batch)
	var err error
	for attempt := range 2 {
		if attempt > 0 {
			time.Sleep(time.Second)
		}
		var retry bool
		if retry, err = x.post(body); err == nil {
			x.exported.Add(uint64(len(batch)))
			return
		} else if !retry {
			break
		}
	}
	x.failed.Add(1)
	x.dropped.Add(uint64(len(batch)))
	x.cfg.Logger.Warn("trace export failed; spans dropped", "spans", len(batch), "endpoint", x.cfg.Endpoint, "err", err)
}

func (x *Exporter) post(body []byte) (retry bool, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, x.cfg.Endpoint, bytes.NewReader(body))
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	for k, v := range x.cfg.Headers {
		req.Header.Set(k, v)
	}
	res, err := x.cfg.Client.Do(req)
	if err != nil {
		return true, err
	}
	defer res.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(res.Body, 64<<10))
	switch {
	case res.StatusCode/100 == 2:
		return false, nil
	case res.StatusCode == http.StatusTooManyRequests || res.StatusCode >= 500:
		return true, fmt.Errorf("collector answered %s", res.Status)
	default:
		return false, errors.New("collector refused the spans: " + res.Status)
	}
}

// DecodeExport reads the spans back out of an ExportTraceServiceRequest that
// EncodeExport wrote (attributes as strings, ints, bools or floats). It is
// what a test's fake collector needs, and a way to look at what was sent.
func DecodeExport(b []byte) (resource []Attr, spans []Span, err error) {
	err = eachField(b, func(num protowire.Number, v []byte, _ uint64) error {
		if num != fExportResourceSpans {
			return nil
		}
		return eachField(v, func(num protowire.Number, v []byte, _ uint64) error {
			switch num {
			case fResourceSpansResource:
				return eachField(v, func(num protowire.Number, v []byte, _ uint64) error {
					if num == fResourceAttributes {
						a, err := decodeAttr(v)
						resource = append(resource, a)
						return err
					}
					return nil
				})
			case fResourceSpansScopeSpans:
				return eachField(v, func(num protowire.Number, v []byte, _ uint64) error {
					if num != fScopeSpansSpans {
						return nil
					}
					s, err := decodeSpan(v)
					spans = append(spans, s)
					return err
				})
			}
			return nil
		})
	})
	return resource, spans, err
}

// eachField calls fn for each field of a message: v is a length-delimited
// field's bytes, n a varint's or fixed field's value.
func eachField(b []byte, fn func(num protowire.Number, v []byte, n uint64) error) error {
	for len(b) > 0 {
		num, typ, l := protowire.ConsumeTag(b)
		if l < 0 {
			return protowire.ParseError(l)
		}
		b = b[l:]
		var v []byte
		var n uint64
		switch typ {
		case protowire.BytesType:
			v, l = protowire.ConsumeBytes(b)
		case protowire.VarintType:
			n, l = protowire.ConsumeVarint(b)
		case protowire.Fixed64Type:
			n, l = protowire.ConsumeFixed64(b)
		case protowire.Fixed32Type:
			var x uint32
			x, l = protowire.ConsumeFixed32(b)
			n = uint64(x)
		default:
			return fmt.Errorf("tracing: unexpected wire type %d", typ)
		}
		if l < 0 {
			return protowire.ParseError(l)
		}
		b = b[l:]
		if err := fn(num, v, n); err != nil {
			return err
		}
	}
	return nil
}

func decodeAttr(b []byte) (Attr, error) {
	var a Attr
	err := eachField(b, func(num protowire.Number, v []byte, _ uint64) error {
		switch num {
		case fKeyValueKey:
			a.Key = string(v)
		case fKeyValueValue:
			return eachField(v, func(num protowire.Number, v []byte, n uint64) error {
				switch num {
				case fAnyString:
					a.Value = string(v)
				case fAnyBool:
					a.Value = n != 0
				case fAnyInt:
					a.Value = int64(n)
				case fAnyDouble:
					a.Value = math.Float64frombits(n)
				}
				return nil
			})
		}
		return nil
	})
	return a, err
}

func decodeSpan(b []byte) (Span, error) {
	var s Span
	err := eachField(b, func(num protowire.Number, v []byte, n uint64) error {
		switch num {
		case fSpanTraceID:
			copy(s.TraceID[:], v)
		case fSpanSpanID:
			copy(s.SpanID[:], v)
		case fSpanTraceState:
			s.State = string(v)
		case fSpanParentSpanID:
			copy(s.Parent[:], v)
		case fSpanName:
			s.Name = string(v)
		case fSpanKind:
			s.Kind = SpanKind(n)
		case fSpanStart:
			s.Start = time.Unix(0, int64(n))
		case fSpanEnd:
			s.End = time.Unix(0, int64(n))
		case fSpanAttributes:
			a, err := decodeAttr(v)
			s.Attrs = append(s.Attrs, a)
			return err
		case fSpanStatus:
			return eachField(v, func(num protowire.Number, v []byte, n uint64) error {
				switch num {
				case fStatusMessage:
					s.StatusMessage = string(v)
				case fStatusCode:
					s.Status = StatusCode(n)
				}
				return nil
			})
		case fSpanFlags:
			s.Sampled = n&1 == 1
		}
		return nil
	})
	return s, err
}

// Attr returns a span's attribute value by key (nil if it has none).
func (s Span) Attr(key string) any {
	for _, a := range s.Attrs {
		if a.Key == key {
			return a.Value
		}
	}
	return nil
}
