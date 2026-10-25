// Package tracing carries W3C Trace Context through workflows and exports
// their spans over OTLP/HTTP (ADR-0024), with no OpenTelemetry dependency:
// traceparent is a fixed text format, and the OTLP request is encoded field
// by field with protowire, from the OpenTelemetry protocol's field numbers.
package tracing

import (
	"crypto/rand"
	"encoding/hex"
	"strings"
)

// TraceID and SpanID are W3C Trace Context identifiers.
type (
	TraceID [16]byte
	SpanID  [8]byte
)

func (t TraceID) String() string { return hex.EncodeToString(t[:]) }
func (s SpanID) String() string  { return hex.EncodeToString(s[:]) }

// IsZero reports an all-zero (invalid) id.
func (t TraceID) IsZero() bool { return t == TraceID{} }
func (s SpanID) IsZero() bool  { return s == SpanID{} }

// NewTraceID and NewSpanID are random, non-zero ids.
func NewTraceID() TraceID {
	var t TraceID
	for t.IsZero() {
		_, _ = rand.Read(t[:])
	}
	return t
}

func NewSpanID() SpanID {
	var s SpanID
	for s.IsZero() {
		_, _ = rand.Read(s[:])
	}
	return s
}

// Context is one span's identity: what a traceparent header carries.
type Context struct {
	TraceID TraceID
	SpanID  SpanID
	Sampled bool
	State   string // tracestate, passed on unchanged
}

// FromIDs builds a Context from recorded ids (invalid if they are not 16
// and 8 bytes).
func FromIDs(traceID, spanID []byte, sampled bool, state string) Context {
	var c Context
	if len(traceID) != 16 || len(spanID) != 8 {
		return c
	}
	copy(c.TraceID[:], traceID)
	copy(c.SpanID[:], spanID)
	c.Sampled, c.State = sampled, state
	return c
}

// Valid reports whether both ids are set.
func (c Context) Valid() bool { return !c.TraceID.IsZero() && !c.SpanID.IsZero() }

// Traceparent formats c as a W3C traceparent header value (version 00).
func (c Context) Traceparent() string {
	if !c.Valid() {
		return ""
	}
	flags := "00"
	if c.Sampled {
		flags = "01"
	}
	return "00-" + c.TraceID.String() + "-" + c.SpanID.String() + "-" + flags
}

// Parse reads a traceparent header value. It accepts version 00 and, as the
// specification asks, later versions by their first four fields; anything
// malformed, or with an all-zero id, is not a context (ok is false).
func Parse(traceparent, tracestate string) (c Context, ok bool) {
	parts := strings.Split(strings.TrimSpace(traceparent), "-")
	if len(parts) < 4 || len(parts[0]) != 2 || parts[0] == "ff" || (parts[0] == "00" && len(parts) != 4) {
		return Context{}, false
	}
	if !decodeHex(c.TraceID[:], parts[1]) || !decodeHex(c.SpanID[:], parts[2]) || len(parts[3]) != 2 {
		return Context{}, false
	}
	var flags [1]byte
	if !decodeHex(flags[:], parts[3]) || !c.Valid() {
		return Context{}, false
	}
	var version [1]byte
	if !decodeHex(version[:], parts[0]) {
		return Context{}, false
	}
	c.Sampled = flags[0]&1 == 1
	c.State = strings.TrimSpace(tracestate)
	return c, true
}

// decodeHex fills dst from lowercase hex of exactly twice its length.
func decodeHex(dst []byte, s string) bool {
	if len(s) != 2*len(dst) || strings.ToLower(s) != s {
		return false
	}
	_, err := hex.Decode(dst, []byte(s))
	return err == nil
}
