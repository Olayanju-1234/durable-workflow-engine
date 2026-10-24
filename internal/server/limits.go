package server

import (
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/record"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// Limits (ADR-0012). A history that grows without end makes every replay,
// recovery and workflow task slower until something breaks, so each history
// is bounded, by events and by bytes. Crossing a share of either (warnShare)
// logs a warning, counts it, and tells the workflow code to continue as new;
// reaching either ends the workflow as FAILED with a message that says so.
// Payloads (inputs, outputs, signals, markers) have one size limit on every
// transport, checked by the engine itself.
const (
	DefaultMaxHistoryEvents = 51_200
	DefaultMaxHistoryBytes  = 50 << 20
	DefaultMaxPayloadBytes  = 2 << 20

	warnShare = 0.8
	// limitReserve keeps room, under the byte limit, for the event that fails
	// a workflow at its limit.
	limitReserve = 4 << 10
)

// ErrHistoryLimit: an append would have taken a history past its limit, so
// the engine ended the workflow instead.
var ErrHistoryLimit = errors.New("server: history limit reached")

// ErrPayloadTooLarge: a payload is over the engine's payload limit.
var ErrPayloadTooLarge = errors.New("server: payload too large")

// Limits are an engine's history and payload bounds. Zero means no bound.
type Limits struct {
	MaxHistoryEvents int
	MaxHistoryBytes  int64
	MaxPayloadBytes  int
}

// Validate refuses negative limits, and history limits too small to hold a
// payload.
func (l Limits) Validate() error {
	if l.MaxHistoryEvents < 0 || l.MaxHistoryBytes < 0 || l.MaxPayloadBytes < 0 {
		return errors.New("limits must not be negative (0 means no limit)")
	}
	if l.MaxHistoryEvents > 0 && l.MaxHistoryEvents < 10 {
		return fmt.Errorf("--max-history-events %d is too small to run a workflow", l.MaxHistoryEvents)
	}
	if l.MaxHistoryBytes > 0 && l.MaxPayloadBytes > 0 && l.MaxHistoryBytes < 4*int64(l.MaxPayloadBytes) {
		return fmt.Errorf("--max-history-bytes %d must be at least 4 payloads (--max-payload-bytes %d)", l.MaxHistoryBytes, l.MaxPayloadBytes)
	}
	return nil
}

// RequestBytes is the largest request body or gRPC message the transports
// accept: room for a batch of commands carrying full-size payloads, base64
// included.
func (l Limits) RequestBytes() int {
	if l.MaxPayloadBytes <= 0 {
		return 64 << 20
	}
	return 4*l.MaxPayloadBytes + 1<<20
}

func (l Limits) warnEvents() int {
	return int(float64(l.MaxHistoryEvents) * warnShare)
}

func (l Limits) warnBytes() int64 {
	return int64(float64(l.MaxHistoryBytes) * warnShare)
}

// WithLimits sets the engine's history and payload limits (zero disables one).
func WithLimits(l Limits) Option {
	return func(e *Engine) { e.limits = l }
}

// Limits returns the engine's limits.
func (e *Engine) Limits() Limits { return e.limits }

// checkPayload refuses a payload over the limit.
func (e *Engine) checkPayload(what string, b []byte) error {
	if n := e.limits.MaxPayloadBytes; n > 0 && len(b) > n {
		return fmt.Errorf("%w: %s is %d bytes, over the %d-byte limit (--max-payload-bytes)", ErrPayloadTooLarge, what, len(b), n)
	}
	return nil
}

// clip shortens an error message to the payload limit: a failure is worth
// recording even when its message is huge.
func (e *Engine) clip(msg string) string {
	if n := e.limits.MaxPayloadBytes; n > 0 && len(msg) > n {
		return msg[:n] + "… (truncated)"
	}
	return msg
}

// overLimit says why appending an event of size bytes would take w's history
// past a limit, or "" if it fits. The event that ends a workflow is always
// written, so a history can end one event over.
func (e *Engine) overLimit(w *wfState, size int64) string {
	l := e.limits
	if l.MaxHistoryEvents > 0 && len(w.events)+1 >= l.MaxHistoryEvents {
		return fmt.Sprintf("history limit reached: %d events (limit %d)", len(w.events), l.MaxHistoryEvents)
	}
	if l.MaxHistoryBytes > 0 && w.bytes+size > l.MaxHistoryBytes-limitReserve {
		return fmt.Sprintf("history limit reached: %d bytes (limit %d)", w.bytes, l.MaxHistoryBytes)
	}
	return ""
}

// crossedWarning reports whether a history of this many events and bytes has
// crossed the warning threshold.
func (l Limits) crossedWarning(events int, bytes int64) bool {
	return (l.MaxHistoryEvents > 0 && events >= l.warnEvents()) || (l.MaxHistoryBytes > 0 && bytes >= l.warnBytes())
}

// eventSize is an event's size in the history file.
func eventSize(ev *history.HistoryEvent) int64 {
	return int64(proto.Size(ev)) + record.HeaderSize
}

// checkDecisionPayloads refuses a decision carrying a payload over the limit.
func (e *Engine) checkDecisionPayloads(d state.Decision) error {
	switch d.Type {
	case state.DecideScheduleStep, state.DecideStartTimer, state.DecideAwaitSignal:
		return e.checkPayload(fmt.Sprintf("the input of step %q", d.StepName), d.Input)
	case state.DecideRecordMarker:
		return e.checkPayload(fmt.Sprintf("marker %q", d.StepName), d.Output)
	case state.DecideCompleteWorkflow:
		return e.checkPayload("workflow output", d.Output)
	case state.DecideContinueAsNew:
		return e.checkPayload("the next run's input", d.Input)
	}
	return nil
}

// failAtLimitLocked ends a workflow whose history is full, instead of
// recording what would have taken it past its limit.
func (e *Engine) failAtLimitLocked(w *wfState, why string) error {
	msg := why + "; a workflow that runs this long should continue as new (docs: Limits)"
	failed := &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowFailed{
		WorkflowFailed: &history.WorkflowFailedEvent{Error: msg},
	}}
	if err := e.appendLocked(w, failed, int64(len(w.events)+1)); err != nil {
		return err
	}
	e.metrics.historyLimit.With("failed").Inc()
	e.metrics.workflowsFinished.With("failed").Inc()
	e.metrics.workflowsActive.Dec()
	e.log.Error("workflow failed at its history limit", "workflow_id", w.id, "reason", why)
	return fmt.Errorf("%w: %s: %s", ErrHistoryLimit, w.id, why)
}
