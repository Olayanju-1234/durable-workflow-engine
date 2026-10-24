package ctl

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"
	"unicode/utf8"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

type jsonRaw = json.RawMessage

func jsonUnmarshal(raw []byte, out any) error {
	if err := json.Unmarshal(raw, out); err != nil {
		return fmt.Errorf("decode the engine's reply: %w", err)
	}
	return nil
}

func compactJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(b)
}

func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

func sortStrings(s []string) { sort.Strings(s) }

// table aligns rows into columns.
type table struct{ tw *tabwriter.Writer }

func newTable(w io.Writer) *table {
	return &table{tw: tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)}
}

func (t *table) row(cols ...string) {
	for i, c := range cols {
		cols[i] = strings.ReplaceAll(c, "\t", " ")
	}
	fmt.Fprintln(t.tw, strings.Join(cols, "\t"))
}

func (t *table) flush() error { return t.tw.Flush() }

// now is the clock relative times are measured from; tests pin it.
var now = time.Now

// when is an RFC 3339 time as a person reads it: UTC to the second, and how
// long ago (or until).
func when(rfc string) string {
	t, err := time.Parse(time.RFC3339Nano, rfc)
	if rfc == "" || err != nil {
		return "-"
	}
	return t.UTC().Format("2006-01-02 15:04:05Z") + " (" + relative(t) + ")"
}

// ago is when, shorter: just the relative part.
func ago(rfc string) string {
	t, err := time.Parse(time.RFC3339Nano, rfc)
	if rfc == "" || err != nil {
		return "-"
	}
	return relative(t)
}

func relative(t time.Time) string {
	d := now().Sub(t)
	ahead := d < 0
	if ahead {
		d = -d
	}
	var s string
	switch {
	case d < time.Minute:
		s = fmt.Sprintf("%ds", int(d.Seconds()))
	case d < time.Hour:
		s = fmt.Sprintf("%dm", int(d.Minutes()))
	case d < 48*time.Hour:
		s = fmt.Sprintf("%dh", int(d.Hours()))
	default:
		s = fmt.Sprintf("%dd", int(d.Hours()/24))
	}
	if ahead {
		return "in " + s
	}
	return s + " ago"
}

// clock is a unix-nanosecond timestamp in UTC, to the millisecond.
func clock(ns int64) string {
	if ns == 0 {
		return "-"
	}
	return time.Unix(0, ns).UTC().Format("2006-01-02 15:04:05.000Z")
}

func sizeOf(n int64) string {
	switch {
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MiB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KiB", float64(n)/(1<<10))
	default:
		return fmt.Sprintf("%d B", n)
	}
}

// payload shows a payload as text, or its size when it is bytes.
func payload(text, b64 string) string {
	if b64 != "" {
		if b, err := base64.StdEncoding.DecodeString(b64); err == nil {
			return fmt.Sprintf("<%d bytes>", len(b))
		}
	}
	if text == "" {
		return "-"
	}
	return oneLine(text, 100)
}

func bytesText(b []byte) string {
	if len(b) == 0 {
		return `""`
	}
	if !utf8.Valid(b) {
		return fmt.Sprintf("<%d bytes>", len(b))
	}
	return strconv.Quote(oneLine(string(b), 60))
}

// oneLine shortens text to one line of at most n characters.
func oneLine(s string, n int) string {
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) > n {
		r := []rune(s)
		s = string(r[:n]) + "…"
	}
	return s
}

// eventFields names an event's kind and its fields worth reading.
func eventFields(ev *history.HistoryEvent) (string, string) {
	var f []string
	add := func(k, v string) {
		if v != "" && v != `""` && v != "0" && v != "false" {
			f = append(f, k+"="+v)
		}
	}
	d := func(ms int64) string {
		if ms == 0 {
			return ""
		}
		return (time.Duration(ms) * time.Millisecond).String()
	}
	kind := "unknown"
	switch a := ev.GetAttributes().(type) {
	case *history.HistoryEvent_WorkflowStarted:
		ws := a.WorkflowStarted
		kind = "WorkflowStarted"
		add("type", ws.GetWorkflowType())
		add("run", ws.GetRunId())
		add("input", bytesText(ws.GetInput()))
		add("task_queue", ws.GetTaskQueue())
		add("execution_timeout", d(ws.GetExecutionTimeoutMs()))
		add("continued_from", ws.GetContinuedFromRunId())
	case *history.HistoryEvent_StepScheduled:
		ss := a.StepScheduled
		kind = "StepScheduled"
		add("step", ss.GetStepName())
		add("seq", strconv.FormatInt(ss.GetSequence(), 10))
		add("signal", ss.GetSignalName())
		add("input", bytesText(ss.GetInput()))
		add("task_queue", ss.GetTaskQueue())
		add("start_to_close", d(ss.GetStartToCloseTimeoutMs()))
		add("schedule_to_close", d(ss.GetScheduleToCloseTimeoutMs()))
	case *history.HistoryEvent_StepCompleted:
		sc := a.StepCompleted
		kind = "StepCompleted"
		add("step", sc.GetStepName())
		add("seq", strconv.FormatInt(sc.GetSequence(), 10))
		add("output", bytesText(sc.GetOutput()))
	case *history.HistoryEvent_StepFailed:
		sf := a.StepFailed
		kind = "StepFailed"
		add("step", sf.GetStepName())
		add("seq", strconv.FormatInt(sf.GetSequence(), 10))
		add("error", strconv.Quote(oneLine(sf.GetError(), 60)))
		add("error_type", sf.GetErrorType())
		add("will_retry", strconv.FormatBool(sf.GetWillRetry()))
		add("cancelled", strconv.FormatBool(sf.GetCancelled()))
		add("non_retryable", strconv.FormatBool(sf.GetNonRetryable()))
	case *history.HistoryEvent_WorkflowCompleted:
		wc := a.WorkflowCompleted
		kind = "WorkflowCompleted"
		add("output", bytesText(wc.GetOutput()))
		if next := wc.GetContinuedAsNew(); next != nil {
			add("continued_as_new", next.GetRunId())
		}
	case *history.HistoryEvent_WorkflowFailed:
		wf := a.WorkflowFailed
		kind = "WorkflowFailed"
		add("error", strconv.Quote(oneLine(wf.GetError(), 60)))
		add("terminated", strconv.FormatBool(wf.GetTerminated()))
		add("timed_out", strconv.FormatBool(wf.GetTimedOut()))
		add("cancelled", strconv.FormatBool(wf.GetCancelled()))
	case *history.HistoryEvent_TimerStarted:
		ts := a.TimerStarted
		kind = "TimerStarted"
		add("step", ts.GetStepName())
		add("seq", strconv.FormatInt(ts.GetSequence(), 10))
		add("fire_at", clock(ts.GetFireAt()))
	case *history.HistoryEvent_TimerFired:
		kind = "TimerFired"
		add("step", a.TimerFired.GetStepName())
		add("seq", strconv.FormatInt(a.TimerFired.GetSequence(), 10))
	case *history.HistoryEvent_SignalReceived:
		sr := a.SignalReceived
		kind = "SignalReceived"
		add("signal", sr.GetSignalName())
		add("payload", bytesText(sr.GetPayload()))
		add("request_id", sr.GetRequestId())
		add("cancel_request", strconv.FormatBool(sr.GetCancelRequest()))
	case *history.HistoryEvent_MarkerRecorded:
		mr := a.MarkerRecorded
		kind = "MarkerRecorded"
		add("step", mr.GetStepName())
		add("seq", strconv.FormatInt(mr.GetSequence(), 10))
		add("marker", mr.GetMarker())
		add("data", bytesText(mr.GetData()))
	}
	if n := ev.GetMinReaderFormat(); n > 0 {
		add("format", strconv.FormatUint(uint64(n), 10))
	}
	return kind, strings.Join(f, " ")
}
