package ctl

import (
	"encoding/base64"
	"flag"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

func commands() map[string]command {
	cmds := []command{
		{"health", "", "Show the engine's version, history format and gauges.", setupHealth},
		{"namespaces", "", "List the namespaces your token can use.", setupNamespaces},
		{"list", "", "List workflows by id; filter by status, type or text.", setupList},
		{"describe", "<workflow-id>", "Show a workflow: status, input, output and every step.", setupDescribe},
		{"history", "<workflow-id>", "Print a run's events exactly as recorded.", setupHistory},
		{"runs", "<workflow-id>", "List a workflow id's runs: the current one and earlier ones.", setupRuns},
		{"start", "<workflow-type>", "Start a workflow.", setupStart},
		{"signal", "<workflow-id> <signal>", "Send a signal to a workflow.", setupSignal},
		{"cancel", "<workflow-id>", "Cancel a workflow (--graceful lets its code compensate first).", setupCancel},
		{"terminate", "<workflow-id>", "End a workflow at once, recording why.", setupTerminate},
		{"schedule list", "", "List schedules.", setupScheduleList},
		{"schedule create", "<schedule-id>", "Create a schedule, or replace one with the same id.", setupScheduleCreate},
		{"schedule delete", "<schedule-id>", "Delete a schedule (its runs carry on).", setupScheduleDelete},
		{"schedule pause", "<schedule-id>", "Stop a schedule firing until it is resumed.", setupSchedulePause},
		{"schedule resume", "<schedule-id>", "Make a paused schedule fire again.", setupScheduleResume},
		{"schedule backfill", "<schedule-id>", "Start the runs a schedule would have started over a past range.", setupScheduleBackfill},
		{"schedule preview", "<schedule-id>", "Show when a schedule fires next.", setupSchedulePreview},
		{"backup", "<file.tar.gz | ->", "Back up every history: from the engine (engine token), or a data directory (--data-dir).", setupBackup},
		{"restore", "<file.tar.gz | ->", "Restore a backup into a new data directory (--data-dir), with the engine stopped.", setupRestore},
		{"archive list", "", "List archived runs (finished histories retention exported).", setupArchiveList},
		{"archive describe", "<workflow-id>", "Show an archived run (the latest, or --run-id).", setupArchiveDescribe},
		{"archive history", "<workflow-id>", "Print an archived run's events.", setupArchiveHistory},
	}
	m := make(map[string]command, len(cmds))
	for _, c := range cmds {
		m[c.name] = c
	}
	return m
}

func wantArgs(pos []string, names ...string) error {
	if len(pos) != len(names) {
		return usagef("want %s, got %d argument(s)", strings.Join(names, " "), len(pos))
	}
	return nil
}

func setupHealth(*flag.FlagSet) func(*cli, []string) error {
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos); err != nil {
			return err
		}
		var h map[string]any
		if printed, err := c.getJSON("/api/health", nil, &h); printed || err != nil {
			return err
		}
		keys := make([]string, 0, len(h))
		for k := range h {
			keys = append(keys, k)
		}
		sortStrings(keys)
		t := newTable(c.out)
		for _, k := range keys {
			v := h[k]
			if m, ok := v.(map[string]any); ok {
				v = compactJSON(m)
			}
			t.row(k, fmt.Sprint(v))
		}
		return t.flush()
	}
}

func setupNamespaces(*flag.FlagSet) func(*cli, []string) error {
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos); err != nil {
			return err
		}
		var r struct {
			Namespaces []string `json:"namespaces"`
			Current    string   `json:"current"`
			Admin      bool     `json:"admin"`
		}
		if printed, err := c.getJSON("/api/namespaces", nil, &r); printed || err != nil {
			return err
		}
		for _, n := range r.Namespaces {
			mark := " "
			if n == r.Current {
				mark = "*"
			}
			fmt.Fprintf(c.out, "%s %s\n", mark, n)
		}
		return nil
	}
}

type summary struct {
	WorkflowID    string `json:"workflow_id"`
	WorkflowType  string `json:"workflow_type"`
	Status        string `json:"status"`
	StartedAt     string `json:"started_at"`
	UpdatedAt     string `json:"updated_at"`
	Error         string `json:"error"`
	HistoryLength int64  `json:"history_length"`
	RunID         string `json:"run_id"`
	Bytes         int64  `json:"history_size_bytes"`
	TaskQueue     string `json:"task_queue"`
}

func setupList(fs *flag.FlagSet) func(*cli, []string) error {
	status := fs.String("status", "", "only workflows in this status: RUNNING, WAITING, COMPLETED, FAILED, TERMINATED, TIMED_OUT, CANCELLED")
	typ := fs.String("type", "", "only workflows of this type")
	query := fs.String("query", "", "only workflows whose id or type contains this text")
	limit := fs.Int("limit", 50, "show at most this many (1-1000)")
	offset := fs.Int("offset", 0, "skip this many first")
	all := fs.Bool("all", false, "show every match, page by page")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos); err != nil {
			return err
		}
		q := url.Values{}
		for k, v := range map[string]string{"status": strings.ToUpper(*status), "type": *typ, "q": *query} {
			if v != "" {
				q.Set(k, v)
			}
		}
		var rows []summary
		total := 0
		for off := *offset; ; {
			q.Set("offset", strconv.Itoa(off))
			q.Set("limit", strconv.Itoa(*limit))
			var page struct {
				Workflows []summary `json:"workflows"`
				Total     int       `json:"total"`
			}
			var raw []byte
			if err := c.request(http.MethodGet, "/api/workflows", q, nil, &raw); err != nil {
				return err
			}
			if c.json && !*all {
				c.printJSON(raw)
				return nil
			}
			if err := jsonUnmarshal(raw, &page); err != nil {
				return err
			}
			rows, total = append(rows, page.Workflows...), page.Total
			off += len(page.Workflows)
			if !*all || len(page.Workflows) == 0 || off >= page.Total {
				break
			}
		}
		if c.json {
			fmt.Fprintln(c.out, compactJSON(map[string]any{"workflows": rows, "total": total}))
			return nil
		}
		t := newTable(c.out)
		t.row("WORKFLOW ID", "TYPE", "STATUS", "STARTED", "UPDATED", "EVENTS", "TASK QUEUE")
		for _, s := range rows {
			t.row(s.WorkflowID, s.WorkflowType, s.Status, ago(s.StartedAt), ago(s.UpdatedAt), strconv.FormatInt(s.HistoryLength, 10), s.TaskQueue)
		}
		if err := t.flush(); err != nil {
			return err
		}
		if len(rows) < total {
			fmt.Fprintf(c.out, "%d of %d shown (--all, --limit or --offset for more)\n", len(rows), total)
		}
		return nil
	}
}

type workflowDetail struct {
	summary
	Input               string `json:"input"`
	InputBase64         string `json:"input_base64"`
	Output              string `json:"output"`
	OutputBase64        string `json:"output_base64"`
	ContinuedFromRunID  string `json:"continued_from_run_id"`
	ContinuedAsNewRunID string `json:"continued_as_new_run_id"`
	CancelRequested     bool   `json:"cancel_requested"`
	CancelReason        string `json:"cancel_reason"`
	Steps               []struct {
		StepName     string `json:"step_name"`
		Sequence     int64  `json:"sequence"`
		Kind         string `json:"kind"`
		Status       string `json:"status"`
		Attempts     int    `json:"attempts"`
		Error        string `json:"error"`
		ErrorType    string `json:"error_type"`
		Output       string `json:"output"`
		OutputBase64 string `json:"output_base64"`
		FireAt       string `json:"fire_at"`
		SignalName   string `json:"signal_name"`
		Marker       string `json:"marker"`
		TaskQueue    string `json:"task_queue"`
	} `json:"steps"`
}

func setupDescribe(fs *flag.FlagSet) func(*cli, []string) error {
	return describeAt(fs, "/api/workflows/", "describe this earlier run instead of the current one")
}

func setupArchiveDescribe(fs *flag.FlagSet) func(*cli, []string) error {
	return describeAt(fs, "/api/archive/workflows/", "describe this archived run instead of the latest")
}

// describeAt shows a workflow run read from base + id: live or archived.
func describeAt(fs *flag.FlagSet, base, runHelp string) func(*cli, []string) error {
	runID := fs.String("run-id", "", runHelp)
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<workflow-id>"); err != nil {
			return err
		}
		q := url.Values{}
		if *runID != "" {
			q.Set("run_id", *runID)
		}
		var w workflowDetail
		if printed, err := c.getJSON(base+url.PathEscape(pos[0]), q, &w); printed || err != nil {
			return err
		}
		t := newTable(c.out)
		t.row("Workflow", w.WorkflowID)
		t.row("Type", w.WorkflowType)
		status := w.Status
		if w.CancelRequested && (status == "RUNNING" || status == "WAITING") {
			status += " (cancel requested: " + w.CancelReason + ")"
		}
		t.row("Status", status)
		run := w.RunID
		if w.ContinuedFromRunID != "" {
			run += " (continued from " + w.ContinuedFromRunID + ")"
		}
		if w.ContinuedAsNewRunID != "" {
			run += " (continued as " + w.ContinuedAsNewRunID + ")"
		}
		t.row("Run", run)
		t.row("Task queue", w.TaskQueue)
		t.row("Started", when(w.StartedAt))
		t.row("Updated", when(w.UpdatedAt))
		t.row("Events", fmt.Sprintf("%d (%s)", w.HistoryLength, sizeOf(w.Bytes)))
		t.row("Input", payload(w.Input, w.InputBase64))
		if w.Output != "" || w.OutputBase64 != "" {
			t.row("Output", payload(w.Output, w.OutputBase64))
		}
		if w.Error != "" {
			t.row("Error", oneLine(w.Error, 200))
		}
		if err := t.flush(); err != nil {
			return err
		}
		if len(w.Steps) == 0 {
			return nil
		}
		fmt.Fprintln(c.out)
		t = newTable(c.out)
		t.row("SEQ", "STEP", "KIND", "STATUS", "ATTEMPTS", "DETAIL")
		for _, s := range w.Steps {
			detail := payload(s.Output, s.OutputBase64)
			switch {
			case s.Error != "":
				detail = oneLine(s.Error, 80)
				if s.ErrorType != "" {
					detail = s.ErrorType + ": " + detail
				}
			case s.SignalName != "" && s.Status == "SCHEDULED":
				detail = "waiting for " + s.SignalName
			case s.FireAt != "" && s.Status == "SCHEDULED":
				detail = "fires " + when(s.FireAt)
			}
			if s.TaskQueue != "" && s.TaskQueue != "default" {
				detail = "[" + s.TaskQueue + "] " + detail
			}
			t.row(strconv.FormatInt(s.Sequence, 10), s.StepName, s.Kind, s.Status, strconv.Itoa(s.Attempts), detail)
		}
		return t.flush()
	}
}

func setupHistory(fs *flag.FlagSet) func(*cli, []string) error {
	return historyAt(fs, "/api/workflows/", "print this earlier run instead of the current one")
}

func setupArchiveHistory(fs *flag.FlagSet) func(*cli, []string) error {
	return historyAt(fs, "/api/archive/workflows/", "print this archived run instead of the latest")
}

func historyAt(fs *flag.FlagSet, base, runHelp string) func(*cli, []string) error {
	runID := fs.String("run-id", "", runHelp)
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<workflow-id>"); err != nil {
			return err
		}
		q := url.Values{}
		if *runID != "" {
			q.Set("run_id", *runID)
		}
		var r struct {
			Events []jsonRaw `json:"events"`
		}
		if printed, err := c.getJSON(base+url.PathEscape(pos[0])+"/history", q, &r); printed || err != nil {
			return err
		}
		return printEvents(c.out, r.Events)
	}
}

func setupArchiveList(fs *flag.FlagSet) func(*cli, []string) error {
	prefix := fs.String("prefix", "", "only workflow ids that start with this")
	limit := fs.Int("limit", 200, "show at most this many runs")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos); err != nil {
			return err
		}
		q := url.Values{"limit": {strconv.Itoa(*limit)}}
		if *prefix != "" {
			q.Set("prefix", *prefix)
		}
		var r struct {
			Runs []struct {
				WorkflowID string `json:"workflow_id"`
				RunID      string `json:"run_id"`
				ClosedAt   string `json:"closed_at"`
			} `json:"runs"`
		}
		if printed, err := c.getJSON("/api/archive/workflows", q, &r); printed || err != nil {
			return err
		}
		t := newTable(c.out)
		t.row("WORKFLOW ID", "RUN ID", "CLOSED")
		for _, run := range r.Runs {
			t.row(run.WorkflowID, run.RunID, when(run.ClosedAt))
		}
		return t.flush()
	}
}

// printEvents renders recorded events one per line: id, time, kind, fields.
func printEvents(w io.Writer, events []jsonRaw) error {
	t := newTable(w)
	t.row("ID", "TIME", "EVENT", "DETAILS")
	for _, raw := range events {
		ev := &history.HistoryEvent{}
		if err := (protojson.UnmarshalOptions{DiscardUnknown: true}).Unmarshal(raw, ev); err != nil {
			return fmt.Errorf("decode event: %w", err)
		}
		kind, fields := eventFields(ev)
		t.row(strconv.FormatInt(ev.GetEventId(), 10), clock(ev.GetTimestamp()), kind, fields)
	}
	return t.flush()
}

func setupRuns(*flag.FlagSet) func(*cli, []string) error {
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<workflow-id>"); err != nil {
			return err
		}
		var r struct {
			Runs []struct {
				RunID     string `json:"run_id"`
				Status    string `json:"status"`
				StartedAt string `json:"started_at"`
				ClosedAt  string `json:"closed_at"`
				Current   bool   `json:"current"`
			} `json:"runs"`
		}
		if printed, err := c.getJSON("/api/workflows/"+url.PathEscape(pos[0])+"/runs", nil, &r); printed || err != nil {
			return err
		}
		t := newTable(c.out)
		t.row("RUN ID", "STATUS", "STARTED", "CLOSED", "")
		for _, run := range r.Runs {
			cur := ""
			if run.Current {
				cur = "current"
			}
			t.row(run.RunID, run.Status, when(run.StartedAt), when(run.ClosedAt), cur)
		}
		return t.flush()
	}
}

// payloadFlags registers --<name> and --<name>-file, and returns what reads
// the value: text, a file's bytes, or standard input for "-".
func payloadFlags(fs *flag.FlagSet, name, help string) func(c *cli) (val []byte, set bool, err error) {
	text := fs.String(name, "", help)
	file := fs.String(name+"-file", "", "read the "+name+" from this file (- for standard input)")
	return func(c *cli) ([]byte, bool, error) {
		switch {
		case *text != "" && *file != "":
			return nil, false, usagef("set --%s or --%s-file, not both", name, name)
		case *file == "-":
			b, err := io.ReadAll(c.stdin)
			return b, true, err
		case *file != "":
			b, err := os.ReadFile(*file)
			return b, true, err
		default:
			return []byte(*text), *text != "", nil
		}
	}
}

// putPayload sets field (or its _base64 twin for bytes that are not text).
func putPayload(body map[string]any, field string, b []byte) {
	if utf8.Valid(b) {
		body[field] = string(b)
	} else {
		body[field+"_base64"] = base64.StdEncoding.EncodeToString(b)
	}
}

func setupStart(fs *flag.FlagSet) func(*cli, []string) error {
	id := fs.String("id", "", "the workflow id (default: generated)")
	input := payloadFlags(fs, "input", "the workflow's input")
	timeout := fs.Duration("execution-timeout", 0, "end the workflow TIMED_OUT if it has not finished this long after it starts")
	queue := fs.String("task-queue", "", "the task queue it runs on (default: the default queue)")
	reuse := fs.String("id-reuse-policy", "", "ALLOW_DUPLICATE, ALLOW_DUPLICATE_FAILED_ONLY or REJECT_DUPLICATE (default)")
	conflict := fs.String("id-conflict-policy", "", "FAIL (default) or USE_EXISTING")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<workflow-type>"); err != nil {
			return err
		}
		in, _, err := input(c)
		if err != nil {
			return err
		}
		body := map[string]any{"type": pos[0]}
		putPayload(body, "input", in)
		for k, v := range map[string]string{"workflow_id": *id, "task_queue": *queue, "id_reuse_policy": *reuse, "id_conflict_policy": *conflict} {
			if v != "" {
				body[k] = v
			}
		}
		if *timeout > 0 {
			body["execution_timeout_ms"] = timeout.Milliseconds()
		}
		var raw []byte
		if err := c.request(http.MethodPost, "/api/workflows", nil, body, &raw); err != nil {
			return err
		}
		if c.json {
			c.printJSON(raw)
			return nil
		}
		var r struct {
			WorkflowID string `json:"workflow_id"`
			RunID      string `json:"run_id"`
			Started    bool   `json:"started"`
		}
		if err := jsonUnmarshal(raw, &r); err != nil {
			return err
		}
		if r.Started {
			fmt.Fprintf(c.out, "started %s (run %s)\n", r.WorkflowID, r.RunID)
		} else {
			fmt.Fprintf(c.out, "%s is already running (run %s)\n", r.WorkflowID, r.RunID)
		}
		return nil
	}
}

func setupSignal(fs *flag.FlagSet) func(*cli, []string) error {
	pl := payloadFlags(fs, "payload", "the signal's payload")
	requestID := fs.String("request-id", "", "an idempotency key: sending again with the same key records the signal once")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<workflow-id>", "<signal>"); err != nil {
			return err
		}
		p, _, err := pl(c)
		if err != nil {
			return err
		}
		body := map[string]any{"name": pos[1]}
		putPayload(body, "payload", p)
		if *requestID != "" {
			body["request_id"] = *requestID
		}
		return c.simple(http.MethodPost, "/api/workflows/"+url.PathEscape(pos[0])+"/signal", body,
			fmt.Sprintf("signalled %s: %s", pos[0], pos[1]))
	}
}

func setupCancel(fs *flag.FlagSet) func(*cli, []string) error {
	reason := fs.String("reason", "", "recorded in the workflow's history")
	graceful := fs.Bool("graceful", false, "tell the workflow code instead, so it can compensate before it ends CANCELLED")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<workflow-id>"); err != nil {
			return err
		}
		body := map[string]any{"reason": *reason}
		done := "cancelled " + pos[0]
		if *graceful {
			body["graceful"] = true
			done = "cancel requested for " + pos[0]
		}
		return c.simple(http.MethodPost, "/api/workflows/"+url.PathEscape(pos[0])+"/cancel", body, done)
	}
}

func setupTerminate(fs *flag.FlagSet) func(*cli, []string) error {
	reason := fs.String("reason", "", "recorded in the workflow's history")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<workflow-id>"); err != nil {
			return err
		}
		return c.simple(http.MethodPost, "/api/workflows/"+url.PathEscape(pos[0])+"/terminate",
			map[string]any{"reason": *reason}, "terminated "+pos[0])
	}
}

// simple sends a request whose answer is a line for people or JSON for scripts.
func (c *cli) simple(method, path string, body any, done string) error {
	var raw []byte
	if err := c.request(method, path, nil, body, &raw); err != nil {
		return err
	}
	if c.json {
		c.printJSON(raw)
	} else {
		fmt.Fprintln(c.out, done)
	}
	return nil
}

func setupScheduleList(*flag.FlagSet) func(*cli, []string) error {
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos); err != nil {
			return err
		}
		var r struct {
			Schedules []map[string]any `json:"schedules"`
		}
		if printed, err := c.getJSON("/api/schedules", nil, &r); printed || err != nil {
			return err
		}
		t := newTable(c.out)
		t.row("SCHEDULE ID", "CRON", "WORKFLOW TYPE", "NEXT FIRE", "OVERLAP", "TASK QUEUE", "STATE")
		for _, s := range r.Schedules {
			cron := str(s["cron"])
			if tz := str(s["timezone"]); tz != "" {
				cron += " (" + tz + ")"
			}
			if j, _ := s["jitter_ms"].(float64); j > 0 {
				cron += fmt.Sprintf(" ±%s", time.Duration(j)*time.Millisecond)
			}
			state := "active"
			if b, _ := s["paused"].(bool); b {
				state = "paused"
				if n := str(s["note"]); n != "" {
					state += ": " + oneLine(n, 40)
				}
			}
			if held := str(s["buffered_fire_at"]); held != "" {
				state += ", holding " + when(held)
			}
			t.row(str(s["schedule_id"]), cron, str(s["workflow_type"]), when(str(s["next_fire_at"])), str(s["overlap_policy"]),
				str(s["task_queue"]), state)
		}
		return t.flush()
	}
}

func setupScheduleCreate(fs *flag.FlagSet) func(*cli, []string) error {
	cron := fs.String("cron", "", `when to fire: five cron fields, a macro (@hourly, @daily, ...) or "@every 15m"`)
	tz := fs.String("timezone", "", "the IANA time zone the cron fields are in (default UTC)")
	typ := fs.String("type", "", "the workflow type each fire starts")
	input := payloadFlags(fs, "input", "each run's input")
	queue := fs.String("task-queue", "", "the task queue runs start on")
	skip := fs.Bool("skip-if-running", false, "skip a fire while an earlier run is still going (same as --overlap skip)")
	overlap := fs.String("overlap", "", "while an earlier run is going: skip, buffer_one (hold one fire) or allow_all (the default)")
	jitter := fs.Duration("jitter", 0, "start each run up to this long after its fire time, e.g. 30s")
	paused := fs.Bool("paused", false, "create it paused")
	note := fs.String("note", "", "a note on the schedule, e.g. why it is paused")
	dryRun := fs.Bool("dry-run", false, "check the schedule and show its next fires, without creating it")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<schedule-id>"); err != nil {
			return err
		}
		if *cron == "" || *typ == "" {
			return usagef("--cron and --type are required")
		}
		in, _, err := input(c)
		if err != nil {
			return err
		}
		body := map[string]any{"schedule_id": pos[0], "cron": *cron, "workflow_type": *typ}
		putPayload(body, "input", in)
		for k, v := range map[string]string{"timezone": *tz, "task_queue": *queue, "overlap_policy": *overlap, "note": *note} {
			if v != "" {
				body[k] = v
			}
		}
		if *skip {
			body["skip_if_running"] = true
		}
		if *jitter > 0 {
			body["jitter_ms"] = jitter.Milliseconds()
		}
		if *paused {
			body["paused"] = true
		}
		if *dryRun {
			body["count"] = 5
			return c.printPreview(http.MethodPost, "/api/schedules/preview", nil, body)
		}
		return c.simple(http.MethodPost, "/api/schedules", body, "created schedule "+pos[0])
	}
}

func setupSchedulePause(fs *flag.FlagSet) func(*cli, []string) error {
	note := fs.String("note", "", "why it is paused (who, until when)")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<schedule-id>"); err != nil {
			return err
		}
		return c.simple(http.MethodPost, "/api/schedules/"+url.PathEscape(pos[0])+"/pause", map[string]any{"note": *note},
			"paused schedule "+pos[0])
	}
}

func setupScheduleResume(*flag.FlagSet) func(*cli, []string) error {
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<schedule-id>"); err != nil {
			return err
		}
		return c.simple(http.MethodPost, "/api/schedules/"+url.PathEscape(pos[0])+"/resume", nil, "resumed schedule "+pos[0])
	}
}

func setupScheduleBackfill(fs *flag.FlagSet) func(*cli, []string) error {
	from := fs.String("from", "", "start of the range, RFC 3339 (2026-10-01T00:00:00Z), or a duration ago (24h)")
	to := fs.String("to", "", "end of the range, likewise (default: now)")
	overlap := fs.String("overlap", "", "allow_all (the default) starts every fire; skip starts one only while no run is going")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<schedule-id>"); err != nil {
			return err
		}
		if *from == "" {
			return usagef("--from is required")
		}
		start, err := pointInTime(*from)
		if err != nil {
			return err
		}
		end := now()
		if *to != "" {
			if end, err = pointInTime(*to); err != nil {
				return err
			}
		}
		body := map[string]any{"start_time": start.UTC().Format(time.RFC3339Nano), "end_time": end.UTC().Format(time.RFC3339Nano)}
		if *overlap != "" {
			body["overlap_policy"] = *overlap
		}
		var raw []byte
		if err := c.request(http.MethodPost, "/api/schedules/"+url.PathEscape(pos[0])+"/backfill", nil, body, &raw); err != nil {
			return err
		}
		if c.json {
			c.printJSON(raw)
			return nil
		}
		var r struct{ Started, Existing, Skipped int }
		if err := jsonUnmarshal(raw, &r); err != nil {
			return err
		}
		fmt.Fprintf(c.out, "backfilled %s: %d started, %d already there, %d skipped\n", pos[0], r.Started, r.Existing, r.Skipped)
		return nil
	}
}

// pointInTime reads an RFC 3339 time, or a duration meaning that long ago.
func pointInTime(v string) (time.Time, error) {
	if d, err := time.ParseDuration(v); err == nil {
		return now().Add(-d), nil
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return time.Time{}, usagef("%q is neither an RFC 3339 time nor a duration", v)
	}
	return t, nil
}

func setupSchedulePreview(fs *flag.FlagSet) func(*cli, []string) error {
	count := fs.Int("count", 10, "how many fires")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<schedule-id>"); err != nil {
			return err
		}
		return c.printPreview(http.MethodGet, "/api/schedules/"+url.PathEscape(pos[0])+"/preview",
			url.Values{"count": {strconv.Itoa(*count)}}, nil)
	}
}

// printPreview shows a schedule's next fires and when each run starts.
func (c *cli) printPreview(method, path string, q url.Values, body any) error {
	var raw []byte
	if err := c.request(method, path, q, body, &raw); err != nil {
		return err
	}
	if c.json {
		c.printJSON(raw)
		return nil
	}
	var r struct {
		Fires []struct {
			FireAt  string `json:"fire_at"`
			StartAt string `json:"start_at"`
		} `json:"fires"`
		Paused bool `json:"paused"`
	}
	if err := jsonUnmarshal(raw, &r); err != nil {
		return err
	}
	if r.Paused {
		fmt.Fprintln(c.out, "paused: these fires happen only once it is resumed")
	}
	t := newTable(c.out)
	t.row("FIRE", "RUN STARTS")
	for _, f := range r.Fires {
		start := when(f.StartAt)
		if f.StartAt == f.FireAt {
			start = "at the fire"
		}
		t.row(when(f.FireAt), start)
	}
	return t.flush()
}

func setupScheduleDelete(*flag.FlagSet) func(*cli, []string) error {
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<schedule-id>"); err != nil {
			return err
		}
		return c.simple(http.MethodDelete, "/api/schedules/"+url.PathEscape(pos[0]), nil, "deleted schedule "+pos[0])
	}
}

func setupBackup(fs *flag.FlagSet) func(*cli, []string) error {
	dataDir := fs.String("data-dir", "", "back up this data directory directly instead of asking the engine")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<file.tar.gz | ->"); err != nil {
			return err
		}
		out, closeOut, err := c.openOutput(pos[0])
		if err != nil {
			return err
		}
		if *dataDir != "" {
			store, err := history.NewHistoryStore(*dataDir)
			if err == nil {
				err = store.Backup(out)
				store.Close()
			}
			if cerr := closeOut(err); err == nil {
				err = cerr
			}
			return err
		}
		c.http.Timeout = 0 // a large backup may take longer than one request's timeout
		counted := &countingWriter{w: out}
		err = c.request(http.MethodGet, "/api/admin/backup", nil, nil, io.Writer(counted))
		if cerr := closeOut(err); err == nil {
			err = cerr
		}
		if err == nil && pos[0] != "-" {
			fmt.Fprintf(c.errOut, "backed up to %s (%s)\n", pos[0], sizeOf(counted.n))
		}
		return err
	}
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

// openOutput opens a file to write (or standard output for "-"). close
// removes a file left half-written by an error.
func (c *cli) openOutput(name string) (io.Writer, func(error) error, error) {
	if name == "-" {
		return c.out, func(error) error { return nil }, nil
	}
	f, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, nil, err
	}
	return f, func(failed error) error {
		if failed != nil {
			f.Close()
			os.Remove(name)
			return nil
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
		return f.Close()
	}, nil
}

func setupRestore(fs *flag.FlagSet) func(*cli, []string) error {
	dataDir := fs.String("data-dir", "", "the new data directory to restore into (must not exist, or be empty)")
	return func(c *cli, pos []string) error {
		if err := wantArgs(pos, "<file.tar.gz | ->"); err != nil {
			return err
		}
		if *dataDir == "" {
			return usagef("--data-dir is required: a restore writes a data directory, with the engine stopped")
		}
		in := c.stdin
		if pos[0] != "-" {
			f, err := os.Open(pos[0])
			if err != nil {
				return err
			}
			defer f.Close()
			in = f
		}
		sum, err := history.Restore(*dataDir, in)
		if err != nil {
			return err
		}
		fmt.Fprintf(c.out, "restored %d histories into %s (history format %d); start workflowd with --data-dir %s\n",
			sum.Histories, *dataDir, sum.Format, *dataDir)
		return nil
	}
}
