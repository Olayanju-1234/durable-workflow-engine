package server

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// HTTP transport for polyglot SDKs.
//
// The gRPC service is the full-fidelity protocol: a WorkflowTask carries raw
// protobuf history and the SDK reconstructs state itself. That is the right
// choice for a language with good protobuf tooling, but it makes a "hello
// world" SDK expensive — every language would have to re-implement the history
// fold before running a single workflow.
//
// So the HTTP transport ships the ALREADY-RECONSTRUCTED view instead: the
// workflow's input, its steps in order with their results, and any buffered
// signals. That is precisely what replay needs, so an SDK is reduced to "walk
// my workflow function against this list". The result is that a complete SDK
// fits in ~200 lines with NO dependencies — Node's fetch, Python's urllib and
// Java's HttpClient are all built in.
//
// Payloads are bytes. Each one travels as a JSON string, plus a "<name>_base64"
// twin when the bytes are not valid UTF-8 (a JSON string can't carry those
// exactly). Requests may send either form; the base64 one wins when present.
//
// Routing: a worker passes ?types=A,B on workflow-tasks and ?activities=x,y on
// activity-tasks to receive only work it can do. Omitting the filter means any
// on its task queue (?task_queue=, default "default").
//
// Long-polling: poll endpoints block up to pollTimeout, then return 204 so the
// client loops. Standard long-poll, and it keeps a dead connection from looking
// like a stalled workflow.

// pollTimeout bounds a long-poll. Comfortably under typical 60s proxy idle
// timeouts, so an intermediary never severs a legitimately waiting request.
const pollTimeout = 25 * time.Second

func (a *HTTPAPI) registerPolyglotRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/workflow-types", a.registerWorkflowTypes)
	mux.HandleFunc("GET /api/workflow-tasks", a.pollWorkflowTask)
	mux.HandleFunc("POST /api/workflow-tasks/complete", a.completeWorkflowTask)
	mux.HandleFunc("GET /api/activity-tasks", a.pollActivityTask)
	mux.HandleFunc("POST /api/activity-tasks/complete", a.completeActivityTask)
	mux.HandleFunc("POST /api/activity-tasks/heartbeat", a.heartbeatActivityTask)
}

// workflowTaskDTO is the replay input an SDK receives: everything needed to
// decide what happens next, and nothing else.
type workflowTaskDTO struct {
	WorkflowID    string `json:"workflow_id"`
	WorkflowType  string `json:"workflow_type"`
	TaskToken     string `json:"task_token"`
	HistoryLength int64  `json:"history_length"`
	// When the workflow started (RFC 3339): the workflow clock's first reading.
	StartedAt string `json:"started_at,omitempty"`
	RunID     string `json:"run_id,omitempty"`
	TaskQueue string `json:"task_queue,omitempty"`
	// The event id at which the history crossed the limits' warning share
	// (absent: not yet). Code that has seen a result at or after it should
	// continue as new; it is an event id, so every replay agrees.
	SuggestContinueAsNewAt int64 `json:"suggest_continue_as_new_at,omitempty"`
	HistorySizeBytes       int64 `json:"history_size_bytes,omitempty"`
	// A graceful cancel request: the event that recorded it, when, and why.
	CancelRequestedEventID int64        `json:"cancel_requested_event_id,omitempty"`
	CancelRequestedAt      string       `json:"cancel_requested_at,omitempty"`
	CancelReason           string       `json:"cancel_reason,omitempty"`
	Input                  string       `json:"input"`
	InputBase64            string       `json:"input_base64,omitempty"`
	Steps                  []*stepDTO   `json:"steps"`
	Signals                []*signalDTO `json:"signals"`
	// The run's span as a W3C traceparent (and tracestate), absent when the
	// run is not traced. The same on every task of the run.
	Traceparent string `json:"traceparent,omitempty"`
	Tracestate  string `json:"tracestate,omitempty"`
}

type signalDTO struct {
	Name          string `json:"name"`
	Payload       string `json:"payload"`
	PayloadBase64 string `json:"payload_base64,omitempty"`
}

// activityTaskDTO is one unit of real work for a polyglot activity worker.
type activityTaskDTO struct {
	WorkflowID      string `json:"workflow_id"`
	StepName        string `json:"step_name"`
	Sequence        int64  `json:"sequence"`
	Input           string `json:"input"`
	InputBase64     string `json:"input_base64,omitempty"`
	LeaseToken      string `json:"lease_token"`
	LeaseDurationMs int64  `json:"lease_duration_ms"`
	// The engine fails the attempt this long after dispatch (absent: no limit).
	StartToCloseTimeoutMs int64 `json:"start_to_close_timeout_ms,omitempty"`
	// Which attempt this is (from 1), and "<workflow_id>/<sequence>", the same
	// for every attempt: an idempotency key for the activity's side effect.
	Attempt    int32  `json:"attempt"`
	ActivityID string `json:"activity_id"`
	TaskQueue  string `json:"task_queue,omitempty"`
	// This attempt's span as a W3C traceparent (and tracestate), a child of the
	// run's; absent when the run is not traced.
	Traceparent string `json:"traceparent,omitempty"`
	Tracestate  string `json:"tracestate,omitempty"`
}

func (a *HTTPAPI) registerWorkflowTypes(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowTypes []string `json:"workflow_types"`
		WorkerID      string   `json:"worker_id"`
		SDK           string   `json:"sdk"`
	}
	if err := a.decodeJSON(r, &body); err != nil {
		writeError(w, decodeStatus(err), err)
		return
	}
	e := a.scope(w, r)
	if e == nil {
		return
	}
	if err := e.RegisterWorkflowTypes(body.WorkflowTypes, body.WorkerID, body.SDK); err != nil {
		if errors.Is(err, ErrNotLeader) {
			a.writeNotLeader(w, err)
			return
		}
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

func (a *HTTPAPI) pollWorkflowTask(w http.ResponseWriter, r *http.Request) {
	types := splitCSV(r.URL.Query().Get("types"))
	workerID := r.URL.Query().Get("worker_id")

	e := a.scope(w, r)
	if e == nil {
		return
	}
	ctx, cancel := contextWithPollTimeout(r)
	defer cancel()

	task, err := e.PollWorkflowTask(ctx, workerID, r.URL.Query().Get("task_queue"), types)
	if errors.Is(err, ErrNotLeader) {
		a.writeNotLeader(w, err)
		return
	}
	if errors.Is(err, ErrInvalidArgument) {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusNoContent) // long-poll expired; client re-polls
		return
	}

	// Reconstruct here so every SDK doesn't have to. A history that cannot be
	// replayed is the engine's problem to report, not the SDK's to puzzle over.
	st, err := state.Reconstruct(task.GetHistory())
	if err != nil {
		writeError(w, http.StatusInternalServerError,
			fmt.Errorf("history for %s is unreadable: %w", task.GetWorkflowId(), err))
		return
	}

	dto := &workflowTaskDTO{
		WorkflowID:    task.GetWorkflowId(),
		WorkflowType:  task.GetWorkflowType(),
		TaskToken:     task.GetTaskToken(),
		HistoryLength: task.GetHistoryLength(),
		StartedAt:     formatNanos(st.StartedAt),
		RunID:         task.GetRunId(),
		TaskQueue:     task.GetTaskQueue(),

		SuggestContinueAsNewAt: task.GetSuggestContinueAsNewAt(),
		HistorySizeBytes:       task.GetHistorySizeBytes(),

		CancelRequestedEventID: st.CancelRequestedEventID,
		CancelRequestedAt:      formatNanos(st.CancelRequestedAt),
		CancelReason:           st.CancelReason,
		Input:                  string(st.Input),
		InputBase64:            base64Twin(st.Input),
		Steps:                  make([]*stepDTO, 0, len(st.Steps)),
		Signals:                make([]*signalDTO, 0, len(st.Signals)),
		Traceparent:            task.GetTraceparent(),
		Tracestate:             task.GetTracestate(),
	}
	for _, s := range st.Steps {
		status := s.Status.String()
		if s.Retrying {
			status = state.StepScheduled.String() // not a result yet: replay must wait on it
		}
		dto.Steps = append(dto.Steps, &stepDTO{
			StepName: s.Name, Sequence: s.Sequence,
			Kind: s.Kind.String(), Status: status,
			Attempts: int32(s.Attempts), Error: s.Error,
			ErrorType: s.ErrorType, NonRetryable: s.NonRetryable,
			Output: string(s.Output), OutputBase64: base64Twin(s.Output),
			FireAt: formatNanos(s.FireAt), SignalName: s.Signal, Marker: s.Marker,
			ResolvedEventID: s.ResolvedEventID, ResolvedAt: formatNanos(s.ResolvedAt),
		})
	}
	for _, sig := range st.Signals {
		dto.Signals = append(dto.Signals, &signalDTO{
			Name: sig.Name, Payload: string(sig.Payload), PayloadBase64: base64Twin(sig.Payload),
		})
	}
	writeJSON(w, http.StatusOK, dto)
}

// commandDTO is the SDK's answer. A flat tagged union keeps it easy to build in
// any language — no nested oneof wrappers to model.
type commandDTO struct {
	Type         string          `json:"type"`
	StepName     string          `json:"step_name,omitempty"`
	Sequence     int64           `json:"sequence,omitempty"`
	Input        string          `json:"input,omitempty"`
	InputBase64  *string         `json:"input_base64,omitempty"`
	DelayMs      int64           `json:"delay_ms,omitempty"`
	SignalName   string          `json:"signal_name,omitempty"`
	Output       string          `json:"output,omitempty"`
	OutputBase64 *string         `json:"output_base64,omitempty"`
	Error        string          `json:"error,omitempty"`
	RetryPolicy  *retryPolicyDTO `json:"retry_policy,omitempty"`
	// schedule_activity: zero or absent means no limit.
	StartToCloseTimeoutMs    int64 `json:"start_to_close_timeout_ms,omitempty"`
	ScheduleToCloseTimeoutMs int64 `json:"schedule_to_close_timeout_ms,omitempty"`
	// cancel_step: why the code no longer wants the step (optional).
	Reason string `json:"reason,omitempty"`
	// fail_workflow: the code let a graceful cancellation end the workflow.
	Cancelled bool `json:"cancelled,omitempty"`
	// continue_as_new: the next run's type and execution timeout (absent:
	// this run's); its input travels in input.
	WorkflowType       string `json:"workflow_type,omitempty"`
	ExecutionTimeoutMs int64  `json:"execution_timeout_ms,omitempty"`
	// schedule_activity: the task queue to run it on (absent: the
	// workflow's); continue_as_new: the next run's (absent: this run's).
	TaskQueue string `json:"task_queue,omitempty"`
	// record_marker: "version" or "side_effect", and the recorded value.
	Marker     string  `json:"marker,omitempty"`
	Data       string  `json:"data,omitempty"`
	DataBase64 *string `json:"data_base64,omitempty"`
}

type retryPolicyDTO struct {
	MaxAttempts            int32    `json:"max_attempts"`
	InitialBackoffMs       int64    `json:"initial_backoff_ms"`
	MaxBackoffMs           int64    `json:"max_backoff_ms"`
	NonRetryableErrorTypes []string `json:"non_retryable_error_types,omitempty"`
}

func (a *HTTPAPI) completeWorkflowTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowID string      `json:"workflow_id"`
		TaskToken  string      `json:"task_token"`
		Command    *commandDTO `json:"command"`
		// Several commands at once (parallel steps); use this or command.
		Commands    []*commandDTO `json:"commands"`
		ReplayError string        `json:"replay_error"`
	}
	if err := a.decodeJSON(r, &body); err != nil {
		writeError(w, decodeStatus(err), err)
		return
	}
	dtos := body.Commands
	if body.Command != nil {
		if len(dtos) > 0 {
			writeError(w, http.StatusBadRequest, errors.New("set command or commands, not both"))
			return
		}
		dtos = []*commandDTO{body.Command}
	}
	var cmds []*WorkflowCommand
	for _, dto := range dtos {
		cmd, err := commandFromDTO(dto)
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if cmd != nil {
			cmds = append(cmds, cmd)
		}
	}

	e := a.scope(w, r)
	if e == nil {
		return
	}
	err := e.CompleteWorkflowTask(body.WorkflowID, body.TaskToken, cmds, body.ReplayError)
	switch {
	case errors.Is(err, ErrNotLeader):
		a.writeNotLeader(w, err)
	case errors.Is(err, ErrStaleDecision):
		// 409: the SDK should discard this decision and poll again. Routine
		// under re-dispatch, so it is not an error the SDK should log loudly.
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, ErrInvalidCommand):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, ErrHistoryLimit):
		// The workflow ended at its history limit: nothing more to decide.
		writeError(w, http.StatusConflict, err)
	case err != nil:
		writeError(w, http.StatusInternalServerError, err)
	default:
		writeJSON(w, http.StatusOK, map[string]string{"status": "accepted"})
	}
}

// commandFromDTO converts the SDK's tagged union into the protobuf command.
// A nil command or "noop" means "the workflow is blocked; nothing to do".
func commandFromDTO(c *commandDTO) (*WorkflowCommand, error) {
	if c == nil || c.Type == "" || c.Type == "noop" {
		return nil, nil
	}
	input, err := payloadFrom(c.Input, c.InputBase64, "input_base64")
	if err != nil {
		return nil, err
	}
	switch c.Type {
	case "schedule_activity":
		var retry *history.RetryPolicy
		if p := c.RetryPolicy; p != nil {
			retry = &history.RetryPolicy{
				MaxAttempts: p.MaxAttempts, InitialBackoffMs: p.InitialBackoffMs, MaxBackoffMs: p.MaxBackoffMs,
				NonRetryableErrorTypes: p.NonRetryableErrorTypes,
			}
		}
		return &WorkflowCommand{Command: &WorkflowCommand_ScheduleActivity{
			ScheduleActivity: &ScheduleActivityCommand{
				StepName: c.StepName, Sequence: c.Sequence, Input: input, RetryPolicy: retry,
				StartToCloseTimeoutMs:    c.StartToCloseTimeoutMs,
				ScheduleToCloseTimeoutMs: c.ScheduleToCloseTimeoutMs,
				TaskQueue:                c.TaskQueue,
			}}}, nil
	case "start_timer":
		return &WorkflowCommand{Command: &WorkflowCommand_StartTimer{
			StartTimer: &StartTimerCommand{
				StepName: c.StepName, Sequence: c.Sequence,
				DelayMs: c.DelayMs, Input: input,
			}}}, nil
	case "await_signal":
		return &WorkflowCommand{Command: &WorkflowCommand_AwaitSignal{
			AwaitSignal: &AwaitSignalCommand{
				StepName: c.StepName, Sequence: c.Sequence,
				SignalName: c.SignalName, Input: input,
			}}}, nil
	case "record_marker":
		data, err := payloadFrom(c.Data, c.DataBase64, "data_base64")
		if err != nil {
			return nil, err
		}
		return &WorkflowCommand{Command: &WorkflowCommand_RecordMarker{
			RecordMarker: &RecordMarkerCommand{
				StepName: c.StepName, Sequence: c.Sequence, Marker: c.Marker, Data: data,
			}}}, nil
	case "complete_workflow":
		output, err := payloadFrom(c.Output, c.OutputBase64, "output_base64")
		if err != nil {
			return nil, err
		}
		return &WorkflowCommand{Command: &WorkflowCommand_CompleteWorkflow{
			CompleteWorkflow: &CompleteWorkflowCommand{Output: output},
		}}, nil
	case "fail_workflow":
		return &WorkflowCommand{Command: &WorkflowCommand_FailWorkflow{
			FailWorkflow: &FailWorkflowCommand{Error: c.Error, Cancelled: c.Cancelled},
		}}, nil
	case "cancel_step":
		return &WorkflowCommand{Command: &WorkflowCommand_CancelStep{
			CancelStep: &CancelStepCommand{StepName: c.StepName, Sequence: c.Sequence, Reason: c.Reason},
		}}, nil
	case "continue_as_new":
		return &WorkflowCommand{Command: &WorkflowCommand_ContinueAsNew{
			ContinueAsNew: &ContinueAsNewCommand{Input: input, WorkflowType: c.WorkflowType, ExecutionTimeoutMs: c.ExecutionTimeoutMs,
				TaskQueue: c.TaskQueue},
		}}, nil
	default:
		return nil, fmt.Errorf("unknown command type %q", c.Type)
	}
}

func (a *HTTPAPI) pollActivityTask(w http.ResponseWriter, r *http.Request) {
	e := a.scope(w, r)
	if e == nil {
		return
	}
	ctx, cancel := contextWithPollTimeout(r)
	defer cancel()

	task, err := e.PollTask(ctx, r.URL.Query().Get("worker_id"), r.URL.Query().Get("task_queue"),
		splitCSV(r.URL.Query().Get("activities"))...)
	if errors.Is(err, ErrNotLeader) {
		a.writeNotLeader(w, err)
		return
	}
	if errors.Is(err, ErrInvalidArgument) {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, &activityTaskDTO{
		WorkflowID:      task.GetWorkflowId(),
		StepName:        task.GetStepName(),
		Sequence:        task.GetSequence(),
		Input:           string(task.GetInput()),
		InputBase64:     base64Twin(task.GetInput()),
		LeaseToken:      task.GetLeaseToken(),
		LeaseDurationMs: task.GetLeaseDurationMs(),

		StartToCloseTimeoutMs: task.GetStartToCloseTimeoutMs(),
		Attempt:               task.GetAttempt(),
		ActivityID:            task.GetActivityId(),
		TaskQueue:             task.GetTaskQueue(),
		Traceparent:           task.GetTraceparent(),
		Tracestate:            task.GetTracestate(),
	})
}

func (a *HTTPAPI) completeActivityTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowID string `json:"workflow_id"`
		StepName   string `json:"step_name"`
		Sequence   int64  `json:"sequence"`
		LeaseToken string `json:"lease_token"`
		Output     string `json:"output"`
		Error      string `json:"error"`
		// Set for binary activity results; takes precedence over Output.
		OutputBase64 string `json:"output_base64"`
		// With an error: its kind (matched against the retry policy's
		// non_retryable_error_types), and whether to skip retries outright.
		ErrorType    string `json:"error_type"`
		NonRetryable bool   `json:"non_retryable"`
	}
	if err := a.decodeJSON(r, &body); err != nil {
		writeError(w, decodeStatus(err), err)
		return
	}

	output := []byte(body.Output)
	if body.OutputBase64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(body.OutputBase64)
		if err != nil {
			writeError(w, http.StatusBadRequest, fmt.Errorf("output_base64: %w", err))
			return
		}
		output = decoded
	}

	e := a.scope(w, r)
	if e == nil {
		return
	}
	var err error
	if body.Error != "" {
		err = e.FailTask(body.WorkflowID, body.StepName, body.Sequence, body.LeaseToken, TaskFailure{
			Message: body.Error, Type: body.ErrorType, NonRetryable: body.NonRetryable,
		})
	} else {
		err = e.CompleteTask(body.WorkflowID, body.StepName, body.Sequence, body.LeaseToken, output)
	}
	if err != nil {
		if errors.Is(err, ErrNotLeader) {
			a.writeNotLeader(w, err)
			return
		}
		// Lost lease, a cancelled step, or a result that doesn't match the in-flight step.
		writeLeaseConflict(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "accepted"})
}

// heartbeatActivityTask extends an activity's lease. 409 means the lease is
// gone: stop the work. "cancelled": true says the work is no longer wanted at
// all (the step was cancelled or the workflow ended), not just reassigned.
func (a *HTTPAPI) heartbeatActivityTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowID string `json:"workflow_id"`
		LeaseToken string `json:"lease_token"`
		WorkerID   string `json:"worker_id"`
	}
	if err := a.decodeJSON(r, &body); err != nil {
		writeError(w, decodeStatus(err), err)
		return
	}
	e := a.scope(w, r)
	if e == nil {
		return
	}
	if err := e.Heartbeat(body.WorkflowID, body.LeaseToken); err != nil {
		if errors.Is(err, ErrNotLeader) {
			a.writeNotLeader(w, err)
			return
		}
		writeLeaseConflict(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "extended"})
}

// writeLeaseConflict answers 409 for a worker that must stop, saying whether
// the work was cancelled. SDKs that predate "cancelled" still stop on the 409.
func writeLeaseConflict(w http.ResponseWriter, err error) {
	writeJSON(w, http.StatusConflict, map[string]any{
		"error":     err.Error(),
		"cancelled": errors.Is(err, ErrActivityCancelled),
	})
}

// base64Twin is a payload's base64 form when its bytes are not valid UTF-8,
// so they survive JSON exactly; empty otherwise (the string form is exact).
func base64Twin(b []byte) string {
	if utf8.Valid(b) {
		return ""
	}
	return base64.StdEncoding.EncodeToString(b)
}

// payloadFrom reads a request payload: the base64 form when sent, else the
// string form's UTF-8 bytes.
func payloadFrom(text string, b64 *string, field string) ([]byte, error) {
	if b64 == nil {
		return []byte(text), nil
	}
	b, err := base64.StdEncoding.DecodeString(*b64)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", field, err)
	}
	return b, nil
}

func splitCSV(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
