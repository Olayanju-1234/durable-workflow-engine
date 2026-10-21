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
// activity-tasks to receive only work it can do. Omitting the filter means any.
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
	WorkflowID    string       `json:"workflow_id"`
	WorkflowType  string       `json:"workflow_type"`
	TaskToken     string       `json:"task_token"`
	HistoryLength int64        `json:"history_length"`
	Input         string       `json:"input"`
	InputBase64   string       `json:"input_base64,omitempty"`
	Steps         []*stepDTO   `json:"steps"`
	Signals       []*signalDTO `json:"signals"`
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
}

func (a *HTTPAPI) registerWorkflowTypes(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowTypes []string `json:"workflow_types"`
		WorkerID      string   `json:"worker_id"`
		SDK           string   `json:"sdk"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	e := a.engine(w)
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

	e := a.engine(w)
	if e == nil {
		return
	}
	ctx, cancel := contextWithPollTimeout(r)
	defer cancel()

	task, err := e.PollWorkflowTask(ctx, workerID, types)
	if errors.Is(err, ErrNotLeader) {
		a.writeNotLeader(w, err)
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
		Input:         string(st.Input),
		InputBase64:   base64Twin(st.Input),
		Steps:         make([]*stepDTO, 0, len(st.Steps)),
		Signals:       make([]*signalDTO, 0, len(st.Signals)),
	}
	for _, s := range st.Steps {
		dto.Steps = append(dto.Steps, &stepDTO{
			StepName: s.Name, Sequence: s.Sequence,
			Kind: s.Kind.String(), Status: s.Status.String(),
			Attempts: int32(s.Attempts), Error: s.Error,
			Output: string(s.Output), OutputBase64: base64Twin(s.Output),
			FireAt: formatNanos(s.FireAt), SignalName: s.Signal, Marker: s.Marker,
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
	// record_marker: "version" or "side_effect", and the recorded value.
	Marker     string  `json:"marker,omitempty"`
	Data       string  `json:"data,omitempty"`
	DataBase64 *string `json:"data_base64,omitempty"`
}

type retryPolicyDTO struct {
	MaxAttempts      int32 `json:"max_attempts"`
	InitialBackoffMs int64 `json:"initial_backoff_ms"`
	MaxBackoffMs     int64 `json:"max_backoff_ms"`
}

func (a *HTTPAPI) completeWorkflowTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowID  string      `json:"workflow_id"`
		TaskToken   string      `json:"task_token"`
		Command     *commandDTO `json:"command"`
		ReplayError string      `json:"replay_error"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	cmd, err := commandFromDTO(body.Command)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	e := a.engine(w)
	if e == nil {
		return
	}
	err = e.CompleteWorkflowTask(body.WorkflowID, body.TaskToken, cmd, body.ReplayError)
	switch {
	case errors.Is(err, ErrNotLeader):
		a.writeNotLeader(w, err)
	case errors.Is(err, ErrStaleDecision):
		// 409: the SDK should discard this decision and poll again. Routine
		// under re-dispatch, so it is not an error the SDK should log loudly.
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, ErrInvalidCommand):
		writeError(w, http.StatusBadRequest, err)
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
			}
		}
		return &WorkflowCommand{Command: &WorkflowCommand_ScheduleActivity{
			ScheduleActivity: &ScheduleActivityCommand{
				StepName: c.StepName, Sequence: c.Sequence, Input: input, RetryPolicy: retry,
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
			FailWorkflow: &FailWorkflowCommand{Error: c.Error},
		}}, nil
	default:
		return nil, fmt.Errorf("unknown command type %q", c.Type)
	}
}

func (a *HTTPAPI) pollActivityTask(w http.ResponseWriter, r *http.Request) {
	e := a.engine(w)
	if e == nil {
		return
	}
	ctx, cancel := contextWithPollTimeout(r)
	defer cancel()

	task, err := e.PollTask(ctx, r.URL.Query().Get("worker_id"),
		splitCSV(r.URL.Query().Get("activities"))...)
	if errors.Is(err, ErrNotLeader) {
		a.writeNotLeader(w, err)
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
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
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

	e := a.engine(w)
	if e == nil {
		return
	}
	if err := e.CompleteTask(body.WorkflowID, body.StepName, body.Sequence,
		body.LeaseToken, output, body.Error, body.Error != ""); err != nil {
		if errors.Is(err, ErrNotLeader) {
			a.writeNotLeader(w, err)
			return
		}
		// Lost lease, or a result that doesn't match the in-flight step.
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "accepted"})
}

// heartbeatActivityTask extends an activity's lease. 409 means the lease is
// gone (the task was reassigned or the workflow ended): stop the work.
func (a *HTTPAPI) heartbeatActivityTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkflowID string `json:"workflow_id"`
		LeaseToken string `json:"lease_token"`
		WorkerID   string `json:"worker_id"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	e := a.engine(w)
	if e == nil {
		return
	}
	if err := e.Heartbeat(body.WorkflowID, body.LeaseToken); err != nil {
		if errors.Is(err, ErrNotLeader) {
			a.writeNotLeader(w, err)
			return
		}
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "extended"})
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
