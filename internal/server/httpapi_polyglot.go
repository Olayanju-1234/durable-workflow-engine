package server

import (
	"encoding/base64"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

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
}

// workflowTaskDTO is the replay input an SDK receives: everything needed to
// decide what happens next, and nothing else.
type workflowTaskDTO struct {
	WorkflowID    string       `json:"workflow_id"`
	WorkflowType  string       `json:"workflow_type"`
	TaskToken     string       `json:"task_token"`
	HistoryLength int64        `json:"history_length"`
	Input         string       `json:"input"`
	Steps         []*stepDTO   `json:"steps"`
	Signals       []*signalDTO `json:"signals"`
}

type signalDTO struct {
	Name    string `json:"name"`
	Payload string `json:"payload"`
}

// activityTaskDTO is one unit of real work for a polyglot activity worker.
type activityTaskDTO struct {
	WorkflowID      string `json:"workflow_id"`
	StepName        string `json:"step_name"`
	Sequence        int64  `json:"sequence"`
	Input           string `json:"input"`
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
	if err := a.engine.RegisterWorkflowTypes(body.WorkflowTypes, body.WorkerID, body.SDK); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

func (a *HTTPAPI) pollWorkflowTask(w http.ResponseWriter, r *http.Request) {
	types := splitCSV(r.URL.Query().Get("types"))
	workerID := r.URL.Query().Get("worker_id")

	ctx, cancel := contextWithPollTimeout(r)
	defer cancel()

	task, err := a.engine.PollWorkflowTask(ctx, workerID, types)
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
		Steps:         make([]*stepDTO, 0, len(st.Steps)),
		Signals:       make([]*signalDTO, 0, len(st.Signals)),
	}
	for _, s := range st.Steps {
		dto.Steps = append(dto.Steps, &stepDTO{
			StepName: s.Name, Sequence: s.Sequence,
			Kind: s.Kind.String(), Status: s.Status.String(),
			Attempts: int32(s.Attempts), Error: s.Error,
			Output: string(s.Output), FireAt: formatNanos(s.FireAt),
			SignalName: s.Signal,
		})
	}
	for _, sig := range st.Signals {
		dto.Signals = append(dto.Signals, &signalDTO{Name: sig.Name, Payload: string(sig.Payload)})
	}
	writeJSON(w, http.StatusOK, dto)
}

// commandDTO is the SDK's answer. A flat tagged union keeps it easy to build in
// any language — no nested oneof wrappers to model.
type commandDTO struct {
	Type       string `json:"type"`
	StepName   string `json:"step_name,omitempty"`
	Sequence   int64  `json:"sequence,omitempty"`
	Input      string `json:"input,omitempty"`
	DelayMs    int64  `json:"delay_ms,omitempty"`
	SignalName string `json:"signal_name,omitempty"`
	Output     string `json:"output,omitempty"`
	Error      string `json:"error,omitempty"`
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

	err = a.engine.CompleteWorkflowTask(body.WorkflowID, body.TaskToken, cmd, body.ReplayError)
	switch {
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
	switch c.Type {
	case "schedule_activity":
		return &WorkflowCommand{Command: &WorkflowCommand_ScheduleActivity{
			ScheduleActivity: &ScheduleActivityCommand{
				StepName: c.StepName, Sequence: c.Sequence, Input: []byte(c.Input),
			}}}, nil
	case "start_timer":
		return &WorkflowCommand{Command: &WorkflowCommand_StartTimer{
			StartTimer: &StartTimerCommand{
				StepName: c.StepName, Sequence: c.Sequence,
				DelayMs: c.DelayMs, Input: []byte(c.Input),
			}}}, nil
	case "await_signal":
		return &WorkflowCommand{Command: &WorkflowCommand_AwaitSignal{
			AwaitSignal: &AwaitSignalCommand{
				StepName: c.StepName, Sequence: c.Sequence,
				SignalName: c.SignalName, Input: []byte(c.Input),
			}}}, nil
	case "complete_workflow":
		return &WorkflowCommand{Command: &WorkflowCommand_CompleteWorkflow{
			CompleteWorkflow: &CompleteWorkflowCommand{Output: []byte(c.Output)},
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
	ctx, cancel := contextWithPollTimeout(r)
	defer cancel()

	task, err := a.engine.PollTask(ctx, r.URL.Query().Get("worker_id"),
		splitCSV(r.URL.Query().Get("activities"))...)
	if err != nil {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	writeJSON(w, http.StatusOK, &activityTaskDTO{
		WorkflowID:      task.GetWorkflowId(),
		StepName:        task.GetStepName(),
		Sequence:        task.GetSequence(),
		Input:           string(task.GetInput()),
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

	if err := a.engine.CompleteTask(body.WorkflowID, body.StepName, body.Sequence,
		body.LeaseToken, output, body.Error, body.Error != ""); err != nil {
		// Lost lease, or a result that doesn't match the in-flight step.
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "accepted"})
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
