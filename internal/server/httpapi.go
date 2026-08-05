package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"
)

// HTTPAPI exposes the visibility surface as JSON over HTTP. Browsers cannot
// speak raw gRPC, so the embedded dashboard (and any third-party UI or script)
// talks to this instead. It is hand-rolled on net/http + encoding/json rather
// than grpc-gateway, keeping the module's dependencies at gRPC + protobuf.
//
// The wire shapes below are deliberately hand-written DTOs, not the generated
// protobuf structs: the HTTP contract stays readable (statuses are strings like
// "RUNNING", timestamps are RFC3339) and stays stable if the proto is renamed.
//
// Routes (all under the mux returned by Handler):
//
//	GET  /api/health
//	GET  /api/workflows[?status=RUNNING]
//	POST /api/workflows                      {"type","input","workflow_id"}
//	GET  /api/workflows/{id}
//	POST /api/workflows/{id}/terminate       {"reason"}
//	POST /api/workflows/{id}/signal          {"name","payload"}
type HTTPAPI struct {
	engine *Engine
	log    *slog.Logger
}

func NewHTTPAPI(engine *Engine, logger *slog.Logger) *HTTPAPI {
	return &HTTPAPI{engine: engine, log: logger}
}

// Handler returns the API routes. Go 1.22+ method+wildcard patterns keep the
// routing declarative, so there is no hand-rolled path parsing.
func (a *HTTPAPI) Handler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", a.health)
	mux.HandleFunc("GET /api/workflows", a.listWorkflows)
	mux.HandleFunc("POST /api/workflows", a.startWorkflow)
	mux.HandleFunc("GET /api/workflows/{id}", a.getWorkflow)
	mux.HandleFunc("POST /api/workflows/{id}/terminate", a.terminateWorkflow)
	mux.HandleFunc("POST /api/workflows/{id}/signal", a.signalWorkflow)
	return mux
}

// summaryDTO is one workflow as the API presents it.
type summaryDTO struct {
	WorkflowID    string `json:"workflow_id"`
	WorkflowType  string `json:"workflow_type"`
	Status        string `json:"status"`
	StartedAt     string `json:"started_at,omitempty"`
	UpdatedAt     string `json:"updated_at,omitempty"`
	Error         string `json:"error,omitempty"`
	HistoryLength int64  `json:"history_length"`
}

type stepDTO struct {
	StepName   string `json:"step_name"`
	Sequence   int64  `json:"sequence"`
	Kind       string `json:"kind"`
	Status     string `json:"status"`
	Attempts   int32  `json:"attempts"`
	Error      string `json:"error,omitempty"`
	Output     string `json:"output,omitempty"`
	FireAt     string `json:"fire_at,omitempty"`
	SignalName string `json:"signal_name,omitempty"`
}

type workflowDTO struct {
	summaryDTO
	Input  string     `json:"input,omitempty"`
	Output string     `json:"output,omitempty"`
	Steps  []*stepDTO `json:"steps"`
}

func (a *HTTPAPI) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *HTTPAPI) listWorkflows(w http.ResponseWriter, r *http.Request) {
	filter, err := parseStatusFilter(r.URL.Query().Get("status"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	summaries, err := a.engine.ListWorkflows(filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]*summaryDTO, 0, len(summaries))
	for _, s := range summaries {
		out = append(out, toSummaryDTO(s))
	}
	writeJSON(w, http.StatusOK, map[string]any{"workflows": out})
}

func (a *HTTPAPI) getWorkflow(w http.ResponseWriter, r *http.Request) {
	resp, err := a.engine.GetWorkflow(r.PathValue("id"))
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err)
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	steps := make([]*stepDTO, 0, len(resp.GetSteps()))
	for _, s := range resp.GetSteps() {
		steps = append(steps, &stepDTO{
			StepName:   s.GetStepName(),
			Sequence:   s.GetSequence(),
			Kind:       s.GetKind(),
			Status:     s.GetStatus(),
			Attempts:   s.GetAttempts(),
			Error:      s.GetError(),
			Output:     string(s.GetOutput()),
			FireAt:     formatNanos(s.GetFireAt()),
			SignalName: s.GetSignalName(),
		})
	}
	writeJSON(w, http.StatusOK, &workflowDTO{
		summaryDTO: *toSummaryDTO(resp.GetSummary()),
		Input:      string(resp.GetInput()),
		Output:     string(resp.GetOutput()),
		Steps:      steps,
	})
}

func (a *HTTPAPI) startWorkflow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Type       string `json:"type"`
		Input      string `json:"input"`
		WorkflowID string `json:"workflow_id"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	id, err := a.engine.StartWorkflow(body.Type, []byte(body.Input), body.WorkflowID)
	if err != nil {
		// Unknown workflow type or duplicate id: the caller must fix the request.
		writeError(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"workflow_id": id})
}

func (a *HTTPAPI) terminateWorkflow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Reason string `json:"reason"`
	}
	// An empty body is a valid terminate (reason is optional).
	if r.ContentLength > 0 {
		if err := decodeJSON(r, &body); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
	}
	err := a.engine.TerminateWorkflow(r.PathValue("id"), body.Reason)
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, ErrAlreadyTerminal):
		writeError(w, http.StatusConflict, err)
	case err != nil:
		writeError(w, http.StatusInternalServerError, err)
	default:
		writeJSON(w, http.StatusOK, map[string]string{"status": "terminated"})
	}
}

func (a *HTTPAPI) signalWorkflow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    string `json:"name"`
		Payload string `json:"payload"`
	}
	if err := decodeJSON(r, &body); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if err := a.engine.SignalWorkflow(r.PathValue("id"), body.Name, []byte(body.Payload)); err != nil {
		// Unknown or terminal workflow.
		writeError(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "delivered"})
}

func toSummaryDTO(s *WorkflowSummary) *summaryDTO {
	return &summaryDTO{
		WorkflowID:    s.GetWorkflowId(),
		WorkflowType:  s.GetWorkflowType(),
		Status:        statusName(s.GetStatus()),
		StartedAt:     formatNanos(s.GetStartedAt()),
		UpdatedAt:     formatNanos(s.GetUpdatedAt()),
		Error:         s.GetError(),
		HistoryLength: s.GetHistoryLength(),
	}
}

// statusName renders the enum as the bare status ("RUNNING"), trimming the
// protobuf-required WORKFLOW_STATUS_ prefix. UNSPECIFIED means the history
// could not be replayed — surfaced as UNKNOWN rather than hidden.
func statusName(s WorkflowStatus) string {
	name := strings.TrimPrefix(s.String(), "WORKFLOW_STATUS_")
	if name == "UNSPECIFIED" {
		return "UNKNOWN"
	}
	return name
}

// parseStatusFilter accepts the bare status names statusName emits.
func parseStatusFilter(v string) (WorkflowStatus, error) {
	if v == "" {
		return WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED, nil
	}
	full := "WORKFLOW_STATUS_" + strings.ToUpper(v)
	code, ok := WorkflowStatus_value[full]
	if !ok || code == 0 {
		return 0, fmt.Errorf("unknown status filter %q", v)
	}
	return WorkflowStatus(code), nil
}

func formatNanos(ns int64) string {
	if ns == 0 {
		return ""
	}
	return time.Unix(0, ns).UTC().Format(time.RFC3339Nano)
}

func decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, 1<<20))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
