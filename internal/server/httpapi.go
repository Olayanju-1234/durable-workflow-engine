package server

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/version"
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
//	GET  /api/workflows[?status=RUNNING&q=&limit=&offset=]
//	POST /api/workflows                      {"type","input","workflow_id"}
//	GET  /api/workflows/{id}
//	POST /api/workflows/{id}/terminate       {"reason"}
//	POST /api/workflows/{id}/signal          {"name","payload"}
type HTTPAPI struct {
	engine *Engine
	log    *slog.Logger
	// authToken guards every mutating /api route when set (Authorization:
	// Bearer <token>). Empty disables auth entirely — the pre-existing default.
	authToken string
	startedAt time.Time
}

func NewHTTPAPI(engine *Engine, logger *slog.Logger, opts ...HTTPOption) *HTTPAPI {
	a := &HTTPAPI{engine: engine, log: logger, startedAt: time.Now()}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// HTTPOption configures the API at construction.
type HTTPOption func(*HTTPAPI)

// WithAuthToken enables bearer-token auth on the JSON API. /api/health stays
// open (liveness probes cannot carry secrets); the dashboard's static assets
// stay open; every other /api route requires "Authorization: Bearer <token>".
func WithAuthToken(token string) HTTPOption {
	return func(a *HTTPAPI) { a.authToken = token }
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
	// Polyglot SDK transport (see httpapi_polyglot.go).
	a.registerPolyglotRoutes(mux)
	// Without this, unmatched /api paths would fall through to the dashboard's
	// catch-all and answer 200 with HTML, which confuses API clients.
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, fmt.Errorf("no API route for %s %s", r.Method, r.URL.Path))
	})
	return mux
}

// Wrap layers request logging and (when configured) bearer-token auth around a
// fully-assembled mux — call it after mounting the dashboard on Handler(), so
// the middleware sees the final routing table. Auth applies only to /api routes
// other than health; static assets stay public.
func (a *HTTPAPI) Wrap(h http.Handler) http.Handler {
	return a.logRequests(a.requireAuth(h))
}

// requireAuth enforces the bearer token on mutating API routes when one is
// configured. Comparison is constant-time; the header form is exact ("Bearer").
func (a *HTTPAPI) requireAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if a.authToken == "" || !strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/api/health" {
			next.ServeHTTP(w, r)
			return
		}
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(token), []byte(a.authToken)) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="workflowd"`)
			writeError(w, http.StatusUnauthorized, errors.New("missing or invalid bearer token"))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// statusWriter captures the response code for request logging.
type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

// logRequests emits one structured line per request: Info for mutations,
// Debug for reads, so a quiet GET-heavy dashboard doesn't flood the log.
func (a *HTTPAPI) logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, code: http.StatusOK}
		next.ServeHTTP(sw, r)
		lvl := slog.LevelDebug
		if r.Method != http.MethodGet {
			lvl = slog.LevelInfo
		}
		a.log.Log(r.Context(), lvl, "http request",
			"method", r.Method, "path", r.URL.Path, "status", sw.code,
			"duration_ms", time.Since(start).Milliseconds())
	})
}

// contextWithPollTimeout bounds a long-poll so it returns before proxies or
// clients give up on it. Cancelled when the caller disconnects, too.
func contextWithPollTimeout(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), pollTimeout)
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
	st := a.engine.Stats()
	writeJSON(w, http.StatusOK, map[string]any{
		"status":                    "ok",
		"version":                   version.Version,
		"uptime_seconds":            int64(time.Since(a.startedAt).Seconds()),
		"workflows_active":          st.ActiveWorkflows,
		"task_queue_depth":          st.TaskQueueDepth,
		"workflow_task_queue_depth": st.WorkflowTaskQueueDepth,
	})
}

// listWorkflows supports status filtering (engine-side), a case-insensitive
// substring query over workflow id and type, and offset/limit pagination.
// The response carries "total" so clients can render page counts.
func (a *HTTPAPI) listWorkflows(w http.ResponseWriter, r *http.Request) {
	filter, err := parseStatusFilter(r.URL.Query().Get("status"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	const (
		defaultLimit = 200
		maxLimit     = 1000
	)
	limit := defaultLimit
	if v := r.URL.Query().Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > maxLimit {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be an integer in [1,%d]", maxLimit))
			return
		}
	}
	offset := 0
	if v := r.URL.Query().Get("offset"); v != "" {
		if offset, err = strconv.Atoi(v); err != nil || offset < 0 {
			writeError(w, http.StatusBadRequest, errors.New("offset must be a non-negative integer"))
			return
		}
	}
	q := strings.ToLower(strings.TrimSpace(r.URL.Query().Get("q")))

	summaries, err := a.engine.ListWorkflows(filter)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	matched := make([]*summaryDTO, 0, len(summaries))
	for _, s := range summaries {
		if q != "" && !strings.Contains(strings.ToLower(s.GetWorkflowId()+" "+s.GetWorkflowType()), q) {
			continue
		}
		matched = append(matched, toSummaryDTO(s))
	}
	total := len(matched)
	if offset > total {
		offset = total
	}
	page := matched[offset:]
	if len(page) > limit {
		page = page[:limit]
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"workflows": page,
		"total":     total,
		"offset":    offset,
		"limit":     limit,
	})
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
	switch {
	case err == nil:
	case errors.Is(err, ErrUnknownType):
		// The caller must fix the request: the type is not registered here.
		writeError(w, http.StatusBadRequest, err)
		return
	case errors.Is(err, ErrAlreadyExists):
		// A supplied id collided with an existing workflow — a conflict, not a
		// malformed request; the caller picks a new id (or reuses the workflow).
		writeError(w, http.StatusConflict, err)
		return
	default:
		writeError(w, http.StatusInternalServerError, err)
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
		switch {
		case errors.Is(err, ErrInvalidArgument):
			writeError(w, http.StatusBadRequest, err)
		case errors.Is(err, ErrNotFound):
			writeError(w, http.StatusNotFound, err)
		case errors.Is(err, ErrAlreadyTerminal):
			writeError(w, http.StatusConflict, err)
		default:
			writeError(w, http.StatusInternalServerError, err)
		}
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
