package server

import (
	"errors"
	"net/http"
	"strconv"
)

// Archive routes (ADR-0019), in the caller's namespace:
//
//	GET /api/archive/workflows[?prefix=&limit=]          archived runs
//	GET /api/archive/workflows/{id}[?run_id=]            an archived run, described
//	GET /api/archive/workflows/{id}/history[?run_id=]    its events, as recorded
func (a *HTTPAPI) registerArchiveRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/archive/workflows", a.listArchived)
	mux.HandleFunc("GET /api/archive/workflows/{id}", a.getArchived)
	mux.HandleFunc("GET /api/archive/workflows/{id}/history", a.archivedHistory)
}

type archivedRunDTO struct {
	WorkflowID string `json:"workflow_id"`
	RunID      string `json:"run_id"`
	ClosedAt   string `json:"closed_at"`
}

func (a *HTTPAPI) listArchived(w http.ResponseWriter, r *http.Request) {
	e := a.scope(w, r)
	if e == nil {
		return
	}
	limit := 200
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 10000 {
			writeError(w, http.StatusBadRequest, errors.New("limit must be an integer in [1,10000]"))
			return
		}
		limit = n
	}
	runs, err := e.ArchivedRuns(r.Context(), r.URL.Query().Get("prefix"), limit)
	if err != nil {
		writeArchiveError(w, err)
		return
	}
	out := make([]archivedRunDTO, 0, len(runs))
	for _, run := range runs {
		out = append(out, archivedRunDTO{WorkflowID: run.WorkflowID, RunID: run.RunID, ClosedAt: formatNanos(run.ClosedAt)})
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": out})
}

func (a *HTTPAPI) getArchived(w http.ResponseWriter, r *http.Request) {
	e := a.scope(w, r)
	if e == nil {
		return
	}
	resp, err := e.DescribeArchived(r.Context(), r.PathValue("id"), r.URL.Query().Get("run_id"))
	if err != nil {
		writeArchiveError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, toWorkflowDTO(resp))
}

func (a *HTTPAPI) archivedHistory(w http.ResponseWriter, r *http.Request) {
	e := a.scope(w, r)
	if e == nil {
		return
	}
	run, events, err := e.ArchivedHistory(r.Context(), r.PathValue("id"), r.URL.Query().Get("run_id"))
	if err != nil {
		writeArchiveError(w, err)
		return
	}
	writeEvents(w, run.WorkflowID, run.RunID, events)
}

func writeArchiveError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrArchiveOff):
		writeError(w, http.StatusNotImplemented, err)
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, err)
	default:
		writeError(w, http.StatusBadGateway, err)
	}
}
