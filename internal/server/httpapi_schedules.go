package server

import (
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// Schedule routes:
//
//	GET    /api/schedules
//	POST   /api/schedules             {"schedule_id","cron","timezone","workflow_type","input","task_queue",
//	                                    "overlap_policy","jitter_ms","paused","note"}
//	DELETE /api/schedules/{id}
//	POST   /api/schedules/{id}/pause     {"note"}
//	POST   /api/schedules/{id}/resume
//	POST   /api/schedules/{id}/backfill  {"start_time","end_time","overlap_policy"}   RFC 3339 times
//	GET    /api/schedules/{id}/preview[?count=]
//	POST   /api/schedules/preview        {a schedule, "count"}: a spec not yet created
func (a *HTTPAPI) registerScheduleRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/schedules", a.listSchedules)
	mux.HandleFunc("POST /api/schedules", a.createSchedule)
	mux.HandleFunc("DELETE /api/schedules/{id}", a.deleteSchedule)
	mux.HandleFunc("POST /api/schedules/{id}/pause", a.pauseSchedule)
	mux.HandleFunc("POST /api/schedules/{id}/resume", a.resumeSchedule)
	mux.HandleFunc("POST /api/schedules/{id}/backfill", a.backfillSchedule)
	mux.HandleFunc("GET /api/schedules/{id}/preview", a.previewSchedule)
	mux.HandleFunc("POST /api/schedules/preview", a.previewSpec)
}

type scheduleDTO struct {
	ScheduleID     string `json:"schedule_id"`
	Cron           string `json:"cron"`
	Timezone       string `json:"timezone,omitempty"`
	WorkflowType   string `json:"workflow_type"`
	Input          string `json:"input,omitempty"`
	InputBase64    string `json:"input_base64,omitempty"`
	SkipIfRunning  bool   `json:"skip_if_running,omitempty"`
	TaskQueue      string `json:"task_queue,omitempty"`
	OverlapPolicy  string `json:"overlap_policy"`
	JitterMs       int64  `json:"jitter_ms,omitempty"`
	Paused         bool   `json:"paused,omitempty"`
	Note           string `json:"note,omitempty"`
	CreatedAt      string `json:"created_at,omitempty"`
	NextFireAt     string `json:"next_fire_at,omitempty"`
	BufferedFireAt string `json:"buffered_fire_at,omitempty"`
}

// scheduleBody is a schedule as a request sends it.
type scheduleBody struct {
	ScheduleID    string  `json:"schedule_id"`
	Cron          string  `json:"cron"`
	Timezone      string  `json:"timezone"`
	WorkflowType  string  `json:"workflow_type"`
	Input         string  `json:"input"`
	InputBase64   *string `json:"input_base64"`
	SkipIfRunning bool    `json:"skip_if_running"`
	TaskQueue     string  `json:"task_queue"`
	OverlapPolicy string  `json:"overlap_policy"`
	JitterMs      int64   `json:"jitter_ms"`
	Paused        bool    `json:"paused"`
	Note          string  `json:"note"`
	// Preview only: how many fires.
	Count int `json:"count"`
}

func (b *scheduleBody) spec() (*Schedule, error) {
	input, err := payloadFrom(b.Input, b.InputBase64, "input_base64")
	if err != nil {
		return nil, err
	}
	overlap, err := parseOverlap(b.OverlapPolicy)
	if err != nil {
		return nil, err
	}
	return &Schedule{
		ScheduleId: b.ScheduleID, Cron: b.Cron, Timezone: b.Timezone,
		WorkflowType: b.WorkflowType, Input: input, SkipIfRunning: b.SkipIfRunning, TaskQueue: b.TaskQueue,
		OverlapPolicy: overlap, JitterMs: b.JitterMs, Paused: b.Paused, Note: b.Note,
	}, nil
}

// parseOverlap reads an overlap policy by name, with or without its prefix.
func parseOverlap(v string) (OverlapPolicy, error) {
	if v == "" {
		return OverlapPolicy_OVERLAP_POLICY_UNSPECIFIED, nil
	}
	n, ok := OverlapPolicy_value["OVERLAP_POLICY_"+strings.TrimPrefix(strings.ToUpper(v), "OVERLAP_POLICY_")]
	if !ok {
		return 0, fmt.Errorf("%w: unknown overlap_policy %q (SKIP, BUFFER_ONE, ALLOW_ALL)", ErrInvalidArgument, v)
	}
	return OverlapPolicy(n), nil
}

func overlapName(p OverlapPolicy) string {
	return strings.TrimPrefix(p.String(), "OVERLAP_POLICY_")
}

func (a *HTTPAPI) listSchedules(w http.ResponseWriter, r *http.Request) {
	e := a.scope(w, r)
	if e == nil {
		return
	}
	scheds, err := e.ListSchedules()
	if err != nil {
		a.writeScheduleError(w, err)
		return
	}
	out := make([]*scheduleDTO, 0, len(scheds))
	for _, s := range scheds {
		sp := s.GetSchedule()
		out = append(out, &scheduleDTO{
			ScheduleID: sp.GetScheduleId(), Cron: sp.GetCron(), Timezone: sp.GetTimezone(),
			WorkflowType: sp.GetWorkflowType(), Input: string(sp.GetInput()), InputBase64: base64Twin(sp.GetInput()),
			SkipIfRunning: sp.GetSkipIfRunning(), TaskQueue: sp.GetTaskQueue(),
			OverlapPolicy: overlapName(overlapOf(sp)), JitterMs: sp.GetJitterMs(), Paused: sp.GetPaused(), Note: sp.GetNote(),
			CreatedAt: formatNanos(s.GetCreatedAt()), NextFireAt: formatNanos(s.GetNextFireAt()),
			BufferedFireAt: formatNanos(s.GetBufferedFireAt()),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": out})
}

func (a *HTTPAPI) createSchedule(w http.ResponseWriter, r *http.Request) {
	var body scheduleBody
	if err := a.decodeJSON(r, &body); err != nil {
		writeError(w, decodeStatus(err), err)
		return
	}
	spec, err := body.spec()
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	e := a.scope(w, r)
	if e == nil {
		return
	}
	if err := e.CreateSchedule(spec); err != nil {
		a.writeScheduleError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"schedule_id": body.ScheduleID})
}

func (a *HTTPAPI) deleteSchedule(w http.ResponseWriter, r *http.Request) {
	e := a.scope(w, r)
	if e == nil {
		return
	}
	if err := e.DeleteSchedule(r.PathValue("id")); err != nil {
		a.writeScheduleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

func (a *HTTPAPI) pauseSchedule(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Note string `json:"note"`
	}
	if r.ContentLength > 0 {
		if err := a.decodeJSON(r, &body); err != nil {
			writeError(w, decodeStatus(err), err)
			return
		}
	}
	e := a.scope(w, r)
	if e == nil {
		return
	}
	if err := e.PauseSchedule(r.PathValue("id"), body.Note); err != nil {
		a.writeScheduleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
}

func (a *HTTPAPI) resumeSchedule(w http.ResponseWriter, r *http.Request) {
	e := a.scope(w, r)
	if e == nil {
		return
	}
	if err := e.ResumeSchedule(r.PathValue("id")); err != nil {
		a.writeScheduleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
}

func (a *HTTPAPI) backfillSchedule(w http.ResponseWriter, r *http.Request) {
	var body struct {
		StartTime     string `json:"start_time"`
		EndTime       string `json:"end_time"`
		OverlapPolicy string `json:"overlap_policy"`
	}
	if err := a.decodeJSON(r, &body); err != nil {
		writeError(w, decodeStatus(err), err)
		return
	}
	from, err1 := time.Parse(time.RFC3339Nano, body.StartTime)
	to, err2 := time.Parse(time.RFC3339Nano, body.EndTime)
	if err := errors.Join(err1, err2); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Errorf("start_time and end_time must be RFC 3339 times: %w", err))
		return
	}
	policy, err := parseOverlap(body.OverlapPolicy)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	e := a.scope(w, r)
	if e == nil {
		return
	}
	res, err := e.BackfillSchedule(r.PathValue("id"), from, to, policy)
	if err != nil {
		a.writeScheduleError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"started": res.Started, "existing": res.Existing, "skipped": res.Skipped})
}

func (a *HTTPAPI) previewSchedule(w http.ResponseWriter, r *http.Request) {
	count := 0
	if v := r.URL.Query().Get("count"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("count must be an integer"))
			return
		}
		count = n
	}
	a.preview(w, r, r.PathValue("id"), nil, count)
}

func (a *HTTPAPI) previewSpec(w http.ResponseWriter, r *http.Request) {
	var body scheduleBody
	if err := a.decodeJSON(r, &body); err != nil {
		writeError(w, decodeStatus(err), err)
		return
	}
	spec, err := body.spec()
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	a.preview(w, r, "", spec, body.Count)
}

func (a *HTTPAPI) preview(w http.ResponseWriter, r *http.Request, id string, spec *Schedule, count int) {
	e := a.scope(w, r)
	if e == nil {
		return
	}
	p, err := e.PreviewSchedule(id, spec, count)
	if err != nil {
		a.writeScheduleError(w, err)
		return
	}
	type fire struct {
		FireAt  string `json:"fire_at"`
		StartAt string `json:"start_at"`
	}
	fires := make([]fire, 0, len(p.Fires))
	for i := range p.Fires {
		fires = append(fires, fire{FireAt: formatNanos(p.Fires[i].UnixNano()), StartAt: formatNanos(p.Starts[i].UnixNano())})
	}
	writeJSON(w, http.StatusOK, map[string]any{"fires": fires, "paused": p.Paused})
}

func (a *HTTPAPI) writeScheduleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotLeader):
		a.writeNotLeader(w, err)
	case errors.Is(err, ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, ErrQuotaExceeded):
		writeTooMany(w, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}
