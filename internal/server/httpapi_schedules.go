package server

import (
	"errors"
	"net/http"
)

// Schedule routes:
//
//	GET    /api/schedules
//	POST   /api/schedules        {"schedule_id","cron","timezone","workflow_type","input","skip_if_running"}
//	DELETE /api/schedules/{id}
func (a *HTTPAPI) registerScheduleRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /api/schedules", a.listSchedules)
	mux.HandleFunc("POST /api/schedules", a.createSchedule)
	mux.HandleFunc("DELETE /api/schedules/{id}", a.deleteSchedule)
}

type scheduleDTO struct {
	ScheduleID    string `json:"schedule_id"`
	Cron          string `json:"cron"`
	Timezone      string `json:"timezone,omitempty"`
	WorkflowType  string `json:"workflow_type"`
	Input         string `json:"input,omitempty"`
	InputBase64   string `json:"input_base64,omitempty"`
	SkipIfRunning bool   `json:"skip_if_running,omitempty"`
	CreatedAt     string `json:"created_at,omitempty"`
	NextFireAt    string `json:"next_fire_at,omitempty"`
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
			SkipIfRunning: sp.GetSkipIfRunning(),
			CreatedAt:     formatNanos(s.GetCreatedAt()), NextFireAt: formatNanos(s.GetNextFireAt()),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"schedules": out})
}

func (a *HTTPAPI) createSchedule(w http.ResponseWriter, r *http.Request) {
	var body struct {
		ScheduleID    string  `json:"schedule_id"`
		Cron          string  `json:"cron"`
		Timezone      string  `json:"timezone"`
		WorkflowType  string  `json:"workflow_type"`
		Input         string  `json:"input"`
		InputBase64   *string `json:"input_base64"`
		SkipIfRunning bool    `json:"skip_if_running"`
	}
	if err := a.decodeJSON(r, &body); err != nil {
		writeError(w, decodeStatus(err), err)
		return
	}
	input, err := payloadFrom(body.Input, body.InputBase64, "input_base64")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	e := a.scope(w, r)
	if e == nil {
		return
	}
	if err := e.CreateSchedule(&Schedule{
		ScheduleId: body.ScheduleID, Cron: body.Cron, Timezone: body.Timezone,
		WorkflowType: body.WorkflowType, Input: input, SkipIfRunning: body.SkipIfRunning,
	}); err != nil {
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

func (a *HTTPAPI) writeScheduleError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotLeader):
		a.writeNotLeader(w, err)
	case errors.Is(err, ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, err)
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}
