package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// The live stream behind the dashboard: GET /api/events answers with
// server-sent events for the request's namespace, so a view updates the rows
// that changed instead of re-polling everything.
//
//	event: hello      {"namespace","time"}            once, on connect
//	event: workflow   a list row (as GET /api/workflows), or {"workflow_id","deleted":true}
//	event: schedules  {}                               the schedules changed; reload them
//	event: resync     {}                               changes were dropped; reload everything
//
// Changes to one workflow within eventsCoalesce are sent once, as the latest.
// A comment line every eventsPing keeps proxies from closing an idle stream.
const (
	eventsCoalesce = 200 * time.Millisecond
	eventsPing     = 15 * time.Second
)

// eventsRetryMillis is how long a client waits before reconnecting.
const eventsRetryMillis = 3000

type deletedDTO struct {
	WorkflowID string `json:"workflow_id"`
	Deleted    bool   `json:"deleted"`
}

func (a *HTTPAPI) events(w http.ResponseWriter, r *http.Request) {
	s := a.scope(w, r)
	if s == nil {
		return
	}
	sub, err := s.Subscribe()
	switch {
	case errors.Is(err, ErrTooManySubscribers):
		w.Header().Set("Retry-After", "10")
		writeError(w, http.StatusServiceUnavailable, err)
		return
	case err != nil:
		writeError(w, http.StatusBadRequest, err)
		return
	}
	defer sub.Close()

	h := w.Header()
	h.Set("Content-Type", "text/event-stream")
	h.Set("Cache-Control", "no-cache")
	// nginx would buffer the stream; this header turns that off per response.
	h.Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	rc := http.NewResponseController(w)
	flush := func() bool { return rc.Flush() == nil }
	if writeEvent(w, "hello", map[string]any{"namespace": s.Namespace(), "time": time.Now().UTC().Format(time.RFC3339Nano)}, eventsRetryMillis) != nil || !flush() {
		return
	}

	ping := time.NewTicker(eventsPing)
	defer ping.Stop()
	tick := time.NewTicker(eventsCoalesce)
	defer tick.Stop()
	var (
		pending   = map[string]any{}
		order     []string
		schedules bool
	)
	for {
		select {
		case <-r.Context().Done():
			return
		case <-sub.Done():
			return
		case c := <-sub.C():
			if c.Schedules {
				schedules = true
				continue
			}
			id := localID(c.Key)
			if _, seen := pending[id]; !seen {
				order = append(order, id)
			}
			if c.Deleted {
				pending[id] = deletedDTO{WorkflowID: id, Deleted: true}
			} else {
				dto := toSummaryDTO(c.Summary)
				dto.WorkflowID = id
				pending[id] = dto
			}
		case <-tick.C:
			if sub.Lagged() {
				pending, order, schedules = map[string]any{}, nil, false
				if writeEvent(w, "resync", struct{}{}, 0) != nil || !flush() {
					return
				}
				continue
			}
			if len(order) == 0 && !schedules {
				continue
			}
			for _, id := range order {
				if writeEvent(w, "workflow", pending[id], 0) != nil {
					return
				}
			}
			if schedules && writeEvent(w, "schedules", struct{}{}, 0) != nil {
				return
			}
			pending, order, schedules = map[string]any{}, nil, false
			if !flush() {
				return
			}
		case <-ping.C:
			if _, err := io.WriteString(w, ": ping\n\n"); err != nil || !flush() {
				return
			}
		}
	}
}

// writeEvent writes one server-sent event; retry, when set, tells the client
// how long to wait before reconnecting.
func writeEvent(w io.Writer, name string, data any, retry int) error {
	b, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if retry > 0 {
		if _, err := fmt.Fprintf(w, "retry: %d\n", retry); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(w, "event: %s\ndata: %s\n\n", name, b)
	return err
}
