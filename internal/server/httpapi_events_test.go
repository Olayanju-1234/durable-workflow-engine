package server_test

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

type sseEvent struct {
	name string
	data map[string]any
}

// openEvents connects to GET /api/events and returns its events as they come.
func openEvents(t *testing.T, base string) (<-chan sseEvent, *http.Response) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, "GET", base+"/api/events", nil)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /api/events: %v", err)
	}
	t.Cleanup(func() { res.Body.Close() })
	if res.StatusCode != http.StatusOK {
		t.Fatalf("GET /api/events: %d", res.StatusCode)
	}
	out := make(chan sseEvent, 1024)
	go func() {
		defer close(out)
		sc := bufio.NewScanner(res.Body)
		var ev sseEvent
		for sc.Scan() {
			line := sc.Text()
			switch {
			case line == "":
				if ev.name != "" {
					out <- ev
				}
				ev = sseEvent{}
			case strings.HasPrefix(line, "event: "):
				ev.name = strings.TrimPrefix(line, "event: ")
			case strings.HasPrefix(line, "data: "):
				_ = json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &ev.data)
			}
		}
	}()
	return out, res
}

// nextEvent waits for an event that satisfies match, skipping others.
func nextEvent(t *testing.T, events <-chan sseEvent, what string, match func(sseEvent) bool) sseEvent {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("stream ended waiting for %s", what)
			}
			if match(ev) {
				return ev
			}
		case <-deadline:
			t.Fatalf("no %s within 5s", what)
		}
	}
}

func TestEventsStreamWorkflowChanges(t *testing.T) {
	h := newAPIHarness(t, state.WorkflowDefinition{Type: "Live", Steps: state.Activities("one")})
	events, res := openEvents(t, h.srv.URL)
	if got := res.Header.Get("X-Accel-Buffering"); got != "no" {
		t.Errorf("X-Accel-Buffering = %q, want no (nginx would buffer the stream)", got)
	}
	if ct := res.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q", ct)
	}
	hello := nextEvent(t, events, "hello", func(e sseEvent) bool { return e.name == "hello" })
	if hello.data["namespace"] != "default" {
		t.Errorf("hello = %v", hello.data)
	}

	if code, body := h.do(t, "POST", "/api/workflows", `{"type":"Live","workflow_id":"live-1"}`); code != http.StatusCreated {
		t.Fatalf("start: %d %v", code, body)
	}
	ev := nextEvent(t, events, "live-1 waiting", func(e sseEvent) bool {
		return e.name == "workflow" && e.data["workflow_id"] == "live-1" && e.data["status"] == "WAITING"
	})
	if ev.data["workflow_type"] != "Live" || ev.data["history_length"] != float64(2) {
		t.Errorf("row = %v", ev.data)
	}
	if code, _ := h.do(t, "POST", "/api/workflows/live-1/terminate", `{"reason":"done"}`); code != http.StatusOK {
		t.Fatalf("terminate: %d", code)
	}
	nextEvent(t, events, "live-1 terminated", func(e sseEvent) bool {
		return e.name == "workflow" && e.data["workflow_id"] == "live-1" && e.data["status"] == "TERMINATED"
	})
}

func TestEventsStreamSchedules(t *testing.T) {
	h := newAPIHarness(t, state.WorkflowDefinition{Type: "Live", Steps: state.Activities("one")})
	events, _ := openEvents(t, h.srv.URL)
	nextEvent(t, events, "hello", func(e sseEvent) bool { return e.name == "hello" })
	if code, body := h.do(t, "POST", "/api/schedules", `{"schedule_id":"s1","cron":"@hourly","workflow_type":"Live"}`); code/100 != 2 {
		t.Fatalf("create schedule: %d %v", code, body)
	}
	nextEvent(t, events, "schedules", func(e sseEvent) bool { return e.name == "schedules" })
}

func TestEventsStreamCoalescesAWorkflow(t *testing.T) {
	h := newAPIHarness(t, state.WorkflowDefinition{Type: "Live", Steps: state.Activities("one")})
	events, _ := openEvents(t, h.srv.URL)
	nextEvent(t, events, "hello", func(e sseEvent) bool { return e.name == "hello" })
	// A start records two events (started, then the first step) at once: one row is sent.
	h.do(t, "POST", "/api/workflows", `{"type":"Live","workflow_id":"c-1"}`)
	nextEvent(t, events, "c-1", func(e sseEvent) bool { return e.name == "workflow" })
	select {
	case ev := <-events:
		t.Fatalf("second event for one burst of changes: %s %v", ev.name, ev.data)
	case <-time.After(500 * time.Millisecond):
	}
}

func TestSubscriptionIsPerNamespace(t *testing.T) {
	h := newHarness(t, state.WorkflowDefinition{Type: "Live", Steps: state.Activities("one")})
	h.engine.In("acme").RegisterWorkflow("Live", state.WorkflowDefinition{Type: "Live", Steps: state.Activities("one")})
	sub, err := h.engine.In("acme").Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	if _, err := h.engine.StartWorkflow("Live", nil, "in-default"); err != nil {
		t.Fatal(err)
	}
	if _, err := h.engine.In("acme").Start("Live", nil, "in-acme"); err != nil {
		t.Fatal(err)
	}
	for {
		select {
		case c := <-sub.C():
			if c.Key != "acme/in-acme" {
				t.Fatalf("acme's subscriber saw %q", c.Key)
			}
			return
		case <-time.After(5 * time.Second):
			t.Fatal("no change for acme/in-acme")
		}
	}
}

func TestSubscriptionsAreBounded(t *testing.T) {
	h := newHarness(t)
	var subs []*server.Subscription
	defer func() {
		for _, s := range subs {
			s.Close()
		}
	}()
	for {
		s, err := h.engine.In("").Subscribe()
		if err != nil {
			if len(subs) < 16 {
				t.Fatalf("refused after %d streams: %v", len(subs), err)
			}
			break
		}
		subs = append(subs, s)
		if len(subs) > 10000 {
			t.Fatal("subscriptions are unbounded")
		}
	}
	subs[0].Close()
	s, err := h.engine.In("").Subscribe()
	if err != nil {
		t.Fatalf("a closed subscription's slot was not freed: %v", err)
	}
	subs[0] = s
}

func TestSubscriptionEndsWhenTheEngineCloses(t *testing.T) {
	h := newHarness(t)
	sub, err := h.engine.In("").Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Close()
	h.engine.Close()
	select {
	case <-sub.Done():
	case <-time.After(time.Second):
		t.Fatal("the stream outlived its engine (a deposed leader's)")
	}
}

func TestListWorkflowsFiltersSortAndCounts(t *testing.T) {
	h := newAPIHarness(t,
		state.WorkflowDefinition{Type: "A", Steps: state.Activities("one")},
		state.WorkflowDefinition{Type: "B", Steps: state.Activities("one")})
	for _, s := range []struct{ typ, id string }{{"A", "a-1"}, {"B", "b-1"}, {"A", "a-2"}} {
		if code, body := h.do(t, "POST", "/api/workflows", fmt.Sprintf(`{"type":%q,"workflow_id":%q}`, s.typ, s.id)); code != http.StatusCreated {
			t.Fatalf("start %s: %d %v", s.id, code, body)
		}
		time.Sleep(5 * time.Millisecond) // distinct start times
	}
	mid := time.Now().UTC()
	time.Sleep(5 * time.Millisecond)
	h.do(t, "POST", "/api/workflows", `{"type":"B","workflow_id":"b-2"}`)
	h.do(t, "POST", "/api/workflows/a-1/terminate", `{}`)

	ids := func(body map[string]any) []string {
		var out []string
		for _, w := range body["workflows"].([]any) {
			out = append(out, w.(map[string]any)["workflow_id"].(string))
		}
		return out
	}
	_, body := h.do(t, "GET", "/api/workflows?sort=-started", "")
	if got := strings.Join(ids(body), ","); got != "b-2,a-2,b-1,a-1" {
		t.Errorf("sort=-started: %s", got)
	}
	if types := fmt.Sprint(body["types"]); types != "[A B]" {
		t.Errorf("types = %s", types)
	}
	counts := body["counts"].(map[string]any)
	if counts["WAITING"] != float64(3) || counts["TERMINATED"] != float64(1) {
		t.Errorf("counts = %v", counts)
	}

	_, body = h.do(t, "GET", "/api/workflows?type=A&status=WAITING", "")
	if got := strings.Join(ids(body), ","); got != "a-2" || body["total"] != float64(1) {
		t.Errorf("type=A&status=WAITING: %s (total %v)", got, body["total"])
	}
	// Counts ignore the status filter, so status chips can show every count.
	if counts := body["counts"].(map[string]any); counts["TERMINATED"] != float64(1) || counts["WAITING"] != float64(1) {
		t.Errorf("counts with type=A = %v", counts)
	}

	_, body = h.do(t, "GET", "/api/workflows?sort=started&started_after="+url.QueryEscape(mid.Format(time.RFC3339Nano)), "")
	if got := strings.Join(ids(body), ","); got != "b-2" {
		t.Errorf("started_after: %s", got)
	}
	_, body = h.do(t, "GET", "/api/workflows?sort=started&started_before="+url.QueryEscape(mid.Format(time.RFC3339Nano)), "")
	if got := strings.Join(ids(body), ","); got != "a-1,b-1,a-2" {
		t.Errorf("started_before: %s", got)
	}
	for _, bad := range []string{"sort=sideways", "started_after=yesterday"} {
		if code, _ := h.do(t, "GET", "/api/workflows?"+bad, ""); code != http.StatusBadRequest {
			t.Errorf("%s: %d, want 400", bad, code)
		}
	}
}
