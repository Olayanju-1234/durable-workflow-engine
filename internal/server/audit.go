package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/internal/audit"
)

// The audit log (ADR-0023) records every operator action at the transport
// edge, where the caller is known: each HTTP route and gRPC method the guard
// counts as a client write (starts, signals, cancels, terminations, resets,
// schedule changes), and backups. A new write route is classified for the
// guard anyway, so it is audited by the same classification. Recording never
// changes the answer: a log that cannot be written is reported, not fatal.

// WithAudit records operator actions on the JSON API in l.
func WithAudit(l *audit.Log) HTTPOption {
	return func(a *HTTPAPI) { a.audit = l }
}

// WithAuthorizer shares one Authorizer with the gRPC server, so a reload of
// the engine token or the namespaces reaches both.
func WithAuthorizer(az *Authorizer) HTTPOption {
	return func(a *HTTPAPI) { a.sharedAuthz = az }
}

// auditedDetails are the request fields an entry keeps. Inputs and payloads
// never: they hold the workflow's data, which may be encrypted at rest.
var auditedDetails = []string{
	"type", "workflow_id", "task_queue", "id_reuse_policy", "id_conflict_policy", "execution_timeout_ms",
	"name", "request_id", "reason", "graceful", "event_id",
	"schedule_id", "cron", "timezone", "workflow_type", "overlap_policy", "jitter_ms", "note", "start_time", "end_time",
}

// httpAuditAction names the action an API request performs ("" if none is audited).
func httpAuditAction(r *http.Request) string {
	p := r.URL.Path
	if r.Method == http.MethodGet && p == "/api/admin/backup" {
		return "admin.backup"
	}
	switch httpOp(r) {
	case opStart:
		return "workflow.start"
	case opSignal:
		return "workflow.signal"
	case opCancel:
		return "workflow.cancel"
	case opTerminate:
		return "workflow.terminate"
	case opReset:
		return "workflow.reset"
	case opSchedule:
		if r.Method == http.MethodDelete {
			return "schedule.delete"
		}
		for _, verb := range []string{"pause", "resume", "backfill"} {
			if strings.HasSuffix(p, "/"+verb) {
				return "schedule." + verb
			}
		}
		return "schedule.create"
	}
	return ""
}

// auditWriter keeps the status and the start of the body of an answer.
type auditWriter struct {
	http.ResponseWriter
	code int
	body bytes.Buffer
}

func (w *auditWriter) WriteHeader(code int) {
	w.code = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *auditWriter) Write(b []byte) (int, error) {
	if room := 4096 - w.body.Len(); room > 0 {
		w.body.Write(b[:min(len(b), room)])
	}
	return w.ResponseWriter.Write(b)
}

func (w *auditWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

// audited serves an API request, recording it if it is an operator action.
func (a *HTTPAPI) audited(g Grant, remote string, w http.ResponseWriter, r *http.Request, next http.Handler) {
	action := httpAuditAction(r)
	if action == "" || a.audit == nil {
		next.ServeHTTP(w, r)
		return
	}
	var req map[string]any
	if r.Body != nil && r.Method != http.MethodGet {
		body, _ := io.ReadAll(io.LimitReader(r.Body, int64(a.maxBody)+1))
		r.Body = io.NopCloser(bytes.NewReader(body))
		_ = json.Unmarshal(body, &req)
	}
	aw := &auditWriter{ResponseWriter: w, code: http.StatusOK}
	next.ServeHTTP(aw, r)

	e := audit.Entry{Actor: g.Actor, Namespace: g.Namespace, Action: action, Status: aw.code, Remote: remote, Via: "http",
		Details: map[string]string{}, Outcome: "ok"}
	for _, k := range auditedDetails {
		if v, ok := req[k]; ok && v != nil {
			e.Details[k] = fmt.Sprint(v)
		}
	}
	if action == "workflow.cancel" && e.Details["graceful"] == "true" {
		e.Action = "workflow.request_cancel"
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/"), "/")
	if len(parts) >= 2 && (parts[0] == "workflows" || parts[0] == "schedules") {
		e.Target = parts[1]
	}
	var resp map[string]any
	_ = json.Unmarshal(aw.body.Bytes(), &resp)
	if v, ok := resp["workflow_id"].(string); ok && e.Target == "" {
		e.Target = v
	}
	if v, ok := resp["run_id"].(string); ok {
		e.RunID = v
	}
	if e.Target == "" {
		e.Target = e.Details["schedule_id"]
	}
	if aw.code >= 400 {
		e.Outcome = "error"
		if v, ok := resp["error"].(string); ok {
			e.Error = v
		}
	}
	a.record(e)
}

func (a *HTTPAPI) record(e audit.Entry) {
	if err := a.audit.Record(e); err != nil {
		a.log.Error("audit log write failed; the action itself went ahead", "action", e.Action, "target", e.Target, "err", err)
	}
}

// listAudit answers GET /api/audit: newest first, filtered by action prefix
// and target. The engine token sees every namespace's entries and the
// engine's own (reloads); a namespace token sees its namespace's only. It is
// never anonymous: entries name callers and their addresses.
func (a *HTTPAPI) listAudit(w http.ResponseWriter, r *http.Request) {
	g := grantOf(r)
	if g.Anonymous {
		writeError(w, http.StatusForbidden, fmt.Errorf("%w: the audit log needs a token", ErrForbidden))
		return
	}
	if a.audit == nil {
		writeJSON(w, http.StatusOK, map[string]any{"entries": []audit.Entry{}, "enabled": false})
		return
	}
	q := r.URL.Query()
	limit := 100
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 1000 {
			writeError(w, http.StatusBadRequest, fmt.Errorf("%w: limit must be an integer in [1,1000]", ErrInvalidArgument))
			return
		}
		limit = n
	}
	action, target := q.Get("action"), q.Get("target")
	entries, err := a.audit.Read(func(e audit.Entry) bool {
		if !g.Admin && e.Namespace != g.Namespace {
			return false
		}
		return strings.HasPrefix(e.Action, action) && (target == "" || e.Target == target)
	}, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if entries == nil {
		entries = []audit.Entry{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"entries": entries, "enabled": true})
}

// GRPCAudit returns server options that record operator actions made over
// gRPC in l, after GRPCAuthorize has named the caller.
func GRPCAudit(l *audit.Log, onError func(error)) []grpc.ServerOption {
	if l == nil {
		return nil
	}
	return []grpc.ServerOption{grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		action, target, details := grpcAuditAction(req)
		if action == "" {
			return handler(ctx, req)
		}
		resp, err := handler(ctx, req)
		g := grantFrom(ctx)
		e := audit.Entry{Actor: g.Actor, Namespace: g.Namespace, Action: action, Target: target, Via: "grpc",
			Details: details, Outcome: "ok", Status: http.StatusOK}
		if p, ok := peer.FromContext(ctx); ok && p.Addr != nil {
			e.Remote = p.Addr.String()
		}
		switch r := resp.(type) {
		case *StartWorkflowResponse:
			e.RunID = r.GetRunId()
			if e.Target == "" {
				e.Target = r.GetWorkflowId()
			}
		case *ResetWorkflowResponse:
			e.RunID = r.GetRunId()
		}
		if err != nil {
			st := status.Convert(err)
			e.Outcome, e.Error, e.Status = "error", st.Message(), httpStatusOf(st.Code())
		}
		if werr := l.Record(e); werr != nil && onError != nil {
			onError(werr)
		}
		return resp, err
	})}
}

// grpcAuditAction names the action a gRPC request performs, what it acts on
// and the settings to keep ("" if it is not audited).
func grpcAuditAction(req any) (action, target string, details map[string]string) {
	d := map[string]string{}
	set := func(k, v string) {
		if v != "" {
			d[k] = v
		}
	}
	switch r := req.(type) {
	case *StartWorkflowRequest:
		set("type", r.GetWorkflowType())
		set("task_queue", r.GetTaskQueue())
		return "workflow.start", r.GetWorkflowId(), d
	case *SignalWorkflowRequest:
		set("name", r.GetSignalName())
		set("request_id", r.GetRequestId())
		return "workflow.signal", r.GetWorkflowId(), d
	case *CancelWorkflowRequest:
		set("reason", r.GetReason())
		if r.GetGraceful() {
			return "workflow.request_cancel", r.GetWorkflowId(), d
		}
		return "workflow.cancel", r.GetWorkflowId(), d
	case *TerminateWorkflowRequest:
		set("reason", r.GetReason())
		return "workflow.terminate", r.GetWorkflowId(), d
	case *ResetWorkflowRequest:
		set("reason", r.GetReason())
		if r.GetEventId() != 0 {
			set("event_id", strconv.FormatInt(r.GetEventId(), 10))
		}
		return "workflow.reset", r.GetWorkflowId(), d
	case *CreateScheduleRequest:
		set("cron", r.GetSchedule().GetCron())
		set("workflow_type", r.GetSchedule().GetWorkflowType())
		return "schedule.create", r.GetSchedule().GetScheduleId(), d
	case *DeleteScheduleRequest:
		return "schedule.delete", r.GetScheduleId(), d
	case *PauseScheduleRequest:
		set("note", r.GetNote())
		return "schedule.pause", r.GetScheduleId(), d
	case *ResumeScheduleRequest:
		return "schedule.resume", r.GetScheduleId(), d
	case *BackfillScheduleRequest:
		return "schedule.backfill", r.GetScheduleId(), d
	}
	return "", "", nil
}

// httpStatusOf is the HTTP status the JSON API answers for a gRPC code, so
// audit entries read the same whichever transport a call came by.
func httpStatusOf(c codes.Code) int {
	switch c {
	case codes.OK:
		return http.StatusOK
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.NotFound:
		return http.StatusNotFound
	case codes.AlreadyExists, codes.FailedPrecondition, codes.Aborted:
		return http.StatusConflict
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}
