package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/olayanju-1234/durable-workflow-engine/internal/audit"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
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
//	GET  /api/workflows[?status=RUNNING&type=&q=&started_after=&started_before=&sort=-started&limit=&offset=]
//	POST /api/workflows                      {"type","input","workflow_id","execution_timeout_ms","id_reuse_policy","id_conflict_policy"}
//	GET  /api/workflows/{id}[?run_id=]
//	GET  /api/workflows/{id}/runs
//	POST /api/workflows/{id}/terminate       {"reason"}
//	POST /api/workflows/{id}/cancel          {"reason"}
//	POST /api/workflows/{id}/signal          {"name","payload"}
//	POST /api/workflows/{id}/reset           {"event_id","reason"}  a finished run, again from a point
//	GET  /api/workflows/{id}/history[?run_id=]   the run's events, as protobuf JSON
//	GET  /api/namespaces
//	GET  /api/events                         the namespace's changes, as server-sent events
//	GET  /api/metrics[?window=15m]           the namespace's throughput, failures, step latency, queue depth
//	GET  /api/audit[?action=&target=&limit=] operator actions, newest first (a token is required)
//	GET  /api/admin/backup                   a tar.gz of every history (engine token only)
//
// Every route but health works in one namespace: the one the caller's token
// grants, or the one named by the X-Workflowd-Namespace header or the
// namespace query parameter (see Authorizer).
type HTTPAPI struct {
	src EngineSource
	log *slog.Logger
	// authToken is the engine's own token: it reaches every namespace. With
	// neither it nor namespace tokens set the API is open, as before 0.4.0.
	authToken string
	// publicReads leaves default-namespace list/describe open with a token set.
	publicReads bool
	namespaces  *NamespaceSet
	authz       *Authorizer
	guard       *Guard // read-only namespaces and rate limits; nil admits all
	// demoWorker is reported by /api/health so the dashboard can guide visitors.
	demoWorker bool
	// clusterStatus, when set, is reported by /api/health under "cluster".
	clusterStatus func() map[string]any
	startedAt     time.Time
	// maxBody is the largest request body accepted (Limits.RequestBytes).
	maxBody int
	// audit records operator actions (nil: not recorded).
	audit *audit.Log
	// sharedAuthz, when set, is the Authorizer the gRPC server uses too.
	sharedAuthz *Authorizer
}

func NewHTTPAPI(engine *Engine, logger *slog.Logger, opts ...HTTPOption) *HTTPAPI {
	limits := Limits{MaxPayloadBytes: DefaultMaxPayloadBytes}
	if engine != nil {
		limits = engine.Limits()
	}
	a := &HTTPAPI{src: staticSource{engine}, log: logger, startedAt: time.Now(), maxBody: limits.RequestBytes()}
	for _, opt := range opts {
		opt(a)
	}
	a.authz = a.sharedAuthz
	if a.authz == nil {
		a.authz = NewAuthorizer(a.authToken, a.publicReads, a.namespaces)
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

// WithPublicReads lets anyone list and describe workflows while a token is
// still required to start, signal or terminate them and to run workers.
func WithPublicReads() HTTPOption {
	return func(a *HTTPAPI) { a.publicReads = true }
}

// WithGuard applies read-only namespaces and rate limits to API calls.
func WithGuard(g *Guard) HTTPOption {
	return func(a *HTTPAPI) { a.guard = g }
}

// WithNamespaceSet gives the API the configured namespaces and their tokens.
func WithNamespaceSet(set *NamespaceSet) HTTPOption {
	return func(a *HTTPAPI) { a.namespaces = set }
}

// WithDemoWorker reports in /api/health that an in-process demo worker is
// executing the built-in demo activities on this node.
func WithDemoWorker() HTTPOption {
	return func(a *HTTPAPI) { a.demoWorker = true }
}

// WithEngineSource serves whichever engine src resolves per request instead
// of a fixed one: in a cluster, the leader's, and a leader hint elsewhere.
func WithEngineSource(src EngineSource) HTTPOption {
	return func(a *HTTPAPI) { a.src = src }
}

// WithRequestLimits makes the API accept request bodies up to
// l.RequestBytes(), for an API whose engine is not known at construction (a
// cluster's).
func WithRequestLimits(l Limits) HTTPOption {
	return func(a *HTTPAPI) { a.maxBody = l.RequestBytes() }
}

// WithClusterStatus adds this node's cluster view to /api/health.
func WithClusterStatus(fn func() map[string]any) HTTPOption {
	return func(a *HTTPAPI) { a.clusterStatus = fn }
}

// engine returns the engine for this request, or answers 503 with a leader
// hint and returns nil.
func (a *HTTPAPI) engine(w http.ResponseWriter) *Engine {
	e, err := a.src.Engine()
	if err != nil {
		a.writeNotLeader(w, err)
		return nil
	}
	return e
}

// scope returns the engine scoped to the request's namespace, or answers (503
// with a leader hint) and returns nil.
func (a *HTTPAPI) scope(w http.ResponseWriter, r *http.Request) *Scope {
	e := a.engine(w)
	if e == nil {
		return nil
	}
	return e.In(grantOf(r).Namespace)
}

// writeNotLeader answers 503: the request was fine, this node just isn't the
// one that can serve it. The body and a header say where the leader is.
func (a *HTTPAPI) writeNotLeader(w http.ResponseWriter, err error) {
	h := leaderHint(a.src, err)
	if h.ID != "" {
		w.Header().Set("X-Workflowd-Leader", h.ID)
	} else {
		w.Header().Set("Retry-After", "1")
	}
	writeJSON(w, http.StatusServiceUnavailable, map[string]string{
		"error":       (&NotLeaderError{Leader: h}).Error(),
		"leader_id":   h.ID,
		"leader_grpc": h.GRPCAddr,
		"leader_http": h.HTTPAddr,
	})
}

// Handler returns the API routes. Go 1.22+ method+wildcard patterns keep the
// routing declarative, so there is no hand-rolled path parsing.
func (a *HTTPAPI) Handler() *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", a.health)
	mux.HandleFunc("GET /api/workflows", a.listWorkflows)
	mux.HandleFunc("POST /api/workflows", a.startWorkflow)
	mux.HandleFunc("GET /api/workflows/{id}", a.getWorkflow)
	mux.HandleFunc("GET /api/workflows/{id}/runs", a.listWorkflowRuns)
	mux.HandleFunc("GET /api/workflows/{id}/history", a.workflowHistory)
	mux.HandleFunc("GET /api/admin/backup", a.backup)
	mux.HandleFunc("POST /api/workflows/{id}/terminate", a.terminateWorkflow)
	mux.HandleFunc("POST /api/workflows/{id}/cancel", a.cancelWorkflow)
	mux.HandleFunc("POST /api/workflows/{id}/signal", a.signalWorkflow)
	mux.HandleFunc("POST /api/workflows/{id}/reset", a.resetWorkflow)
	mux.HandleFunc("GET /api/namespaces", a.listNamespaces)
	mux.HandleFunc("GET /api/events", a.events)
	mux.HandleFunc("GET /api/metrics", a.metricsSummary)
	mux.HandleFunc("GET /api/audit", a.listAudit)
	// Polyglot SDK transport (see httpapi_polyglot.go).
	a.registerPolyglotRoutes(mux)
	a.registerScheduleRoutes(mux)
	a.registerArchiveRoutes(mux)
	// Without this, unmatched /api paths would fall through to the dashboard's
	// catch-all and answer 200 with HTML, which confuses API clients.
	mux.HandleFunc("/api/", func(w http.ResponseWriter, r *http.Request) {
		writeError(w, http.StatusNotFound, fmt.Errorf("no API route for %s %s", r.Method, r.URL.Path))
	})
	return mux
}

// Wrap layers request logging and auth around a fully-assembled mux — call
// it after mounting the dashboard on Handler(), so the middleware sees the
// final routing table. Auth applies only to /api routes other than health;
// static assets stay public.
func (a *HTTPAPI) Wrap(h http.Handler) http.Handler {
	return a.logRequests(a.authorize(h))
}

// HeaderNamespace names the namespace a request works in (or ?namespace=).
const HeaderNamespace = "X-Workflowd-Namespace"

type grantKey struct{}

// grantOf is the request's grant; a request that never passed authorize (a
// handler mounted without Wrap) gets the default namespace.
func grantOf(r *http.Request) Grant {
	if g, ok := r.Context().Value(grantKey{}).(Grant); ok {
		return g
	}
	return Grant{Namespace: DefaultNamespace, Anonymous: true, Actor: "anonymous"}
}

// authorize resolves every API request's namespace and token into a Grant
// (see Authorizer) and refuses the ones it does not allow: 401 for a missing
// or unknown token, 403 for a token used outside its namespace.
func (a *HTTPAPI) authorize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/") || r.URL.Path == "/api/health" {
			next.ServeHTTP(w, r)
			return
		}
		requested := r.Header.Get(HeaderNamespace)
		if q := r.URL.Query().Get("namespace"); q != "" {
			if requested != "" && requested != q {
				writeError(w, http.StatusBadRequest, fmt.Errorf("%w: the %s header says %q but ?namespace= says %q",
					ErrInvalidArgument, HeaderNamespace, requested, q))
				return
			}
			requested = q
		}
		token, hasAuth := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !hasAuth && r.Header.Get("Authorization") != "" {
			token = "\x00" // a malformed header is an invalid token, not a missing one
		}
		g, err := a.authz.Authorize(token, requested, isPublicRead(r))
		if err != nil {
			writeAuthError(w, err)
			return
		}
		remote := a.guard.clientIP(r.RemoteAddr, r.Header.Values("X-Forwarded-For"))
		if err := a.guard.admit(g, httpOp(r), remote); err != nil {
			writeAuthError(w, err)
			return
		}
		a.audited(g, remote, w, r.WithContext(context.WithValue(r.Context(), grantKey{}, g)), next)
	})
}

// writeAuthError answers a refused request.
func writeAuthError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrForbidden), errors.Is(err, ErrReadOnly):
		writeError(w, http.StatusForbidden, err)
	case errors.Is(err, ErrRateLimited), errors.Is(err, ErrQuotaExceeded):
		writeTooMany(w, err)
	case errors.Is(err, ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, err)
	default:
		w.Header().Set("WWW-Authenticate", `Bearer realm="workflowd"`)
		writeError(w, http.StatusUnauthorized, err)
	}
}

// listNamespaces lists the namespaces the caller can use, which one this
// request is in, and whether that one refuses changes (so a UI can say so).
func (a *HTTPAPI) listNamespaces(w http.ResponseWriter, r *http.Request) {
	g := grantOf(r)
	p := a.guard.policy(g.Namespace)
	writeJSON(w, http.StatusOK, map[string]any{
		"namespaces":          a.authz.Visible(g),
		"current":             g.Namespace,
		"admin":               g.Admin,
		"read_only":           p.ReadOnly,
		"read_only_schedules": p.ReadOnly || p.ReadOnlySchedules,
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

// Unwrap lets http.ResponseController reach the connection's Flush (events).
func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

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
	RunID         string `json:"run_id,omitempty"`
	// The history's size on disk, against the engine's history size limit.
	HistorySizeBytes int64  `json:"history_size_bytes,omitempty"`
	TaskQueue        string `json:"task_queue,omitempty"`
}

type stepDTO struct {
	StepName string `json:"step_name"`
	Sequence int64  `json:"sequence"`
	Kind     string `json:"kind"`
	Status   string `json:"status"`
	Attempts int32  `json:"attempts"`
	Error    string `json:"error,omitempty"`
	// Failed steps: the kind of failure, and whether the worker said not to retry it.
	ErrorType    string `json:"error_type,omitempty"`
	NonRetryable bool   `json:"non_retryable,omitempty"`
	Output       string `json:"output,omitempty"`
	// Set only when Output's bytes are not valid UTF-8 (see base64Twin).
	OutputBase64 string `json:"output_base64,omitempty"`
	FireAt       string `json:"fire_at,omitempty"`
	SignalName   string `json:"signal_name,omitempty"`
	Marker       string `json:"marker,omitempty"`
	// Workflow tasks: the id of the event that gave the step its result, which
	// orders results for WaitAny, and when it was recorded (ctx.now()).
	ResolvedEventID int64  `json:"resolved_event_id,omitempty"`
	ResolvedAt      string `json:"resolved_at,omitempty"`
	// Activity steps: the task queue their tasks go to.
	TaskQueue string `json:"task_queue,omitempty"`
}

type workflowDTO struct {
	summaryDTO
	Input        string     `json:"input,omitempty"`
	InputBase64  string     `json:"input_base64,omitempty"`
	Output       string     `json:"output,omitempty"`
	OutputBase64 string     `json:"output_base64,omitempty"`
	Steps        []*stepDTO `json:"steps"`
	// The run this one continued from, and the one that followed it.
	ContinuedFromRunID  string `json:"continued_from_run_id,omitempty"`
	ContinuedAsNewRunID string `json:"continued_as_new_run_id,omitempty"`
	// A graceful cancel was requested: the code has been told.
	CancelRequested bool   `json:"cancel_requested,omitempty"`
	CancelReason    string `json:"cancel_reason,omitempty"`
	// The run this one was reset from, and where a reset of this finished run
	// would start over by default.
	ResetFromRunID string         `json:"reset_from_run_id,omitempty"`
	ResetPoint     *resetPointDTO `json:"reset_point,omitempty"`
}

type resetPointDTO struct {
	EventID  int64  `json:"event_id"`
	StepName string `json:"step_name,omitempty"`
	Sequence int64  `json:"sequence,omitempty"`
}

// health is answered by every node, leader or not: a follower is healthy too.
func (a *HTTPAPI) health(w http.ResponseWriter, _ *http.Request) {
	var st EngineStats
	if e, err := a.src.Engine(); err == nil {
		st = e.Stats()
	}
	body := map[string]any{
		"status":                    "ok",
		"version":                   version.Version,
		"uptime_seconds":            int64(time.Since(a.startedAt).Seconds()),
		"workflows_active":          st.ActiveWorkflows,
		"task_queue_depth":          st.TaskQueueDepth,
		"workflow_task_queue_depth": st.WorkflowTaskQueueDepth,
		"demo_worker":               a.demoWorker,
		"archiving":                 st.Archive != "",
		"encryption":                st.EncryptionKey != "",
		// The data directory's history format, and the newest this binary reads.
		"history_format":       st.HistoryFormat,
		"reads_history_format": history.FormatVersion,
	}
	if st.EncryptionKey != "" {
		body["encryption_key"] = st.EncryptionKey
	}
	if a.clusterStatus != nil {
		body["cluster"] = a.clusterStatus()
	}
	writeJSON(w, http.StatusOK, body)
}

// listWorkflows supports filtering by status, type, start time and a
// case-insensitive substring of the id or type, sorting, and offset/limit
// pagination. The response carries "total" (after every filter), "counts"
// (per status, after every filter but status) and the namespace's "types", so
// a UI can draw its filters without fetching every workflow.
func (a *HTTPAPI) listWorkflows(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	filter, err := parseStatusFilter(query.Get("status"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	const (
		defaultLimit = 200
		maxLimit     = 1000
	)
	limit := defaultLimit
	if v := query.Get("limit"); v != "" {
		if limit, err = strconv.Atoi(v); err != nil || limit < 1 || limit > maxLimit {
			writeError(w, http.StatusBadRequest, fmt.Errorf("limit must be an integer in [1,%d]", maxLimit))
			return
		}
	}
	offset := 0
	if v := query.Get("offset"); v != "" {
		if offset, err = strconv.Atoi(v); err != nil || offset < 0 {
			writeError(w, http.StatusBadRequest, errors.New("offset must be a non-negative integer"))
			return
		}
	}
	after, err := parseTimeParam(query, "started_after")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	before, err := parseTimeParam(query, "started_before")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	less, err := summaryOrder(query.Get("sort"))
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	q := strings.ToLower(strings.TrimSpace(query.Get("q")))
	wantType := query.Get("type")

	e := a.scope(w, r)
	if e == nil {
		return
	}
	summaries, err := e.ListWorkflows(WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	matched := make([]*WorkflowSummary, 0, len(summaries))
	counts := map[string]int{}
	types := map[string]bool{}
	for _, s := range summaries {
		if t := s.GetWorkflowType(); t != "" {
			types[t] = true
		}
		if q != "" && !strings.Contains(strings.ToLower(s.GetWorkflowId()+" "+s.GetWorkflowType()), q) {
			continue
		}
		if wantType != "" && s.GetWorkflowType() != wantType {
			continue
		}
		if (after != 0 && s.GetStartedAt() < after) || (before != 0 && s.GetStartedAt() >= before) {
			continue
		}
		counts[statusName(s.GetStatus())]++
		if filter != WorkflowStatus_WORKFLOW_STATUS_UNSPECIFIED && s.GetStatus() != filter {
			continue
		}
		matched = append(matched, s)
	}
	if less != nil {
		sort.SliceStable(matched, func(i, j int) bool { return less(matched[i], matched[j]) })
	}
	total := len(matched)
	if offset > total {
		offset = total
	}
	page := matched[offset:]
	if len(page) > limit {
		page = page[:limit]
	}
	rows := make([]*summaryDTO, 0, len(page))
	for _, s := range page {
		rows = append(rows, toSummaryDTO(s))
	}
	typeList := make([]string, 0, len(types))
	for t := range types {
		typeList = append(typeList, t)
	}
	sort.Strings(typeList)
	writeJSON(w, http.StatusOK, map[string]any{
		"workflows": rows,
		"total":     total,
		"offset":    offset,
		"limit":     limit,
		"counts":    counts,
		"types":     typeList,
	})
}

// metricsSummary answers the metrics panel: the namespace's pulse over a
// window from 1 minute to an hour (default 15 minutes).
func (a *HTTPAPI) metricsSummary(w http.ResponseWriter, r *http.Request) {
	window := 15 * time.Minute
	if v := r.URL.Query().Get("window"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Minute || d > pulseKeep {
			writeError(w, http.StatusBadRequest, fmt.Errorf("%w: window must be a duration from 1m to 1h", ErrInvalidArgument))
			return
		}
		window = d
	}
	e := a.scope(w, r)
	if e == nil {
		return
	}
	writeJSON(w, http.StatusOK, e.MetricsSummary(window))
}

// parseTimeParam reads an RFC 3339 time from the query as unix nanoseconds
// (0 when absent).
func parseTimeParam(query url.Values, name string) (int64, error) {
	v := query.Get(name)
	if v == "" {
		return 0, nil
	}
	t, err := time.Parse(time.RFC3339Nano, v)
	if err != nil {
		return 0, fmt.Errorf("%w: %s must be an RFC 3339 time, e.g. 2026-10-25T09:00:00Z", ErrInvalidArgument, name)
	}
	return t.UnixNano(), nil
}

// summaryOrder is the list order a ?sort= names: id (the default, nil since
// the index already sorts by id), started or updated, newest first with "-".
func summaryOrder(v string) (func(a, b *WorkflowSummary) bool, error) {
	key := func(s *WorkflowSummary) int64 { return s.GetStartedAt() }
	switch strings.TrimPrefix(v, "-") {
	case "", "id":
		return nil, nil
	case "started":
	case "updated":
		key = func(s *WorkflowSummary) int64 { return s.GetUpdatedAt() }
	default:
		return nil, fmt.Errorf("%w: sort must be id, started, -started, updated or -updated", ErrInvalidArgument)
	}
	if strings.HasPrefix(v, "-") {
		return func(a, b *WorkflowSummary) bool { return key(a) > key(b) }, nil
	}
	return func(a, b *WorkflowSummary) bool { return key(a) < key(b) }, nil
}

func (a *HTTPAPI) getWorkflow(w http.ResponseWriter, r *http.Request) {
	e := a.scope(w, r)
	if e == nil {
		return
	}
	resp, err := e.GetWorkflowRun(r.PathValue("id"), r.URL.Query().Get("run_id"))
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err)
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, toWorkflowDTO(resp))
}

// toWorkflowDTO is one workflow run as the API presents it.
func toWorkflowDTO(resp *GetWorkflowResponse) *workflowDTO {
	steps := make([]*stepDTO, 0, len(resp.GetSteps()))
	for _, s := range resp.GetSteps() {
		steps = append(steps, &stepDTO{
			StepName:     s.GetStepName(),
			Sequence:     s.GetSequence(),
			Kind:         s.GetKind(),
			Status:       s.GetStatus(),
			Attempts:     s.GetAttempts(),
			Error:        s.GetError(),
			ErrorType:    s.GetErrorType(),
			Output:       string(s.GetOutput()),
			OutputBase64: base64Twin(s.GetOutput()),
			FireAt:       formatNanos(s.GetFireAt()),
			SignalName:   s.GetSignalName(),
			Marker:       s.GetMarker(),
			TaskQueue:    s.GetTaskQueue(),
		})
	}
	dto := &workflowDTO{
		summaryDTO:   *toSummaryDTO(resp.GetSummary()),
		Input:        string(resp.GetInput()),
		InputBase64:  base64Twin(resp.GetInput()),
		Output:       string(resp.GetOutput()),
		OutputBase64: base64Twin(resp.GetOutput()),
		Steps:        steps,

		ContinuedFromRunID:  resp.GetContinuedFromRunId(),
		ContinuedAsNewRunID: resp.GetContinuedAsNewRunId(),
		CancelRequested:     resp.GetCancelRequested(),
		CancelReason:        resp.GetCancelReason(),
		ResetFromRunID:      resp.GetResetFromRunId(),
	}
	if resp.GetResetEventId() > 0 {
		dto.ResetPoint = &resetPointDTO{EventID: resp.GetResetEventId(), StepName: resp.GetResetStepName(), Sequence: resp.GetResetStepSequence()}
	}
	return dto
}

// runDTO is one run of a workflow id.
type runDTO struct {
	RunID     string `json:"run_id"`
	Status    string `json:"status"`
	StartedAt string `json:"started_at,omitempty"`
	ClosedAt  string `json:"closed_at,omitempty"`
	Current   bool   `json:"current,omitempty"`
}

func (a *HTTPAPI) listWorkflowRuns(w http.ResponseWriter, r *http.Request) {
	e := a.scope(w, r)
	if e == nil {
		return
	}
	runs, err := e.ListWorkflowRuns(r.PathValue("id"))
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err)
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	out := make([]*runDTO, 0, len(runs))
	for _, run := range runs {
		out = append(out, &runDTO{
			RunID: run.GetRunId(), Status: statusName(run.GetStatus()), StartedAt: formatNanos(run.GetStartedAt()),
			ClosedAt: formatNanos(run.GetClosedAt()), Current: run.GetCurrent(),
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": out})
}

// workflowHistory returns a run's events exactly as recorded, each in the
// protobuf JSON mapping (proto field names; 64-bit numbers and bytes as
// strings), for tools that want more than describe's replayed view.
func (a *HTTPAPI) workflowHistory(w http.ResponseWriter, r *http.Request) {
	e := a.scope(w, r)
	if e == nil {
		return
	}
	events, err := e.History(r.PathValue("id"), r.URL.Query().Get("run_id"))
	switch {
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err)
		return
	case err != nil:
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	writeEvents(w, r.PathValue("id"), "", events)
}

// writeEvents answers with events in the protobuf JSON mapping.
func writeEvents(w http.ResponseWriter, workflowID, runID string, events []*history.HistoryEvent) {
	out := make([]json.RawMessage, 0, len(events))
	for _, ev := range events {
		b, err := protojson.MarshalOptions{UseProtoNames: true}.Marshal(ev)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		out = append(out, b)
	}
	body := map[string]any{"workflow_id": workflowID, "events": out}
	if runID != "" {
		body["run_id"] = runID
	}
	writeJSON(w, http.StatusOK, body)
}

// backup streams a backup of the whole engine (every namespace), so it needs
// the engine's own token.
func (a *HTTPAPI) backup(w http.ResponseWriter, r *http.Request) {
	if !grantOf(r).Admin {
		writeError(w, http.StatusForbidden, fmt.Errorf("%w: a backup holds every namespace; it needs the engine's token (--auth-token-file)", ErrForbidden))
		return
	}
	e := a.engine(w)
	if e == nil {
		return
	}
	w.Header().Set("Content-Type", "application/gzip")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="workflowd-backup-%s.tar.gz"`, time.Now().UTC().Format("20060102T150405Z")))
	if err := e.Backup(w); err != nil {
		// The status is sent; a truncated archive fails to unpack, which is the signal.
		a.log.Error("backup failed", "err", err)
	}
}

func (a *HTTPAPI) startWorkflow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Type        string  `json:"type"`
		Input       string  `json:"input"`
		InputBase64 *string `json:"input_base64"`
		WorkflowID  string  `json:"workflow_id"`
		// Optional: the workflow times out this long after it starts.
		ExecutionTimeoutMs int64 `json:"execution_timeout_ms"`
		// Optional: "ALLOW_DUPLICATE", "ALLOW_DUPLICATE_FAILED_ONLY" or
		// "REJECT_DUPLICATE" (default); "FAIL" (default) or "USE_EXISTING".
		IDReusePolicy    string `json:"id_reuse_policy"`
		IDConflictPolicy string `json:"id_conflict_policy"`
		// Optional: the task queue it runs on (default: the default queue).
		TaskQueue string `json:"task_queue"`
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
	reuse, conflict, err := parseIDPolicies(body.IDReusePolicy, body.IDConflictPolicy)
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	started, err := e.Start(body.Type, input, body.WorkflowID,
		WithExecutionTimeout(time.Duration(body.ExecutionTimeoutMs)*time.Millisecond),
		WithIDReusePolicy(reuse), WithIDConflictPolicy(conflict), WithTaskQueue(body.TaskQueue),
		WithTraceParent(r.Header.Get("traceparent"), r.Header.Get("tracestate")))
	switch {
	case err == nil:
	case errors.Is(err, ErrNotLeader):
		a.writeNotLeader(w, err)
		return
	case errors.Is(err, ErrUnknownType), errors.Is(err, ErrInvalidArgument):
		// The caller must fix the request: the type is not registered here.
		writeError(w, http.StatusBadRequest, err)
		return
	case errors.Is(err, ErrPayloadTooLarge):
		writeError(w, http.StatusRequestEntityTooLarge, err)
		return
	case errors.Is(err, ErrQuotaExceeded):
		writeTooMany(w, err)
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
	code := http.StatusCreated
	if started.Existing {
		code = http.StatusOK // the run that already held the id (USE_EXISTING)
	}
	writeJSON(w, code, map[string]any{"workflow_id": started.WorkflowID, "run_id": started.RunID, "started": !started.Existing})
}

// parseIDPolicies reads the start policies by name, case-insensitively, with
// or without their enum prefix. Empty means the default.
func parseIDPolicies(reuse, conflict string) (IdReusePolicy, IdConflictPolicy, error) {
	var r IdReusePolicy
	if reuse != "" {
		v, ok := IdReusePolicy_value["ID_REUSE_POLICY_"+strings.TrimPrefix(strings.ToUpper(reuse), "ID_REUSE_POLICY_")]
		if !ok {
			return 0, 0, fmt.Errorf("%w: unknown id_reuse_policy %q (ALLOW_DUPLICATE, ALLOW_DUPLICATE_FAILED_ONLY, REJECT_DUPLICATE)", ErrInvalidArgument, reuse)
		}
		r = IdReusePolicy(v)
	}
	var c IdConflictPolicy
	if conflict != "" {
		v, ok := IdConflictPolicy_value["ID_CONFLICT_POLICY_"+strings.TrimPrefix(strings.ToUpper(conflict), "ID_CONFLICT_POLICY_")]
		if !ok {
			return 0, 0, fmt.Errorf("%w: unknown id_conflict_policy %q (FAIL, USE_EXISTING)", ErrInvalidArgument, conflict)
		}
		c = IdConflictPolicy(v)
	}
	return r, c, nil
}

func (a *HTTPAPI) terminateWorkflow(w http.ResponseWriter, r *http.Request) {
	a.endWorkflow(w, r, "terminated", (*Scope).TerminateWorkflow)
}

func (a *HTTPAPI) cancelWorkflow(w http.ResponseWriter, r *http.Request) {
	a.endWorkflow(w, r, "cancelled", (*Scope).CancelWorkflow)
}

// endWorkflow serves terminate and cancel: {"reason"} in, {"status"} out.
// Cancel takes "graceful": true too, which tells the workflow code instead.
func (a *HTTPAPI) endWorkflow(w http.ResponseWriter, r *http.Request, done string, end func(*Scope, string, string) error) {
	var body struct {
		Reason   string `json:"reason"`
		Graceful bool   `json:"graceful"`
	}
	// An empty body is valid (reason is optional).
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
	if body.Graceful {
		if done != "cancelled" {
			writeError(w, http.StatusBadRequest, fmt.Errorf("%w: only cancel can be graceful", ErrInvalidArgument))
			return
		}
		end, done = (*Scope).RequestCancelWorkflow, "cancel_requested"
	}
	err := end(e, r.PathValue("id"), body.Reason)
	switch {
	case errors.Is(err, ErrNotLeader):
		a.writeNotLeader(w, err)
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, ErrAlreadyTerminal):
		writeError(w, http.StatusConflict, err)
	case err != nil:
		writeError(w, http.StatusInternalServerError, err)
	default:
		writeJSON(w, http.StatusOK, map[string]string{"status": done})
	}
}

// resetWorkflow starts a finished run again from a point in its history
// (event_id; omitted or 0 picks the step whose failure ended it).
func (a *HTTPAPI) resetWorkflow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		EventID int64  `json:"event_id"`
		Reason  string `json:"reason"`
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
	res, err := e.ResetWorkflow(r.PathValue("id"), ResetOptions{EventID: body.EventID, Reason: body.Reason})
	switch {
	case err == nil, res.RunID != "":
		// A run that reset but could not resume yet is still reset.
		writeJSON(w, http.StatusOK, map[string]any{
			"workflow_id": r.PathValue("id"), "run_id": res.RunID, "previous_run_id": res.PreviousRunID,
			"reset_event_id": res.ResetEventID, "reapplied_signals": res.ReappliedSignals,
		})
	case errors.Is(err, ErrNotLeader):
		a.writeNotLeader(w, err)
	case errors.Is(err, ErrNotFound):
		writeError(w, http.StatusNotFound, err)
	case errors.Is(err, ErrNotFinished):
		writeError(w, http.StatusConflict, err)
	case errors.Is(err, ErrInvalidArgument):
		writeError(w, http.StatusBadRequest, err)
	default:
		writeError(w, http.StatusInternalServerError, err)
	}
}

func (a *HTTPAPI) signalWorkflow(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name          string  `json:"name"`
		Payload       string  `json:"payload"`
		PayloadBase64 *string `json:"payload_base64"`
		// Optional idempotency key: a retry with the same key is recorded once.
		RequestID string `json:"request_id"`
	}
	if err := a.decodeJSON(r, &body); err != nil {
		writeError(w, decodeStatus(err), err)
		return
	}
	payload, err := payloadFrom(body.Payload, body.PayloadBase64, "payload_base64")
	if err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	e := a.scope(w, r)
	if e == nil {
		return
	}
	if err := e.SignalWorkflowOnce(r.PathValue("id"), body.Name, body.RequestID, payload); err != nil {
		switch {
		case errors.Is(err, ErrNotLeader):
			a.writeNotLeader(w, err)
		case errors.Is(err, ErrInvalidArgument):
			writeError(w, http.StatusBadRequest, err)
		case errors.Is(err, ErrPayloadTooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, err)
		case errors.Is(err, ErrNotFound):
			writeError(w, http.StatusNotFound, err)
		case errors.Is(err, ErrAlreadyTerminal), errors.Is(err, ErrHistoryLimit):
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

		RunID:            s.GetRunId(),
		HistorySizeBytes: s.GetHistorySizeBytes(),
		TaskQueue:        s.GetTaskQueue(),
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

// decodeJSON reads a request body of at most the engine's request limit. An
// over-long body is ErrPayloadTooLarge (413).
func (a *HTTPAPI) decodeJSON(r *http.Request, dst any) error {
	dec := json.NewDecoder(http.MaxBytesReader(nil, r.Body, int64(a.maxBody)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		var tooLong *http.MaxBytesError
		if errors.As(err, &tooLong) {
			return fmt.Errorf("%w: the request body is over %d bytes", ErrPayloadTooLarge, a.maxBody)
		}
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	return nil
}

// decodeStatus is the answer to a body decodeJSON refused.
func decodeStatus(err error) int {
	if errors.Is(err, ErrPayloadTooLarge) {
		return http.StatusRequestEntityTooLarge
	}
	return http.StatusBadRequest
}

// writeTooMany answers 429 with when to try again.
func writeTooMany(w http.ResponseWriter, err error) {
	w.Header().Set("Retry-After", strconv.Itoa(retryAfterOf(err)))
	writeError(w, http.StatusTooManyRequests, err)
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}

func writeError(w http.ResponseWriter, code int, err error) {
	writeJSON(w, code, map[string]string{"error": err.Error()})
}
