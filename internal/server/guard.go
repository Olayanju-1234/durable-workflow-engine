package server

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"time"

	"github.com/olayanju-1234/durable-workflow-engine/internal/metrics"
)

// Guardrails at the API (ADR-0018): read-only namespaces and per-client rate
// limits, applied to HTTP and gRPC calls alike after authorization. Quotas on
// starts are the engine's own (quota.go), so schedules respect them too.

// Errors the API layers map to 403 and 429.
var (
	ErrReadOnly    = errors.New("server: read-only")
	ErrRateLimited = errors.New("server: rate limit exceeded")
)

// RetryAfter is a refusal that says when to try again.
type RetryAfter struct {
	Err   error
	After time.Duration
}

func (e *RetryAfter) Error() string { return e.Err.Error() }
func (e *RetryAfter) Unwrap() error { return e.Err }

// retryAfterOf is how long a refused call should wait, rounded up to whole
// seconds as Retry-After wants, and at least one.
func retryAfterOf(err error) int {
	var ra *RetryAfter
	if !errors.As(err, &ra) {
		return 1
	}
	return max(1, int(math.Ceil(ra.After.Seconds())))
}

// op is the kind of call a guard admits.
type op int

const (
	opRead     op = iota // reads and long-polls
	opWorker             // a worker reporting on a task it holds (complete, heartbeat)
	opRegister           // a worker registering the workflow types it runs
	opOther              // any other write: limited, but no client change to refuse read-only
	opStart
	opSignal
	opCancel
	opTerminate
	opReset
	opSchedule // schedule changes
)

// clientWrite reports whether a read-only namespace refuses op.
func (o op) clientWrite() bool { return o >= opStart }

// limited reports whether op counts against the rate limit. Completions and
// heartbeats need a live lease, so limiting them would only fail work.
func (o op) limited() bool { return o >= opRegister }

// Guard admits calls into a namespace. The zero value admits everything.
type Guard struct {
	namespaces *NamespaceSet
	defaults   Policy
	trusted    []netip.Prefix

	mu       sync.Mutex
	limiters map[string]*limiter // by namespace

	refused *metrics.CounterVec // reason: read_only | rate_limit; nil without a registry
}

// NewGuard builds a guard over the configured namespaces, with defaults for
// any namespace that sets none, trusting X-Forwarded-For only from trusted.
// reg, if not nil, gets workflowd_requests_refused_total.
func NewGuard(namespaces *NamespaceSet, defaults Policy, trusted []netip.Prefix, reg *metrics.Registry) *Guard {
	g := &Guard{namespaces: namespaces, defaults: defaults, trusted: trusted, limiters: map[string]*limiter{}}
	if reg != nil {
		g.refused = reg.CounterVec("workflowd_requests_refused_total",
			"API calls refused by a guardrail: a read-only namespace, or a client over its rate limit.", "reason")
	}
	return g
}

// policy is a namespace's guardrails.
func (g *Guard) policy(ns string) Policy {
	if g == nil {
		return Policy{}
	}
	return g.namespaces.Policy(ns, g.defaults)
}

// admit decides whether a call of kind o, by grant from client address ip,
// may go ahead.
func (g *Guard) admit(grant Grant, o op, ip string) error {
	if g == nil {
		return nil
	}
	p := g.policy(grant.Namespace)
	switch {
	case p.ReadOnly && o.clientWrite():
		g.count("read_only")
		return fmt.Errorf("%w: namespace %q is read-only: it refuses starts, signals, cancels, terminations, resets and schedule changes",
			ErrReadOnly, grant.Namespace)
	case p.ReadOnlySchedules && o == opSchedule:
		g.count("read_only")
		return fmt.Errorf("%w: schedules are read-only in namespace %q", ErrReadOnly, grant.Namespace)
	}
	if !o.limited() || grant.Admin || p.RateLimitPerMinute <= 0 {
		return nil
	}
	key := grant.Client
	if key == "" {
		key = "ip:" + ip
	}
	if ok, wait := g.limiter(grant.Namespace, p).take(key, time.Now()); !ok {
		g.count("rate_limit")
		return &RetryAfter{After: wait, Err: fmt.Errorf("%w: %d writes per minute per client in namespace %q; retry in %s",
			ErrRateLimited, p.RateLimitPerMinute, grant.Namespace, wait.Round(time.Second))}
	}
	return nil
}

func (g *Guard) count(reason string) {
	if g.refused != nil {
		g.refused.With(reason).Inc()
	}
}

func (g *Guard) limiter(ns string, p Policy) *limiter {
	g.mu.Lock()
	defer g.mu.Unlock()
	burst := p.RateLimitBurst
	if burst <= 0 {
		burst = p.RateLimitPerMinute
	}
	l := g.limiters[ns]
	if l == nil || l.perMinute != p.RateLimitPerMinute || l.burst != float64(burst) {
		l = newLimiter(p.RateLimitPerMinute, burst)
		g.limiters[ns] = l
	}
	return l
}

// clientIP is the address a request came from. X-Forwarded-For is believed
// only when the connection itself comes from a trusted proxy, and then read
// from the right: the first address that is not a trusted proxy is the
// client (anything to its left could have been sent by the client itself).
func (g *Guard) clientIP(remoteAddr string, forwardedFor []string) string {
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		host = remoteAddr
	}
	if g == nil || !g.isTrusted(host) {
		return host
	}
	var hops []string
	for _, v := range forwardedFor {
		for _, h := range strings.Split(v, ",") {
			if h = strings.TrimSpace(h); h != "" {
				hops = append(hops, h)
			}
		}
	}
	for i := len(hops) - 1; i >= 0; i-- {
		if !g.isTrusted(hops[i]) {
			return hops[i]
		}
	}
	if len(hops) > 0 {
		return hops[0]
	}
	return host
}

func (g *Guard) isTrusted(addr string) bool {
	ip, err := netip.ParseAddr(addr)
	if err != nil {
		return false
	}
	ip = ip.Unmap()
	for _, p := range g.trusted {
		if p.Contains(ip) {
			return true
		}
	}
	return false
}

// ParseTrustedProxies reads --trusted-proxies: comma-separated addresses or
// CIDR ranges.
func ParseTrustedProxies(s string) ([]netip.Prefix, error) {
	var out []netip.Prefix
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if !strings.Contains(part, "/") {
			ip, err := netip.ParseAddr(part)
			if err != nil {
				return nil, fmt.Errorf("--trusted-proxies: %q: %w", part, err)
			}
			out = append(out, netip.PrefixFrom(ip.Unmap(), ip.Unmap().BitLen()))
			continue
		}
		p, err := netip.ParsePrefix(part)
		if err != nil {
			return nil, fmt.Errorf("--trusted-proxies: %q: %w", part, err)
		}
		out = append(out, p.Masked())
	}
	return out, nil
}

// httpOp classifies an API request.
func httpOp(r *http.Request) op {
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		return opRead
	}
	p := r.URL.Path
	switch {
	case p == "/api/workflow-tasks/complete", p == "/api/activity-tasks/complete", p == "/api/activity-tasks/heartbeat":
		return opWorker
	case p == "/api/workflow-types":
		return opRegister
	case p == "/api/workflows":
		return opStart
	case p == "/api/schedules/preview":
		return opRead // answers a question, changes nothing
	case strings.HasPrefix(p, "/api/schedules"):
		return opSchedule
	case strings.HasSuffix(p, "/signal"):
		return opSignal
	case strings.HasSuffix(p, "/cancel"):
		return opCancel
	case strings.HasSuffix(p, "/terminate"):
		return opTerminate
	case strings.HasSuffix(p, "/reset"):
		return opReset
	default:
		return opOther
	}
}

// grpcOps classifies gRPC calls; anything missing is a read.
var grpcOps = map[string]op{
	WorkflowService_StartWorkflow_FullMethodName:        opStart,
	WorkflowService_SignalWorkflow_FullMethodName:       opSignal,
	WorkflowService_CancelWorkflow_FullMethodName:       opCancel,
	WorkflowService_TerminateWorkflow_FullMethodName:    opTerminate,
	WorkflowService_ResetWorkflow_FullMethodName:        opReset,
	WorkflowService_CreateSchedule_FullMethodName:       opSchedule,
	WorkflowService_DeleteSchedule_FullMethodName:       opSchedule,
	WorkflowService_PauseSchedule_FullMethodName:        opSchedule,
	WorkflowService_ResumeSchedule_FullMethodName:       opSchedule,
	WorkflowService_BackfillSchedule_FullMethodName:     opSchedule,
	WorkflowService_RegisterWorkflowType_FullMethodName: opRegister,
	WorkflowService_CompleteTask_FullMethodName:         opWorker,
	WorkflowService_Heartbeat_FullMethodName:            opWorker,
	WorkflowService_CompleteWorkflowTask_FullMethodName: opWorker,
}

// limiter is a token bucket per key: perMinute tokens a minute, up to burst.
type limiter struct {
	perMinute int
	burst     float64

	mu      sync.Mutex
	buckets map[string]*bucket
	swept   time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

func newLimiter(perMinute, burst int) *limiter {
	return &limiter{perMinute: perMinute, burst: float64(burst), buckets: map[string]*bucket{}}
}

// take spends a token for key, or says how long until one is there.
func (l *limiter) take(key string, now time.Time) (bool, time.Duration) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rate := float64(l.perMinute) / 60 // tokens a second
	l.sweep(now, rate)
	b := l.buckets[key]
	if b == nil {
		b = &bucket{tokens: l.burst, last: now}
		l.buckets[key] = b
	}
	b.tokens = min(l.burst, b.tokens+now.Sub(b.last).Seconds()*rate)
	b.last = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, time.Duration((1 - b.tokens) / rate * float64(time.Second))
}

// sweep forgets buckets that have refilled, so the map holds only recent
// clients; a forgotten client starts again from a full bucket, which is
// where it was. The caller holds l.mu.
func (l *limiter) sweep(now time.Time, rate float64) {
	if now.Sub(l.swept) < time.Minute {
		return
	}
	l.swept = now
	for k, b := range l.buckets {
		if b.tokens+now.Sub(b.last).Seconds()*rate >= l.burst {
			delete(l.buckets, k)
		}
	}
}
