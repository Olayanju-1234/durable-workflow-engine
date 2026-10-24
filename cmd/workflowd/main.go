// Command workflowd is the durable workflow engine daemon: one binary serving
// gRPC, the JSON API + embedded dashboard, and Prometheus metrics.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/olayanju-1234/durable-workflow-engine/internal/archive"
	"github.com/olayanju-1234/durable-workflow-engine/internal/ctl"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/raft"
	"github.com/olayanju-1234/durable-workflow-engine/internal/replay"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
	"github.com/olayanju-1234/durable-workflow-engine/internal/version"
	"github.com/olayanju-1234/durable-workflow-engine/web"
	"github.com/olayanju-1234/durable-workflow-engine/workflow"
)

func main() {
	// `workflowd ctl ...` is the operator's command line, a client of the API.
	if len(os.Args) > 1 && os.Args[1] == "ctl" {
		os.Exit(ctl.Main(os.Args[2:], os.Stdin, os.Stdout, os.Stderr, os.Getenv))
	}
	var (
		dataDir     = flag.String("data-dir", "data", "directory for workflow history files")
		showVersion = flag.Bool("version", false, "print version and exit")
		demo        = flag.Bool("demo", false, "append and read back a sample event, then exit (smoke test)")
		serve       = flag.Bool("serve", false, "run the gRPC workflow server")
		addr        = flag.String("addr", ":7233", "gRPC listen address (serve mode)")
		metricsAddr = flag.String("metrics-addr", ":9090", "HTTP listen address for /metrics (serve mode; empty disables)")
		httpAddr    = flag.String("http-addr", ":8233", "HTTP listen address for the dashboard + JSON API (serve mode; empty disables)")
		demoWorker  = flag.Bool("demo-worker", false, "run an in-process worker for the built-in demo activities (reserve, charge, ship, release)")
		retention   = flag.Duration("retention", 0, "delete finished workflows this long after their last event, e.g. 168h (0 keeps them forever)")
		maxEvents   = flag.Int("max-history-events", server.DefaultMaxHistoryEvents, "events a workflow run's history may hold; at 80% the engine warns and suggests continue-as-new, at the limit the workflow fails (0: no limit)")
		maxBytes    = flag.Int64("max-history-bytes", server.DefaultMaxHistoryBytes, "bytes a workflow run's history may hold, like --max-history-events (0: no limit)")
		maxPayload  = flag.Int("max-payload-bytes", server.DefaultMaxPayloadBytes, "the largest input, output, signal payload or marker value, on HTTP and gRPC alike (0: no limit)")
		archiveSpec = flag.String("archive", "", "before retention deletes a finished history, export it here: a directory, file:///dir, or s3://bucket/prefix?endpoint=https://...&region=... (S3, R2); a history that cannot be exported is kept")
		archiveCred = flag.String("archive-credentials-file", "", "S3 credentials for --archive: lines access_key_id=... and secret_access_key=... (default: $WORKFLOWD_ARCHIVE_ACCESS_KEY_ID and $WORKFLOWD_ARCHIVE_SECRET_ACCESS_KEY, else $AWS_ACCESS_KEY_ID and $AWS_SECRET_ACCESS_KEY)")
		checkFmt    = flag.Bool("check-format", false, "report the data directory's history format and whether this binary can serve it, then exit (run with the engine stopped)")
		rescanFmt   = flag.Bool("rescan-format", false, "like --check-format, then record the format the histories need now (lowers it once newer histories are gone)")

		// Clustering is opt-in; with none of these set the daemon is single-node.
		nodeID   = flag.String("node-id", "", "this node's id in --peers (cluster mode)")
		peers    = flag.String("peers", "", "every cluster member as id=host:port of its Raft listener, comma-separated, e.g. n1=127.0.0.1:7333,n2=127.0.0.1:7334 (enables cluster mode)")
		raftAddr = flag.String("raft-addr", "", "listen address for Raft peer traffic (cluster mode; default: this node's address in --peers)")
		raftTick = flag.Duration("raft-tick", raft.DefaultTickInterval, "Raft tick: leaders heartbeat every tick, followers start an election after 10–20 silent ticks (cluster mode)")
	)
	secFlags := registerSecurityFlags(flag.CommandLine)
	flag.Parse()

	if *showVersion {
		fmt.Printf("workflowd %s (reads history format %d)\n", version.Version, history.FormatVersion)
		return
	}
	if *checkFmt || *rescanFmt {
		if err := checkFormat(os.Stdout, *dataDir, *rescanFmt); err != nil {
			fmt.Fprintln(os.Stderr, "workflowd:", err)
			os.Exit(1)
		}
		return
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))

	clusterCfg, clustered, err := parseClusterFlags(*nodeID, *peers, *raftAddr, *raftTick)
	if err != nil {
		logger.Error("invalid cluster flags", "err", err)
		os.Exit(2)
	}
	sec, err := secFlags.resolve(logger)
	if err == nil && sec.peerTLS != nil && !clustered {
		err = errors.New("--raft-tls-* need cluster mode (--peers)")
	}
	if err != nil {
		logger.Error("invalid auth/TLS flags", "err", err)
		os.Exit(2)
	}

	store, err := history.NewHistoryStore(*dataDir)
	if err != nil {
		// A directory written by a newer binary is refused here, before
		// recovery could misread a single history.
		logger.Error("open history store", "data_dir", *dataDir, "err", err)
		os.Exit(1)
	}
	defer store.Close()
	logger.Info("history store ready", "data_dir", *dataDir, "version", version.Version)

	if *demo {
		if err := runDemo(logger, store); err != nil {
			logger.Error("demo failed", "err", err)
			os.Exit(1)
		}
		return
	}

	if *serve {
		cfg := serveConfig{
			addr: *addr, metricsAddr: *metricsAddr, httpAddr: *httpAddr,
			sec: sec, demoWorker: *demoWorker, retention: *retention,
			limits: server.Limits{MaxHistoryEvents: *maxEvents, MaxHistoryBytes: *maxBytes, MaxPayloadBytes: *maxPayload},
		}
		if err := cfg.limits.Validate(); err != nil {
			logger.Error("invalid limits", "err", err)
			os.Exit(2)
		}
		if *archiveSpec != "" {
			creds, err := archiveCredentials(*archiveCred, os.Getenv)
			if err == nil {
				cfg.archive, err = archive.Open(*archiveSpec, creds)
			}
			if err != nil {
				logger.Error("invalid --archive", "err", err)
				os.Exit(2)
			}
		}
		if clustered {
			if err := runCluster(logger, store, *dataDir, cfg, clusterCfg); err != nil {
				logger.Error("cluster node stopped", "err", err)
				os.Exit(1)
			}
			return
		}
		if err := runServe(logger, store, cfg); err != nil {
			logger.Error("server stopped", "err", err)
			os.Exit(1)
		}
		return
	}

	logger.Info("engine bootstrapped; pass --serve to run the gRPC server or --demo for a smoke test")
}

// listen opens every socket the daemon serves on. The chaos tests swap it for
// sockets the test process bound, so a port never changes hands between runs.
var listen = net.Listen

// shutdownGracePeriod bounds how long GracefulStop waits for in-flight RPCs to
// drain before a forced Stop, so an idle worker parked on a long-poll PollTask
// can't hang shutdown indefinitely.
const shutdownGracePeriod = 10 * time.Second

// serveConfig is the --serve flag set.
type serveConfig struct {
	addr, metricsAddr, httpAddr string
	sec                         security
	demoWorker                  bool
	retention                   time.Duration
	archive                     archive.Store // nil: retention deletes outright
	limits                      server.Limits
}

// shortestRetention is the shortest retention any namespace has (the
// engine's own, or a namespace's override), zero if none purges.
func (cfg serveConfig) shortestRetention() time.Duration {
	shortest := time.Duration(0)
	consider := func(d time.Duration) {
		if d > 0 && (shortest == 0 || d < shortest) {
			shortest = d
		}
	}
	for _, name := range cfg.sec.namespaces.Names() {
		if ns := cfg.sec.namespaces.Get(name); ns != nil && ns.Retention != nil {
			consider(*ns.Retention)
		} else {
			consider(cfg.retention)
		}
	}
	return shortest
}

// grpcServerOptions are the client-facing gRPC server's options: TLS and
// auth, and a message size that fits the largest request the limits allow.
func (cfg serveConfig) grpcServerOptions() []grpc.ServerOption {
	return append(cfg.sec.grpcServerOptions(), grpc.MaxRecvMsgSize(cfg.limits.RequestBytes()))
}

// demoWorkerConcurrency is how many demo activities run at once.
const demoWorkerConcurrency = 4

// runServe starts the gRPC workflow server (plus the /metrics HTTP endpoint)
// and blocks until SIGINT/SIGTERM, then drains in-flight RPCs with
// GracefulStop. The engine is wired to the same store the daemon opened, and
// the Phase-1 workflow types are registered up front.
func runServe(logger *slog.Logger, store *history.HistoryStore, cfg serveConfig) error {
	if cfg.retention < 0 {
		return fmt.Errorf("--retention must not be negative, got %s", cfg.retention)
	}
	engine := server.NewEngine(store, logger, server.WithLimits(cfg.limits), server.WithNamespaces(cfg.sec.namespaces),
		server.WithGuardrails(cfg.sec.guardrails), server.WithArchive(cfg.archive))
	defer engine.Close()
	cfg.sec.guard = server.NewGuard(cfg.sec.namespaces, cfg.sec.guardrails, cfg.sec.trusted, engine.Metrics())
	registerDemoTypes(engine)

	// Rebuild runtime state from disk before accepting traffic: re-enqueue steps
	// that were in flight when we last died, and advance any workflow caught
	// mid-decision. The task queue is ephemeral; the history is the source of truth.
	if err := engine.Recover(); err != nil {
		return fmt.Errorf("recover: %w", err)
	}

	// Deferred before stop(), so it runs after it: stop cancels ctx, which ends the
	// background workers, and they finish before the engine and store close.
	var bg sync.WaitGroup
	defer bg.Wait()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	startEngineServices(ctx, &bg, engine, logger, cfg)

	// Two HTTP listeners, deliberately separate from each other and from gRPC:
	// the dashboard + JSON API are for humans and operators, while /metrics is
	// scraped by Prometheus and is usually kept on an internal-only port. Either
	// can be disabled with an empty address.
	var httpSrvs []*http.Server
	cfg.sec.logPosture(logger, cfg.httpAddr != "")

	if cfg.httpAddr != "" {
		opts := cfg.sec.httpOptions()
		if cfg.demoWorker {
			opts = append(opts, server.WithDemoWorker())
		}
		api := server.NewHTTPAPI(engine, logger, opts...)
		mux := api.Handler()
		mux.Handle("/", web.Handler()) // embedded dashboard; API routes win by specificity
		httpSrvs = append(httpSrvs, startHTTP(ctx, cfg.httpAddr, api.Wrap(mux), "dashboard", cfg.sec.tls, logger))
		logger.Info("dashboard listening", "addr", cfg.httpAddr, "url", browseURL(cfg.httpAddr, cfg.sec.tls != nil))
	}

	if cfg.metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", cfg.sec.metricsHandler(engine.Metrics().Handler()))
		httpSrvs = append(httpSrvs, startHTTP(ctx, cfg.metricsAddr, mux, "metrics", cfg.sec.tls, logger))
		logger.Info("metrics endpoint listening", "addr", cfg.metricsAddr, "path", "/metrics")
	}

	lis, err := listen("tcp", cfg.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.addr, err)
	}

	gs := grpc.NewServer(cfg.grpcServerOptions()...)
	server.RegisterWorkflowServiceServer(gs, server.NewGRPCServer(engine, logger))

	go func() {
		<-ctx.Done()
		logger.Info("shutdown signal received; draining servers", "grace", shutdownGracePeriod)
		drainServers(logger, httpSrvs, gs)
	}()

	logger.Info("gRPC server listening", "addr", cfg.addr, "version", version.Version)
	if err := gs.Serve(lis); err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// startEngineServices runs the in-process helpers an engine may have (the demo
// worker, retention) until ctx ends; bg tracks them. In a cluster they run on
// the leader only, for as long as its engine does.
func startEngineServices(ctx context.Context, bg *sync.WaitGroup, engine *server.Engine, logger *slog.Logger, cfg serveConfig) {
	if cfg.demoWorker {
		bg.Add(1)
		go func() {
			defer bg.Done()
			newDemoWorker(engine, logger).run(ctx, demoWorkerConcurrency)
		}()
		logger.Info("demo worker running", "steps", demoSteps, "concurrency", demoWorkerConcurrency)
	}
	if shortest := cfg.shortestRetention(); shortest > 0 {
		interval := min(max(shortest/10, time.Minute), time.Hour)
		bg.Add(1)
		go func() {
			defer bg.Done()
			engine.RunRetention(ctx, cfg.retention, interval)
		}()
		logger.Info("retention enabled", "keep", cfg.retention, "shortest", shortest, "interval", interval)
		if cfg.archive != nil {
			logger.Info("finished histories are archived before retention deletes them", "archive", cfg.archive.String())
		}
	} else if cfg.archive != nil {
		logger.Warn("--archive is set but no retention is: nothing is deleted, so nothing is archived", "hint", "set --retention")
	}
}

// drainServers shuts the HTTP servers down and drains gRPC, bounded by
// shutdownGracePeriod.
func drainServers(logger *slog.Logger, httpSrvs []*http.Server, gs *grpc.Server) {
	sctx, cancel := context.WithTimeout(context.Background(), shutdownGracePeriod)
	defer cancel()
	for _, srv := range httpSrvs {
		_ = srv.Shutdown(sctx)
	}
	// GracefulStop waits for in-flight RPCs, but an idle worker parked on a
	// long-poll PollTask never returns on its own. Bound the drain and force a
	// stop so shutdown can't hang on idle pollers.
	drained := make(chan struct{})
	go func() { gs.GracefulStop(); close(drained) }()
	select {
	case <-drained:
	case <-time.After(shutdownGracePeriod):
		logger.Warn("graceful drain timed out; forcing stop")
		gs.Stop()
	}
}

// startHTTP runs an HTTP server in the background, logging (but not fatally
// failing on) a listen error — a busy dashboard port must not take down an
// otherwise healthy engine. A non-nil tlsCfg serves HTTPS.
func startHTTP(ctx context.Context, addr string, handler http.Handler, name string, tlsCfg *tls.Config, logger *slog.Logger) *http.Server {
	srv := &http.Server{
		Addr:              addr,
		Handler:           handler,
		TLSConfig:         tlsCfg,
		ReadHeaderTimeout: 10 * time.Second, // basic slowloris guard
		// Request contexts end on SIGTERM, so SDK long-polls return at once
		// instead of holding Shutdown open for up to the 25 s poll window.
		BaseContext: func(net.Listener) context.Context { return ctx },
	}
	go func() {
		lis, err := listen("tcp", addr)
		if err == nil {
			if tlsCfg != nil {
				err = srv.ServeTLS(lis, "", "")
			} else {
				err = srv.Serve(lis)
			}
		}
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server stopped", "server", name, "addr", addr, "err", err)
		}
	}()
	return srv
}

// browseURL turns a listen address into a URL a person can open: ":8233" and
// "0.0.0.0:8233" become localhost, while an explicit host is kept as-is.
func browseURL(addr string, https bool) string {
	scheme := "http://"
	if https {
		scheme = "https://"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return scheme + addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "localhost"
	}
	return scheme + net.JoinHostPort(host, port)
}

// registerDemoTypes registers the built-in example workflows every node runs.
func registerDemoTypes(engine *server.Engine) {
	engine.Register(demoDef)
	// A code-based Saga workflow (Phase 2): charge is retried up to 3× with backoff,
	// and if it still fails the workflow releases the reservation (compensation).
	engine.RegisterWorkflow("OrderSaga", replay.Planner{Fn: orderSaga})
	engine.SetRetryPolicy("charge", server.RetryPolicy{
		MaxAttempts: 3, Backoff: 200 * time.Millisecond, MaxBackoff: 5 * time.Second,
	})
}

// demoDef is the Phase-1 stand-in for "workflow code": a fixed, ordered pipeline.
// In Phase 2 the deterministic replay engine derives these steps from a real Go
// function instead.
var demoDef = state.WorkflowDefinition{
	Type:  "OrderWorkflow",
	Steps: state.Activities("reserve", "charge", "ship"),
}

// orderSaga is the Phase-2 code-based workflow: ordinary Go whose execution the
// replay engine drives durably. reserve → sleep → charge (auto-retried) → ship; if
// charge ultimately fails, or a graceful cancel arrives, release the reservation
// and fail with a wrapped error (CANCELLED for the cancel).
func orderSaga(ctx workflow.Context, input []byte) ([]byte, error) {
	reserved, err := ctx.ExecuteActivity("reserve", input)
	if err != nil {
		return nil, err
	}
	ctx.Sleep("settle", 2*time.Second) // durable wait — survives restarts
	if ctx.CancelRequested() {
		ctx.ExecuteActivity("release", reserved)
		return nil, fmt.Errorf("order cancelled, reservation released: %w", workflow.ErrWorkflowCancelled)
	}
	if _, err := ctx.ExecuteActivity("charge", reserved); err != nil {
		ctx.ExecuteActivity("release", reserved) // compensation
		return nil, fmt.Errorf("charge failed, reservation released: %w", err)
	}
	return ctx.ExecuteActivity("ship", reserved)
}

// runDemo drives a whole workflow to completion through the Week-2 state machine,
// persisting every event to the store. There is no gRPC worker yet (Week 3), so a
// worker that always succeeds is simulated inline. The loop is the entire engine:
// replay history → Decide → record the resulting event → repeat until terminal.
func runDemo(logger *slog.Logger, store *history.HistoryStore) error {
	wfID := fmt.Sprintf("demo-%d", time.Now().UnixNano())
	log := logger.With("workflow_id", wfID)

	// record stamps the next monotonic event id and a wall-clock timestamp, then
	// durably appends the event. Reading the current length to assign sequential
	// ids is how the engine numbers events today; Week 3's server owns this.
	record := func(ev *history.HistoryEvent) error {
		existing, err := store.ReadAll(wfID)
		if err != nil {
			return err
		}
		ev.EventId = int64(len(existing) + 1)
		ev.Timestamp = time.Now().UnixNano()
		return store.Append(wfID, ev)
	}

	// The engine records WorkflowStarted before the first decision.
	if err := record(&history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{
		WorkflowStarted: &history.WorkflowStartedEvent{
			WorkflowId: wfID, WorkflowType: demoDef.Type, Input: []byte("order-7"),
		},
	}}); err != nil {
		return err
	}
	log.Info("workflow started", "type", demoDef.Type)

	for step := 0; step < 100; step++ { // bound guards against a decision-loop bug
		events, err := store.ReadAll(wfID)
		if err != nil {
			return err
		}
		decision, err := state.Decide(demoDef, events)
		if err != nil {
			return fmt.Errorf("decide: %w", err)
		}
		log.Info("engine decided", "decision", decision.Type.String(), "step", decision.StepName)

		switch decision.Type {
		case state.DecideScheduleStep:
			if err := record(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepScheduled{
				StepScheduled: &history.StepScheduledEvent{
					StepName: decision.StepName, Sequence: decision.Sequence, Input: decision.Input,
				},
			}}); err != nil {
				return err
			}
			// Simulated worker: succeed immediately, echoing the step name.
			if err := record(&history.HistoryEvent{Attributes: &history.HistoryEvent_StepCompleted{
				StepCompleted: &history.StepCompletedEvent{
					StepName: decision.StepName, Sequence: decision.Sequence, Output: []byte("done:" + decision.StepName),
				},
			}}); err != nil {
				return err
			}
			log.Info("worker completed step", "step", decision.StepName, "sequence", decision.Sequence)

		case state.DecideCompleteWorkflow:
			if err := record(&history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowCompleted{
				WorkflowCompleted: &history.WorkflowCompletedEvent{Output: decision.Output},
			}}); err != nil {
				return err
			}
			final, err := store.ReadAll(wfID)
			if err != nil {
				return err
			}
			log.Info("workflow completed", "output", string(decision.Output), "events_on_disk", len(final))
			return nil

		case state.DecideFailWorkflow:
			if err := record(&history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowFailed{
				WorkflowFailed: &history.WorkflowFailedEvent{Error: decision.Error},
			}}); err != nil {
				return err
			}
			log.Warn("workflow failed", "error", decision.Error)
			return nil

		default: // DecideNoop with no terminal event means the loop is stuck.
			return fmt.Errorf("unexpected no-op decision; workflow neither progressed nor finished")
		}
	}
	return fmt.Errorf("decision loop did not terminate within bound")
}
