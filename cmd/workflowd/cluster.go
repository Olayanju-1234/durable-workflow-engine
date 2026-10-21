package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/olayanju-1234/durable-workflow-engine/internal/cluster"
	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
	"github.com/olayanju-1234/durable-workflow-engine/internal/version"
	"github.com/olayanju-1234/durable-workflow-engine/web"
)

// clusterConfig is the clustering flag set. Clustering is opt-in: without
// --peers the daemon runs single-node exactly as before.
type clusterConfig struct {
	nodeID   string
	peers    map[string]string // node id -> Raft address, every member included
	raftAddr string            // where this node listens for peer traffic
	tick     time.Duration
}

// parseClusterFlags validates the clustering flags. It returns ok=false when
// clustering is not requested at all.
func parseClusterFlags(nodeID, peers, raftAddr string, tick time.Duration) (clusterConfig, bool, error) {
	if peers == "" {
		if nodeID != "" || raftAddr != "" {
			return clusterConfig{}, false, errors.New("--node-id and --raft-addr need --peers")
		}
		return clusterConfig{}, false, nil
	}
	members, err := parsePeers(peers)
	if err != nil {
		return clusterConfig{}, false, err
	}
	if nodeID == "" {
		return clusterConfig{}, false, errors.New("--peers needs --node-id, naming this node")
	}
	self, ok := members[nodeID]
	if !ok {
		return clusterConfig{}, false, fmt.Errorf("--node-id %q is not listed in --peers", nodeID)
	}
	if raftAddr == "" {
		raftAddr = self
	}
	if tick <= 0 {
		return clusterConfig{}, false, fmt.Errorf("--raft-tick must be positive, got %s", tick)
	}
	return clusterConfig{nodeID: nodeID, peers: members, raftAddr: raftAddr, tick: tick}, true, nil
}

// parsePeers reads "n1=host:port,n2=host:port,...".
func parsePeers(s string) (map[string]string, error) {
	peers := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("--peers entry %q: want id=host:port", part)
		}
		if _, _, err := net.SplitHostPort(addr); err != nil {
			return nil, fmt.Errorf("--peers entry %q: %w", part, err)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("--peers lists node %q twice", id)
		}
		peers[id] = addr
	}
	return peers, nil
}

func (c clusterConfig) members() []string {
	ids := make([]string, 0, len(c.peers))
	for id := range c.peers {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

// runCluster runs this node as a member of a Raft cluster until SIGINT/SIGTERM
// (ADR-0004). Every node serves the same API; only the leader runs an engine,
// and the others answer with a hint naming it.
func runCluster(logger *slog.Logger, store *history.HistoryStore, dataDir string, cfg serveConfig, cc clusterConfig) error {
	if cfg.retention < 0 {
		return fmt.Errorf("--retention must not be negative, got %s", cfg.retention)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	selfHost, _, _ := net.SplitHostPort(cc.peers[cc.nodeID])
	cl, err := cluster.Start(cluster.Config{
		NodeID: cc.nodeID, Peers: cc.peers, RaftAddr: cc.raftAddr,
		GRPCAddr: advertised(cfg.addr, selfHost), HTTPAddr: advertised(cfg.httpAddr, selfHost),
		DataDir: dataDir, Store: store, Logger: logger, TickInterval: cc.tick,
		NewEngine: func(hl server.HistoryLog) *server.Engine {
			e := server.NewEngine(store, logger, server.WithHistoryLog(hl))
			registerDemoTypes(e)
			return e
		},
		OnEngine: func(ctx context.Context, e *server.Engine) {
			var bg sync.WaitGroup
			startEngineServices(ctx, &bg, e, logger, cfg)
			bg.Wait()
		},
	})
	if err != nil {
		return err
	}
	// Deferred first, so it runs last: the API servers drain before Raft stops.
	defer cl.Stop()
	logger.Info("cluster member running", "node_id", cc.nodeID, "raft_addr", cc.raftAddr, "version", version.Version)

	var httpSrvs []*http.Server
	if cfg.httpAddr != "" {
		opts := []server.HTTPOption{
			server.WithAuthToken(cfg.authToken), server.WithEngineSource(cl), server.WithClusterStatus(cl.StatusMap),
		}
		if cfg.demoWorker {
			opts = append(opts, server.WithDemoWorker())
		}
		api := server.NewHTTPAPI(nil, logger, opts...)
		mux := api.Handler()
		mux.Handle("/", web.Handler())
		httpSrvs = append(httpSrvs, startHTTP(ctx, cfg.httpAddr, api.Wrap(mux), "dashboard", logger))
		logger.Info("dashboard listening", "addr", cfg.httpAddr, "url", browseURL(cfg.httpAddr))
	}
	if cfg.metricsAddr != "" {
		mux := http.NewServeMux()
		mux.Handle("/metrics", cl.MetricsHandler())
		httpSrvs = append(httpSrvs, startHTTP(ctx, cfg.metricsAddr, mux, "metrics", logger))
	}
	lis, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cfg.addr, err)
	}
	gs := grpc.NewServer()
	server.RegisterWorkflowServiceServer(gs, server.NewGRPCServerFrom(cl, logger))
	go func() { _ = gs.Serve(lis) }()
	logger.Info("gRPC server listening", "addr", cfg.addr)

	var stopErr error
	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received; draining servers", "grace", shutdownGracePeriod)
	case <-cl.Done():
		stopErr = fmt.Errorf("raft node stopped: %w", cl.Err())
	}
	drainServers(logger, httpSrvs, gs)
	return stopErr
}

// advertised turns a listen address into one peers can hand to clients: a
// wildcard or empty host becomes this node's host from --peers.
func advertised(listen, selfHost string) string {
	if listen == "" {
		return ""
	}
	host, port, err := net.SplitHostPort(listen)
	if err != nil {
		return listen
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = selfHost
	}
	return net.JoinHostPort(host, port)
}
