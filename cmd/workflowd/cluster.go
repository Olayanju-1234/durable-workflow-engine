package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"github.com/olayanju-1234/durable-workflow-engine/internal/raft"
	"github.com/olayanju-1234/durable-workflow-engine/internal/version"
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

// runCluster runs this node as a member of a Raft cluster until SIGINT/SIGTERM.
// For now the node takes part in elections only: the workflow API is not
// replicated yet, so it is not served in this mode.
func runCluster(baseLogger *slog.Logger, dataDir string, serve serveConfig, cc clusterConfig) error {
	logger := baseLogger.With("node_id", cc.nodeID) // package raft tags its own lines
	logger.Warn("cluster mode is experimental: this node runs Raft only and does not serve the workflow API yet")

	storage, err := raft.OpenDiskStorage(filepath.Join(dataDir, "raft"))
	if err != nil {
		return err
	}
	transport, err := raft.NewGRPCTransport(cc.nodeID, cc.peers, baseLogger)
	if err != nil {
		return err
	}
	defer transport.Close()
	node, err := raft.NewNode(raft.Config{
		ID: cc.nodeID, Members: cc.members(), Storage: storage, Transport: transport,
		Logger: baseLogger, TickInterval: cc.tick,
	})
	if err != nil {
		return err
	}

	lis, err := net.Listen("tcp", cc.raftAddr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", cc.raftAddr, err)
	}
	gs := grpc.NewServer()
	raft.RegisterServer(gs, node)
	go func() { _ = gs.Serve(lis) }()
	defer gs.Stop()
	node.Start()
	logger.Info("raft listening", "addr", cc.raftAddr, "members", cc.members(), "version", version.Version)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if serve.httpAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /api/health", func(w http.ResponseWriter, _ *http.Request) {
			writeHealth(w, node.Status())
		})
		srv := startHTTP(ctx, serve.httpAddr, mux, "health", logger)
		defer srv.Close()
		logger.Info("health endpoint listening", "url", browseURL(serve.httpAddr)+"/api/health")
	}

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received; stopping raft node")
		return node.Stop()
	case <-node.Done():
		return fmt.Errorf("raft node stopped: %w", node.Err())
	}
}

func writeHealth(w http.ResponseWriter, st raft.Status) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":  "ok",
		"version": version.Version,
		"cluster": map[string]any{
			"node_id": st.ID, "role": st.Role.String(), "term": st.Term, "leader_id": st.Leader,
		},
	})
}
