package server

import (
	"errors"
	"fmt"
)

// ErrNotLeader means this node can't serve workflow requests: in a cluster
// only the leader runs an engine. Single-node mode never returns it.
var ErrNotLeader = errors.New("server: not the leader")

// ErrEngineClosed is returned to calls still waiting on an engine that has
// been shut down, which in a cluster means its leadership ended.
var ErrEngineClosed = fmt.Errorf("%w: engine closed", ErrNotLeader)

// LeaderHint tells a client where to retry. Empty fields mean unknown, for
// example while an election is under way.
type LeaderHint struct {
	ID       string
	GRPCAddr string
	HTTPAddr string
}

// NotLeaderError is ErrNotLeader plus where the leader is.
type NotLeaderError struct {
	Leader LeaderHint
}

func (e *NotLeaderError) Error() string {
	if e.Leader.ID == "" {
		return "server: not the leader, and no leader is known yet; retry shortly"
	}
	return fmt.Sprintf("server: not the leader; leader is %s (gRPC %s, HTTP %s)",
		e.Leader.ID, e.Leader.GRPCAddr, e.Leader.HTTPAddr)
}

func (e *NotLeaderError) Is(target error) bool { return target == ErrNotLeader }

// EngineSource resolves the engine that serves requests on this node. In
// single-node mode that is always the same engine; in a cluster it is the
// current leadership's engine, or a *NotLeaderError on any other node.
type EngineSource interface {
	Engine() (*Engine, error)
}

type staticSource struct{ e *Engine }

func (s staticSource) Engine() (*Engine, error) { return s.e, nil }

// leaderHint finds out where a client should go after err. An engine call
// that failed because leadership changed mid-flight carries no hint, so the
// source is asked again: by then it knows the new leader, or that none is
// elected yet.
func leaderHint(src EngineSource, err error) LeaderHint {
	var nle *NotLeaderError
	if errors.As(err, &nle) {
		return nle.Leader
	}
	if _, err := src.Engine(); errors.As(err, &nle) {
		return nle.Leader
	}
	return LeaderHint{}
}
