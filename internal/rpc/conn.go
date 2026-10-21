// Package rpc is the connection the public client and worker share: gRPC stubs
// for every known workflowd node, and a call wrapper that follows the leader
// hints cluster followers return (ADR-0004, ADR-0005).
package rpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/olayanju-1234/durable-workflow-engine/internal/server"
)

// Retry budget for calls turned away with Unavailable: long enough to ride out
// an election (about a second in practice), short enough to fail visibly.
const (
	maxAttempts    = 12
	initialBackoff = 50 * time.Millisecond
	maxBackoff     = time.Second
)

// ConnOf returns the Conn behind a *client.Client. Package client sets it, so
// package worker can share a client's connection without client exporting it.
var ConnOf func(c any) *Conn

// Conn talks to whichever node currently serves workflow requests.
type Conn struct {
	dialOpts []grpc.DialOption
	log      *slog.Logger

	mu      sync.Mutex
	addrs   []string // seed addresses plus learned leaders, in rotation order
	current int
	conns   map[string]*grpc.ClientConn
	epoch   uint64
	closed  bool
}

// New prepares a connection to addrs. Nothing is dialed until the first call.
func New(addrs []string, dialOpts []grpc.DialOption, logger *slog.Logger) (*Conn, error) {
	if len(addrs) == 0 {
		return nil, errors.New("rpc: at least one address is required")
	}
	for _, a := range addrs {
		if a == "" {
			return nil, errors.New("rpc: empty address")
		}
	}
	return &Conn{
		dialOpts: dialOpts,
		log:      logger,
		addrs:    append([]string(nil), addrs...),
		conns:    make(map[string]*grpc.ClientConn),
	}, nil
}

// Epoch counts the times a call was turned away (a node down or restarting, a
// follower answering, a leader deposed). A worker re-registers its workflow
// types when it changes, since every new engine starts with none.
func (c *Conn) Epoch() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.epoch
}

// Close closes every underlying gRPC connection.
func (c *Conn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	var errs []error
	for _, cc := range c.conns {
		errs = append(errs, cc.Close())
	}
	clear(c.conns)
	return errors.Join(errs...)
}

// CallFunc is one RPC against a stub. It must pass opt to the stub method so
// the leader-hint trailers can be read.
type CallFunc func(ctx context.Context, stub server.WorkflowServiceClient, opt grpc.CallOption) error

// Call runs fn, retrying on another node while the cluster turns it away. It
// returns the number of attempts made along with the result.
func (c *Conn) Call(ctx context.Context, fn CallFunc) (attempts int, err error) {
	backoff := initialBackoff
	for attempts = 1; ; attempts++ {
		err = c.Once(ctx, fn)
		if status.Code(err) != codes.Unavailable || attempts >= maxAttempts {
			return attempts, err
		}
		select {
		case <-ctx.Done():
			return attempts, err
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, maxBackoff)
	}
}

// Once runs fn a single time. If the node turns the call away it moves to the
// leader it names, or to the next known node, so the caller's retry goes there.
func (c *Conn) Once(ctx context.Context, fn CallFunc) error {
	addr, stub, err := c.stub()
	if err != nil {
		return err
	}
	var trailer metadata.MD
	err = fn(ctx, stub, grpc.Trailer(&trailer))
	if status.Code(err) == codes.Unavailable && ctx.Err() == nil {
		c.redirect(addr, first(trailer.Get(server.TrailerLeaderGRPC)))
	}
	return err
}

func (c *Conn) stub() (string, server.WorkflowServiceClient, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return "", nil, errors.New("rpc: connection closed")
	}
	addr := c.addrs[c.current]
	cc, ok := c.conns[addr]
	if !ok {
		var err error
		if cc, err = grpc.NewClient(addr, c.dialOpts...); err != nil {
			return "", nil, fmt.Errorf("rpc: dial %s: %w", addr, err)
		}
		c.conns[addr] = cc
	}
	return addr, server.NewWorkflowServiceClient(cc), nil
}

// redirect moves off a node that turned a call away: to the hinted leader if
// there is one, otherwise round-robin to the next known address.
func (c *Conn) redirect(from, leader string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.epoch++
	if c.addrs[c.current] != from {
		return // another call already moved on
	}
	if leader != "" && leader != from {
		for i, a := range c.addrs {
			if a == leader {
				c.current = i
				c.log.Debug("following leader hint", "from", from, "to", leader)
				return
			}
		}
		c.addrs = append(c.addrs, leader)
		c.current = len(c.addrs) - 1
		c.log.Debug("following leader hint to a new address", "from", from, "to", leader)
		return
	}
	c.current = (c.current + 1) % len(c.addrs)
}

func first(vals []string) string {
	if len(vals) == 0 {
		return ""
	}
	return vals[0]
}
