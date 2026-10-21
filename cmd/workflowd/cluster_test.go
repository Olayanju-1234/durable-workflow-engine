package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseClusterFlags(t *testing.T) {
	const peers = "n1=127.0.0.1:7333, n2=127.0.0.1:7334,n3=127.0.0.1:7335"

	if _, ok, err := parseClusterFlags("", "", "", time.Second); ok || err != nil {
		t.Fatalf("no flags: ok=%v err=%v, want single-node mode", ok, err)
	}

	cc, ok, err := parseClusterFlags("n2", peers, "", time.Second)
	if err != nil || !ok {
		t.Fatalf("valid flags: ok=%v err=%v", ok, err)
	}
	if cc.raftAddr != "127.0.0.1:7334" {
		t.Errorf("raft addr defaults to this node's peer entry: got %q", cc.raftAddr)
	}
	if got := strings.Join(cc.members(), ","); got != "n1,n2,n3" {
		t.Errorf("members = %s", got)
	}
	if cc, _, _ := parseClusterFlags("n2", peers, "0.0.0.0:7334", time.Second); cc.raftAddr != "0.0.0.0:7334" {
		t.Errorf("explicit --raft-addr ignored: %q", cc.raftAddr)
	}

	bad := []struct {
		name                    string
		nodeID, peers, raftAddr string
		tick                    time.Duration
		wantErr                 string
	}{
		{"node id without peers", "n1", "", "", time.Second, "need --peers"},
		{"peers without node id", "", peers, "", time.Second, "needs --node-id"},
		{"node not in peers", "n9", peers, "", time.Second, "not listed"},
		{"malformed entry", "n1", "n1=127.0.0.1:1,n2", "", time.Second, "want id=host:port"},
		{"missing port", "n1", "n1=localhost", "", time.Second, "missing port"},
		{"duplicate id", "n1", "n1=a:1,n1=b:2", "", time.Second, "twice"},
		{"zero tick", "n1", peers, "", 0, "positive"},
	}
	for _, tt := range bad {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := parseClusterFlags(tt.nodeID, tt.peers, tt.raftAddr, tt.tick)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("got %v, want an error containing %q", err, tt.wantErr)
			}
		})
	}
}
