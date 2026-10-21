#!/usr/bin/env bash
# Runs a 3-node workflowd cluster on localhost until Ctrl-C.
#
#   node  raft   gRPC   HTTP   metrics
#   n1    7333   7233   8233   9090
#   n2    7334   7234   8234   9091
#   n3    7335   7235   8235   9092
#
# Data and logs live under data/cluster/<node>/. The leader runs the engine and
# the in-process demo worker; followers answer API calls with 503 and the
# leader's address. `curl localhost:8233/api/health` shows each node's role,
# term and leader. Kill the leader's process to watch another take over.
set -euo pipefail
cd "$(dirname "$0")/.."

go build -o bin/workflowd ./cmd/workflowd
PEERS="n1=127.0.0.1:7333,n2=127.0.0.1:7334,n3=127.0.0.1:7335"
pids=()
cleanup() {
  kill "${pids[@]}" 2>/dev/null || true
  wait 2>/dev/null || true
  echo "cluster stopped"
}
trap cleanup EXIT

for i in 1 2 3; do
  dir="data/cluster/n$i"
  mkdir -p "$dir"
  bin/workflowd --serve --node-id "n$i" --peers "$PEERS" --data-dir "$dir" \
    --addr "127.0.0.1:723$((2 + i))" --http-addr "127.0.0.1:823$((2 + i))" \
    --metrics-addr "127.0.0.1:909$((i - 1))" --demo-worker >"$dir/workflowd.log" 2>&1 &
  pids+=($!)
  echo "n$i  pid $!  health http://127.0.0.1:823$((2 + i))/api/health  log $dir/workflowd.log"
done
echo "Ctrl-C stops all three."
wait || true
