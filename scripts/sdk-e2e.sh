#!/usr/bin/env bash
# Runs one SDK's example worker against a freshly built engine and drives two
# orders through it: one approved (completes), one rejected (compensates, fails).
#
#   scripts/sdk-e2e.sh typescript|python|java
#
# E2E_SECURE=1 runs the same check over TLS (a throwaway CA) with a required
# token, and also checks that plaintext and token-less calls are refused.
# WORKFLOWD_BIN skips the build. Needs Go, curl and jq, plus node, python3 or a JDK.
set -euo pipefail

lang=${1:?usage: sdk-e2e.sh typescript|python|java}
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
http_port=${E2E_HTTP_PORT:-18233}
grpc_port=${E2E_GRPC_PORT:-17233}
scheme=http
curl_flags=()
engine_flags=()
worker_env=()
pids=()

cleanup() {
  for p in ${pids[@]+"${pids[@]}"}; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

label=$lang
log() { printf '[e2e:%s] %s\n' "$label" "$*"; }
fail() {
  log "FAIL: $*"
  log "--- engine log"; cat "$work/engine.log" || true
  log "--- worker log"; cat "$work/worker.log" || true
  exit 1
}
api() { curl -sS ${curl_flags[@]+"${curl_flags[@]}"} "$@"; }

bin=${WORKFLOWD_BIN:-}
if [ -z "$bin" ]; then
  bin="$work/workflowd"
  (cd "$root" && go build -o "$bin" ./cmd/workflowd)
fi

if [ "${E2E_SECURE:-}" = 1 ]; then
  label="$lang+tls"
  (cd "$root" && go run ./internal/tlsconfig/tlstest/gencert "$work")
  token="e2e-$RANDOM$RANDOM$RANDOM"
  (umask 077 && printf '%s\n' "$token" >"$work/token")
  scheme=https
  engine_flags=(--tls-cert "$work/server.pem" --tls-key "$work/server-key.pem" --auth-token-file "$work/token")
  curl_flags=(--cacert "$work/test-ca.pem" -H "Authorization: Bearer $token")
  worker_env=(WORKFLOWD_AUTH_TOKEN="$token" WORKFLOWD_CA_FILE="$work/test-ca.pem" NODE_EXTRA_CA_CERTS="$work/test-ca.pem")
fi
addr="$scheme://127.0.0.1:$http_port"

"$bin" --serve --data-dir "$work/data" --addr "127.0.0.1:$grpc_port" \
  --http-addr "127.0.0.1:$http_port" --metrics-addr "" ${engine_flags[@]+"${engine_flags[@]}"} >"$work/engine.log" 2>&1 &
pids+=($!)
for _ in $(seq 50); do api -f "$addr/api/health" >/dev/null 2>&1 && break; sleep 0.2; done
api -f "$addr/api/health" >/dev/null || fail "engine did not come up at $addr"

if [ "$scheme" = https ]; then
  code=$(curl -sS -o /dev/null -w '%{http_code}' --cacert "$work/test-ca.pem" "$addr/api/workflows") || true
  [ "$code" = 401 ] || fail "a request without the token got HTTP $code, want 401"
  if code=$(curl -sS -o /dev/null -w '%{http_code}' "http://127.0.0.1:$http_port/api/health" 2>/dev/null) && [ "$code" = 200 ]; then
    fail "the TLS listener answered plaintext HTTP"
  fi
  log "token-less and plaintext requests refused"
fi

# Each example polls its own task queue (ts-orders, py-orders, java-orders).
case "$lang" in
  typescript)
    type=TsOrder queue=ts-orders
    (cd "$root/sdks/typescript" && exec env WORKFLOWD_ADDR="$addr" ${worker_env[@]+"${worker_env[@]}"} node example.mjs) >"$work/worker.log" 2>&1 &
    ;;
  python)
    type=PyOrder queue=py-orders
    (cd "$root/sdks/python" && exec env WORKFLOWD_ADDR="$addr" ${worker_env[@]+"${worker_env[@]}"} python3 -u example.py) >"$work/worker.log" 2>&1 &
    ;;
  java)
    type=JavaOrder queue=java-orders
    javac --release 11 -Xlint:all,-serial -Werror -d "$work/classes" "$root/sdks/java/Workflowd.java" "$root/sdks/java/Example.java"
    (exec env WORKFLOWD_ADDR="$addr" ${worker_env[@]+"${worker_env[@]}"} java -cp "$work/classes" Example) >"$work/worker.log" 2>&1 &
    ;;
  *) echo "unknown SDK: $lang" >&2; exit 2 ;;
esac
pids+=($!)

# The type is unknown (400) until the worker has registered it.
start() {
  local code=""
  for _ in $(seq 100); do
    code=$(api -o "$work/start.json" -w '%{http_code}' -X POST -H 'Content-Type: application/json' \
      -d "{\"type\":\"$type\",\"input\":\"$1\",\"workflow_id\":\"$1\",\"task_queue\":\"$queue\"}" "$addr/api/workflows") || true
    [ "$code" = 201 ] && return 0
    sleep 0.3
  done
  fail "start $1: HTTP $code $(cat "$work/start.json" 2>/dev/null)"
}

# Signals sent before the workflow reaches its wait are buffered, so no race.
signal() {
  api -f -X POST -H 'Content-Type: application/json' \
    -d "{\"name\":\"approved\",\"payload\":\"$2\"}" "$addr/api/workflows/$1/signal" >/dev/null || fail "signal $1"
}

await() {
  local status=""
  for _ in $(seq 300); do
    api -f "$addr/api/workflows/$1" >"$work/wf.json" 2>/dev/null || true
    status=$(jq -r '.status // ""' "$work/wf.json" 2>/dev/null || true)
    case "$status" in COMPLETED | FAILED | TERMINATED) break ;; esac
    sleep 0.2
  done
  [ "$status" = "$2" ] || fail "$1 ended $status, want $2: $(cat "$work/wf.json")"
}

start "$lang-ok"
signal "$lang-ok" yes
await "$lang-ok" COMPLETED
out=$(jq -r .output "$work/wf.json")
[ "$out" = "shipped(reserved($lang-ok))" ] || fail "$lang-ok output = $out"
jq -e --arg q "$queue" '.task_queue == $q and all(.steps[] | select(.kind == "ACTIVITY"); .task_queue == $q)' "$work/wf.json" >/dev/null ||
  fail "$lang-ok did not run on task queue $queue: $(cat "$work/wf.json")"
log "approved order completed on task queue $queue: $out"

start "$lang-no"
signal "$lang-no" no
await "$lang-no" FAILED
jq -e '.error | contains("order rejected: no")' "$work/wf.json" >/dev/null || fail "$lang-no error: $(jq -r .error "$work/wf.json")"
jq -e '[.steps[] | select(.step_name == "release" and .status == "COMPLETED")] | length == 1' "$work/wf.json" >/dev/null ||
  fail "$lang-no: compensation (release) did not run once"
log "rejected order failed after compensation: $(jq -r .error "$work/wf.json")"
log PASS
