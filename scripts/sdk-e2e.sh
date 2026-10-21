#!/usr/bin/env bash
# Runs one SDK's example worker against a freshly built engine and drives two
# orders through it: one approved (completes), one rejected (compensates, fails).
#
#   scripts/sdk-e2e.sh typescript|python|java
#
# WORKFLOWD_BIN skips the build; E2E_ENGINE_FLAGS adds engine flags; E2E_SCHEME,
# E2E_CURL_FLAGS and E2E_WORKER_ENV let a caller run the same check over TLS
# with a token. Needs curl and jq, plus node, python3 or a JDK.
set -euo pipefail

lang=${1:?usage: sdk-e2e.sh typescript|python|java}
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
http_port=${E2E_HTTP_PORT:-18233}
grpc_port=${E2E_GRPC_PORT:-17233}
addr="${E2E_SCHEME:-http}://127.0.0.1:$http_port"
read -r -a curl_flags <<<"${E2E_CURL_FLAGS:-}"
read -r -a engine_flags <<<"${E2E_ENGINE_FLAGS:-}"
read -r -a worker_env <<<"${E2E_WORKER_ENV:-}"
pids=()

cleanup() {
  for p in ${pids[@]+"${pids[@]}"}; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$work"
}
trap cleanup EXIT

log() { printf '[e2e:%s] %s\n' "$lang" "$*"; }
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

"$bin" --serve --data-dir "$work/data" --addr "127.0.0.1:$grpc_port" \
  --http-addr "127.0.0.1:$http_port" --metrics-addr "" ${engine_flags[@]+"${engine_flags[@]}"} >"$work/engine.log" 2>&1 &
pids+=($!)
for _ in $(seq 50); do api -f "$addr/api/health" >/dev/null 2>&1 && break; sleep 0.2; done
api -f "$addr/api/health" >/dev/null || fail "engine did not come up at $addr"

case "$lang" in
  typescript)
    type=TsOrder
    (cd "$root/sdks/typescript" && exec env WORKFLOWD_ADDR="$addr" ${worker_env[@]+"${worker_env[@]}"} node example.mjs) >"$work/worker.log" 2>&1 &
    ;;
  python)
    type=PyOrder
    (cd "$root/sdks/python" && exec env WORKFLOWD_ADDR="$addr" ${worker_env[@]+"${worker_env[@]}"} python3 -u example.py) >"$work/worker.log" 2>&1 &
    ;;
  java)
    type=JavaOrder
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
      -d "{\"type\":\"$type\",\"input\":\"$1\",\"workflow_id\":\"$1\"}" "$addr/api/workflows") || true
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
log "approved order completed: $out"

start "$lang-no"
signal "$lang-no" no
await "$lang-no" FAILED
jq -e '.error | contains("order rejected: no")' "$work/wf.json" >/dev/null || fail "$lang-no error: $(jq -r .error "$work/wf.json")"
jq -e '[.steps[] | select(.step_name == "release" and .status == "COMPLETED")] | length == 1' "$work/wf.json" >/dev/null ||
  fail "$lang-no: compensation (release) did not run once"
log "rejected order failed after compensation: $(jq -r .error "$work/wf.json")"
log PASS
