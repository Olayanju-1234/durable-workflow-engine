"""End-to-end check of the SDK against a live engine, run by sdks/sdk_e2e_test.go:

    python3 e2e.py <follower-url> <leader-url>

The worker is given only the follower, so everything below also proves it
follows the leader hint. Prints PASS on success.
"""

import json
import os
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from workflowd import DEFAULT_VERSION, RetryPolicy, Worker  # noqa: E402

follower, leader = sys.argv[1], sys.argv[2]


def fail(msg):
    print(f"[py e2e] FAIL: {msg}", file=sys.stderr, flush=True)
    os._exit(1)


worker = Worker(addresses=[follower], worker_id="py-e2e")
flaky_calls = [0]


@worker.workflow("PyE2E")
def py_e2e(ctx):
    order = ctx.input_as("json")
    v = ctx.get_version("pricing-v2", DEFAULT_VERSION, 2)
    oid = ctx.side_effect("order-id", lambda: f"id-{order['n']}")
    flaky = ctx.activity("py-flaky", order, retry=RetryPolicy(max_attempts=3, initial_backoff=0.01), decode="json")
    binary = ctx.activity("py-binary", b"\xff\x00\x7f", decode="bytes")
    slow = ctx.activity("py-slow", "x")
    ctx.sleep("nap", 0.02)
    sig = ctx.wait_for_signal("wait", "go", decode="json")
    return {"v": v, "id": oid, "flaky": flaky, "bin": list(binary), "slow": slow, "sig": sig}


# Three activities at once (each waits until all three are running, so this
# hangs unless they really run in parallel), then a signal raced against a
# deadline that wins.
@worker.workflow("PyParallel")
def py_parallel(ctx):
    parts = ctx.wait_all(*[ctx.activity_async("py-part", n, decode="json") for n in (1, 2, 3)])
    reply = ctx.wait_for_signal_async("reply", "never-sent")
    deadline = ctx.sleep_async("deadline", 0.2)
    return {"parts": parts, "winner": ctx.wait_any(reply, deadline)}


parts_barrier = threading.Barrier(3, timeout=10)


@worker.activity("py-part", decode="json")
def py_part(n):
    parts_barrier.wait()
    return n * 10


@worker.workflow("PyAfterRestart")
def py_after_restart(ctx):
    return ctx.activity("py-flaky", {"n": 41}, decode="json")


@worker.activity("py-flaky", decode="json")
def py_flaky(order):
    if order["n"] == 1:
        flaky_calls[0] += 1
        if flaky_calls[0] < 3:
            raise RuntimeError("flaky")
    return {"ok": order["n"] + 1}


@worker.activity("py-binary", decode="bytes")
def py_binary(data):
    return bytes(reversed(data))


@worker.activity("py-slow")
def py_slow(_payload, info):
    # Longer than the engine's 600 ms lease: it only survives by heartbeating.
    time.sleep(1.5)
    return "lost" if info["lease_lost"].is_set() else "slow-done"


def wait_done(wid):
    for _ in range(300):
        wf = worker.describe(wid)
        if wf["status"] == "COMPLETED":
            return wf
        if wf["status"] not in ("RUNNING", "WAITING"):
            fail(f"workflow {wid} is {wf['status']}: {wf.get('error')}")
        time.sleep(0.05)
    fail(f"workflow {wid} did not complete")


worker.run(block=False)

wid = worker.start_workflow("PyE2E", {"n": 1})
if worker.address != leader:
    fail(f"worker uses {worker.address}, want the leader {leader}")
for i in range(300):
    if any(s["step_name"] == "wait" for s in worker.describe(wid)["steps"]):
        break
    time.sleep(0.05)
else:
    fail("workflow never reached its signal wait")
worker.signal(wid, "go", {"go": True})
wf = wait_done(wid)
want = {"v": 2, "id": "id-1", "flaky": {"ok": 2}, "bin": [0x7F, 0x00, 0xFF], "slow": "slow-done", "sig": {"go": True}}
if json.loads(wf["output"]) != want:
    fail(f"output {wf['output']}, want {want}")
steps = {s["step_name"]: s for s in wf["steps"]}
if steps["py-flaky"]["attempts"] != 3:
    fail(f"py-flaky attempts {steps['py-flaky']['attempts']}, want 3")
if steps["py-slow"]["attempts"] != 1:
    fail(f"py-slow attempts {steps['py-slow']['attempts']}, want 1 (heartbeats kept the lease)")

par = wait_done(worker.start_workflow("PyParallel"))
if json.loads(par["output"]) != {"parts": [10, 20, 30], "winner": 1}:
    fail(f"parallel: output {par['output']}")

# A restarted engine has forgotten every registration: the worker must notice
# and register again before this workflow can be dispatched.
print("RESTART", flush=True)
sys.stdin.readline()
after_id = None
for i in range(100):
    try:
        after_id = worker.start_workflow("PyAfterRestart")
        break
    except Exception as err:  # 400 until the worker has registered with the new engine
        if getattr(err, "status", None) != 400:
            fail(f"start after restart: {err}")
        time.sleep(0.05)
else:
    fail("the worker never registered with the restarted engine")
after = wait_done(after_id)
if after["output"] != '{"ok": 42}':
    fail(f"after restart: output {after['output']}")

print("PASS", flush=True)
os._exit(0)
