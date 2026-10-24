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
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from workflowd import (  # noqa: E402
    DEFAULT_VERSION,
    ActivityFailure,
    CancelledFailure,
    NonRetryableError,
    RetryPolicy,
    Worker,
)

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


# Timeouts: an attempt that outlives its start-to-close fails (and the
# activity's lease_lost event is set), and a workflow past its execution timeout ends.
@worker.workflow("PyTimeouts")
def py_timeouts(ctx):
    try:
        ctx.activity("py-stall", "", start_to_close_timeout=0.3, schedule_to_close_timeout=5)
        return "finished"
    except ActivityFailure as err:
        return f"failed: {err}"


stall_stopped = threading.Event()


@worker.activity("py-stall")
def py_stall(_payload, info):
    if not info["lease_lost"].wait(5):
        return "never told to stop"
    stall_stopped.set()
    raise RuntimeError("stopped at the start-to-close timeout")


@worker.workflow("PyForever")
def py_forever(ctx):
    return ctx.wait_for_signal("never", "never-sent")


# Cancellation: liftoff's custom-step race with the loser cancelled, so a late
# callback reaches the next wait; and a cancelled workflow tells its activity.
@worker.workflow("PyRace")
def py_race(ctx):
    callback = ctx.wait_for_signal_async("await-callback", "callback")
    deadline = ctx.sleep_async("deadline", 0.1)
    if ctx.wait_any(callback, deadline, cancel_rest=True) == 0:
        return "called back"
    try:
        callback.result()
    except CancelledFailure:
        pass
    return "late: " + ctx.wait_for_signal("late-callback", "callback")


@worker.workflow("PyProvision")
def py_provision(ctx):
    return ctx.activity("py-provision")


provision_started = threading.Event()
provision_cancelled = threading.Event()


@worker.activity("py-provision")
def py_provision_activity(_payload, info):
    provision_started.set()
    if info["cancelled"].wait(5):
        provision_cancelled.set()
    raise RuntimeError("stopped")


# Non-retryable failures: a flagged 404 and a policy-listed BadRequest fail at
# once, a 503 retries; against an older engine the failure still arrives.
class BadRequest(Exception):
    pass


webhook_calls = {}


@worker.workflow("PyWebhook")
def py_webhook(ctx):
    legacy = ctx.input == "legacy"
    try:
        ctx.activity(
            "py-legacy" if legacy else "py-webhook",
            ctx.input,
            retry=RetryPolicy(max_attempts=1 if legacy else 4, initial_backoff=0.01, non_retryable_error_types=("BadRequest",)),
        )
        return "delivered"
    except ActivityFailure as err:
        return f"{err.type}|{err.non_retryable}|{err}"


def webhook(code):
    webhook_calls[code] = webhook_calls.get(code, 0) + 1
    if code == "404":
        raise NonRetryableError("webhook answered 404")
    if code in ("400", "legacy"):
        raise BadRequest("webhook answered 400")
    raise RuntimeError("webhook answered 503")


worker.activity("py-webhook")(webhook)
worker.activity("py-legacy")(webhook)


# The deterministic clock and random numbers read the same on every replay
# (side_effect records the first run's values to compare against), the clock
# moves with results, and a signal wait with a timeout gives up or gets one.
@worker.workflow("PyClock")
def py_clock(ctx):
    start, r = ctx.now(), ctx.random()
    first = ctx.side_effect("first", lambda: json.dumps([start.isoformat(), r]))
    ctx.activity("py-tick")
    after = ctx.now()
    ctx.sleep_until("until", after + timedelta(milliseconds=50))
    slept = ctx.now()
    # The timed-out wait is cancelled, so the "reply" sent later goes to the second wait.
    late = ctx.wait_for_signal("maybe", "reply", timeout=0.1)
    timed_out = ctx.now()
    got = ctx.wait_for_signal("reply", "reply", timeout=10)
    return {
        "replayed_same": json.loads(first) == [start.isoformat(), r],
        "moved": after > start and slept >= after + timedelta(milliseconds=50),
        "timeout_moved_clock": timed_out >= slept + timedelta(milliseconds=100),
        "late": late,
        "got": got,
        "second_random_differs": ctx.random() != r,
    }


worker.activity("py-tick")(lambda _payload: time.sleep(0.02) or "tick")


# Continue-as-new: a loop keeps its history small by starting a fresh run
# whenever the engine suggests it (the harness's engine allows 60 events).
@worker.workflow("PyLoop")
def py_loop(ctx):
    total = int(ctx.input or "0")
    while not ctx.continue_as_new_suggested():
        if total >= 50:
            return f"{total} ticks; run {ctx.run_id}"
        ctx.activity("py-tick")
        total += 1
    return ctx.continue_as_new(str(total))


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

bounded = wait_done(worker.start_workflow("PyTimeouts"))
if "start-to-close" not in bounded["output"] or not stall_stopped.wait(2):
    fail(f"timeouts: output {bounded['output']}, stopped={stall_stopped.is_set()}")
for code, want, calls in (
    ("404", "NonRetryableError|True|webhook answered 404", 1),
    ("400", "BadRequest|False|webhook answered 400", 1),
    ("503", "RuntimeError|False|webhook answered 503", 4),
    ("legacy", "|False|webhook answered 400", 1),  # the older engine never saw the type
):
    hook = wait_done(worker.start_workflow("PyWebhook", code))
    if hook["output"] != want or webhook_calls.get(code) != calls:
        fail(f"webhook {code}: output {hook['output']} after {webhook_calls.get(code)} calls, want {want} after {calls}")

race_id = worker.start_workflow("PyRace")
for _ in range(100):
    if any(s["step_name"] == "late-callback" for s in worker.describe(race_id)["steps"]):
        break
    time.sleep(0.05)
else:
    fail("race: never reached the second wait")
worker.signal(race_id, "callback", "hello")
race = wait_done(race_id)
race_steps = " ".join(f"{s['step_name']}={s['status']}" for s in race["steps"])
if race["output"] != "late: hello" or race_steps != "await-callback=CANCELLED deadline=COMPLETED late-callback=COMPLETED":
    fail(f"race: output {race['output']}, steps {race_steps}")
provision_id = worker.start_workflow("PyProvision")
if not provision_started.wait(5):
    fail("the provision activity never started")
worker.cancel(provision_id, "tenant deleted")
if not provision_cancelled.wait(5) or worker.describe(provision_id)["status"] != "CANCELLED":
    fail("the cancelled workflow's activity was never told")

clock_id = worker.start_workflow("PyClock")
for _ in range(200):
    if any(s["step_name"] == "reply" for s in worker.describe(clock_id)["steps"]):
        break
    time.sleep(0.05)
else:
    fail("clock: never reached the reply wait")
worker.signal(clock_id, "reply", "hi")
clock = wait_done(clock_id)
want = {
    "replayed_same": True,
    "moved": True,
    "timeout_moved_clock": True,
    "late": None,
    "got": "hi",
    "second_random_differs": True,
}
if json.loads(clock["output"]) != want:
    fail(f"clock: output {clock['output']}, want {want}")
clock_steps = " ".join(f"{s['step_name']}={s['status']}" for s in clock["steps"])
if "maybe=CANCELLED maybe.timeout=COMPLETED" not in clock_steps:
    fail(f"clock: the timed-out wait was not cancelled: {clock_steps}")

loop_id = worker.start_workflow("PyLoop", "0")
loop = wait_done(loop_id)
runs = worker.runs(loop_id)
earlier = worker.describe(loop_id, run_id=runs[-1]["run_id"])
if (
    loop["output"] != f"50 ticks; run {loop['run_id']}"
    or len(runs) != 3
    or loop["history_length"] > 60
    or any(r["status"] != "CONTINUED_AS_NEW" for r in runs[1:])
    or loop.get("continued_from_run_id") != runs[1]["run_id"]
    or earlier["input"] != "0"
    or earlier.get("continued_as_new_run_id") != runs[1]["run_id"]
):
    fail(f"continue-as-new: output {loop['output']}, {loop['history_length']} events, runs {runs}")

# Workflow id reuse: a finished id is refused by default, returned with
# USE_EXISTING, and starts a new run when the reuse policy allows it.
first = worker.start("PyAfterRestart", "", "py-reuse")
wait_done("py-reuse")
try:
    worker.start("PyAfterRestart", "", "py-reuse")
    fail("reuse: a finished id was started again by default")
except Exception as err:
    if getattr(err, "status", None) != 409:
        fail(f"reuse: {err}")
same = worker.start("PyAfterRestart", "", "py-reuse", id_conflict_policy="use_existing")
again = worker.start("PyAfterRestart", "", "py-reuse", id_reuse_policy="allow_duplicate")
wait_done("py-reuse")
if same != {"workflow_id": "py-reuse", "run_id": first["run_id"], "started": False} or not again["started"] or (
    [r["run_id"] for r in worker.runs("py-reuse")] != [again["run_id"], first["run_id"]]
):
    fail(f"reuse: first {first}, same {same}, again {again}")

forever_id = worker.start_workflow("PyForever", execution_timeout=0.3)
for _ in range(100):
    wf = worker.describe(forever_id)
    if wf["status"] == "TIMED_OUT":
        break
    if wf["status"] not in ("RUNNING", "WAITING"):
        fail(f"execution timeout: {wf['status']}")
    time.sleep(0.05)
else:
    fail("the workflow never timed out")

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
