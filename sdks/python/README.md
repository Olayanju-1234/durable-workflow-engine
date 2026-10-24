# workflowd (Python SDK)

Write durable workflows in plain Python and run them on
[workflowd](https://github.com/Olayanju-1234/durable-workflow-engine), a single-binary
durable workflow engine. Zero dependencies: the standard library only.

```python
from workflowd import ActivityFailure, Worker, WorkflowFailure

worker = Worker(address="http://localhost:8233")

@worker.workflow("Order")
def order(ctx):
    reserved = ctx.activity("reserve", ctx.input)
    ctx.sleep("settle", 2.0)  # durable: survives an engine restart
    try:
        ctx.activity("charge", reserved)
    except ActivityFailure as err:
        ctx.activity("release", reserved)  # compensate
        raise WorkflowFailure(f"charge failed: {err}")
    return ctx.activity("ship", reserved)

@worker.activity("reserve")
def reserve(payload):
    return f"reserved({payload})"

worker.run()
```

More of the API:

```python
# Run activities in parallel; race a signal against a deadline.
stock, price = ctx.wait_all(ctx.activity_async("stock", sku), ctx.activity_async("price", sku))
late = ctx.wait_any(ctx.wait_for_signal_async("reply", "approved"), ctx.sleep_async("sla", 3600), cancel_rest=True) == 1

# Retry a failed attempt before the workflow sees the error; JSON and bytes payloads.
receipt = ctx.activity("charge", {"amount": 42}, retry=RetryPolicy(max_attempts=3, initial_backoff=0.5), decode="json")

# Some failures are not worth retrying: raise NonRetryableError in the activity, or name
# exception classes in the policy: RetryPolicy(max_attempts=5, non_retryable_error_types=("ValueError",)).

# Bound one attempt and the whole step (seconds); bound a whole run when you start it.
ctx.activity("render", doc, start_to_close_timeout=30, schedule_to_close_timeout=300)
worker.start_workflow("Order", order, f"order-{order['id']}", execution_timeout=86400)
# Idempotent start: the run already holding the id comes back instead of a 409.
run = worker.start("Order", order, f"order-{order['id']}", id_conflict_policy="USE_EXISTING")

# Change code safely while executions are in flight; record a non-deterministic value once.
if ctx.get_version("fraud-check", DEFAULT_VERSION, 1) == 1:
    ctx.activity("fraud-check", receipt)
order_id = ctx.side_effect("id", lambda: str(uuid.uuid4()))

# A loop that runs forever keeps its history small: a fresh run, same workflow id.
if ctx.continue_as_new_suggested():
    return ctx.continue_as_new(state)

# A deterministic clock and random numbers (the same on every replay); wait with a timeout.
deadline = ctx.now() + timedelta(days=2)
ctx.sleep_until("reminder", deadline)
jitter = ctx.random()
reply = ctx.wait_for_signal("reply", "approved", timeout=3600)  # None if none arrives in time
```

Cancel gracefully with `worker.cancel(id, reason, graceful=True)`: the step the workflow waits
on raises `CancelledFailure`, steps it starts afterwards run normally (compensate there), and
re-raising it ends the workflow `CANCELLED`; `ctx.cancel_requested` says it was told.

A cluster: `Worker(addresses=["http://n1:8233", "http://n2:8233", "http://n3:8233"])` follows
the leader. Activities longer than the lease heartbeat automatically; an activity taking a
second `info` argument can check `info["lease_lost"]`.

For an engine with auth and TLS: `Worker(address="https://…:8233", token=..., ca_file=...)`
(`ca_file` only for a private CA). With a namespace's token the worker works in that namespace;
with the engine's token, pick one with `namespace="acme"` (engine 0.4.0+).

Task queues (engine 0.4.0+): `Worker(task_queue="billing")` polls only the `billing` queue, and
its `start()` calls run workflows there; their activities run there too unless a call says
`ctx.activity("send", payload, task_queue="email")`. Without one, everything uses the default
queue.

Workflow code must be deterministic: no clocks, randomness or I/O except through `ctx`
(`ctx.now()`, `ctx.random()`).
How replay works, and the protocol, are in the
[SDK guide](https://github.com/Olayanju-1234/durable-workflow-engine/tree/main/sdks#readme).
Pre-alpha: the API may change. Apache-2.0.
