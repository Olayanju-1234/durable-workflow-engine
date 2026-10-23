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

# Change code safely while executions are in flight; record a non-deterministic value once.
if ctx.get_version("fraud-check", DEFAULT_VERSION, 1) == 1:
    ctx.activity("fraud-check", receipt)
order_id = ctx.side_effect("id", lambda: str(uuid.uuid4()))
```

A cluster: `Worker(addresses=["http://n1:8233", "http://n2:8233", "http://n3:8233"])` follows
the leader. Activities longer than the lease heartbeat automatically; an activity taking a
second `info` argument can check `info["lease_lost"]`.

For an engine with auth and TLS: `Worker(address="https://…:8233", token=..., ca_file=...)`
(`ca_file` only for a private CA).

Workflow code must be deterministic: no clocks, randomness or I/O except through `ctx`.
How replay works, and the protocol, are in the
[SDK guide](https://github.com/Olayanju-1234/durable-workflow-engine/tree/main/sdks#readme).
Pre-alpha: the API may change. Apache-2.0.
