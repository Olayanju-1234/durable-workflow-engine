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

For an engine with auth and TLS: `Worker(address="https://…:8233", token=..., ca_file=...)`
(`ca_file` only for a private CA).

Workflow code must be deterministic: no clocks, randomness or I/O except through `ctx`.
How replay works, and the protocol, are in the
[SDK guide](https://github.com/Olayanju-1234/durable-workflow-engine/tree/main/sdks#readme).
Pre-alpha: the API may change. Apache-2.0.
