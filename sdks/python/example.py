"""Runnable example: an order workflow written entirely in Python,
orchestrated by a Go engine that has never seen this code.

    1. start the engine:  ./workflowd --serve
    2. run this:          python3 example.py
"""

import os

from workflowd import ActivityFailure, Worker, WorkflowFailure

ADDRESS = os.environ.get("WORKFLOWD_ADDR", "http://localhost:8233")
worker = Worker(
    address=ADDRESS,
    worker_id="py-example",
    token=os.environ.get("WORKFLOWD_AUTH_TOKEN"),  # for an engine with auth on
    ca_file=os.environ.get("WORKFLOWD_CA_FILE"),  # for a private CA over https
)


# ---- the workflow: ordinary Python ----
@worker.workflow("PyOrder")
def py_order(ctx):
    reserved = ctx.activity("reserve", ctx.input)

    ctx.sleep("settle", 0.3)  # durable: survives an engine restart

    approval = ctx.wait_for_signal("await-approval", "approved")
    if approval != "yes":
        ctx.activity("release", reserved)
        raise WorkflowFailure(f"order rejected: {approval}")

    try:
        ctx.activity("charge", reserved)
    except ActivityFailure as err:
        ctx.activity("release", reserved)  # Saga compensation
        raise WorkflowFailure(f"charge failed, reservation released: {err}")

    return ctx.activity("ship", reserved)


# ---- the activities: where real side effects happen ----
@worker.activity("reserve")
def reserve(payload):
    return f"reserved({payload})"


@worker.activity("charge")
def charge(payload):
    return f"charged({payload})"


@worker.activity("ship")
def ship(payload):
    return f"shipped({payload})"


@worker.activity("release")
def release(payload):
    return f"released({payload})"


if __name__ == "__main__":
    print(f'[py] worker running against {ADDRESS}; workflow type "PyOrder" registered')
    worker.run()
