"""workflowd — Python SDK.

Write durable workflows in Python. Zero dependencies: this module uses only the
standard library (urllib, json, threading), so there is no pip install and no
protobuf toolchain.

The model in one paragraph: your workflow function is re-run from the top every
time the engine needs a decision. Each durable call (``ctx.activity``,
``ctx.sleep``, ``ctx.wait_for_signal``) is matched against what already
happened. Completed steps return their recorded result instantly; the first
step with no result yet becomes the command sent back to the engine. The
function runs many times, but each side effect happens exactly once.

Usage::

    from workflowd import Worker, WorkflowFailure, ActivityFailure

    worker = Worker(address="http://localhost:8233")

    @worker.workflow("OrderWorkflow")
    def order_workflow(ctx):
        reserved = ctx.activity("reserve", ctx.input)
        ctx.sleep("settle", 2.0)
        try:
            ctx.activity("charge", reserved)
        except ActivityFailure as err:
            ctx.activity("release", reserved)      # compensate
            raise WorkflowFailure(f"charge failed: {err}")
        return ctx.activity("ship", reserved)

    @worker.activity("reserve")
    def reserve(payload):
        return f"reserved:{payload}"

    worker.run()

Workflow code must be deterministic: no clocks, no randomness, no I/O except
through ``ctx``. Anything else will diverge on replay.
"""

from __future__ import annotations

import json
import random
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, List, Optional

__all__ = [
    "Worker",
    "Context",
    "WorkflowFailure",
    "ActivityFailure",
    "decide",
]

SDK_VERSION = "python/0.1.0"


class WorkflowFailure(Exception):
    """Raise inside a workflow to fail it deliberately."""


class ActivityFailure(Exception):
    """Raised when an activity recorded a failure. Catch it to compensate."""

    def __init__(self, step_name: str, message: str):
        super().__init__(message)
        self.step_name = step_name


class _Yield(BaseException):
    """Internal control-flow signal: replay reached a step with no result yet.

    Deliberately derived from BaseException, not Exception, so a workflow's
    ``except Exception`` around an activity cannot accidentally swallow it.
    """

    def __init__(self, command: Optional[dict]):
        super().__init__("workflowd internal yield")
        self.command = command


class _NonDeterminism(Exception):
    """The workflow code no longer matches the recorded history."""


class Context:
    """What a workflow function receives. Every method is durable."""

    def __init__(self, task: dict):
        self.task = task
        self.input: str = task.get("input") or ""
        self.workflow_id: str = task.get("workflow_id", "")
        self._steps: List[dict] = task.get("steps") or []
        self._signals: List[dict] = task.get("signals") or []
        self._next = 0

    def activity(self, name: str, payload: Any = "") -> str:
        """Run a named activity on a worker and return its output."""
        return self._command(
            "ACTIVITY",
            name,
            {"type": "schedule_activity", "step_name": name, "input": _as_str(payload)},
        )

    def sleep(self, name: str, seconds: float) -> str:
        """Pause durably. Survives engine restarts."""
        return self._command(
            "TIMER",
            name,
            {"type": "start_timer", "step_name": name, "delay_ms": max(0, int(seconds * 1000))},
        )

    def wait_for_signal(self, name: str, signal_name: str) -> str:
        """Block until a matching external signal arrives; return its payload."""
        return self._command(
            "SIGNAL",
            name,
            {"type": "await_signal", "step_name": name, "signal_name": signal_name},
        )

    def _command(self, kind: str, name: str, command: dict) -> str:
        """Return the recorded result, stay blocked, or yield the next command."""
        self._next += 1
        seq = self._next
        recorded = self._steps[seq - 1] if seq <= len(self._steps) else None

        if recorded is not None:
            # Determinism: the call being made now must be the step history
            # recorded at this position.
            if recorded.get("kind") != kind or recorded.get("step_name") != name:
                raise _NonDeterminism(
                    f'non-determinism at step {seq}: code issued {kind} "{name}" but '
                    f'history has {recorded.get("kind")} "{recorded.get("step_name")}"'
                )
            status = recorded.get("status")
            if status == "COMPLETED":
                return recorded.get("output") or ""
            if status == "FAILED":
                raise ActivityFailure(name, recorded.get("error") or "activity failed")
            raise _Yield(None)  # SCHEDULED: still in flight

        command = dict(command)
        command["sequence"] = seq
        raise _Yield(command)


def decide(fn: Callable[[Context], Any], task: dict) -> dict:
    """Replay a workflow function against a task and produce the next command.

    Pure: no I/O, no shared state. Returns ``{"command": ...}`` or
    ``{"replay_error": ...}``.
    """
    ctx = Context(task)
    try:
        output = fn(ctx)
        return {"command": {"type": "complete_workflow", "output": _as_str(output)}}
    except _Yield as y:
        return {"command": y.command}  # None command == "blocked, nothing to do"
    except _NonDeterminism as err:
        return {"replay_error": str(err)}
    except Exception as err:  # WorkflowFailure, uncaught ActivityFailure, bugs
        return {"command": {"type": "fail_workflow", "error": str(err) or repr(err)}}


class Worker:
    """Connects to an engine, registers workflow types, and serves both
    workflow decisions and activity executions."""

    def __init__(
        self,
        address: str = "http://localhost:8233",
        worker_id: Optional[str] = None,
        sdk: str = SDK_VERSION,
    ):
        self.address = address.rstrip("/")
        self.worker_id = worker_id or f"py-worker-{random.randrange(16**8):08x}"
        self.sdk = sdk
        self.workflows: Dict[str, Callable[[Context], Any]] = {}
        self.activities: Dict[str, Callable[..., Any]] = {}
        self._stop = threading.Event()

    # -- registration (usable as decorators) --

    def workflow(self, workflow_type: str):
        def wrap(fn):
            self.workflows[workflow_type] = fn
            return fn

        return wrap

    def activity(self, step_name: str):
        def wrap(fn):
            self.activities[step_name] = fn
            return fn

        return wrap

    def stop(self) -> None:
        self._stop.set()

    # -- HTTP plumbing --

    def _request(self, method: str, path: str, body: Optional[dict] = None, timeout: float = 40.0):
        data = json.dumps(body).encode() if body is not None else None
        req = urllib.request.Request(
            self.address + path,
            data=data,
            method=method,
            headers={"Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as res:
                if res.status == 204:
                    return None  # long-poll expired
                raw = res.read()
                return json.loads(raw) if raw else {}
        except urllib.error.HTTPError as err:
            raw = err.read()
            try:
                message = json.loads(raw).get("error", str(err))
            except Exception:
                message = str(err)
            raise _HTTPError(err.code, message) from None

    # -- client helpers --

    def register(self) -> None:
        """Tell the engine which workflow types this worker can replay."""
        if not self.workflows:
            return
        self._request(
            "POST",
            "/api/workflow-types",
            {
                "workflow_types": list(self.workflows),
                "worker_id": self.worker_id,
                "sdk": self.sdk,
            },
        )

    def start_workflow(self, workflow_type: str, payload: Any = "", workflow_id: str = "") -> str:
        body = self._request(
            "POST",
            "/api/workflows",
            {"type": workflow_type, "input": _as_str(payload), "workflow_id": workflow_id},
        )
        return body["workflow_id"]

    def signal(self, workflow_id: str, name: str, payload: Any = "") -> None:
        self._request(
            "POST",
            f"/api/workflows/{workflow_id}/signal",
            {"name": name, "payload": _as_str(payload)},
        )

    def describe(self, workflow_id: str) -> dict:
        return self._request("GET", f"/api/workflows/{workflow_id}")

    # -- serving loops --

    def _workflow_loop(self) -> None:
        # An empty filter means "any type", so a worker with no workflows must not poll.
        if not self.workflows:
            return
        types = ",".join(self.workflows)
        while not self._stop.is_set():
            try:
                task = self._request(
                    "GET", f"/api/workflow-tasks?types={types}&worker_id={self.worker_id}"
                )
            except Exception:
                time.sleep(0.5)  # engine unreachable; back off
                continue
            if not task:
                continue

            fn = self.workflows.get(task.get("workflow_type", ""))
            if fn is None:
                result = {"replay_error": f'no workflow registered for type {task.get("workflow_type")}'}
            else:
                result = decide(fn, task)

            try:
                self._request(
                    "POST",
                    "/api/workflow-tasks/complete",
                    {
                        "workflow_id": task["workflow_id"],
                        "task_token": task["task_token"],
                        "command": result.get("command"),
                        "replay_error": result.get("replay_error", ""),
                    },
                )
            except _HTTPError as err:
                # 409 = the decision went stale because history moved on.
                # Polling again is the correct response, so stay quiet.
                if err.status != 409:
                    print(f"[workflowd] complete failed: {err}")

    def _activity_loop(self) -> None:
        # Name our activities so the engine never hands us another worker's step.
        if not self.activities:
            return
        names = urllib.parse.quote(",".join(self.activities))
        while not self._stop.is_set():
            try:
                task = self._request("GET", f"/api/activity-tasks?activities={names}&worker_id={self.worker_id}")
            except Exception:
                time.sleep(0.5)
                continue
            if not task:
                continue

            fn = self.activities.get(task["step_name"])
            output, error = "", ""
            if fn is None:
                error = f'no activity registered for step "{task["step_name"]}"'
            else:
                try:
                    output = _as_str(fn(task.get("input", "")))
                except Exception as err:
                    error = str(err) or repr(err)

            try:
                self._request(
                    "POST",
                    "/api/activity-tasks/complete",
                    {
                        "workflow_id": task["workflow_id"],
                        "step_name": task["step_name"],
                        "sequence": task["sequence"],
                        "lease_token": task["lease_token"],
                        "output": output,
                        "error": error,
                    },
                )
            except _HTTPError as err:
                if err.status != 409:  # lost lease: task was reassigned
                    print(f"[workflowd] activity complete failed: {err}")

    def run(self, block: bool = True) -> None:
        """Register, then serve workflow decisions and activities."""
        self.register()
        threads = [
            threading.Thread(target=self._workflow_loop, daemon=True, name="workflowd-decisions"),
            threading.Thread(target=self._activity_loop, daemon=True, name="workflowd-activities"),
        ]
        for t in threads:
            t.start()
        if not block:
            return
        try:
            while not self._stop.is_set():
                time.sleep(0.2)
        except KeyboardInterrupt:
            self.stop()


class _HTTPError(Exception):
    def __init__(self, status: int, message: str):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status


def _as_str(value: Any) -> str:
    """Payloads cross the wire as strings; None becomes empty."""
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode()
    return value if isinstance(value, str) else json.dumps(value)
