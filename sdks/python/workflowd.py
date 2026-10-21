"""workflowd — Python SDK.

Write durable workflows in Python. Zero dependencies: this module uses only the
standard library (urllib, json, threading), so there is no pip install and no
protobuf toolchain.

The model in one paragraph: your workflow function is re-run from the top every
time the engine needs a decision. Each durable call (``ctx.activity``,
``ctx.sleep``, ``ctx.wait_for_signal``, ``ctx.get_version``,
``ctx.side_effect``) is matched against what already happened. Completed steps
return their recorded result instantly; the first step with no result yet
becomes the command sent back to the engine. The function runs many times, but
each side effect happens exactly once.

Usage::

    from workflowd import Worker, WorkflowFailure, ActivityFailure, RetryPolicy

    worker = Worker(address="http://localhost:8233")

    @worker.workflow("OrderWorkflow")
    def order_workflow(ctx):
        order = ctx.input_as("json")
        reserved = ctx.activity("reserve", order, decode="json")
        ctx.sleep("settle", 2.0)
        try:
            ctx.activity("charge", reserved, retry=RetryPolicy(max_attempts=3, initial_backoff=0.5))
        except ActivityFailure as err:
            ctx.activity("release", reserved)      # compensate
            raise WorkflowFailure(f"charge failed: {err}")
        return ctx.activity("ship", reserved)

    @worker.activity("reserve", decode="json")
    def reserve(order):
        return {"id": order["id"], "held": True}

    worker.run()

Payloads: ``str`` travels as UTF-8 text, ``bytes`` as raw bytes, anything else
as JSON. Results come back as ``str`` unless you pass ``decode="json"`` or
``decode="bytes"``.

Clusters: pass every node in ``addresses``. A follower answers 503 naming the
leader; the worker moves there and registers its workflow types again.

Workflow code must be deterministic: no clocks, no randomness, no I/O except
through ``ctx``. Anything else will diverge on replay.
"""

from __future__ import annotations

import base64
import inspect
import json
import random
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional

__all__ = [
    "Worker",
    "Context",
    "RetryPolicy",
    "WorkflowFailure",
    "ActivityFailure",
    "DEFAULT_VERSION",
    "decide",
]

SDK_VERSION = "python/0.1.0"

# The version get_version returns to executions that predate the change.
DEFAULT_VERSION = -1


class WorkflowFailure(Exception):
    """Raise inside a workflow to fail it deliberately."""


class ActivityFailure(Exception):
    """Raised when an activity recorded a failure. Catch it to compensate."""

    def __init__(self, step_name: str, message: str):
        super().__init__(message)
        self.step_name = step_name


@dataclass(frozen=True)
class RetryPolicy:
    """Retry a failed activity before the workflow sees its error.

    ``max_attempts`` counts the first attempt (1 means no retries). The wait
    before each retry starts at ``initial_backoff`` seconds and doubles, capped
    at ``max_backoff`` (0 means no cap). It is recorded with the step, so the
    retries survive engine restarts.
    """

    max_attempts: int = 1
    initial_backoff: float = 0.0
    max_backoff: float = 0.0

    def to_wire(self) -> dict:
        return {
            "max_attempts": self.max_attempts,
            "initial_backoff_ms": max(0, int(self.initial_backoff * 1000)),
            "max_backoff_ms": max(0, int(self.max_backoff * 1000)),
        }


class _Yield(BaseException):
    """Internal control-flow signal: replay reached a step with no result yet.

    Deliberately derived from BaseException, not Exception, so a workflow's
    ``except Exception`` around an activity cannot accidentally swallow it.
    """

    def __init__(self, command: Optional[dict]):
        super().__init__("workflowd internal yield")
        self.command = command


class _NonDeterminism(BaseException):
    """The workflow code no longer matches the recorded history.

    A BaseException for the same reason as _Yield: workflow code must not be
    able to catch it and carry on with a corrupt replay.
    """


# ---- payloads ----


def _put_payload(obj: dict, field: str, value: Any) -> dict:
    """Encode value onto obj[field] (text) or obj[field + "_base64"] (bytes)."""
    if value is None:
        obj[field] = ""
    elif isinstance(value, str):
        obj[field] = value
    elif isinstance(value, (bytes, bytearray, memoryview)):
        obj[field + "_base64"] = base64.b64encode(bytes(value)).decode("ascii")
    else:
        obj[field] = json.dumps(value)
    return obj


def read_payload(obj: Optional[dict], field: str, decode: str = "text") -> Any:
    """Read a payload. The engine adds ``<field>_base64`` only when the bytes
    are not valid UTF-8, so the text field is exact otherwise."""
    obj = obj or {}
    b64 = obj.get(field + "_base64")
    if decode == "bytes":
        return base64.b64decode(b64) if b64 else (obj.get(field) or "").encode()
    text = base64.b64decode(b64).decode("utf-8", errors="replace") if b64 else (obj.get(field) or "")
    if decode == "json":
        return None if text == "" else json.loads(text)
    if decode == "text":
        return text
    raise ValueError(f"unknown decode {decode!r}; use 'text', 'json' or 'bytes'")


class Context:
    """What a workflow function receives. Every method is durable."""

    def __init__(self, task: dict):
        self.task = task
        self.input: str = read_payload(task, "input")
        self.workflow_id: str = task.get("workflow_id", "")
        self._steps: List[dict] = task.get("steps") or []
        self._signals: List[dict] = task.get("signals") or []
        self._next = 0
        self._versions: Dict[str, int] = {}

    def input_as(self, decode: str = "text") -> Any:
        """The start input decoded as "text" (same as ctx.input), "json" or "bytes"."""
        return read_payload(self.task, "input", decode)

    def activity(
        self,
        name: str,
        payload: Any = "",
        *,
        retry: Optional[RetryPolicy] = None,
        decode: str = "text",
    ) -> Any:
        """Run a named activity on a worker and return its output."""
        command = _put_payload({"type": "schedule_activity", "step_name": name}, "input", payload)
        if retry is not None:
            command["retry_policy"] = retry.to_wire()
        return self._command("ACTIVITY", name, command, decode)

    def sleep(self, name: str, seconds: float) -> str:
        """Pause durably. Survives engine restarts."""
        return self._command(
            "TIMER",
            name,
            {"type": "start_timer", "step_name": name, "delay_ms": max(0, int(seconds * 1000))},
        )

    def wait_for_signal(self, name: str, signal_name: str, *, decode: str = "text") -> Any:
        """Block until a matching external signal arrives; return its payload."""
        return self._command(
            "SIGNAL",
            name,
            {"type": "await_signal", "step_name": name, "signal_name": signal_name},
            decode,
        )

    def get_version(self, change_id: str, min_supported: int, max_supported: int) -> int:
        """Change workflow code while executions are in flight::

            if ctx.get_version("add-fraud-check", DEFAULT_VERSION, 1) == 1:
                ctx.activity("fraud-check", order)

        An execution reaching this point for the first time records
        max_supported and keeps it on every replay. One that ran past this point
        under the old code gets DEFAULT_VERSION. A recorded version outside
        [min_supported, max_supported] fails the execution.
        """
        v = self._versions.get(change_id)
        if v is None:
            seq = self._next + 1
            rec = self._steps[seq - 1] if seq <= len(self._steps) else None
            if rec is None:
                self._next = seq
                raise _Yield(
                    {
                        "type": "record_marker",
                        "step_name": change_id,
                        "sequence": seq,
                        "marker": "version",
                        "data": str(max_supported),
                    }
                )
            if rec.get("kind") != "MARKER" or rec.get("marker") != "version" or rec.get("step_name") != change_id:
                v = DEFAULT_VERSION  # this execution ran past here before the change
            else:
                self._next = seq
                v = int(read_payload(rec, "output"))
            self._versions[change_id] = v
        if v < min_supported or v > max_supported:
            raise _NonDeterminism(
                f'non-determinism: execution recorded version {v} of "{change_id}", '
                f"but the code supports {min_supported}..{max_supported}"
            )
        return v

    def side_effect(self, name: str, fn: Callable[[], Any], *, decode: str = "text") -> Any:
        """Run fn once, record its result, return the recorded result on every
        replay. For small non-deterministic values; it must not fail or do I/O."""
        seq = self._next + 1
        if seq > len(self._steps):
            self._next = seq
            command = {"type": "record_marker", "step_name": name, "sequence": seq, "marker": "side_effect"}
            raise _Yield(_put_payload(command, "data", fn()))
        return self._command("MARKER", name, None, decode)

    def _command(self, kind: str, name: str, command: Optional[dict], decode: str = "text") -> Any:
        """Return the recorded result, stay blocked, or yield the next command."""
        self._next += 1
        seq = self._next
        recorded = self._steps[seq - 1] if seq <= len(self._steps) else None

        if recorded is not None:
            # Determinism: the call being made now must be the step history
            # recorded at this position.
            marker_mismatch = kind == "MARKER" and recorded.get("marker") != "side_effect"
            if recorded.get("kind") != kind or recorded.get("step_name") != name or marker_mismatch:
                raise _NonDeterminism(
                    f'non-determinism at step {seq}: code issued {kind} "{name}" but '
                    f'history has {recorded.get("kind")} "{recorded.get("step_name")}"'
                )
            status = recorded.get("status")
            if status == "COMPLETED":
                return read_payload(recorded, "output", decode)
            if status == "FAILED":
                raise ActivityFailure(name, recorded.get("error") or "activity failed")
            raise _Yield(None)  # SCHEDULED: still in flight

        command = dict(command or {})
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
        return {"command": _put_payload({"type": "complete_workflow"}, "output", output)}
    except _Yield as y:
        return {"command": y.command}  # None command == "blocked, nothing to do"
    except _NonDeterminism as err:
        return {"replay_error": str(err)}
    except Exception as err:  # WorkflowFailure, uncaught ActivityFailure, bugs
        return {"command": {"type": "fail_workflow", "error": str(err) or repr(err)}}


def _accepts_two_args(fn: Callable[..., Any]) -> bool:
    try:
        params = list(inspect.signature(fn).parameters.values())
    except (TypeError, ValueError):
        return False
    if any(p.kind == p.VAR_POSITIONAL for p in params):
        return True
    positional = [p for p in params if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]
    return len(positional) >= 2


class _HTTPError(Exception):
    def __init__(self, status: int, message: str, body: Optional[dict] = None):
        super().__init__(f"HTTP {status}: {message}")
        self.status = status
        self.body = body or {}


class _Connection:
    """Talks to whichever node currently serves requests. A 503 from a cluster
    follower names the leader (``leader_http``); without a name, or when a node
    is unreachable, it moves to the next address. ``epoch`` counts those moves
    so the worker registers again: every new engine starts with none."""

    def __init__(self, addresses: List[str], token: Optional[str] = None, ssl_context: Optional[ssl.SSLContext] = None):
        if not addresses:
            raise ValueError("workflowd: at least one address is required")
        self.token = token
        self._ssl = ssl_context
        self.addrs = [a.rstrip("/") for a in addresses]
        self.current = 0
        self.epoch = 0
        self._lock = threading.Lock()

    @property
    def base(self) -> str:
        with self._lock:
            return self.addrs[self.current]

    def _move_on(self, frm: str, leader_http: str) -> None:
        with self._lock:
            self.epoch += 1
            if self.addrs[self.current] != frm:
                return  # another call already moved on
            if leader_http:
                url = f"{urllib.parse.urlsplit(frm).scheme}://{leader_http}"
                if url not in self.addrs:
                    self.addrs.append(url)
                i = self.addrs.index(url)
                if i != self.current:
                    self.current = i
                    return
            self.current = (self.current + 1) % len(self.addrs)

    def request(self, method: str, path: str, body: Optional[dict] = None, attempts: int = 12, timeout: float = 40.0):
        backoff = 0.05
        for attempt in range(1, attempts + 1):
            frm = self.base
            data = json.dumps(body).encode() if body is not None else None
            headers = {"Content-Type": "application/json"}
            if self.token:
                headers["Authorization"] = f"Bearer {self.token}"
            req = urllib.request.Request(frm + path, data=data, method=method, headers=headers)
            try:
                with urllib.request.urlopen(req, timeout=timeout, context=self._ssl) as res:
                    if res.status == 204:
                        return None  # long-poll expired
                    raw = res.read()
                    return json.loads(raw) if raw else {}
            except urllib.error.HTTPError as err:
                raw = err.read()
                try:
                    parsed = json.loads(raw) if raw else {}
                except ValueError:
                    parsed = {"error": raw.decode(errors="replace")}
                if err.code != 503:
                    raise _HTTPError(err.code, parsed.get("error", str(err)), parsed) from None
                self._move_on(frm, parsed.get("leader_http") or "")
                if attempt >= attempts:
                    raise _HTTPError(503, parsed.get("error", "HTTP 503"), parsed) from None
                if parsed.get("leader_http"):
                    continue
            except (urllib.error.URLError, OSError):
                self._move_on(frm, "")
                if attempt >= attempts:
                    raise
            time.sleep(backoff)
            backoff = min(backoff * 2, 1.0)
        return None


class Worker:
    """Connects to an engine, registers workflow types, and serves both
    workflow decisions and activity executions."""

    def __init__(
        self,
        address: str = "http://localhost:8233",
        worker_id: Optional[str] = None,
        sdk: str = SDK_VERSION,
        token: Optional[str] = None,
        ca_file: Optional[str] = None,
        *,
        addresses: Optional[List[str]] = None,
        activity_concurrency: int = 4,
        workflow_concurrency: int = 2,
    ):
        """``token`` is sent as ``Authorization: Bearer <token>`` for an engine
        started with --auth-token-file. ``ca_file`` trusts a private CA for an
        https address; without it the system trust store is used. ``addresses``
        lists every cluster node (it replaces ``address``)."""
        ctx = ssl.create_default_context(cafile=ca_file) if ca_file else None
        self.conn = _Connection(addresses or [address], token, ctx)
        self.worker_id = worker_id or f"py-worker-{random.randrange(16**8):08x}"
        self.sdk = sdk
        self.workflows: Dict[str, Callable[[Context], Any]] = {}
        self.activities: Dict[str, Callable[..., Any]] = {}
        self._activity_decode: Dict[str, str] = {}
        self._activity_takes_info: Dict[str, bool] = {}
        self.activity_concurrency = activity_concurrency
        self.workflow_concurrency = workflow_concurrency
        self._registered_epoch = -1
        self._stop = threading.Event()

    @property
    def address(self) -> str:
        """The base URL of the node currently in use."""
        return self.conn.base

    # -- registration (usable as decorators) --

    def workflow(self, workflow_type: str):
        def wrap(fn):
            self.workflows[workflow_type] = fn
            return fn

        return wrap

    def activity(self, step_name: str, *, decode: str = "text"):
        """Register an activity. fn(input) gets the input decoded per
        ``decode``; fn(input, info) also gets the task, whose ``lease_lost``
        event is set if the engine reassigns the work."""

        def wrap(fn):
            self.activities[step_name] = fn
            self._activity_decode[step_name] = decode
            self._activity_takes_info[step_name] = _accepts_two_args(fn)
            return fn

        return wrap

    def stop(self) -> None:
        self._stop.set()

    # -- client helpers --

    def register(self) -> None:
        """Tell the engine which workflow types this worker can replay."""
        if not self.workflows:
            return
        epoch = self.conn.epoch
        self.conn.request(
            "POST",
            "/api/workflow-types",
            {"workflow_types": list(self.workflows), "worker_id": self.worker_id, "sdk": self.sdk},
        )
        # The epoch from before the call: if the call moved nodes, the next
        # poll registers again, which is harmless.
        self._registered_epoch = epoch

    def start_workflow(self, workflow_type: str, payload: Any = "", workflow_id: str = "") -> str:
        # Chosen here so a retry after a lost reply cannot start a second execution.
        wid = workflow_id or f"{workflow_type}-{random.randrange(16**16):016x}"
        try:
            self.conn.request(
                "POST", "/api/workflows", _put_payload({"type": workflow_type, "workflow_id": wid}, "input", payload)
            )
        except _HTTPError as err:
            if not (err.status == 409 and not workflow_id):
                raise
        return wid

    def signal(self, workflow_id: str, name: str, payload: Any = "") -> None:
        self.conn.request(
            "POST",
            f"/api/workflows/{urllib.parse.quote(workflow_id, safe='')}/signal",
            _put_payload({"name": name}, "payload", payload),
        )

    def describe(self, workflow_id: str) -> dict:
        return self.conn.request("GET", f"/api/workflows/{urllib.parse.quote(workflow_id, safe='')}")

    # -- serving loops --

    def _workflow_loop(self) -> None:
        types = urllib.parse.quote(",".join(self.workflows))
        wid = urllib.parse.quote(self.worker_id)
        while not self._stop.is_set():
            try:
                if self._registered_epoch != self.conn.epoch:
                    self.register()
                task = self.conn.request("GET", f"/api/workflow-tasks?types={types}&worker_id={wid}", attempts=1)
            except Exception:
                time.sleep(0.2)  # engine unreachable or turned us away; retry
                continue
            if not task:
                continue

            fn = self.workflows.get(task.get("workflow_type", ""))
            if fn is None:
                result = {"replay_error": f'no workflow registered for type {task.get("workflow_type")}'}
            else:
                result = decide(fn, task)

            try:
                self.conn.request(
                    "POST",
                    "/api/workflow-tasks/complete",
                    {
                        "workflow_id": task["workflow_id"],
                        "task_token": task["task_token"],
                        "command": result.get("command"),
                        "replay_error": result.get("replay_error", ""),
                    },
                    attempts=3,
                )
            except Exception as err:
                # 409 = the decision went stale because history moved on.
                # Polling again is the correct response, so stay quiet.
                if getattr(err, "status", None) != 409:
                    print(f"[workflowd] complete failed: {err}")

    def _activity_loop(self) -> None:
        # Name our activities so the engine never hands us another worker's step.
        names = urllib.parse.quote(",".join(self.activities))
        wid = urllib.parse.quote(self.worker_id)
        while not self._stop.is_set():
            try:
                task = self.conn.request("GET", f"/api/activity-tasks?activities={names}&worker_id={wid}", attempts=1)
            except Exception:
                time.sleep(0.2)
                continue
            if task:
                self._execute(task)

    def _execute(self, task: dict) -> None:
        step = task["step_name"]
        fn = self.activities.get(step)
        lease_lost = threading.Event()
        stop_beats = self._heartbeat(task, lease_lost)
        result = {
            "workflow_id": task["workflow_id"],
            "step_name": step,
            "sequence": task["sequence"],
            "lease_token": task["lease_token"],
        }
        try:
            if fn is None:
                raise RuntimeError(f'no activity registered for step "{step}"')
            arg = read_payload(task, "input", self._activity_decode.get(step, "text"))
            if self._activity_takes_info.get(step, False):
                out = fn(arg, dict(task, lease_lost=lease_lost))
            else:
                out = fn(arg)
            _put_payload(result, "output", out)
        except Exception as err:
            result["error"] = str(err) or repr(err)
        finally:
            stop_beats()
        try:
            self.conn.request("POST", "/api/activity-tasks/complete", result, attempts=3)
        except Exception as err:
            if getattr(err, "status", None) != 409:  # lost lease: task was reassigned
                print(f"[workflowd] activity complete failed: {err}")

    def _heartbeat(self, task: dict, lease_lost: threading.Event) -> Callable[[], None]:
        """Extend the lease every third of its duration until stopped; a 409
        means the lease is gone, so lease_lost is set."""
        every = (task.get("lease_duration_ms") or 0) / 3000.0
        if every <= 0:
            return lambda: None
        done = threading.Event()

        def beat() -> None:
            body = {"workflow_id": task["workflow_id"], "lease_token": task["lease_token"], "worker_id": self.worker_id}
            while not done.wait(every):
                try:
                    self.conn.request("POST", "/api/activity-tasks/heartbeat", body, attempts=2)
                except Exception as err:
                    if getattr(err, "status", None) == 409:
                        lease_lost.set()
                        return

        t = threading.Thread(target=beat, daemon=True, name="workflowd-heartbeat")
        t.start()

        def stop() -> None:
            done.set()
            t.join()

        return stop

    def run(self, block: bool = True) -> None:
        """Register, then serve workflow decisions and activities."""
        self._stop.clear()
        threads = []
        if self.workflows:
            try:
                self.register()
            except _HTTPError as err:
                if err.status != 503:
                    raise  # a refused type is a bug to surface
            except OSError:
                pass  # not up yet: the workflow loop keeps trying
            for i in range(self.workflow_concurrency):
                threads.append(threading.Thread(target=self._workflow_loop, daemon=True, name=f"workflowd-decisions-{i}"))
        # An empty filter means "any activity", so a worker with none must not poll.
        if self.activities:
            for i in range(self.activity_concurrency):
                threads.append(threading.Thread(target=self._activity_loop, daemon=True, name=f"workflowd-activities-{i}"))
        for t in threads:
            t.start()
        if not block:
            return
        try:
            while not self._stop.is_set():
                time.sleep(0.2)
        except KeyboardInterrupt:
            self.stop()
