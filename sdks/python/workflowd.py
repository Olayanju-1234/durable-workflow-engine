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
import re
import ssl
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, List, Optional, Union

__all__ = [
    "Worker",
    "Context",
    "Future",
    "RetryPolicy",
    "WorkflowFailure",
    "ActivityFailure",
    "CancelledFailure",
    "NonRetryableError",
    "DEFAULT_VERSION",
    "decide",
]

SDK_VERSION = "python/0.3.0"

# The version get_version returns to executions that predate the change.
DEFAULT_VERSION = -1


class WorkflowFailure(Exception):
    """Raise inside a workflow to fail it deliberately."""


class ActivityFailure(Exception):
    """Raised when an activity recorded a failure. Catch it to compensate.

    ``type`` is the failure's kind (the activity's exception class name, or one
    of the engine's: LeaseExpired, StartToCloseTimeout, ScheduleToCloseTimeout);
    ``non_retryable`` says the worker flagged it as not worth retrying.
    """

    def __init__(self, step_name: str, message: str, type: str = "", non_retryable: bool = False):
        super().__init__(message)
        self.step_name = step_name
        self.type = type
        self.non_retryable = non_retryable


class NonRetryableError(Exception):
    """Raise from an activity to fail its step at once, without retries: a 4xx
    answer, a validation error, anything that would fail the same way again.
    Any exception with ``non_retryable = True`` does the same."""

    non_retryable = True

    def __init__(self, message: str, *, error_type: Optional[str] = None):
        super().__init__(message)
        if error_type:
            self.error_type = error_type


def _error_type(err: BaseException) -> str:
    """The kind of failure an activity exception reports: its error_type, else its class name."""
    return getattr(err, "error_type", None) or type(err).__name__


class CancelledFailure(Exception):
    """Raised by a cancelled step's ``result()``. Catch it to carry on without it."""

    def __init__(self, step_name: str, message: str = "cancelled"):
        super().__init__(message)
        self.step_name = step_name


@dataclass(frozen=True)
class RetryPolicy:
    """Retry a failed activity before the workflow sees its error.

    ``max_attempts`` counts the first attempt (1 means no retries). The wait
    before each retry starts at ``initial_backoff`` seconds and doubles, capped
    at ``max_backoff`` (0 means no cap). Failures whose type (the exception
    class name, or ``error_type``) is in ``non_retryable_error_types`` are not
    retried. It is recorded with the step, so the retries survive engine restarts.
    """

    max_attempts: int = 1
    initial_backoff: float = 0.0
    max_backoff: float = 0.0
    non_retryable_error_types: tuple = ()

    def to_wire(self) -> dict:
        wire = {
            "max_attempts": self.max_attempts,
            "initial_backoff_ms": max(0, int(self.initial_backoff * 1000)),
            "max_backoff_ms": max(0, int(self.max_backoff * 1000)),
        }
        # Only when set: an engine that predates the field refuses it.
        if self.non_retryable_error_types:
            wire["non_retryable_error_types"] = list(self.non_retryable_error_types)
        return wire


def _put_timeouts(obj: dict, **seconds: Optional[float]) -> dict:
    """Set each positive timeout (in seconds) on obj as <name>_ms."""
    for field, s in seconds.items():
        if s is not None and s > 0:
            obj[field + "_ms"] = max(1, int(round(s * 1000)))
    return obj


class _Yield(BaseException):
    """Internal control-flow signal: the code waits on a step with no result yet.

    Deliberately derived from BaseException, not Exception, so a workflow's
    ``except Exception`` around an activity cannot accidentally swallow it. The
    commands to send are the ones the context collected before it was raised.
    """

    def __init__(self):
        super().__init__("workflowd internal yield")


class _ContinueAsNew(BaseException):
    """Raised by ctx.continue_as_new: ends the replay with that command. A
    BaseException, so an ``except Exception`` in the workflow cannot stop it."""

    def __init__(self, command: dict):
        super().__init__("workflowd continue as new")
        self.command = command


class _NonDeterminism(BaseException):
    """The workflow code no longer matches the recorded history.

    A BaseException for the same reason as _Yield: workflow code must not be
    able to catch it and carry on with a corrupt replay.
    """


_RFC3339 = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$")


def _parse_time(value: Optional[str]) -> Optional[datetime]:
    """An RFC 3339 time from the engine (nanoseconds trimmed to micro), in UTC."""
    m = _RFC3339.match(value or "")
    if not m:
        return None
    zone = "+00:00" if m.group(3) == "Z" else m.group(3)
    frac = (m.group(2) or "0")[:6].ljust(6, "0")
    return datetime.fromisoformat(f"{m.group(1)}.{frac}{zone}").astimezone(timezone.utc)


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


class Future:
    """The result of a durable call started without waiting for it (the
    ``*_async`` methods). ``result()`` returns it, or blocks the workflow until
    the step has one."""

    def __init__(self, ctx: "Context", seq: int, name: str, decode: str):
        self._ctx, self._seq, self._name, self._decode = ctx, seq, name, decode

    def _recorded(self) -> Optional[dict]:
        return self._ctx._visible(self._seq)  # None: not started yet, in flight, retrying, or past the cutoff

    def _resolved_id(self) -> Optional[float]:
        """Orders this step's result for wait_any: its event id, or for a step
        this replay cancelled, a place after every recorded result."""
        rec = self._recorded()
        if rec is not None:
            return rec.get("resolved_event_id", 0)
        if self._seq in self._ctx._interrupted:
            return self._ctx._cancel_at
        n = self._ctx._cancelled.get(self._seq)
        return None if n is None else 2**62 + n

    def cancel(self, reason: str = "") -> None:
        """Cancel the step if it has no result yet: a timer stops mattering, a
        signal wait stops taking signals (a later wait for the same signal gets
        the next one), and an activity's worker is told to stop. ``result()``
        then raises CancelledFailure."""
        if self._resolved_id() is not None:
            return
        self._ctx._cancelled[self._seq] = len(self._ctx._cancelled) + 1
        command = {"type": "cancel_step", "step_name": self._name, "sequence": self._seq}
        if reason:
            command["reason"] = reason
        # A replay may hide a cancel history already has (past its cutoff): don't send it again.
        if not self._ctx._has_result(self._seq):
            self._ctx._pending.append(command)

    def result(self) -> Any:
        ctx = self._ctx
        if self._seq in ctx._interrupted:
            raise ctx._cancel_failure(self._name)
        rec = self._recorded()
        if rec is None:
            if self._seq in ctx._cancelled:
                raise CancelledFailure(self._name)
            if ctx._cancel_pending():
                ctx._deliver_cancel()
                if self._seq in ctx._interrupted:
                    raise ctx._cancel_failure(self._name)
            raise _Yield()
        if rec.get("status") == "CANCELLED":
            raise CancelledFailure(self._name, rec.get("error") or "cancelled")
        self._ctx._observe(self._seq)
        if rec.get("status") == "FAILED":
            raise ActivityFailure(
                self._name,
                rec.get("error") or "activity failed",
                rec.get("error_type") or "",
                rec.get("non_retryable") is True,
            )
        return read_payload(rec, "output", self._decode)


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
        self._pending: List[dict] = []  # new commands, in sequence order
        self._cancelled: Dict[int, int] = {}  # seq -> cancel order, for steps cancelled in this replay
        self._started_at = _parse_time(task.get("started_at"))
        self._now = self._started_at  # the latest result the code has received
        self._random: Optional[random.Random] = None
        self.run_id: str = task.get("run_id", "")  # a workflow that continues as new gets a new one
        self._suggest_at: int = task.get("suggest_continue_as_new_at") or 0
        self._observed_event_id = 0  # the newest result the code has received
        # Graceful cancellation: told at the first point the code waits. The
        # decision that tells it records a cancel marker with the history length
        # it saw; a replay hides later results until it reaches the marker, so
        # the code is told at the same point every time.
        self._cancel_at: int = task.get("cancel_requested_event_id") or 0
        self._cancel_time = _parse_time(task.get("cancel_requested_at"))
        self._cancel_reason: str = task.get("cancel_reason") or ""
        self._history_length: int = task.get("history_length") or 0
        self._cutoff, self._cutoff_seq = 0, 0
        for s in self._steps:
            if s.get("kind") == "MARKER" and s.get("marker") == "cancel":
                self._cutoff_seq = s.get("sequence", 0)
                try:
                    self._cutoff = int(read_payload(s, "output"))
                except ValueError:
                    self._cutoff = -1
                break
        self._delivered = False
        self._interrupted: set = set()  # steps the code was waiting on when told

    @property
    def cancel_requested(self) -> bool:
        """True once a graceful cancel has reached this workflow's code."""
        return self._delivered

    def _has_result(self, seq: int) -> bool:
        rec = self._steps[seq - 1] if seq <= len(self._steps) else None
        return rec is not None and rec.get("status") in ("COMPLETED", "FAILED", "CANCELLED")

    def _visible(self, seq: int) -> Optional[dict]:
        """A step's recorded result as the code may see it now, or None. Before
        a replay reaches its cancel marker, results past the cutoff stay hidden:
        the decision that told the code had not seen them."""
        if not self._has_result(seq):
            return None
        rec = self._steps[seq - 1]
        if self._cutoff and not self._delivered and rec.get("kind") != "MARKER":
            if (rec.get("resolved_event_id") or 0) > self._cutoff:
                return None
        return rec

    def _cancel_pending(self) -> bool:
        return self._cancel_at > 0 and not self._delivered

    def _cancel_failure(self, step_name: str) -> "CancelledFailure":
        why = "the workflow is being cancelled"
        return CancelledFailure(step_name, f"{why} ({self._cancel_reason})" if self._cancel_reason else why)

    def _kind_at(self, seq: int):
        if seq <= len(self._steps):
            rec = self._steps[seq - 1]
            return rec.get("step_name", ""), rec.get("kind", "")
        kinds = {"schedule_activity": "ACTIVITY", "start_timer": "TIMER", "await_signal": "SIGNAL", "record_marker": "MARKER"}
        for c in self._pending:
            if c.get("sequence") == seq and c.get("type") != "cancel_step":
                return c.get("step_name", ""), kinds.get(c.get("type"), "")
        return "", "MARKER"

    def _deliver_cancel(self) -> None:
        """Tell the code it is being cancelled, where it waits: every step it
        started with no result it can see is interrupted (``result()`` raises
        CancelledFailure) and, if still open, cancelled; the clock moves to the
        request. Steps it starts afterwards (compensation) run normally."""
        seq = self._next + 1
        if self._cutoff_seq == 0:
            self._pending.append(
                {
                    "type": "record_marker",
                    "step_name": "cancellation",
                    "sequence": seq,
                    "marker": "cancel",
                    "data": str(self._history_length),
                }
            )
        elif self._cutoff < 0:
            raise _NonDeterminism(f"cancel marker at step {self._cutoff_seq} is unreadable")
        elif seq != self._cutoff_seq:
            raise _NonDeterminism(
                f"non-determinism: the cancellation reached the code at step {seq}, "
                f"but history has it at step {self._cutoff_seq}"
            )
        self._next = seq
        for s in range(1, seq):
            name, kind = self._kind_at(s)
            if kind == "MARKER" or self._visible(s) is not None or s in self._cancelled:
                continue
            self._interrupted.add(s)
            if not self._has_result(s):
                self._pending.append(
                    {"type": "cancel_step", "step_name": name, "sequence": s, "reason": "the workflow is being cancelled"}
                )
        self._delivered = True
        if self._cancel_time is not None and (self._now is None or self._cancel_time > self._now):
            self._now = self._cancel_time

    def continue_as_new_suggested(self) -> bool:
        """True once the history has grown past the engine's warning share of
        its limits, as of the latest result the code has received, so every
        replay answers the same at the same point. Then ``return
        ctx.continue_as_new(state)``: a history that reaches its limit fails
        the workflow."""
        return self._suggest_at > 0 and self._observed_event_id >= self._suggest_at

    def continue_as_new(
        self, payload: Any = "", *, workflow_type: Optional[str] = None, execution_timeout: Optional[float] = None
    ) -> Any:
        """End this run and start a fresh one under the same workflow id, with an
        empty history and ``payload`` as its input: ``return
        ctx.continue_as_new(state)``. It does not return. Steps started before it
        still run; signals this run has not consumed are not carried over.
        ``workflow_type`` and ``execution_timeout`` (seconds) change the next
        run's type and timeout (default: this run's)."""
        if self._next < len(self._steps):
            raise _NonDeterminism(
                f"non-determinism: continued as new after {self._next} steps, but history has {len(self._steps)}"
            )
        command = _put_payload({"type": "continue_as_new"}, "input", payload)
        if workflow_type:
            command["workflow_type"] = workflow_type
        _put_timeouts(command, execution_timeout=execution_timeout)
        raise _ContinueAsNew(command)

    def now(self) -> datetime:
        """The workflow's deterministic clock (UTC): when the latest step result
        the code has received was recorded, the start at first. It moves only
        as results arrive and reads the same on every replay, so use it instead
        of ``datetime.now()``."""
        if self._now is None:
            raise RuntimeError("ctx.now() needs a workflowd engine that sends started_at (0.2.0 or later)")
        return self._now

    def random(self) -> float:
        """A deterministic random number in [0, 1): the same sequence on every
        replay of this execution. Fine for jitter or picking a variant; not for
        secrets (it is derived from the workflow id), use side_effect for those."""
        if self._random is None:
            self._random = random.Random(f"{self.workflow_id}\0{self.task.get('started_at') or ''}")
        return self._random.random()

    def sleep_until(self, name: str, when: Union[datetime, float]) -> str:
        """Pause durably until ``when`` (an aware datetime, or epoch seconds),
        measured on ``ctx.now()``."""
        at = when.timestamp() if isinstance(when, datetime) else float(when)
        return self.sleep(name, max(0.0, at - self.now().timestamp()))

    def _observe(self, seq: int) -> None:
        rec = self._steps[seq - 1] if seq <= len(self._steps) else None
        if rec is None or rec.get("kind") == "MARKER" or rec.get("status") == "CANCELLED":
            return
        at = _parse_time(rec.get("resolved_at"))
        if at is not None and (self._now is None or at > self._now):
            self._now = at
        self._observed_event_id = max(self._observed_event_id, rec.get("resolved_event_id") or 0)

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
        start_to_close_timeout: Optional[float] = None,
        schedule_to_close_timeout: Optional[float] = None,
    ) -> Any:
        """Run a named activity on a worker and return its output.

        ``start_to_close_timeout`` (seconds) fails an attempt that runs longer,
        heartbeats or not; ``schedule_to_close_timeout`` bounds every attempt
        and backoff together, from when the step is first scheduled.
        """
        return self.activity_async(
            name,
            payload,
            retry=retry,
            decode=decode,
            start_to_close_timeout=start_to_close_timeout,
            schedule_to_close_timeout=schedule_to_close_timeout,
        ).result()

    def activity_async(
        self,
        name: str,
        payload: Any = "",
        *,
        retry: Optional[RetryPolicy] = None,
        decode: str = "text",
        start_to_close_timeout: Optional[float] = None,
        schedule_to_close_timeout: Optional[float] = None,
    ) -> Future:
        """Start an activity and return at once, so several run in parallel::

            charge = ctx.activity_async("charge", order)
            email = ctx.activity_async("email", order)
            ctx.wait_all(charge, email)
        """
        command = _put_payload({"type": "schedule_activity", "step_name": name}, "input", payload)
        if retry is not None:
            command["retry_policy"] = retry.to_wire()
        _put_timeouts(
            command,
            start_to_close_timeout=start_to_close_timeout,
            schedule_to_close_timeout=schedule_to_close_timeout,
        )
        return self._issue("ACTIVITY", name, command, decode)

    def sleep(self, name: str, seconds: float) -> str:
        """Pause durably. Survives engine restarts."""
        return self.sleep_async(name, seconds).result()

    def sleep_async(self, name: str, seconds: float) -> Future:
        """Start a durable timer and return at once; result() returns when it fires."""
        return self._issue(
            "TIMER", name, {"type": "start_timer", "step_name": name, "delay_ms": max(0, int(seconds * 1000))}, "text"
        )

    def wait_for_signal(
        self, name: str, signal_name: str, *, decode: str = "text", timeout: Optional[float] = None
    ) -> Any:
        """Block until a matching external signal arrives; return its payload.

        With ``timeout`` (seconds) it returns None if none arrives in time, and
        the wait is cancelled so a late signal goes to the next wait for it.
        (That uses a timer step named ``<name>.timeout``.)
        """
        wait = self.wait_for_signal_async(name, signal_name, decode=decode)
        if timeout is None:
            return wait.result()
        deadline = self.sleep_async(f"{name}.timeout", timeout)
        if self.wait_any(wait, deadline, cancel_rest=True) == 1:
            return None
        return wait.result()

    def wait_for_signal_async(self, name: str, signal_name: str, *, decode: str = "text") -> Future:
        """Start waiting for a signal and return at once."""
        return self._issue(
            "SIGNAL", name, {"type": "await_signal", "step_name": name, "signal_name": signal_name}, decode
        )

    def wait_all(self, *futures: Future) -> List[Any]:
        """Wait for every future and return their results, in order."""
        return [f.result() for f in futures]

    def wait_any(self, *futures: Future, cancel_rest: bool = False) -> int:
        """Wait until one of the futures has a result and return its index. When
        several have one, the step history recorded first wins, so every replay
        agrees::

            reply = ctx.wait_for_signal_async("reply", "approved")
            deadline = ctx.sleep_async("deadline", 48 * 3600)
            if ctx.wait_any(reply, deadline, cancel_rest=True) == 1:
                ...  # timed out

        ``cancel_rest`` cancels the others once one wins (Future.cancel): use it
        when nothing will wait on the losers again.
        """
        best, best_id = -1, 0.0
        for i, f in enumerate(futures):
            rid = f._resolved_id()
            if rid is not None and (best < 0 or rid < best_id):
                best, best_id = i, rid
        if best < 0 and self._cancel_pending():
            self._deliver_cancel()
            raise self._cancel_failure("")
        if best < 0:
            raise _Yield()
        if cancel_rest:
            for i, f in enumerate(futures):
                if i != best:
                    f.cancel("lost a wait_any race")
        if futures[best]._recorded() is not None:
            self._observe(futures[best]._seq)
        return best

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
                self._pending.append(
                    {
                        "type": "record_marker",
                        "step_name": change_id,
                        "sequence": seq,
                        "marker": "version",
                        "data": str(max_supported),
                    }
                )
                v = max_supported
            elif rec.get("kind") != "MARKER" or rec.get("marker") != "version" or rec.get("step_name") != change_id:
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
            command = _put_payload(
                {"type": "record_marker", "step_name": name, "sequence": seq, "marker": "side_effect"}, "data", fn()
            )
            self._pending.append(command)
            return read_payload(command, "data", decode)  # what every replay will return
        return self._issue("MARKER", name, None, decode).result()

    def _issue(self, kind: str, name: str, command: Optional[dict], decode: str) -> Future:
        """Give the next durable call its sequence, check it against history,
        and start the step if history has none yet."""
        self._next += 1
        seq = self._next
        recorded = self._steps[seq - 1] if seq <= len(self._steps) else None
        if recorded is None:
            command = dict(command or {})
            command["sequence"] = seq
            self._pending.append(command)
        else:
            # Determinism: the call being made now must be the step history
            # recorded at this position.
            marker_mismatch = kind == "MARKER" and recorded.get("marker") != "side_effect"
            if recorded.get("kind") != kind or recorded.get("step_name") != name or marker_mismatch:
                raise _NonDeterminism(
                    f'non-determinism at step {seq}: code issued {kind} "{name}" but '
                    f'history has {recorded.get("kind")} "{recorded.get("step_name")}"'
                )
        return Future(self, seq, name, decode)


def decide(fn: Callable[[Context], Any], task: dict) -> dict:
    """Replay a workflow function against a task and produce what happens next.

    Pure: no I/O, no shared state. Returns ``{"commands": [...]}`` (empty when
    the workflow is blocked on steps in flight) or ``{"replay_error": ...}``.
    """
    ctx = Context(task)
    try:
        output = fn(ctx)
        return {"commands": ctx._pending + [_put_payload({"type": "complete_workflow"}, "output", output)]}
    except _Yield:
        return {"commands": list(ctx._pending)}
    except _ContinueAsNew as can:
        return {"commands": ctx._pending + [can.command]}
    except _NonDeterminism as err:
        return {"replay_error": str(err)}
    except Exception as err:  # WorkflowFailure, uncaught ActivityFailure, bugs
        fail = {"type": "fail_workflow", "error": str(err) or repr(err)}
        if ctx._delivered and (isinstance(err, CancelledFailure) or isinstance(err.__cause__, CancelledFailure)):
            fail["cancelled"] = True  # a graceful cancel ends it CANCELLED
        return {"commands": ctx._pending + [fail]}


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
        event is set when the work should stop: the engine reassigned it, the
        attempt's start-to-close timeout passed, or it was cancelled (then
        ``cancelled`` is set too)."""

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

    def start_workflow(
        self,
        workflow_type: str,
        payload: Any = "",
        workflow_id: str = "",
        *,
        execution_timeout: Optional[float] = None,
        id_reuse_policy: Optional[str] = None,
        id_conflict_policy: Optional[str] = None,
    ) -> str:
        """Start a workflow and return its id. ``execution_timeout`` (seconds)
        ends it as TIMED_OUT if it has not finished by then; the policies are
        as for start()."""
        return self.start(
            workflow_type,
            payload,
            workflow_id,
            execution_timeout=execution_timeout,
            id_reuse_policy=id_reuse_policy,
            id_conflict_policy=id_conflict_policy,
        )["workflow_id"]

    def start(
        self,
        workflow_type: str,
        payload: Any = "",
        workflow_id: str = "",
        *,
        execution_timeout: Optional[float] = None,
        id_reuse_policy: Optional[str] = None,
        id_conflict_policy: Optional[str] = None,
    ) -> dict:
        """Start a workflow and say what happened: ``{"workflow_id", "run_id",
        "started"}``. ``id_reuse_policy`` ("ALLOW_DUPLICATE",
        "ALLOW_DUPLICATE_FAILED_ONLY" or the default "REJECT_DUPLICATE") decides
        whether an id whose run has finished may start a new run;
        ``id_conflict_policy="USE_EXISTING"`` returns the run that holds a taken
        id (started False) instead of failing with a 409."""
        # Chosen here so a retry after a lost reply cannot start a second execution.
        wid = workflow_id or f"{workflow_type}-{random.randrange(16**16):016x}"
        body = _put_payload({"type": workflow_type, "workflow_id": wid}, "input", payload)
        _put_timeouts(body, execution_timeout=execution_timeout)
        # Only when set: an engine before 0.3.0 refuses the fields.
        if id_reuse_policy:
            body["id_reuse_policy"] = id_reuse_policy.upper()
        if id_conflict_policy:
            body["id_conflict_policy"] = id_conflict_policy.upper()
        try:
            res = self.conn.request("POST", "/api/workflows", body) or {}
        except _HTTPError as err:
            if not (err.status == 409 and not workflow_id):
                raise
            res = {}
        return {"workflow_id": wid, "run_id": res.get("run_id", ""), "started": res.get("started") is not False}

    def signal(self, workflow_id: str, name: str, payload: Any = "", request_id: Optional[str] = None) -> None:
        """Send a signal. It is recorded once even if the request is retried
        (after a lost reply or a leader change): every attempt carries the same
        idempotency key, ``request_id`` or a random one."""
        body = {"name": name, "request_id": request_id or f"{random.randrange(16**16):016x}"}
        self.conn.request(
            "POST",
            f"/api/workflows/{urllib.parse.quote(workflow_id, safe='')}/signal",
            _put_payload(body, "payload", payload),
        )

    def cancel(self, workflow_id: str, reason: str = "", *, graceful: bool = False) -> None:
        """End a running workflow as CANCELLED. Workers running its activities
        are told to stop (the activity's ``lease_lost`` and ``cancelled`` events
        are set). With ``graceful`` the workflow code is told instead: the step
        it waits on raises CancelledFailure, and it may compensate before it
        re-raises it."""
        body: dict = {"reason": reason}
        if graceful:
            body["graceful"] = True
        self.conn.request("POST", f"/api/workflows/{urllib.parse.quote(workflow_id, safe='')}/cancel", body)

    def describe(self, workflow_id: str, run_id: Optional[str] = None) -> dict:
        """A workflow's current state; with ``run_id``, an earlier run's."""
        path = f"/api/workflows/{urllib.parse.quote(workflow_id, safe='')}"
        if run_id:
            path += f"?run_id={urllib.parse.quote(run_id, safe='')}"
        return self.conn.request("GET", path)

    def runs(self, workflow_id: str) -> List[dict]:
        """The runs kept for a workflow id: the current one, then earlier ones
        (continued as new), newest first."""
        return self.conn.request("GET", f"/api/workflows/{urllib.parse.quote(workflow_id, safe='')}/runs")["runs"]

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

            body = {
                "workflow_id": task["workflow_id"],
                "task_token": task["task_token"],
                "replay_error": result.get("replay_error", ""),
            }
            commands = result.get("commands") or []
            # One command travels in the field every engine version understands.
            if len(commands) > 1:
                body["commands"] = commands
            else:
                body["command"] = commands[0] if commands else None
            try:
                self.conn.request("POST", "/api/workflow-tasks/complete", body, attempts=3)
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
        cancelled = threading.Event()
        stop_beats = self._heartbeat(task, lease_lost, cancelled)
        # The engine fails the attempt at its start-to-close timeout: tell the activity too.
        limit = (task.get("start_to_close_timeout_ms") or 0) / 1000.0
        timer = threading.Timer(limit, lease_lost.set) if limit > 0 else None
        if timer is not None:
            timer.daemon = True
            timer.start()
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
                out = fn(arg, dict(task, lease_lost=lease_lost, cancelled=cancelled))
            else:
                out = fn(arg)
            _put_payload(result, "output", out)
        except Exception as err:
            result["error"] = str(err) or repr(err)
            result["error_type"] = _error_type(err)
            if getattr(err, "non_retryable", False) is True:
                result["non_retryable"] = True
        finally:
            stop_beats()
            if timer is not None:
                timer.cancel()
        try:
            try:
                self.conn.request("POST", "/api/activity-tasks/complete", result, attempts=3)
            except _HTTPError as err:
                # An engine before 0.2.0 refuses the fields it doesn't know:
                # report the failure without them rather than lose it.
                if not (err.status == 400 and "unknown field" in str(err) and "error_type" in result):
                    raise
                result.pop("error_type", None)
                result.pop("non_retryable", None)
                self.conn.request("POST", "/api/activity-tasks/complete", result, attempts=3)
        except Exception as err:
            if getattr(err, "status", None) != 409:  # lost lease: task was reassigned
                print(f"[workflowd] activity complete failed: {err}")

    def _heartbeat(self, task: dict, lease_lost: threading.Event, cancelled: threading.Event) -> Callable[[], None]:
        """Extend the lease every third of its duration until stopped; a 409
        means the lease is gone, so lease_lost is set, and cancelled too when
        the engine says the work is no longer wanted."""
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
                        if getattr(err, "body", {}).get("cancelled"):
                            cancelled.set()
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
