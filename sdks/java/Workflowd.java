package io.github.olayanju1234.workflowd;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

/**
 * workflowd — Java SDK.
 *
 * <p>Write durable workflows in Java. Zero dependencies: this file uses only the JDK
 * ({@code java.net.http.HttpClient}, Java 11+), including a small JSON codec at the bottom,
 * because the JDK has no built-in JSON support and pulling in Jackson would break the
 * engine's dependency-free promise.
 *
 * <p>The model: your workflow function is re-run from the top every time the engine needs a
 * decision. Each durable call ({@code ctx.activity}, {@code ctx.sleep},
 * {@code ctx.waitForSignal}) is matched against what already happened. Completed steps return
 * their recorded result instantly; the first step with no result yet becomes the command sent
 * back. The function runs many times; each side effect happens exactly once.
 *
 * <pre>{@code
 * Workflowd.Worker worker = new Workflowd.Worker("http://localhost:8233");
 *
 * worker.workflow("OrderWorkflow", ctx -> {
 *     String reserved = ctx.activity("reserve", ctx.input());
 *     ctx.sleep("settle", Duration.ofSeconds(2));
 *     try {
 *         ctx.activity("charge", reserved);
 *     } catch (Workflowd.ActivityFailure e) {
 *         ctx.activity("release", reserved);          // compensate
 *         throw new Workflowd.WorkflowFailure("charge failed: " + e.getMessage());
 *     }
 *     return ctx.activity("ship", reserved);
 * });
 *
 * worker.activity("reserve", input -> "reserved:" + input);
 * worker.run();
 * }</pre>
 *
 * <p>Workflow code must be deterministic: no clocks, no randomness, no I/O except through
 * {@code ctx}. Anything else diverges on replay.
 */
public final class Workflowd {

    public static final String SDK_VERSION = "java/0.4.0";

    private Workflowd() {}

    /** Throw inside a workflow to fail it deliberately. */
    public static class WorkflowFailure extends RuntimeException {
        public WorkflowFailure(String message) { super(message); }
    }

    /**
     * Thrown when an activity recorded a failure. Catch it to compensate. {@code type} is the
     * failure's kind (the activity exception's simple class name, or one of the engine's:
     * LeaseExpired, StartToCloseTimeout, ScheduleToCloseTimeout); {@code nonRetryable} says the
     * worker flagged it as not worth retrying.
     */
    public static class ActivityFailure extends RuntimeException {
        public final String stepName;
        public final String type;
        public final boolean nonRetryable;

        public ActivityFailure(String stepName, String message) {
            this(stepName, message, "", false);
        }

        public ActivityFailure(String stepName, String message, String type, boolean nonRetryable) {
            super(message);
            this.stepName = stepName;
            this.type = type == null ? "" : type;
            this.nonRetryable = nonRetryable;
        }
    }

    /**
     * Throw from an activity to fail its step at once, without retries: a 4xx answer, a
     * validation error, anything that would fail the same way again.
     */
    public static class NonRetryableException extends RuntimeException {
        final String errorType;

        public NonRetryableException(String message) { this(message, null); }

        /** {@code errorType} names the kind of failure; null means this class's name. */
        public NonRetryableException(String message, String errorType) {
            super(message);
            this.errorType = errorType;
        }
    }

    /** Thrown for a step that was cancelled. Catch it to carry on without the step. */
    public static class CancelledFailure extends RuntimeException {
        public final String stepName;
        public CancelledFailure(String stepName, String message) {
            super(message);
            this.stepName = stepName;
        }
    }

    /**
     * Internal control-flow signal: replay reached a step with no recorded result.
     *
     * <p>It extends {@link Error}, not {@link Exception}, on purpose. Workflow code
     * legitimately writes {@code catch (Exception e)} around an activity to compensate, and
     * that must never swallow the suspend signal — doing so would run compensation on every
     * replay pass. Only {@code catch (Throwable)} could intercept this, which workflow code
     * should never do.
     */
    static final class Suspend extends Error {
        Suspend() {
            super("workflowd internal suspend", null, false, false); // no stack trace: hot path
        }
    }

    /**
     * Thrown by {@link Context#continueAsNew}: ends the replay with that command. An Error for
     * the same reason as {@link Suspend}: {@code catch (Exception e)} must not stop it.
     */
    static final class ContinueAsNew extends Error {
        final transient Map<String, Object> command;
        ContinueAsNew(Map<String, Object> command) {
            super("workflowd continue as new", null, false, false);
            this.command = command;
        }
    }

    /** Replay found that the code no longer matches the recorded history. */
    static final class NonDeterminism extends Error {
        NonDeterminism(String message) { super(message, null, false, false); }
    }

    /** A workflow function: given a context, produce the workflow's output. */
    public interface WorkflowFunction {
        String run(Context ctx);
    }

    /** An activity implementation: given an input, produce an output. */
    public interface ActivityFunction {
        String run(String input) throws Exception;
    }

    /** An activity over raw bytes, for payloads that are not text. */
    public interface BytesActivityFunction {
        byte[] run(byte[] input) throws Exception;
    }

    /** An activity that also gets its task's {@link ActivityContext}: who it runs for, and when to stop. */
    public interface ContextActivityFunction {
        String run(String input, ActivityContext ctx) throws Exception;
    }

    /**
     * What an activity registered with {@link Worker#activityWithContext} learns about its task.
     * Long-running work should check {@link #isLeaseLost()} and stop when it turns true.
     */
    public static final class ActivityContext {
        private final Map<String, Object> task;
        private volatile boolean leaseLost;
        private volatile boolean cancelled;

        ActivityContext(Map<String, Object> task) { this.task = task; }

        public String workflowId() { return str(task.get("workflow_id")); }
        public String stepName() { return str(task.get("step_name")); }
        /** {@code <workflowId>/<sequence>}: the same for every attempt, an idempotency key. */
        public String activityId() { return str(task.get("activity_id")); }
        /** Which attempt this is, from 1. */
        public int attempt() {
            Object a = task.get("attempt");
            return a instanceof Number ? Math.max(1, ((Number) a).intValue()) : 1;
        }
        /**
         * True once the work should stop: the engine gave the task to another worker, the
         * attempt's start-to-close timeout passed, or the work was cancelled.
         */
        public boolean isLeaseLost() { return leaseLost; }
        /** True once the engine said the work is no longer wanted: its step was cancelled or its workflow ended. */
        public boolean isCancelled() { return cancelled; }
    }

    /** How the worker runs any registered activity. */
    private interface ActivityRunner {
        byte[] run(byte[] input, ActivityContext ctx) throws Exception;
    }

    /** The version {@code getVersion} returns to executions that predate the change. */
    public static final int DEFAULT_VERSION = -1;

    /**
     * Retry a failed activity before the workflow sees its error. {@code maxAttempts} counts
     * the first attempt (1 = no retries); the wait starts at {@code initialBackoff} and
     * doubles, capped at {@code maxBackoff} (zero = no cap). It is recorded with the step, so
     * the retries survive engine restarts.
     */
    public static final class RetryPolicy {
        final int maxAttempts;
        final Duration initialBackoff;
        final Duration maxBackoff;
        final List<String> nonRetryableErrorTypes;

        public RetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
            this(maxAttempts, initialBackoff, maxBackoff, List.of());
        }

        private RetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff, List<String> nonRetryableErrorTypes) {
            this.maxAttempts = maxAttempts;
            this.initialBackoff = initialBackoff == null ? Duration.ZERO : initialBackoff;
            this.maxBackoff = maxBackoff == null ? Duration.ZERO : maxBackoff;
            this.nonRetryableErrorTypes = nonRetryableErrorTypes;
        }

        /** A copy that does not retry failures of these types (activity exception class names). */
        public RetryPolicy withNonRetryableErrorTypes(String... types) {
            return new RetryPolicy(maxAttempts, initialBackoff, maxBackoff, List.of(types));
        }

        Map<String, Object> toWire() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("max_attempts", (long) maxAttempts);
            m.put("initial_backoff_ms", Math.max(0L, initialBackoff.toMillis()));
            m.put("max_backoff_ms", Math.max(0L, maxBackoff.toMillis()));
            // Only when set: an engine that predates the field refuses it.
            if (!nonRetryableErrorTypes.isEmpty()) m.put("non_retryable_error_types", new ArrayList<Object>(nonRetryableErrorTypes));
            return m;
        }
    }

    /**
     * Options for one activity call: a retry policy and timeouts (zero or null means none).
     * {@code startToClose} fails an attempt that runs longer, heartbeats or not;
     * {@code scheduleToClose} bounds every attempt and backoff together. Immutable: each
     * {@code with} method returns a copy.
     */
    public static final class ActivityOptions {
        final RetryPolicy retry;
        final Duration startToClose;
        final Duration scheduleToClose;
        final String taskQueue;

        public ActivityOptions() { this(null, null, null, null); }

        private ActivityOptions(RetryPolicy retry, Duration startToClose, Duration scheduleToClose, String taskQueue) {
            this.retry = retry;
            this.startToClose = startToClose;
            this.scheduleToClose = scheduleToClose;
            this.taskQueue = taskQueue;
        }

        public ActivityOptions withRetry(RetryPolicy retry) {
            return new ActivityOptions(retry, startToClose, scheduleToClose, taskQueue);
        }

        public ActivityOptions withStartToCloseTimeout(Duration d) {
            return new ActivityOptions(retry, d, scheduleToClose, taskQueue);
        }

        public ActivityOptions withScheduleToCloseTimeout(Duration d) {
            return new ActivityOptions(retry, startToClose, d, taskQueue);
        }

        /** Run the activity on this task queue instead of the workflow's. */
        public ActivityOptions withTaskQueue(String q) {
            return new ActivityOptions(retry, startToClose, scheduleToClose, q);
        }

        void putOn(Map<String, Object> cmd) {
            if (retry != null) cmd.put("retry_policy", retry.toWire());
            putMillis(cmd, "start_to_close_timeout_ms", startToClose);
            putMillis(cmd, "schedule_to_close_timeout_ms", scheduleToClose);
            if (taskQueue != null && !taskQueue.isEmpty()) cmd.put("task_queue", taskQueue);
        }
    }

    /** Whether a workflow id whose run has finished may start a new run (the finished one is kept). */
    public enum IdReusePolicy {
        /** Start a new run whatever the finished one's outcome. */
        ALLOW_DUPLICATE,
        /** Only if the finished run failed, timed out, was cancelled or terminated. */
        ALLOW_DUPLICATE_FAILED_ONLY,
        /** Refuse (HTTP 409): the default. */
        REJECT_DUPLICATE
    }

    /** What a start does when its workflow id is taken. */
    public enum IdConflictPolicy {
        /** Fail with HTTP 409: the default. */
        FAIL,
        /** Return the run that holds the id instead ({@link Started#started} is false). */
        USE_EXISTING
    }

    /** Options for {@link Worker#start}. Immutable: each {@code with} method returns a copy. */
    public static final class StartOptions {
        final Duration executionTimeout;
        final IdReusePolicy reuse;
        final IdConflictPolicy conflict;
        final String taskQueue;

        public StartOptions() { this(null, null, null, null); }

        private StartOptions(Duration executionTimeout, IdReusePolicy reuse, IdConflictPolicy conflict, String taskQueue) {
            this.executionTimeout = executionTimeout;
            this.reuse = reuse;
            this.conflict = conflict;
            this.taskQueue = taskQueue;
        }

        /** End the workflow as TIMED_OUT if it has not finished this long after it starts. */
        public StartOptions withExecutionTimeout(Duration d) { return new StartOptions(d, reuse, conflict, taskQueue); }
        public StartOptions withIdReusePolicy(IdReusePolicy p) { return new StartOptions(executionTimeout, p, conflict, taskQueue); }
        public StartOptions withIdConflictPolicy(IdConflictPolicy p) { return new StartOptions(executionTimeout, reuse, p, taskQueue); }
        /** Run the workflow on this task queue instead of the worker's own. */
        public StartOptions withTaskQueue(String q) { return new StartOptions(executionTimeout, reuse, conflict, q); }
    }

    /** What {@link Worker#start} did: the run it began, or with USE_EXISTING the run that held the id. */
    public static final class Started {
        public final String workflowId;
        public final String runId;
        public final boolean started;

        Started(String workflowId, String runId, boolean started) {
            this.workflowId = workflowId;
            this.runId = runId;
            this.started = started;
        }
    }

    static void putMillis(Map<String, Object> obj, String field, Duration d) {
        if (d != null && !d.isZero() && !d.isNegative()) obj.put(field, Math.max(1L, d.toMillis()));
    }

    /**
     * What a workflow function receives. Every method is durable. The plain methods block until
     * the step has a result; the {@code Async} ones start the step and return a {@link Future}, so
     * several activities, timers and signal waits can be in flight at once:
     *
     * <pre>{@code
     * Workflowd.Future a = ctx.activityAsync("charge", order);
     * Workflowd.Future b = ctx.activityAsync("email", order);
     * ctx.waitAll(a, b);
     * }</pre>
     */
    public static final class Context {
        private final Map<String, Object> task;
        private final String workflowId;
        private final List<Object> steps;
        private final Map<String, Integer> versions = new LinkedHashMap<>();
        private final List<Map<String, Object>> pending = new ArrayList<>(); // new commands, in sequence order
        private final Map<Integer, Integer> cancelled = new LinkedHashMap<>(); // seq -> cancel order, this replay
        private Instant now; // the latest step result the code has received (the start, at first)
        private java.util.Random random;
        private int next = 0;
        private long observedEventId; // the newest step result the code has received

        // Graceful cancellation: told at the first point the code waits. The decision that
        // tells it records a cancel marker with the history length it saw; a replay hides later
        // results until it reaches the marker, so the code is told at the same point every time.
        private final long cancelAt;
        private final Instant cancelTime;
        private final String cancelReason;
        private final long historyLength;
        private long cutoff;
        private int cutoffSeq;
        private boolean delivered;
        private final java.util.Set<Integer> interrupted = new java.util.HashSet<>();

        @SuppressWarnings("unchecked")
        Context(Map<String, Object> task) {
            this.task = task;
            this.workflowId = str(task.get("workflow_id"));
            Object s = task.get("steps");
            this.steps = s instanceof List ? (List<Object>) s : new ArrayList<>();
            this.now = instant(task.get("started_at"));
            this.cancelAt = number(task.get("cancel_requested_event_id"));
            this.cancelTime = instant(task.get("cancel_requested_at"));
            this.cancelReason = str(task.get("cancel_reason"));
            this.historyLength = number(task.get("history_length"));
            for (Object o : steps) {
                Map<String, Object> rec = (Map<String, Object>) o;
                if ("MARKER".equals(str(rec.get("kind"))) && "cancel".equals(str(rec.get("marker")))) {
                    cutoffSeq = (int) number(rec.get("sequence"));
                    try {
                        cutoff = Long.parseLong(text(rec, "output"));
                    } catch (NumberFormatException e) {
                        cutoff = -1;
                    }
                    break;
                }
            }
        }

        private static long number(Object o) {
            return o instanceof Number ? ((Number) o).longValue() : 0;
        }

        /** True once a graceful cancel has reached this workflow's code. */
        public boolean cancelRequested() { return delivered; }

        public String input() { return text(task, "input"); }
        public byte[] inputBytes() { return bytes(task, "input"); }
        public String workflowId() { return workflowId; }

        /** This run's id: a workflow that continues as new gets a new one. */
        public String runId() { return str(task.get("run_id")); }

        /**
         * True once the history has grown past the engine's warning share of its limits, as of
         * the latest result the code has received, so every replay answers the same at the same
         * point. Then {@code return ctx.continueAsNew(state)}: a history that reaches its limit
         * fails the workflow.
         */
        public boolean continueAsNewSuggested() {
            Object at = task.get("suggest_continue_as_new_at");
            long suggestAt = at instanceof Number ? ((Number) at).longValue() : 0;
            return suggestAt > 0 && observedEventId >= suggestAt;
        }

        /**
         * End this run and start a fresh one under the same workflow id, with an empty history
         * and {@code input}: {@code return ctx.continueAsNew(state);}. It never returns. Steps
         * started before it still run; signals this run has not consumed are not carried over.
         */
        public String continueAsNew(String input) {
            return continueAsNew(input, null, null);
        }

        /** {@link #continueAsNew(String)} as another workflow type and/or with a new execution timeout (null: this run's). */
        public String continueAsNew(String input, String workflowType, Duration executionTimeout) {
            return continueAsNew(input, workflowType, executionTimeout, null);
        }

        /** {@link #continueAsNew(String, String, Duration)}, and on another task queue (null: this run's). */
        public String continueAsNew(String input, String workflowType, Duration executionTimeout, String taskQueue) {
            if (next < steps.size()) {
                throw new NonDeterminism("non-determinism: continued as new after " + next
                        + " steps, but history has " + steps.size());
            }
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("type", "continue_as_new");
            cmd.put("input", input == null ? "" : input);
            if (workflowType != null && !workflowType.isEmpty()) cmd.put("workflow_type", workflowType);
            if (taskQueue != null && !taskQueue.isEmpty()) cmd.put("task_queue", taskQueue);
            putMillis(cmd, "execution_timeout_ms", executionTimeout);
            throw new ContinueAsNew(cmd);
        }

        /**
         * The workflow's deterministic clock: when the latest step result the code has received
         * was recorded (the start, at first). It moves only as results arrive and reads the same on
         * every replay, so use it instead of {@code Instant.now()}.
         */
        public Instant now() {
            if (now == null) {
                throw new IllegalStateException("ctx.now() needs a workflowd engine that sends started_at (0.2.0 or later)");
            }
            return now;
        }

        /**
         * A deterministic random number in [0, 1): the same sequence on every replay of this
         * execution. Fine for jitter or picking a variant; not for secrets (it is derived from the
         * workflow id), use {@link #sideEffect} for those.
         */
        public double random() {
            if (random == null) random = new java.util.Random(seed(workflowId + "\u0000" + str(task.get("started_at"))));
            return random.nextDouble();
        }

        /** Pause durably until {@code when}, measured on {@link #now()}. */
        public void sleepUntil(String name, Instant when) {
            Duration d = Duration.between(now(), when);
            sleep(name, d.isNegative() ? Duration.ZERO : d);
        }

        /** Run a named activity on a worker and return its output. */
        public String activity(String name, String input) {
            return activityAsync(name, input).get();
        }

        /** Run a named activity, retrying a failed attempt per {@code retry} first. */
        public String activity(String name, String input, RetryPolicy retry) {
            return activity(name, input, new ActivityOptions().withRetry(retry));
        }

        /** Run a named activity with a retry policy and timeouts. */
        public String activity(String name, String input, ActivityOptions opts) {
            return activityAsync(name, input, opts).get();
        }

        /** Run a named activity over raw bytes. */
        public byte[] activityBytes(String name, byte[] input, RetryPolicy retry) {
            return activityBytes(name, input, new ActivityOptions().withRetry(retry));
        }

        /** Run a named activity over raw bytes, with a retry policy and timeouts. */
        public byte[] activityBytes(String name, byte[] input, ActivityOptions opts) {
            return activityBytesAsync(name, input, opts).getBytes();
        }

        /** Start an activity and return at once; {@link Future#get()} waits for its output. */
        public Future activityAsync(String name, String input) {
            return activityAsync(name, input, new ActivityOptions());
        }

        /** Start an activity with a retry policy and timeouts, and return at once. */
        public Future activityAsync(String name, String input, ActivityOptions opts) {
            Map<String, Object> cmd = scheduleActivity(name, opts);
            cmd.put("input", input == null ? "" : input);
            return issue("ACTIVITY", name, cmd);
        }

        /** Start an activity over raw bytes and return at once; {@link Future#getBytes()} waits. */
        public Future activityBytesAsync(String name, byte[] input, ActivityOptions opts) {
            Map<String, Object> cmd = scheduleActivity(name, opts);
            putBytes(cmd, "input", input);
            return issue("ACTIVITY", name, cmd);
        }

        private static Map<String, Object> scheduleActivity(String name, ActivityOptions opts) {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("type", "schedule_activity");
            cmd.put("step_name", name);
            if (opts != null) opts.putOn(cmd);
            return cmd;
        }

        /** Pause durably. Survives engine restarts. */
        public void sleep(String name, Duration duration) {
            sleepAsync(name, duration).get();
        }

        /** Start a durable timer and return at once; {@link Future#get()} returns when it fires. */
        public Future sleepAsync(String name, Duration duration) {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("type", "start_timer");
            cmd.put("step_name", name);
            cmd.put("delay_ms", Math.max(0L, duration.toMillis()));
            return issue("TIMER", name, cmd);
        }

        /** Block until a matching external signal arrives; return its payload. */
        public String waitForSignal(String name, String signalName) {
            return waitForSignalAsync(name, signalName).get();
        }

        /**
         * Block until a matching signal arrives, or return null if none arrives within
         * {@code timeout}. A timed-out wait is cancelled, so a late signal goes to the next wait
         * for it. (That uses a timer step named {@code <name>.timeout}.)
         */
        public String waitForSignal(String name, String signalName, Duration timeout) {
            Future wait = waitForSignalAsync(name, signalName);
            Future deadline = sleepAsync(name + ".timeout", timeout);
            return waitAny(true, wait, deadline) == 0 ? wait.get() : null;
        }

        /** Start waiting for a signal and return at once; {@link Future#get()} returns its payload. */
        public Future waitForSignalAsync(String name, String signalName) {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("type", "await_signal");
            cmd.put("step_name", name);
            cmd.put("signal_name", signalName);
            return issue("SIGNAL", name, cmd);
        }

        /** Wait for every future and return their outputs, in order. */
        public List<String> waitAll(Future... futures) {
            List<String> out = new ArrayList<>();
            for (Future f : futures) out.add(f.get());
            return out;
        }

        /**
         * Wait until one of the futures has a result and return its index. When several have one,
         * the step history recorded first wins, so every replay agrees:
         *
         * <pre>{@code
         * Workflowd.Future reply = ctx.waitForSignalAsync("reply", "approved");
         * Workflowd.Future deadline = ctx.sleepAsync("deadline", Duration.ofHours(48));
         * if (ctx.waitAny(true, reply, deadline) == 1) { ... timed out ... }
         * }</pre>
         */
        public int waitAny(Future... futures) {
            return waitAny(false, futures);
        }

        /** {@link #waitAny(Future...)}; with {@code cancelRest} the losers are cancelled. */
        public int waitAny(boolean cancelRest, Future... futures) {
            int best = -1;
            long bestId = 0;
            for (int i = 0; i < futures.length; i++) {
                Long id = futures[i].resolvedId();
                if (id != null && (best < 0 || id < bestId)) {
                    best = i;
                    bestId = id;
                }
            }
            if (best < 0 && cancelPending()) {
                deliverCancel();
                throw cancelFailure("");
            }
            if (best < 0) throw new Suspend();
            if (cancelRest) {
                for (int i = 0; i < futures.length; i++) {
                    if (i != best) futures[i].cancel("lost a waitAny race");
                }
            }
            if (futures[best].recorded() != null) observe(futures[best].seq);
            return best;
        }

        /**
         * Change workflow code while executions are in flight. An execution reaching this
         * point for the first time records {@code maxSupported}; one that ran past it under the
         * old code gets {@link Workflowd#DEFAULT_VERSION}. A recorded version outside the range fails
         * the execution.
         */
        @SuppressWarnings("unchecked")
        public int getVersion(String changeId, int minSupported, int maxSupported) {
            Integer v = versions.get(changeId);
            if (v == null) {
                int seq = next + 1;
                if (seq > steps.size()) {
                    next = seq;
                    Map<String, Object> cmd = new LinkedHashMap<>();
                    cmd.put("type", "record_marker");
                    cmd.put("step_name", changeId);
                    cmd.put("sequence", (long) seq);
                    cmd.put("marker", "version");
                    cmd.put("data", Integer.toString(maxSupported));
                    pending.add(cmd);
                    v = maxSupported;
                } else {
                    Map<String, Object> rec = (Map<String, Object>) steps.get(seq - 1);
                    if (!"MARKER".equals(str(rec.get("kind"))) || !"version".equals(str(rec.get("marker")))
                            || !changeId.equals(str(rec.get("step_name")))) {
                        v = DEFAULT_VERSION; // this execution ran past here before the change
                    } else {
                        next = seq;
                        v = Integer.parseInt(text(rec, "output"));
                    }
                }
                versions.put(changeId, v);
            }
            if (v < minSupported || v > maxSupported) {
                throw new NonDeterminism("non-determinism: execution recorded version " + v + " of \""
                        + changeId + "\", but the code supports " + minSupported + ".." + maxSupported);
            }
            return v;
        }

        /**
         * Run {@code fn} once, record its result and return the recorded result on every
         * replay. For small non-deterministic values; it must not fail or do I/O.
         */
        public String sideEffect(String name, java.util.function.Supplier<String> fn) {
            int seq = next + 1;
            if (seq > steps.size()) {
                next = seq;
                Map<String, Object> cmd = new LinkedHashMap<>();
                cmd.put("type", "record_marker");
                cmd.put("step_name", name);
                cmd.put("sequence", (long) seq);
                cmd.put("marker", "side_effect");
                String data = fn.get();
                cmd.put("data", data == null ? "" : data);
                pending.add(cmd);
                return data == null ? "" : data; // what every replay will return
            }
            return issue("MARKER", name, null).get();
        }

        /** Give the next durable call its sequence, check it against history, start it if new. */
        @SuppressWarnings("unchecked")
        private Future issue(String kind, String name, Map<String, Object> cmd) {
            int seq = ++next;
            Map<String, Object> recorded = seq <= steps.size() ? (Map<String, Object>) steps.get(seq - 1) : null;
            if (recorded == null) {
                Map<String, Object> c = new LinkedHashMap<>();
                if (cmd != null) c.putAll(cmd);
                c.put("sequence", (long) seq);
                pending.add(c);
            } else {
                // Determinism: the call being made now must be the step history recorded here.
                boolean markerMismatch = kind.equals("MARKER") && !"side_effect".equals(str(recorded.get("marker")));
                if (!kind.equals(str(recorded.get("kind"))) || !name.equals(str(recorded.get("step_name"))) || markerMismatch) {
                    throw new NonDeterminism("non-determinism at step " + seq + ": code issued "
                            + kind + " \"" + name + "\" but history has "
                            + str(recorded.get("kind")) + " \"" + str(recorded.get("step_name")) + "\"");
                }
            }
            return new Future(this, seq, name);
        }

        @SuppressWarnings("unchecked")
        private Map<String, Object> step(int seq) {
            return seq >= 1 && seq <= steps.size() ? (Map<String, Object>) steps.get(seq - 1) : null;
        }

        private boolean hasResult(int seq) {
            Map<String, Object> rec = step(seq);
            if (rec == null) return false;
            String status = str(rec.get("status"));
            return "COMPLETED".equals(status) || "FAILED".equals(status) || "CANCELLED".equals(status);
        }

        /**
         * A step's recorded result as the code may see it now, or null. Before a replay reaches
         * its cancel marker, results past the cutoff stay hidden: the decision that told the code
         * had not seen them. Markers are always there.
         */
        private Map<String, Object> visible(int seq) {
            if (!hasResult(seq)) return null;
            Map<String, Object> rec = step(seq);
            if (cutoff != 0 && !delivered && !"MARKER".equals(str(rec.get("kind")))
                    && number(rec.get("resolved_event_id")) > cutoff) {
                return null;
            }
            return rec;
        }

        private boolean cancelPending() { return cancelAt > 0 && !delivered; }

        private CancelledFailure cancelFailure(String stepName) {
            String why = "the workflow is being cancelled";
            return new CancelledFailure(stepName, cancelReason.isEmpty() ? why : why + " (" + cancelReason + ")");
        }

        /** The name and kind of the step at seq, recorded or started in this replay. */
        private String[] stepAt(int seq) {
            Map<String, Object> rec = step(seq);
            if (rec != null) return new String[] {str(rec.get("step_name")), str(rec.get("kind"))};
            for (Map<String, Object> c : pending) {
                if (number(c.get("sequence")) == seq && !"cancel_step".equals(str(c.get("type")))) {
                    String type = str(c.get("type"));
                    String kind = "schedule_activity".equals(type) ? "ACTIVITY" : "start_timer".equals(type) ? "TIMER"
                            : "await_signal".equals(type) ? "SIGNAL" : "MARKER";
                    return new String[] {str(c.get("step_name")), kind};
                }
            }
            return new String[] {"", "MARKER"};
        }

        /**
         * Tell the code it is being cancelled, where it waits: every step it started with no
         * result it can see is interrupted (get() throws CancelledFailure) and, if still open,
         * cancelled; the clock moves to the request. Steps it starts afterwards run normally.
         */
        private void deliverCancel() {
            int seq = next + 1;
            if (cutoffSeq == 0) {
                Map<String, Object> cmd = new LinkedHashMap<>();
                cmd.put("type", "record_marker");
                cmd.put("step_name", "cancellation");
                cmd.put("sequence", (long) seq);
                cmd.put("marker", "cancel");
                cmd.put("data", Long.toString(historyLength));
                pending.add(cmd);
            } else if (cutoff < 0) {
                throw new NonDeterminism("cancel marker at step " + cutoffSeq + " is unreadable");
            } else if (seq != cutoffSeq) {
                throw new NonDeterminism("non-determinism: the cancellation reached the code at step " + seq
                        + ", but history has it at step " + cutoffSeq);
            }
            next = seq;
            for (int s = 1; s < seq; s++) {
                String[] at = stepAt(s);
                if ("MARKER".equals(at[1]) || visible(s) != null || cancelled.containsKey(s)) continue;
                interrupted.add(s);
                if (!hasResult(s)) {
                    Map<String, Object> cmd = new LinkedHashMap<>();
                    cmd.put("type", "cancel_step");
                    cmd.put("step_name", at[0]);
                    cmd.put("sequence", (long) s);
                    cmd.put("reason", "the workflow is being cancelled");
                    pending.add(cmd);
                }
            }
            delivered = true;
            if (cancelTime != null && (now == null || cancelTime.isAfter(now))) now = cancelTime;
        }

        /** Move the clock to a step's result as the code receives it (not markers or cancels). */
        private void observe(int seq) {
            Map<String, Object> rec = step(seq);
            if (rec == null || "MARKER".equals(str(rec.get("kind"))) || "CANCELLED".equals(str(rec.get("status")))) return;
            Instant at = instant(rec.get("resolved_at"));
            if (at != null && (now == null || at.isAfter(now))) now = at;
            Object id = rec.get("resolved_event_id");
            if (id instanceof Number) observedEventId = Math.max(observedEventId, ((Number) id).longValue());
        }
    }

    /**
     * The result of a durable call started without waiting for it. {@link #get()} returns it, or
     * blocks the workflow until the step has one. Use a future only with the context that made it.
     */
    public static final class Future {
        private final Context ctx;
        private final int seq;
        private final String name;

        Future(Context ctx, int seq, String name) {
            this.ctx = ctx;
            this.seq = seq;
            this.name = name;
        }

        /** The step's output as text: throws ActivityFailure or CancelledFailure, or blocks. */
        public String get() { return text(result(), "output"); }

        /** The step's output as bytes. */
        public byte[] getBytes() { return bytes(result(), "output"); }

        /**
         * Cancel the step if it has no result yet: a timer stops mattering, a signal wait stops
         * taking signals (a later wait for the same signal gets the next one), and an activity's
         * worker is told to stop. {@link #get()} then throws {@link CancelledFailure}.
         */
        public void cancel() { cancel(""); }

        /** {@link #cancel()}, recording why. */
        public void cancel(String reason) {
            if (resolvedId() != null) return;
            ctx.cancelled.put(seq, ctx.cancelled.size() + 1);
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("type", "cancel_step");
            cmd.put("step_name", name);
            cmd.put("sequence", (long) seq);
            if (reason != null && !reason.isEmpty()) cmd.put("reason", reason);
            // A replay may hide a cancel history already has (past its cutoff): don't send it again.
            if (!ctx.hasResult(seq)) ctx.pending.add(cmd);
        }

        /** The recorded result as the code may see it: not started, in flight, retrying or hidden is null. */
        Map<String, Object> recorded() {
            return ctx.visible(seq);
        }

        /**
         * Orders this step's result for waitAny: its event id, or for a step this replay cancelled,
         * a place after every recorded result (the cancel is recorded next). Null without one.
         */
        Long resolvedId() {
            Map<String, Object> rec = recorded();
            if (rec != null) {
                Object id = rec.get("resolved_event_id");
                return id instanceof Number ? ((Number) id).longValue() : 0L;
            }
            if (ctx.interrupted.contains(seq)) return ctx.cancelAt;
            Integer n = ctx.cancelled.get(seq);
            return n == null ? null : Long.MAX_VALUE / 2 + n;
        }

        private Map<String, Object> result() {
            if (ctx.interrupted.contains(seq)) throw ctx.cancelFailure(name);
            Map<String, Object> rec = recorded();
            if (rec == null) {
                if (ctx.cancelled.containsKey(seq)) throw new CancelledFailure(name, "cancelled");
                if (ctx.cancelPending()) {
                    ctx.deliverCancel();
                    if (ctx.interrupted.contains(seq)) throw ctx.cancelFailure(name);
                }
                throw new Suspend(); // not started yet, in flight or being retried
            }
            String status = str(rec.get("status"));
            String err = str(rec.get("error"));
            if ("CANCELLED".equals(status)) throw new CancelledFailure(name, err.isEmpty() ? "cancelled" : err);
            ctx.observe(seq);
            if ("FAILED".equals(status)) {
                throw new ActivityFailure(name, err.isEmpty() ? "activity failed" : err,
                        str(rec.get("error_type")), Boolean.TRUE.equals(rec.get("non_retryable")));
            }
            return rec;
        }
    }

    /** The outcome of one replay: the commands to send (none: blocked), or a replay error. */
    static final class Decision {
        final List<Map<String, Object>> commands;
        final String replayError;
        Decision(List<Map<String, Object>> commands, String replayError) {
            this.commands = commands;
            this.replayError = replayError;
        }
    }

    /** Replay a workflow function against a task. Pure: no I/O, no shared state. */
    static Decision decide(WorkflowFunction fn, Map<String, Object> task) {
        Context ctx = new Context(task);
        try {
            String output = fn.run(ctx);
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("type", "complete_workflow");
            cmd.put("output", output == null ? "" : output);
            return new Decision(withLast(ctx.pending, cmd), null);
        } catch (Suspend s) {
            return new Decision(new ArrayList<>(ctx.pending), null);
        } catch (ContinueAsNew c) {
            return new Decision(withLast(ctx.pending, c.command), null);
        } catch (NonDeterminism e) {
            return new Decision(null, e.getMessage());
        } catch (Throwable t) {
            // WorkflowFailure, an uncaught ActivityFailure, or a bug: fail the workflow. A
            // CancelledFailure after a graceful cancel ends it CANCELLED.
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("type", "fail_workflow");
            String m = t.getMessage();
            cmd.put("error", m == null ? t.toString() : m);
            if (ctx.delivered && (t instanceof CancelledFailure || t.getCause() instanceof CancelledFailure)) {
                cmd.put("cancelled", Boolean.TRUE);
            }
            return new Decision(withLast(ctx.pending, cmd), null);
        }
    }

    private static List<Map<String, Object>> withLast(List<Map<String, Object>> pending, Map<String, Object> last) {
        List<Map<String, Object>> out = new ArrayList<>(pending);
        out.add(last);
        return out;
    }

    /** An RFC 3339 time from the engine, or null. */
    static Instant instant(Object v) {
        String s = str(v);
        if (s.isEmpty()) return null;
        try {
            return Instant.parse(s);
        } catch (java.time.format.DateTimeParseException e) {
            return null;
        }
    }

    /** FNV-1a over the string's UTF-8 bytes: a seed that is the same on every JVM. */
    static long seed(String s) {
        long h = 0xcbf29ce484222325L;
        for (byte b : s.getBytes(StandardCharsets.UTF_8)) {
            h ^= b & 0xff;
            h *= 0x100000001b3L;
        }
        return h;
    }

    /**
     * Connects to an engine and serves workflow decisions plus activity executions. Give it
     * every cluster node: a follower answers 503 naming the leader, and the worker moves there
     * and registers its workflow types again (a new engine starts with none).
     */
    public static final class Worker {
        private final List<String> addrs = new ArrayList<>();
        private int current = 0;
        private long epoch = 0;
        private volatile long registeredEpoch = -1;
        private final String workerId;
        private final String authToken;
        private volatile String namespace;
        private volatile String taskQueue = "";
        private final HttpClient http;
        private final Map<String, WorkflowFunction> workflows = new ConcurrentHashMap<>();
        private final Map<String, ActivityRunner> activities = new ConcurrentHashMap<>();
        private final AtomicBoolean stopped = new AtomicBoolean(false);
        private final java.util.concurrent.ScheduledExecutorService beats =
                java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "workflowd-heartbeats");
                    t.setDaemon(true);
                    return t;
                });

        public Worker(String address) {
            this(List.of(address), "java-worker-" + Long.toHexString(System.nanoTime()), null, null);
        }

        public Worker(String address, String workerId) {
            this(List.of(address), workerId, null, null);
        }

        /**
         * @param authToken  sent as {@code Authorization: Bearer <token>} for an engine started
         *                   with --auth-token-file; null for none
         * @param sslContext trust settings for an https address, e.g. {@link #trustPem}; null
         *                   uses the JVM's default trust store
         */
        public Worker(String address, String workerId, String authToken, SSLContext sslContext) {
            this(List.of(address), workerId, authToken, sslContext);
        }

        /** A worker for a cluster: every node's address, followed by leader hints. */
        public Worker(List<String> addresses, String workerId, String authToken, SSLContext sslContext) {
            if (addresses.isEmpty()) throw new IllegalArgumentException("workflowd: at least one address is required");
            for (String a : addresses) addrs.add(a.endsWith("/") ? a.substring(0, a.length() - 1) : a);
            this.workerId = workerId;
            this.authToken = authToken;
            HttpClient.Builder http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10));
            if (sslContext != null) http.sslContext(sslContext);
            this.http = http.build();
        }

        /**
         * The namespace every call works in. Unset, it is the default namespace, or the one a
         * namespace token grants. Set it before {@link #start} or {@link #run}.
         */
        public Worker namespace(String namespace) {
            this.namespace = namespace;
            return this;
        }

        /**
         * The task queue this worker polls, and where its {@link #start} calls run workflows unless
         * their options name another. Unset, it is the default queue. Set it before {@link #start}.
         */
        public Worker taskQueue(String taskQueue) {
            this.taskQueue = taskQueue == null || taskQueue.equals("default") ? "" : taskQueue;
            return this;
        }

        private String queueParam() {
            String q = taskQueue;
            return q.isEmpty() ? "" : "&task_queue=" + enc(q);
        }

        public Worker workflow(String type, WorkflowFunction fn) {
            workflows.put(type, fn);
            return this;
        }

        public Worker activity(String name, ActivityFunction fn) {
            return activityWithContext(name, (input, ctx) -> fn.run(input));
        }

        public Worker activityBytes(String name, BytesActivityFunction fn) {
            activities.put(name, (input, ctx) -> fn.run(input));
            return this;
        }

        /**
         * Register an activity that gets its task's {@link ActivityContext}: an idempotency key,
         * the attempt, and whether to stop (lease lost, start-to-close timeout, cancelled).
         */
        public Worker activityWithContext(String name, ContextActivityFunction fn) {
            activities.put(name, (input, ctx) -> {
                String out = fn.run(new String(input, StandardCharsets.UTF_8), ctx);
                return out == null ? new byte[0] : out.getBytes(StandardCharsets.UTF_8);
            });
            return this;
        }

        public void stop() { stopped.set(true); }

        // -- HTTP plumbing --

        private synchronized String base() { return addrs.get(current); }

        private synchronized long epoch() { return epoch; }

        /** Move off a node that turned us away: to the named leader, else the next address. */
        private synchronized void moveOn(String from, String leaderHttp) {
            epoch++;
            if (!addrs.get(current).equals(from)) return; // another call already moved on
            if (!leaderHttp.isEmpty()) {
                String url = URI.create(from).getScheme() + "://" + leaderHttp;
                int i = addrs.indexOf(url);
                if (i < 0) {
                    addrs.add(url);
                    i = addrs.size() - 1;
                }
                if (i != current) {
                    current = i;
                    return;
                }
            }
            current = (current + 1) % addrs.size();
        }

        private Object request(String method, String path, Map<String, Object> body) throws IOException, InterruptedException {
            return request(method, path, body, 12);
        }

        private Object request(String method, String path, Map<String, Object> body, int attempts)
                throws IOException, InterruptedException {
            long backoff = 50;
            for (int attempt = 1; ; attempt++) {
                String from = base();
                HttpRequest.BodyPublisher pub = body == null
                        ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(Json.write(body));
                HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(from + path))
                        .header("Content-Type", "application/json")
                        .timeout(Duration.ofSeconds(40))
                        .method(method, pub);
                if (authToken != null && !authToken.isEmpty()) req.header("Authorization", "Bearer " + authToken);
                String ns = namespace;
                if (ns != null && !ns.isEmpty()) req.header("X-Workflowd-Namespace", ns);
                HttpResponse<String> res;
                try {
                    res = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
                } catch (IOException e) {
                    moveOn(from, "");
                    if (attempt >= attempts) throw e;
                    Thread.sleep(backoff);
                    backoff = Math.min(backoff * 2, 1000);
                    continue;
                }
                if (res.statusCode() == 204) return null; // long-poll expired
                Object parsed = res.body().isEmpty() ? new LinkedHashMap<String, Object>() : Json.parse(res.body());
                String msg = parsed instanceof Map ? str(((Map<?, ?>) parsed).get("error")) : res.body();
                if (res.statusCode() == 503) {
                    String leader = parsed instanceof Map ? str(((Map<?, ?>) parsed).get("leader_http")) : "";
                    moveOn(from, leader);
                    if (attempt >= attempts) throw new HttpError(503, msg);
                    if (leader.isEmpty()) {
                        Thread.sleep(backoff); // no leader yet: give the election a moment
                        backoff = Math.min(backoff * 2, 1000);
                    }
                    continue;
                }
                if (res.statusCode() >= 400) {
                    boolean cancelled = parsed instanceof Map && Boolean.TRUE.equals(((Map<?, ?>) parsed).get("cancelled"));
                    throw new HttpError(res.statusCode(), msg, cancelled);
                }
                return parsed;
            }
        }

        static final class HttpError extends IOException {
            final int status;
            final boolean cancelled; // a 409 saying the work is no longer wanted
            HttpError(int status, String message) { this(status, message, false); }
            HttpError(int status, String message, boolean cancelled) {
                super("HTTP " + status + ": " + message);
                this.status = status;
                this.cancelled = cancelled;
            }
        }

        // -- client helpers --

        /** Tell the engine which workflow types this worker can replay. */
        public void register() throws IOException, InterruptedException {
            if (workflows.isEmpty()) return;
            long before = epoch();
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("workflow_types", new ArrayList<Object>(workflows.keySet()));
            body.put("worker_id", workerId);
            body.put("sdk", SDK_VERSION);
            request("POST", "/api/workflow-types", body);
            registeredEpoch = before; // if the call moved nodes, the next poll registers again
        }

        /** Start a workflow; returns its id. */
        public String startWorkflow(String type, String input, String workflowId) throws IOException, InterruptedException {
            return startWorkflow(type, input, workflowId, null);
        }

        /**
         * Start a workflow that ends as TIMED_OUT if it has not finished {@code executionTimeout}
         * after it starts (null: no limit); returns its id.
         */
        public String startWorkflow(String type, String input, String workflowId, Duration executionTimeout)
                throws IOException, InterruptedException {
            return start(type, input, workflowId, new StartOptions().withExecutionTimeout(executionTimeout)).workflowId;
        }

        /**
         * Start a workflow and say what happened. {@link StartOptions#withIdReusePolicy} decides
         * whether an id whose run has finished may start a new run; with
         * {@link IdConflictPolicy#USE_EXISTING} a taken id returns the run that holds it instead
         * of failing with a 409.
         */
        @SuppressWarnings("unchecked")
        public Started start(String type, String input, String workflowId, StartOptions opts)
                throws IOException, InterruptedException {
            boolean generated = workflowId == null || workflowId.isEmpty();
            // Chosen here so a retry after a lost reply cannot start a second execution.
            String id = generated ? type + "-" + Long.toHexString(System.nanoTime()) + Long.toHexString(Double.doubleToLongBits(Math.random())) : workflowId;
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("type", type);
            body.put("input", input == null ? "" : input);
            body.put("workflow_id", id);
            if (opts != null) {
                putMillis(body, "execution_timeout_ms", opts.executionTimeout);
                // Only when set: an engine before 0.3.0 refuses the fields.
                if (opts.reuse != null) body.put("id_reuse_policy", opts.reuse.name());
                if (opts.conflict != null) body.put("id_conflict_policy", opts.conflict.name());
            }
            // Only a named queue is sent: an engine before 0.4.0 refuses the field.
            String queue = opts != null && opts.taskQueue != null ? opts.taskQueue : taskQueue;
            if (!queue.isEmpty() && !queue.equals("default")) body.put("task_queue", queue);
            try {
                Object res = request("POST", "/api/workflows", body);
                Map<String, Object> m = res instanceof Map ? (Map<String, Object>) res : Map.of();
                return new Started(id, str(m.get("run_id")), !Boolean.FALSE.equals(m.get("started")));
            } catch (HttpError e) {
                if (!(generated && e.status == 409)) throw e;
                return new Started(id, "", true);
            }
        }

        /**
         * End a running workflow as CANCELLED, recording the reason. Workers running its
         * activities are turned away at their next heartbeat or completion.
         */
        public void cancel(String workflowId, String reason) throws IOException, InterruptedException {
            cancel(workflowId, reason, false);
        }

        /**
         * With {@code graceful}, tell the workflow code instead of ending it at once: the step it
         * waits on throws {@link CancelledFailure}, and it may compensate before rethrowing it.
         */
        public void cancel(String workflowId, String reason, boolean graceful) throws IOException, InterruptedException {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("reason", reason == null ? "" : reason);
            if (graceful) body.put("graceful", Boolean.TRUE);
            request("POST", "/api/workflows/" + enc(workflowId) + "/cancel", body);
        }

        /** A workflow's current state, as the engine's JSON API describes it. */
        public Map<String, Object> describe(String workflowId) throws IOException, InterruptedException {
            return describe(workflowId, null);
        }

        /** An earlier run's state (see {@link #runs}); null means the current run. */
        @SuppressWarnings("unchecked")
        public Map<String, Object> describe(String workflowId, String runId) throws IOException, InterruptedException {
            String q = runId == null || runId.isEmpty() ? "" : "?run_id=" + enc(runId);
            return (Map<String, Object>) request("GET", "/api/workflows/" + enc(workflowId) + q, null);
        }

        /** The runs kept for a workflow id: the current one, then earlier ones (continued as new), newest first. */
        @SuppressWarnings("unchecked")
        public List<Map<String, Object>> runs(String workflowId) throws IOException, InterruptedException {
            Map<String, Object> res = (Map<String, Object>) request("GET", "/api/workflows/" + enc(workflowId) + "/runs", null);
            List<Map<String, Object>> out = new ArrayList<>();
            for (Object r : (List<Object>) res.get("runs")) out.add((Map<String, Object>) r);
            return out;
        }

        /** The base URL of the node currently in use. */
        public String address() { return base(); }

        /**
         * Send an external signal to a running workflow. It is recorded once even if the
         * request is retried: every attempt carries the same idempotency key.
         */
        public void signal(String workflowId, String name, String payload) throws IOException, InterruptedException {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("name", name);
            body.put("payload", payload == null ? "" : payload);
            body.put("request_id", java.util.UUID.randomUUID().toString());
            request("POST", "/api/workflows/" + enc(workflowId) + "/signal", body);
        }

        // -- serving loops --

        @SuppressWarnings("unchecked")
        private void workflowLoop() {
            String types = String.join(",", workflows.keySet());
            while (!stopped.get()) {
                Map<String, Object> task;
                try {
                    if (registeredEpoch != epoch()) register();
                    Object res = request("GET",
                            "/api/workflow-tasks?types=" + enc(types) + "&worker_id=" + enc(workerId) + queueParam(), null, 1);
                    if (res == null) continue; // long-poll expired
                    task = (Map<String, Object>) res;
                } catch (Exception e) {
                    sleepQuietly(200); // engine unreachable or turned us away; retry
                    continue;
                }

                WorkflowFunction fn = workflows.get(str(task.get("workflow_type")));
                Decision decision = fn == null
                        ? new Decision(null, "no workflow registered for type " + str(task.get("workflow_type")))
                        : decide(fn, task);

                Map<String, Object> body = new LinkedHashMap<>();
                body.put("workflow_id", str(task.get("workflow_id")));
                body.put("task_token", str(task.get("task_token")));
                List<Map<String, Object>> cmds = new ArrayList<>();
                if (decision.commands != null) cmds.addAll(decision.commands);
                // One command travels in the field every engine version understands.
                if (cmds.size() > 1) body.put("commands", new ArrayList<Object>(cmds));
                else body.put("command", cmds.isEmpty() ? null : cmds.get(0));
                body.put("replay_error", decision.replayError == null ? "" : decision.replayError);
                try {
                    request("POST", "/api/workflow-tasks/complete", body, 3);
                } catch (HttpError e) {
                    // 409 = the decision went stale; polling again is the right response.
                    if (e.status != 409) System.err.println("[workflowd] complete failed: " + e.getMessage());
                } catch (Exception e) {
                    System.err.println("[workflowd] complete failed: " + e);
                }
            }
        }

        @SuppressWarnings("unchecked")
        private void activityLoop() {
            // Name our activities so the engine never hands us another worker's step.
            String names = String.join(",", activities.keySet());
            while (!stopped.get()) {
                Map<String, Object> task;
                try {
                    Object res = request("GET",
                            "/api/activity-tasks?activities=" + enc(names) + "&worker_id=" + enc(workerId) + queueParam(), null, 1);
                    if (res == null) continue;
                    task = (Map<String, Object>) res;
                } catch (Exception e) {
                    sleepQuietly(200);
                    continue;
                }
                execute(task);
            }
        }

        private void execute(Map<String, Object> task) {
            String step = str(task.get("step_name"));
            ActivityRunner fn = activities.get(step);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("workflow_id", str(task.get("workflow_id")));
            body.put("step_name", step);
            body.put("sequence", task.get("sequence"));
            body.put("lease_token", str(task.get("lease_token")));
            ActivityContext actx = new ActivityContext(task);
            java.util.concurrent.ScheduledFuture<?> beat = heartbeat(task, actx);
            // The engine fails the attempt at its start-to-close timeout: tell the activity too.
            Object limit = task.get("start_to_close_timeout_ms");
            java.util.concurrent.ScheduledFuture<?> stopBy = limit instanceof Number && ((Number) limit).longValue() > 0
                    ? beats.schedule(() -> { actx.leaseLost = true; }, ((Number) limit).longValue(), java.util.concurrent.TimeUnit.MILLISECONDS)
                    : null;
            try {
                if (fn == null) throw new IllegalStateException("no activity registered for step \"" + step + "\"");
                putBytes(body, "output", fn.run(bytes(task, "input"), actx));
            } catch (Exception e) {
                body.put("error", e.getMessage() == null ? e.toString() : e.getMessage());
                String type = e.getClass().getSimpleName();
                if (e instanceof NonRetryableException) {
                    NonRetryableException n = (NonRetryableException) e;
                    if (n.errorType != null && !n.errorType.isEmpty()) type = n.errorType;
                    body.put("non_retryable", Boolean.TRUE);
                }
                body.put("error_type", type);
            } finally {
                if (beat != null) beat.cancel(false);
                if (stopBy != null) stopBy.cancel(false);
            }
            try {
                try {
                    request("POST", "/api/activity-tasks/complete", body, 3);
                } catch (HttpError e) {
                    // An engine before 0.2.0 refuses the fields it doesn't know: report the
                    // failure without them rather than lose it.
                    if (e.status != 400 || !String.valueOf(e.getMessage()).contains("unknown field") || !body.containsKey("error_type")) throw e;
                    body.remove("error_type");
                    body.remove("non_retryable");
                    request("POST", "/api/activity-tasks/complete", body, 3);
                }
            } catch (HttpError e) {
                if (e.status != 409) System.err.println("[workflowd] activity complete failed: " + e.getMessage());
            } catch (Exception e) {
                System.err.println("[workflowd] activity complete failed: " + e);
            }
        }

        /**
         * Extend the task's lease every third of its duration while the activity runs. A 409
         * means the lease is gone; it says whether the work was cancelled.
         */
        private java.util.concurrent.ScheduledFuture<?> heartbeat(Map<String, Object> task, ActivityContext actx) {
            Object d = task.get("lease_duration_ms");
            long every = d instanceof Number ? ((Number) d).longValue() / 3 : 0;
            if (every <= 0) return null;
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("workflow_id", str(task.get("workflow_id")));
            body.put("lease_token", str(task.get("lease_token")));
            body.put("worker_id", workerId);
            return beats.scheduleAtFixedRate(() -> {
                try {
                    request("POST", "/api/activity-tasks/heartbeat", body, 2);
                } catch (HttpError e) {
                    if (e.status == 409) {
                        actx.cancelled = actx.cancelled || e.cancelled;
                        actx.leaseLost = true;
                    }
                } catch (Exception e) {
                    // Unreachable for now: the next beat tries again.
                }
            }, every, every, java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        /** Register, then serve workflow decisions and activities until stop(). */
        public void run() throws IOException, InterruptedException {
            start();
            while (!stopped.get()) Thread.sleep(200);
        }

        /** Register, then serve on background (daemon) threads until stop(); returns at once. */
        public void start() throws IOException, InterruptedException {
            stopped.set(false);
            List<Thread> threads = new ArrayList<>();
            if (!workflows.isEmpty()) {
                try {
                    register();
                } catch (HttpError e) {
                    if (e.status != 503 && e.status != 429) throw e; // a refused type is a bug to surface
                } catch (IOException e) {
                    // Not up yet: the workflow loop keeps trying.
                }
                for (int i = 0; i < 2; i++) threads.add(new Thread(this::workflowLoop, "workflowd-decisions-" + i));
            }
            // An empty filter means "any activity", so a worker with none must not poll.
            if (!activities.isEmpty()) {
                for (int i = 0; i < 4; i++) threads.add(new Thread(this::activityLoop, "workflowd-activities-" + i));
            }
            for (Thread t : threads) {
                t.setDaemon(true);
                t.start();
            }
        }
    }

    /** An SSLContext trusting only the PEM certificates in caFile: for an engine with a private CA. */
    public static SSLContext trustPem(Path caFile) throws IOException, GeneralSecurityException {
        KeyStore trust = KeyStore.getInstance(KeyStore.getDefaultType());
        trust.load(null, null);
        try (InputStream in = Files.newInputStream(caFile)) {
            int n = 0;
            for (Certificate c : CertificateFactory.getInstance("X.509").generateCertificates(in)) {
                trust.setCertificateEntry("ca-" + n++, c);
            }
        }
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trust);
        SSLContext ctx = SSLContext.getInstance("TLS");
        ctx.init(null, tmf.getTrustManagers(), null);
        return ctx;
    }

    // ---- helpers ----

    static String str(Object o) { return o == null ? "" : o.toString(); }

    static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }

    /**
     * Payloads travel as a JSON string, plus a {@code <field>_base64} twin when the bytes are
     * not valid UTF-8; the twin wins when present.
     */
    static byte[] bytes(Map<String, Object> obj, String field) {
        String b64 = str(obj.get(field + "_base64"));
        if (!b64.isEmpty()) return java.util.Base64.getDecoder().decode(b64);
        return str(obj.get(field)).getBytes(StandardCharsets.UTF_8);
    }

    static String text(Map<String, Object> obj, String field) {
        return new String(bytes(obj, field), StandardCharsets.UTF_8);
    }

    static void putBytes(Map<String, Object> obj, String field, byte[] b) {
        if (b == null) b = new byte[0];
        java.nio.charset.CharsetDecoder dec = StandardCharsets.UTF_8.newDecoder();
        try {
            obj.put(field, dec.decode(java.nio.ByteBuffer.wrap(b)).toString());
        } catch (java.nio.charset.CharacterCodingException e) {
            obj.put(field + "_base64", java.util.Base64.getEncoder().encodeToString(b));
        }
    }

    static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Minimal JSON codec. The JDK ships no JSON support, and adding Jackson or Gson would
     * make this SDK the only part of the project with a third-party dependency. This handles
     * exactly the subset the protocol uses: objects, arrays, strings, numbers, booleans, null.
     */
    static final class Json {
        private final String s;
        private int i;

        private Json(String s) { this.s = s; }

        static Object parse(String text) {
            Json p = new Json(text);
            p.ws();
            Object v = p.value();
            p.ws();
            return v;
        }

        private void ws() {
            while (i < s.length() && Character.isWhitespace(s.charAt(i))) i++;
        }

        private Object value() {
            char c = s.charAt(i);
            switch (c) {
                case '{': return object();
                case '[': return array();
                case '"': return string();
                case 't': i += 4; return Boolean.TRUE;
                case 'f': i += 5; return Boolean.FALSE;
                case 'n': i += 4; return null;
                default: return number();
            }
        }

        private Map<String, Object> object() {
            Map<String, Object> m = new LinkedHashMap<>();
            i++; // '{'
            ws();
            if (i < s.length() && s.charAt(i) == '}') { i++; return m; }
            while (true) {
                ws();
                String k = string();
                ws();
                i++; // ':'
                ws();
                m.put(k, value());
                ws();
                char c = s.charAt(i++);
                if (c == '}') return m;
                // otherwise ',' — continue
            }
        }

        private List<Object> array() {
            List<Object> l = new ArrayList<>();
            i++; // '['
            ws();
            if (i < s.length() && s.charAt(i) == ']') { i++; return l; }
            while (true) {
                ws();
                l.add(value());
                ws();
                char c = s.charAt(i++);
                if (c == ']') return l;
            }
        }

        private String string() {
            StringBuilder b = new StringBuilder();
            i++; // opening quote
            while (true) {
                char c = s.charAt(i++);
                if (c == '"') return b.toString();
                if (c != '\\') { b.append(c); continue; }
                char e = s.charAt(i++);
                switch (e) {
                    case 'n': b.append('\n'); break;
                    case 't': b.append('\t'); break;
                    case 'r': b.append('\r'); break;
                    case 'b': b.append('\b'); break;
                    case 'f': b.append('\f'); break;
                    case 'u':
                        b.append((char) Integer.parseInt(s.substring(i, i + 4), 16));
                        i += 4;
                        break;
                    default: b.append(e); // ", \, /
                }
            }
        }

        private Object number() {
            int start = i;
            while (i < s.length() && "-+.eE0123456789".indexOf(s.charAt(i)) >= 0) i++;
            String n = s.substring(start, i);
            if (n.contains(".") || n.contains("e") || n.contains("E")) return Double.parseDouble(n);
            return Long.parseLong(n);
        }

        static String write(Object v) {
            StringBuilder b = new StringBuilder();
            writeTo(v, b);
            return b.toString();
        }

        @SuppressWarnings("unchecked")
        private static void writeTo(Object v, StringBuilder b) {
            if (v == null) { b.append("null"); return; }
            if (v instanceof String) { quote((String) v, b); return; }
            if (v instanceof Number || v instanceof Boolean) { b.append(v); return; }
            if (v instanceof Map) {
                b.append('{');
                boolean first = true;
                for (Map.Entry<String, Object> e : ((Map<String, Object>) v).entrySet()) {
                    if (!first) b.append(',');
                    first = false;
                    quote(e.getKey(), b);
                    b.append(':');
                    writeTo(e.getValue(), b);
                }
                b.append('}');
                return;
            }
            if (v instanceof List) {
                b.append('[');
                boolean first = true;
                for (Object o : (List<Object>) v) {
                    if (!first) b.append(',');
                    first = false;
                    writeTo(o, b);
                }
                b.append(']');
                return;
            }
            quote(v.toString(), b);
        }

        private static void quote(String s, StringBuilder b) {
            b.append('"');
            for (int j = 0; j < s.length(); j++) {
                char c = s.charAt(j);
                switch (c) {
                    case '"': b.append("\\\""); break;
                    case '\\': b.append("\\\\"); break;
                    case '\n': b.append("\\n"); break;
                    case '\r': b.append("\\r"); break;
                    case '\t': b.append("\\t"); break;
                    default:
                        if (c < 0x20) b.append(String.format("\\u%04x", (int) c));
                        else b.append(c);
                }
            }
            b.append('"');
        }
    }
}
