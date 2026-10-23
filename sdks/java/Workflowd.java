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

    public static final String SDK_VERSION = "java/0.1.1";

    private Workflowd() {}

    /** Throw inside a workflow to fail it deliberately. */
    public static class WorkflowFailure extends RuntimeException {
        public WorkflowFailure(String message) { super(message); }
    }

    /** Thrown when an activity recorded a failure. Catch it to compensate. */
    public static class ActivityFailure extends RuntimeException {
        public final String stepName;
        public ActivityFailure(String stepName, String message) {
            super(message);
            this.stepName = stepName;
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
        final Map<String, Object> command; // null == "blocked, nothing to do"
        Suspend(Map<String, Object> command) {
            super("workflowd internal suspend", null, false, false); // no stack trace: hot path
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

        public RetryPolicy(int maxAttempts, Duration initialBackoff, Duration maxBackoff) {
            this.maxAttempts = maxAttempts;
            this.initialBackoff = initialBackoff == null ? Duration.ZERO : initialBackoff;
            this.maxBackoff = maxBackoff == null ? Duration.ZERO : maxBackoff;
        }

        Map<String, Object> toWire() {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("max_attempts", (long) maxAttempts);
            m.put("initial_backoff_ms", Math.max(0L, initialBackoff.toMillis()));
            m.put("max_backoff_ms", Math.max(0L, maxBackoff.toMillis()));
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

        public ActivityOptions() { this(null, null, null); }

        private ActivityOptions(RetryPolicy retry, Duration startToClose, Duration scheduleToClose) {
            this.retry = retry;
            this.startToClose = startToClose;
            this.scheduleToClose = scheduleToClose;
        }

        public ActivityOptions withRetry(RetryPolicy retry) {
            return new ActivityOptions(retry, startToClose, scheduleToClose);
        }

        public ActivityOptions withStartToCloseTimeout(Duration d) {
            return new ActivityOptions(retry, d, scheduleToClose);
        }

        public ActivityOptions withScheduleToCloseTimeout(Duration d) {
            return new ActivityOptions(retry, startToClose, d);
        }

        void putOn(Map<String, Object> cmd) {
            if (retry != null) cmd.put("retry_policy", retry.toWire());
            putMillis(cmd, "start_to_close_timeout_ms", startToClose);
            putMillis(cmd, "schedule_to_close_timeout_ms", scheduleToClose);
        }
    }

    static void putMillis(Map<String, Object> obj, String field, Duration d) {
        if (d != null && !d.isZero() && !d.isNegative()) obj.put(field, Math.max(1L, d.toMillis()));
    }

    /** What a workflow function receives. Every method is durable. */
    public static final class Context {
        private final Map<String, Object> task;
        private final String workflowId;
        private final List<Object> steps;
        private final Map<String, Integer> versions = new LinkedHashMap<>();
        private int next = 0;

        @SuppressWarnings("unchecked")
        Context(Map<String, Object> task) {
            this.task = task;
            this.workflowId = str(task.get("workflow_id"));
            Object s = task.get("steps");
            this.steps = s instanceof List ? (List<Object>) s : new ArrayList<>();
        }

        public String input() { return text(task, "input"); }
        public byte[] inputBytes() { return bytes(task, "input"); }
        public String workflowId() { return workflowId; }

        /** Run a named activity on a worker and return its output. */
        public String activity(String name, String input) {
            return activity(name, input, (ActivityOptions) null);
        }

        /** Run a named activity, retrying a failed attempt per {@code retry} first. */
        public String activity(String name, String input, RetryPolicy retry) {
            return activity(name, input, new ActivityOptions().withRetry(retry));
        }

        /** Run a named activity with a retry policy and timeouts. */
        public String activity(String name, String input, ActivityOptions opts) {
            Map<String, Object> cmd = scheduleActivity(name, opts);
            cmd.put("input", input == null ? "" : input);
            return text(command("ACTIVITY", name, cmd), "output");
        }

        /** Run a named activity over raw bytes. */
        public byte[] activityBytes(String name, byte[] input, RetryPolicy retry) {
            return activityBytes(name, input, new ActivityOptions().withRetry(retry));
        }

        /** Run a named activity over raw bytes, with a retry policy and timeouts. */
        public byte[] activityBytes(String name, byte[] input, ActivityOptions opts) {
            Map<String, Object> cmd = scheduleActivity(name, opts);
            putBytes(cmd, "input", input);
            return bytes(command("ACTIVITY", name, cmd), "output");
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
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("type", "start_timer");
            cmd.put("step_name", name);
            cmd.put("delay_ms", Math.max(0L, duration.toMillis()));
            command("TIMER", name, cmd);
        }

        /** Block until a matching external signal arrives; return its payload. */
        public String waitForSignal(String name, String signalName) {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("type", "await_signal");
            cmd.put("step_name", name);
            cmd.put("signal_name", signalName);
            return text(command("SIGNAL", name, cmd), "output");
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
                    throw new Suspend(cmd);
                }
                Map<String, Object> rec = (Map<String, Object>) steps.get(seq - 1);
                if (!"MARKER".equals(str(rec.get("kind"))) || !"version".equals(str(rec.get("marker")))
                        || !changeId.equals(str(rec.get("step_name")))) {
                    v = DEFAULT_VERSION; // this execution ran past here before the change
                } else {
                    next = seq;
                    v = Integer.parseInt(text(rec, "output"));
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
                throw new Suspend(cmd);
            }
            return text(command("MARKER", name, null), "output");
        }

        /** Return the recorded step, stay blocked, or suspend with the next command. */
        @SuppressWarnings("unchecked")
        private Map<String, Object> command(String kind, String name, Map<String, Object> cmd) {
            int seq = ++next;
            Map<String, Object> recorded = seq <= steps.size()
                    ? (Map<String, Object>) steps.get(seq - 1)
                    : null;

            if (recorded != null) {
                // Determinism: the call being made now must be the step history recorded here.
                boolean markerMismatch = kind.equals("MARKER") && !"side_effect".equals(str(recorded.get("marker")));
                if (!kind.equals(str(recorded.get("kind"))) || !name.equals(str(recorded.get("step_name"))) || markerMismatch) {
                    throw new NonDeterminism("non-determinism at step " + seq + ": code issued "
                            + kind + " \"" + name + "\" but history has "
                            + str(recorded.get("kind")) + " \"" + str(recorded.get("step_name")) + "\"");
                }
                String status = str(recorded.get("status"));
                if ("COMPLETED".equals(status)) return recorded;
                if ("FAILED".equals(status)) {
                    String err = str(recorded.get("error"));
                    throw new ActivityFailure(name, err.isEmpty() ? "activity failed" : err);
                }
                if ("CANCELLED".equals(status)) {
                    String err = str(recorded.get("error"));
                    throw new CancelledFailure(name, err.isEmpty() ? "cancelled" : err);
                }
                throw new Suspend(null); // SCHEDULED: still in flight
            }

            cmd.put("sequence", (long) seq);
            throw new Suspend(cmd);
        }
    }

    /** The outcome of one replay: either a command to send, or a replay error. */
    static final class Decision {
        final Map<String, Object> command;
        final String replayError;
        Decision(Map<String, Object> command, String replayError) {
            this.command = command;
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
            return new Decision(cmd, null);
        } catch (Suspend s) {
            return new Decision(s.command, null);
        } catch (NonDeterminism e) {
            return new Decision(null, e.getMessage());
        } catch (Throwable t) {
            // WorkflowFailure, an uncaught ActivityFailure, or a bug: fail the workflow.
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("type", "fail_workflow");
            String m = t.getMessage();
            cmd.put("error", m == null ? t.toString() : m);
            return new Decision(cmd, null);
        }
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
        private final HttpClient http;
        private final Map<String, WorkflowFunction> workflows = new ConcurrentHashMap<>();
        private final Map<String, BytesActivityFunction> activities = new ConcurrentHashMap<>();
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

        public Worker workflow(String type, WorkflowFunction fn) {
            workflows.put(type, fn);
            return this;
        }

        public Worker activity(String name, ActivityFunction fn) {
            activities.put(name, input -> {
                String out = fn.run(new String(input, StandardCharsets.UTF_8));
                return out == null ? new byte[0] : out.getBytes(StandardCharsets.UTF_8);
            });
            return this;
        }

        public Worker activityBytes(String name, BytesActivityFunction fn) {
            activities.put(name, fn);
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
                if (res.statusCode() >= 400) throw new HttpError(res.statusCode(), msg);
                return parsed;
            }
        }

        static final class HttpError extends IOException {
            final int status;
            HttpError(int status, String message) {
                super("HTTP " + status + ": " + message);
                this.status = status;
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
            boolean generated = workflowId == null || workflowId.isEmpty();
            // Chosen here so a retry after a lost reply cannot start a second execution.
            String id = generated ? type + "-" + Long.toHexString(System.nanoTime()) + Long.toHexString(Double.doubleToLongBits(Math.random())) : workflowId;
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("type", type);
            body.put("input", input == null ? "" : input);
            body.put("workflow_id", id);
            putMillis(body, "execution_timeout_ms", executionTimeout);
            try {
                request("POST", "/api/workflows", body);
            } catch (HttpError e) {
                if (!(generated && e.status == 409)) throw e;
            }
            return id;
        }

        /**
         * End a running workflow as CANCELLED, recording the reason. Workers running its
         * activities are turned away at their next heartbeat or completion.
         */
        public void cancel(String workflowId, String reason) throws IOException, InterruptedException {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("reason", reason == null ? "" : reason);
            request("POST", "/api/workflows/" + enc(workflowId) + "/cancel", body);
        }

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
                            "/api/workflow-tasks?types=" + enc(types) + "&worker_id=" + enc(workerId), null, 1);
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
                body.put("command", decision.command);
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
                            "/api/activity-tasks?activities=" + enc(names) + "&worker_id=" + enc(workerId), null, 1);
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
            BytesActivityFunction fn = activities.get(step);
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("workflow_id", str(task.get("workflow_id")));
            body.put("step_name", step);
            body.put("sequence", task.get("sequence"));
            body.put("lease_token", str(task.get("lease_token")));
            java.util.concurrent.ScheduledFuture<?> beat = heartbeat(task);
            try {
                if (fn == null) throw new IllegalStateException("no activity registered for step \"" + step + "\"");
                putBytes(body, "output", fn.run(bytes(task, "input")));
            } catch (Exception e) {
                body.put("error", e.getMessage() == null ? e.toString() : e.getMessage());
            } finally {
                if (beat != null) beat.cancel(false);
            }
            try {
                request("POST", "/api/activity-tasks/complete", body, 3);
            } catch (HttpError e) {
                if (e.status != 409) System.err.println("[workflowd] activity complete failed: " + e.getMessage());
            } catch (Exception e) {
                System.err.println("[workflowd] activity complete failed: " + e);
            }
        }

        /** Extend the task's lease every third of its duration while the activity runs. */
        private java.util.concurrent.ScheduledFuture<?> heartbeat(Map<String, Object> task) {
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
                } catch (Exception e) {
                    // A 409 means the lease is gone; the result will be rejected too.
                }
            }, every, every, java.util.concurrent.TimeUnit.MILLISECONDS);
        }

        /** Register, then serve workflow decisions and activities until stop(). */
        public void run() throws IOException, InterruptedException {
            List<Thread> threads = new ArrayList<>();
            if (!workflows.isEmpty()) {
                try {
                    register();
                } catch (HttpError e) {
                    if (e.status != 503) throw e; // a refused type is a bug to surface
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
            while (!stopped.get()) Thread.sleep(200);
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
