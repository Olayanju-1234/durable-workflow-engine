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

    public static final String SDK_VERSION = "java/0.1.0";

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

    /** What a workflow function receives. Every method is durable. */
    public static final class Context {
        private final String input;
        private final String workflowId;
        private final List<Object> steps;
        private int next = 0;

        @SuppressWarnings("unchecked")
        Context(Map<String, Object> task) {
            this.input = str(task.get("input"));
            this.workflowId = str(task.get("workflow_id"));
            Object s = task.get("steps");
            this.steps = s instanceof List ? (List<Object>) s : new ArrayList<>();
        }

        public String input() { return input; }
        public String workflowId() { return workflowId; }

        /** Run a named activity on a worker and return its output. */
        public String activity(String name, String input) {
            Map<String, Object> cmd = new LinkedHashMap<>();
            cmd.put("type", "schedule_activity");
            cmd.put("step_name", name);
            cmd.put("input", input == null ? "" : input);
            return command("ACTIVITY", name, cmd);
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
            return command("SIGNAL", name, cmd);
        }

        /** Return the recorded result, stay blocked, or suspend with the next command. */
        @SuppressWarnings("unchecked")
        private String command(String kind, String name, Map<String, Object> cmd) {
            int seq = ++next;
            Map<String, Object> recorded = seq <= steps.size()
                    ? (Map<String, Object>) steps.get(seq - 1)
                    : null;

            if (recorded != null) {
                // Determinism: the call being made now must be the step history recorded here.
                if (!kind.equals(str(recorded.get("kind"))) || !name.equals(str(recorded.get("step_name")))) {
                    throw new NonDeterminism("non-determinism at step " + seq + ": code issued "
                            + kind + " \"" + name + "\" but history has "
                            + str(recorded.get("kind")) + " \"" + str(recorded.get("step_name")) + "\"");
                }
                String status = str(recorded.get("status"));
                if ("COMPLETED".equals(status)) return str(recorded.get("output"));
                if ("FAILED".equals(status)) {
                    String err = str(recorded.get("error"));
                    throw new ActivityFailure(name, err.isEmpty() ? "activity failed" : err);
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

    /** Connects to an engine and serves workflow decisions plus activity executions. */
    public static final class Worker {
        private final String address;
        private final String workerId;
        private final String authToken;
        private final HttpClient http;
        private final Map<String, WorkflowFunction> workflows = new ConcurrentHashMap<>();
        private final Map<String, ActivityFunction> activities = new ConcurrentHashMap<>();
        private final AtomicBoolean stopped = new AtomicBoolean(false);

        public Worker(String address) {
            this(address, "java-worker-" + Long.toHexString(System.nanoTime()));
        }

        public Worker(String address, String workerId) {
            this(address, workerId, null, null);
        }

        /**
         * @param authToken  sent as {@code Authorization: Bearer <token>} for an engine started
         *                   with --auth-token-file; null for none
         * @param sslContext trust settings for an https address, e.g. {@link #trustPem}; null
         *                   uses the JVM's default trust store
         */
        public Worker(String address, String workerId, String authToken, SSLContext sslContext) {
            this.address = address.endsWith("/") ? address.substring(0, address.length() - 1) : address;
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
            activities.put(name, fn);
            return this;
        }

        public void stop() { stopped.set(true); }

        // -- HTTP plumbing --

        private Object request(String method, String path, Map<String, Object> body) throws IOException, InterruptedException {
            HttpRequest.BodyPublisher pub = body == null
                    ? HttpRequest.BodyPublishers.noBody()
                    : HttpRequest.BodyPublishers.ofString(Json.write(body));
            HttpRequest.Builder req = HttpRequest.newBuilder(URI.create(address + path))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofSeconds(40))
                    .method(method, pub);
            if (authToken != null && !authToken.isEmpty()) req.header("Authorization", "Bearer " + authToken);
            HttpResponse<String> res = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
            if (res.statusCode() == 204) return null; // long-poll expired
            Object parsed = res.body().isEmpty() ? new LinkedHashMap<String, Object>() : Json.parse(res.body());
            if (res.statusCode() >= 400) {
                String msg = parsed instanceof Map ? str(((Map<?, ?>) parsed).get("error")) : res.body();
                throw new HttpError(res.statusCode(), msg);
            }
            return parsed;
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
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("workflow_types", new ArrayList<Object>(workflows.keySet()));
            body.put("worker_id", workerId);
            body.put("sdk", SDK_VERSION);
            request("POST", "/api/workflow-types", body);
        }

        /** Start a workflow; returns its id. */
        @SuppressWarnings("unchecked")
        public String startWorkflow(String type, String input, String workflowId) throws IOException, InterruptedException {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("type", type);
            body.put("input", input == null ? "" : input);
            body.put("workflow_id", workflowId == null ? "" : workflowId);
            Object res = request("POST", "/api/workflows", body);
            return str(((Map<String, Object>) res).get("workflow_id"));
        }

        /** Send an external signal to a running workflow. */
        public void signal(String workflowId, String name, String payload) throws IOException, InterruptedException {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("name", name);
            body.put("payload", payload == null ? "" : payload);
            request("POST", "/api/workflows/" + enc(workflowId) + "/signal", body);
        }

        // -- serving loops --

        @SuppressWarnings("unchecked")
        private void workflowLoop() {
            // An empty filter means "any type", so a worker with no workflows must not poll.
            if (workflows.isEmpty()) return;
            String types = String.join(",", workflows.keySet());
            while (!stopped.get()) {
                Map<String, Object> task;
                try {
                    Object res = request("GET",
                            "/api/workflow-tasks?types=" + enc(types) + "&worker_id=" + enc(workerId), null);
                    if (res == null) continue; // long-poll expired
                    task = (Map<String, Object>) res;
                } catch (Exception e) {
                    sleepQuietly(500); // engine unreachable; back off
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
                    request("POST", "/api/workflow-tasks/complete", body);
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
            if (activities.isEmpty()) return;
            String names = String.join(",", activities.keySet());
            while (!stopped.get()) {
                Map<String, Object> task;
                try {
                    Object res = request("GET",
                            "/api/activity-tasks?activities=" + enc(names) + "&worker_id=" + enc(workerId), null);
                    if (res == null) continue;
                    task = (Map<String, Object>) res;
                } catch (Exception e) {
                    sleepQuietly(500);
                    continue;
                }

                String step = str(task.get("step_name"));
                ActivityFunction fn = activities.get(step);
                String output = "";
                String error = "";
                if (fn == null) {
                    error = "no activity registered for step \"" + step + "\"";
                } else {
                    try {
                        String out = fn.run(str(task.get("input")));
                        output = out == null ? "" : out;
                    } catch (Exception e) {
                        error = e.getMessage() == null ? e.toString() : e.getMessage();
                    }
                }

                Map<String, Object> body = new LinkedHashMap<>();
                body.put("workflow_id", str(task.get("workflow_id")));
                body.put("step_name", step);
                body.put("sequence", task.get("sequence"));
                body.put("lease_token", str(task.get("lease_token")));
                body.put("output", output);
                body.put("error", error);
                try {
                    request("POST", "/api/activity-tasks/complete", body);
                } catch (HttpError e) {
                    if (e.status != 409) System.err.println("[workflowd] activity complete failed: " + e.getMessage());
                } catch (Exception e) {
                    System.err.println("[workflowd] activity complete failed: " + e);
                }
            }
        }

        /** Register, then serve workflow decisions and activities until stop(). */
        public void run() throws IOException, InterruptedException {
            register();
            Thread w = new Thread(this::workflowLoop, "workflowd-decisions");
            Thread a = new Thread(this::activityLoop, "workflowd-activities");
            w.setDaemon(true);
            a.setDaemon(true);
            w.start();
            a.start();
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
