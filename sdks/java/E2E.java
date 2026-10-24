import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.olayanju1234.workflowd.Workflowd;

/**
 * End-to-end check of the SDK against a live engine, run by sdks/sdk_e2e_test.go:
 *
 * <pre>
 *   javac -d out Workflowd.java E2E.java &amp;&amp; java -cp out E2E &lt;follower-url&gt; &lt;leader-url&gt;
 * </pre>
 *
 * The worker is given only the follower, so everything below also proves it follows the leader
 * hint. Prints PASS on success.
 */
public final class E2E {
    private static void fail(String msg) {
        System.err.println("[java e2e] FAIL: " + msg);
        System.exit(1);
    }

    private static Map<String, Object> waitDone(Workflowd.Worker worker, String id) throws Exception {
        for (int i = 0; i < 300; i++) {
            Map<String, Object> wf = worker.describe(id);
            String status = String.valueOf(wf.get("status"));
            if (status.equals("COMPLETED")) return wf;
            if (!status.equals("RUNNING") && !status.equals("WAITING")) fail(id + " is " + status + ": " + wf.get("error"));
            Thread.sleep(50);
        }
        fail(id + " did not complete");
        return null;
    }

    @SuppressWarnings("unchecked")
    private static boolean reached(Workflowd.Worker worker, String id, String step) throws Exception {
        for (int i = 0; i < 200; i++) {
            for (Object s : (List<Object>) worker.describe(id).get("steps")) {
                if (step.equals(((Map<String, Object>) s).get("step_name"))) return true;
            }
            Thread.sleep(50);
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    private static String steps(Map<String, Object> wf) {
        StringBuilder b = new StringBuilder();
        for (Object o : (List<Object>) wf.get("steps")) {
            Map<String, Object> s = (Map<String, Object>) o;
            if (b.length() > 0) b.append(' ');
            b.append(s.get("step_name")).append('=').append(s.get("status"));
        }
        return b.toString();
    }

    public static void main(String[] args) throws Exception {
        String follower = args[0];
        String leader = args[1];
        BufferedReader stdin = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        Workflowd.Worker worker = new Workflowd.Worker(List.of(follower), "java-e2e", null, null);

        // Three activities at once (each waits until all three are running, so this hangs unless
        // they really run in parallel), then a signal raced against a deadline that wins; the
        // losing wait is cancelled.
        CountDownLatch parts = new CountDownLatch(3);
        worker.workflow("JavaParallel", ctx -> {
            List<String> out = ctx.waitAll(ctx.activityAsync("java-part", "1"), ctx.activityAsync("java-part", "2"),
                    ctx.activityAsync("java-part", "3"));
            Workflowd.Future reply = ctx.waitForSignalAsync("reply", "never-sent");
            Workflowd.Future deadline = ctx.sleepAsync("deadline", Duration.ofMillis(200));
            int winner = ctx.waitAny(true, reply, deadline);
            String loser;
            try {
                loser = "got " + reply.get();
            } catch (Workflowd.CancelledFailure e) {
                loser = "cancelled";
            }
            return String.join(",", out) + " winner=" + winner + " " + loser;
        });
        worker.activity("java-part", input -> {
            parts.countDown();
            if (!parts.await(10, TimeUnit.SECONDS)) throw new IllegalStateException("parts never ran together");
            return String.valueOf(Integer.parseInt(input) * 10);
        });

        // The deterministic clock and random numbers read the same on every replay (sideEffect
        // records the first run's values to compare against), the clock moves with results, and
        // a signal wait with a timeout gives up or gets one.
        worker.workflow("JavaClock", ctx -> {
            Instant start = ctx.now();
            double r = ctx.random();
            String first = ctx.sideEffect("first", () -> start + "|" + r);
            int v = ctx.getVersion("clock-v2", Workflowd.DEFAULT_VERSION, 2);
            ctx.activity("java-tick", "");
            Instant after = ctx.now();
            ctx.sleepUntil("until", after.plusMillis(50));
            Instant slept = ctx.now();
            // The timed-out wait is cancelled, so the "reply" sent later goes to the second wait.
            String late = ctx.waitForSignal("maybe", "reply", Duration.ofMillis(100));
            Instant timedOut = ctx.now();
            String got = ctx.waitForSignal("reply", "reply", Duration.ofSeconds(10));
            return "same=" + first.equals(start + "|" + r)
                    + " moved=" + (after.isAfter(start) && !slept.isBefore(after.plusMillis(50)))
                    + " timeoutMoved=" + !timedOut.isBefore(slept.plusMillis(100))
                    + " late=" + late + " got=" + got + " v=" + v + " differs=" + (ctx.random() != r);
        });
        worker.activity("java-tick", input -> {
            Thread.sleep(20);
            return "tick";
        });

        // Step cancellation: an activity over its budget is cancelled, and its worker is told.
        CountDownLatch provisionCancelled = new CountDownLatch(1);
        worker.workflow("JavaCancel", ctx -> {
            Workflowd.Future provision = ctx.activityAsync("java-provision", "");
            Workflowd.Future budget = ctx.sleepAsync("budget", Duration.ofMillis(300));
            if (ctx.waitAny(provision, budget) == 1) provision.cancel("over budget");
            try {
                return "done " + provision.get();
            } catch (Workflowd.CancelledFailure e) {
                return "cancelled";
            }
        });
        worker.activityWithContext("java-provision", (input, actx) -> {
            for (int i = 0; i < 100 && !actx.isCancelled(); i++) Thread.sleep(50);
            if (actx.isCancelled() && actx.isLeaseLost() && actx.attempt() == 1 && !actx.activityId().isEmpty()) {
                provisionCancelled.countDown();
            }
            throw new IllegalStateException("stopped");
        });

        // Continue-as-new: a loop keeps its history small by starting a fresh run whenever the
        // engine suggests it (the harness's engine allows 60 events).
        worker.workflow("JavaLoop", ctx -> {
            int total = ctx.input().isEmpty() ? 0 : Integer.parseInt(ctx.input());
            while (!ctx.continueAsNewSuggested()) {
                if (total >= 50) return total + " ticks; run " + ctx.runId();
                ctx.activity("java-tick", "");
                total++;
            }
            return ctx.continueAsNew(String.valueOf(total));
        });

        // Graceful cancellation: the code is told at its wait, compensates, and lets the
        // cancellation end the workflow CANCELLED.
        java.util.concurrent.atomic.AtomicInteger released = new java.util.concurrent.atomic.AtomicInteger();
        worker.workflow("JavaGraceful", ctx -> {
            String reserved = ctx.activity("java-tick", "reserve");
            try {
                ctx.sleep("settle", Duration.ofHours(1));
            } catch (Workflowd.CancelledFailure e) {
                if (!ctx.cancelRequested()) throw new IllegalStateException("cancelled without a request");
                ctx.activity("java-release", reserved);
                throw e;
            }
            return "shipped";
        });
        worker.activity("java-release", input -> {
            released.incrementAndGet();
            return "released";
        });

        worker.workflow("JavaAfterRestart", ctx -> ctx.activity("java-tick", ""));
        worker.start();

        String parallelId = worker.startWorkflow("JavaParallel", "", "");
        if (!worker.address().equals(leader)) fail("worker uses " + worker.address() + ", want the leader " + leader);
        Map<String, Object> par = waitDone(worker, parallelId);
        if (!"10,20,30 winner=1 cancelled".equals(par.get("output"))) fail("parallel: output " + par.get("output"));
        if (!steps(par).endsWith("reply=CANCELLED deadline=COMPLETED")) fail("parallel: steps " + steps(par));

        String clockId = worker.startWorkflow("JavaClock", "", "");
        if (!reached(worker, clockId, "reply")) fail("clock: never reached the reply wait");
        worker.signal(clockId, "reply", "hi");
        Map<String, Object> clock = waitDone(worker, clockId);
        if (!"same=true moved=true timeoutMoved=true late=null got=hi v=2 differs=true".equals(clock.get("output"))) {
            fail("clock: output " + clock.get("output"));
        }
        if (!steps(clock).contains("maybe=CANCELLED maybe.timeout=COMPLETED")) fail("clock: steps " + steps(clock));

        Map<String, Object> cancel = waitDone(worker, worker.startWorkflow("JavaCancel", "", ""));
        if (!"cancelled".equals(cancel.get("output"))) fail("cancel: output " + cancel.get("output"));
        if (!provisionCancelled.await(5, TimeUnit.SECONDS)) fail("cancel: the activity was never told");

        String loopId = worker.startWorkflow("JavaLoop", "0", "");
        Map<String, Object> loop = waitDone(worker, loopId);
        List<Map<String, Object>> runs = worker.runs(loopId);
        Map<String, Object> earlier = worker.describe(loopId, String.valueOf(runs.get(runs.size() - 1).get("run_id")));
        boolean runsOk = runs.size() == 3;
        for (int i = 1; i < runs.size(); i++) runsOk &= "CONTINUED_AS_NEW".equals(runs.get(i).get("status"));
        if (!("50 ticks; run " + loop.get("run_id")).equals(loop.get("output")) || !runsOk
                || ((Number) loop.get("history_length")).longValue() > 60
                || !String.valueOf(runs.get(1).get("run_id")).equals(loop.get("continued_from_run_id"))
                || !"0".equals(earlier.get("input"))
                || !String.valueOf(runs.get(1).get("run_id")).equals(earlier.get("continued_as_new_run_id"))) {
            fail("continue-as-new: output " + loop.get("output") + ", runs " + runs);
        }

        String gracefulId = worker.startWorkflow("JavaGraceful", "", "");
        if (!reached(worker, gracefulId, "settle")) fail("graceful: never reached its sleep");
        worker.cancel(gracefulId, "customer left", true);
        Map<String, Object> graceful = null;
        for (int i = 0; i < 100; i++) {
            graceful = worker.describe(gracefulId);
            String status = String.valueOf(graceful.get("status"));
            if (status.equals("CANCELLED")) break;
            if (!status.equals("RUNNING") && !status.equals("WAITING")) fail("graceful: " + status + " " + graceful.get("error"));
            Thread.sleep(50);
        }
        if (!"java-tick=COMPLETED settle=CANCELLED cancellation=COMPLETED java-release=COMPLETED".equals(steps(graceful))
                || released.get() != 1 || !Boolean.TRUE.equals(graceful.get("cancel_requested"))
                || !String.valueOf(graceful.get("error")).contains("being cancelled (customer left)")) {
            fail("graceful: steps " + steps(graceful) + ", released " + released.get() + ", " + graceful);
        }

        // Workflow id reuse: a finished id is refused by default, returned with USE_EXISTING, and
        // starts a new run when the reuse policy allows it.
        Workflowd.Started first = worker.start("JavaAfterRestart", "", "java-reuse", new Workflowd.StartOptions());
        waitDone(worker, "java-reuse");
        int refused = 0;
        try {
            worker.start("JavaAfterRestart", "", "java-reuse", null);
        } catch (java.io.IOException e) {
            refused = e.getMessage().startsWith("HTTP 409") ? 409 : -1;
        }
        Workflowd.Started same = worker.start("JavaAfterRestart", "", "java-reuse",
                new Workflowd.StartOptions().withIdConflictPolicy(Workflowd.IdConflictPolicy.USE_EXISTING));
        Workflowd.Started again = worker.start("JavaAfterRestart", "", "java-reuse",
                new Workflowd.StartOptions().withIdReusePolicy(Workflowd.IdReusePolicy.ALLOW_DUPLICATE));
        waitDone(worker, "java-reuse");
        List<Map<String, Object>> reuseRuns = worker.runs("java-reuse");
        if (refused != 409 || same.started || !same.runId.equals(first.runId) || !again.started || reuseRuns.size() != 2
                || !again.runId.equals(reuseRuns.get(0).get("run_id")) || !first.runId.equals(reuseRuns.get(1).get("run_id"))) {
            fail("reuse: refused " + refused + ", same " + same.runId + "/" + same.started + ", again " + again.runId + ", runs " + reuseRuns);
        }

        // A restarted engine has forgotten every registration: the worker must notice and
        // register again before this workflow can be dispatched.
        System.out.println("RESTART");
        System.out.flush();
        stdin.readLine();
        String afterId = null;
        for (int i = 0; i < 100 && afterId == null; i++) {
            try {
                afterId = worker.startWorkflow("JavaAfterRestart", "", "");
            } catch (java.io.IOException e) {
                if (!e.getMessage().startsWith("HTTP 400")) fail("start after restart: " + e.getMessage());
                Thread.sleep(50);
            }
        }
        if (afterId == null) fail("the worker never registered with the restarted engine");
        Map<String, Object> after = waitDone(worker, afterId);
        if (!"tick".equals(after.get("output"))) fail("after restart: output " + after.get("output"));

        System.out.println("PASS");
        System.out.flush();
        System.exit(0);
    }
}
