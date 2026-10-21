import java.time.Duration;

/**
 * Runnable example: an order workflow written entirely in Java, orchestrated by a Go engine
 * that has never seen this code.
 *
 * <pre>
 *   1. start the engine:  ./workflowd --serve
 *   2. compile and run:   javac -d out Workflowd.java Example.java &amp;&amp; java -cp out Example
 * </pre>
 */
public final class Example {
    public static void main(String[] args) throws Exception {
        String address = System.getenv().getOrDefault("WORKFLOWD_ADDR", "http://localhost:8233");
        Workflowd.Worker worker = new Workflowd.Worker(address, "java-example");

        // ---- the workflow: ordinary Java ----
        worker.workflow("JavaOrder", ctx -> {
            String reserved = ctx.activity("reserve", ctx.input());

            ctx.sleep("settle", Duration.ofMillis(300)); // durable: survives an engine restart

            String approval = ctx.waitForSignal("await-approval", "approved");
            if (!"yes".equals(approval)) {
                ctx.activity("release", reserved);
                throw new Workflowd.WorkflowFailure("order rejected: " + approval);
            }

            try {
                ctx.activity("charge", reserved);
            } catch (Workflowd.ActivityFailure e) {
                ctx.activity("release", reserved); // Saga compensation
                throw new Workflowd.WorkflowFailure("charge failed, reservation released: " + e.getMessage());
            }

            return ctx.activity("ship", reserved);
        });

        // ---- the activities: where real side effects happen ----
        worker.activity("reserve", input -> "reserved(" + input + ")");
        worker.activity("charge", input -> "charged(" + input + ")");
        worker.activity("ship", input -> "shipped(" + input + ")");
        worker.activity("release", input -> "released(" + input + ")");

        System.out.println("[java] worker running against " + address + "; workflow type \"JavaOrder\" registered");
        worker.run();
    }
}
