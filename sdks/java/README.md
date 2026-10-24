# workflowd (Java SDK)

Write durable workflows in plain Java and run them on
[workflowd](https://github.com/Olayanju-1234/durable-workflow-engine), a single-binary
durable workflow engine. Zero dependencies: the JDK only (Java 11+).

```xml
<dependency>
  <groupId>io.github.olayanju-1234</groupId>
  <artifactId>workflowd</artifactId>
  <version>0.3.0</version>
</dependency>
```

```java
import io.github.olayanju1234.workflowd.Workflowd;

Workflowd.Worker worker = new Workflowd.Worker("http://localhost:8233");

worker.workflow("Order", ctx -> {
    String reserved = ctx.activity("reserve", ctx.input());
    ctx.sleep("settle", Duration.ofSeconds(2)); // durable: survives an engine restart
    try {
        ctx.activity("charge", reserved);
    } catch (Workflowd.ActivityFailure e) {
        ctx.activity("release", reserved); // compensate
        throw new Workflowd.WorkflowFailure("charge failed: " + e.getMessage());
    }
    return ctx.activity("ship", reserved);
});
worker.activity("reserve", input -> "reserved(" + input + ")");
worker.run();
```

More of the API:

```java
// Run activities in parallel; race a signal against a deadline and cancel the loser.
List<String> parts = ctx.waitAll(ctx.activityAsync("stock", sku), ctx.activityAsync("price", sku));
Workflowd.Future reply = ctx.waitForSignalAsync("reply", "approved");
Workflowd.Future sla = ctx.sleepAsync("sla", Duration.ofHours(1));
boolean late = ctx.waitAny(true, reply, sla) == 1;

// Or the same in one call: null if no signal arrives in time.
String answer = ctx.waitForSignal("answer", "approved", Duration.ofHours(1));

// A deterministic clock and random numbers: the same on every replay.
ctx.sleepUntil("reminder", ctx.now().plus(Duration.ofDays(2)));
double jitter = ctx.random();

// A loop that runs forever keeps its history small: a fresh run, same workflow id.
if (ctx.continueAsNewSuggested()) return ctx.continueAsNew(state);

// An activity that learns when to stop (lease lost, timeout, cancelled).
worker.activityWithContext("provision", (input, actx) -> {
    while (!actx.isLeaseLost()) { /* a chunk of work */ }
    return "stopped";
});
```

Idempotent starts: `worker.start(type, input, id, new Workflowd.StartOptions().withIdConflictPolicy(Workflowd.IdConflictPolicy.USE_EXISTING))`
returns the run already holding the id (`started` false) instead of failing with a 409;
`withIdReusePolicy(Workflowd.IdReusePolicy.ALLOW_DUPLICATE)` lets a finished id start again.

Cancel gracefully with `worker.cancel(id, reason, true)`: the step the workflow waits on throws
`Workflowd.CancelledFailure`, steps it starts afterwards run normally (compensate there), and
rethrowing it ends the workflow `CANCELLED`; `ctx.cancelRequested()` says it was told.

For an engine with auth and TLS:
`new Workflowd.Worker("https://…:8233", "worker-1", token, Workflowd.trustPem(Path.of("ca.pem")))`
(pass `null` for the SSLContext with a public CA). With a namespace's token the worker works in
that namespace; with the engine's token, pick one with `.namespace("acme")` (engine 0.4.0+).

Task queues (engine 0.4.0+): `worker.taskQueue("billing")` polls only the `billing` queue, and its
`start()` calls run workflows there; their activities run there too unless a call passes
`new Workflowd.ActivityOptions().withTaskQueue("email")`. Without one, everything uses the
default queue.

Without Maven, `Workflowd.java` is one file: copy it into your source tree. A runnable
example is [`Example.java`](Example.java). Workflow code must be deterministic (use `ctx.now()` and `ctx.random()`); how replay
works is in the [SDK guide](https://github.com/Olayanju-1234/durable-workflow-engine/tree/main/sdks#readme).
Pre-alpha: the API may change. Apache-2.0.
