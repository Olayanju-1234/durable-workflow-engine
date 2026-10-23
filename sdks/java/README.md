# workflowd (Java SDK)

Write durable workflows in plain Java and run them on
[workflowd](https://github.com/Olayanju-1234/durable-workflow-engine), a single-binary
durable workflow engine. Zero dependencies: the JDK only (Java 11+).

```xml
<dependency>
  <groupId>io.github.olayanju-1234</groupId>
  <artifactId>workflowd</artifactId>
  <version>0.2.0</version>
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

For an engine with auth and TLS:
`new Workflowd.Worker("https://…:8233", "worker-1", token, Workflowd.trustPem(Path.of("ca.pem")))`
(pass `null` for the SSLContext with a public CA).

Without Maven, `Workflowd.java` is one file: copy it into your source tree. A runnable
example is [`Example.java`](Example.java). Workflow code must be deterministic; how replay
works is in the [SDK guide](https://github.com/Olayanju-1234/durable-workflow-engine/tree/main/sdks#readme).
Pre-alpha: the API may change. Apache-2.0.
