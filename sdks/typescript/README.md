# workflowd (TypeScript / JavaScript SDK)

Write durable workflows in plain async JavaScript or TypeScript and run them on
[workflowd](https://github.com/Olayanju-1234/durable-workflow-engine), a single-binary
durable workflow engine. Zero dependencies: Node 18+'s built-in `fetch` only.

```js
import { Worker, WorkflowFailure } from 'workflowd';

const worker = new Worker({ address: 'http://localhost:8233' });

worker.workflow('Order', async (ctx) => {
  const reserved = await ctx.activity('reserve', ctx.input);
  await ctx.sleep('settle', 2000); // durable: survives an engine restart
  try {
    await ctx.activity('charge', reserved);
  } catch (err) {
    await ctx.activity('release', reserved); // compensate
    throw new WorkflowFailure(`charge failed: ${err.message}`);
  }
  return ctx.activity('ship', reserved);
});

worker.activity('reserve', async (input) => `reserved(${input})`);
await worker.run();
```

For an engine with auth and TLS: `new Worker({ address: 'https://…:8233', token })`, and
`NODE_EXTRA_CA_CERTS=/path/ca.pem` for a private CA.

Workflow code must be deterministic: no clocks, randomness or I/O except through `ctx`.
How replay works, and the protocol, are in the
[SDK guide](https://github.com/Olayanju-1234/durable-workflow-engine/tree/main/sdks#readme).
Pre-alpha: the API may change. Apache-2.0.
