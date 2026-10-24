# workflowd (TypeScript / JavaScript SDK)

Write durable workflows in plain async JavaScript or TypeScript and run them on
[workflowd](https://github.com/Olayanju-1234/durable-workflow-engine), a single-binary
durable workflow engine. Zero dependencies: Node 18+'s built-in `fetch` only.

```js
import { Worker, WorkflowFailure } from 'workflowd'; // or: const { Worker } = require('workflowd')

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

More of the API:

```js
// Run activities in parallel; race a signal against a deadline, cancelling the loser.
const [stock, price] = await Promise.all([ctx.activity('stock', sku), ctx.activity('price', sku)]);
const late = (await ctx.waitAny([ctx.waitForSignal('reply', 'approved'), ctx.sleep('sla', 3_600_000)], { cancelRest: true })) === 1;

// Retry a failed attempt before the workflow sees the error; JSON and bytes payloads.
const receipt = await ctx.activity('charge', { amount: 42 }, { retry: { maxAttempts: 3, initialBackoffMs: 500 }, as: 'json' });

// Some failures are not worth retrying: throw NonRetryableError, or list error names in the policy.
worker.activity('deliver', async (event) => {
  const res = await fetch(url, { method: 'POST', body: JSON.stringify(event) });
  if (res.status >= 400 && res.status < 500) throw new NonRetryableError(`webhook answered ${res.status}`);
  if (!res.ok) throw new Error(`webhook answered ${res.status}`); // retried
});

// Bound one attempt and the whole step; bound a whole run when you start it.
await ctx.activity('render', doc, { startToCloseTimeoutMs: 30_000, scheduleToCloseTimeoutMs: 300_000 });
await worker.startWorkflow('Order', order, `order-${order.id}`, { executionTimeoutMs: 86_400_000 });
// Idempotent start: the run already holding the id comes back instead of a 409.
const { runId, started } = await worker.start('Order', order, { workflowId: `order-${order.id}`, idConflictPolicy: 'USE_EXISTING' });

// Time and randomness that replay the same: no Date.now() or Math.random() in workflow code.
const due = new Date(ctx.now().getTime() + 24 * 3600_000);
await ctx.sleepUntil('until-due', due);
const variant = ctx.random() < 0.5 ? 'a' : 'b';

// Wait for a signal, or give up: undefined if none arrives in time.
const reply = await ctx.waitForSignal('reply', 'approved', { timeoutMs: 48 * 3600_000 });
if (reply === undefined) return 'expired';

// Change code safely while executions are in flight; record a non-deterministic value once.
if ((await ctx.getVersion('fraud-check', DEFAULT_VERSION, 1)) === 1) await ctx.activity('fraud-check', receipt);
const id = await ctx.sideEffect('id', () => crypto.randomUUID());

// A loop that runs forever keeps its history small: a fresh run, same workflow id.
if (ctx.continueAsNewSuggested()) return ctx.continueAsNew(state);
```

A cluster: `new Worker({ addresses: ['http://n1:8233', 'http://n2:8233', 'http://n3:8233'] })`
follows the leader. Activities longer than the lease heartbeat automatically. An activity's
second argument is its task: `task.signal` aborts if the lease is lost, the attempt times out
or the work is cancelled; `task.attempt` counts from 1; `task.activityId` is the same on every
attempt, a ready-made idempotency key.

ESM and CommonJS both work (`import` or `require`, Node 18+), and both load the same module,
so `instanceof ActivityFailure` holds wherever the error came from.

Cancel gracefully with `worker.cancel(id, reason, { graceful: true })`: what the workflow awaits
rejects with `CancelledFailure`, steps it starts afterwards run normally (compensate there), and
rethrowing the failure ends it `CANCELLED`; `ctx.cancelRequested` says it was told.

Cancel a whole workflow with `worker.cancel(id, reason)`: it ends as `CANCELLED` and the
activities it was running get their `task.signal` aborted with a `CancelledFailure`.

For an engine with auth and TLS: `new Worker({ address: 'https://…:8233', token })`, and
`NODE_EXTRA_CA_CERTS=/path/ca.pem` for a private CA. With a namespace's token the worker works in
that namespace; with the engine's token, pick one with `namespace: 'acme'` (engine 0.4.0+).

Workflow code must be deterministic: no clocks, randomness or I/O except through `ctx`.
How replay works, and the protocol, are in the
[SDK guide](https://github.com/Olayanju-1234/durable-workflow-engine/tree/main/sdks#readme).
Pre-alpha: the API may change. Apache-2.0.
