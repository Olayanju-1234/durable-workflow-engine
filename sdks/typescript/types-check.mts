// Compile-only check that workflowd.d.mts describes the SDK as the examples
// use it (CI: tsc --noEmit). Not shipped in the npm package.
import {
  ActivityFailure,
  CancelledFailure,
  DEFAULT_VERSION,
  decide,
  NonRetryableError,
  Worker,
  WorkflowFailure,
  type Context,
} from 'workflowd';

const worker = new Worker({ address: 'https://localhost:8233', workerId: 'types', token: 'tok' });

worker.workflow('Order', async (ctx: Context) => {
  const reserved: string = await ctx.activity('reserve', ctx.input);
  await ctx.sleep('settle', 300);
  const approval: string = await ctx.waitForSignal('await-approval', 'approved');
  if (approval !== 'yes') throw new WorkflowFailure(`rejected: ${approval}`);
  try {
    await ctx.activity('charge', reserved);
  } catch (err) {
    if (err instanceof ActivityFailure && !err.nonRetryable && err.type !== 'BadRequest') await ctx.activity('release', err.stepName);
    throw err;
  }
  return ctx.activity('ship', reserved);
});
worker.activity('reserve', async (input: string) => `reserved(${input})`).activity('ship', (input) => input);
worker.activity('charge', async () => {
  throw new NonRetryableError('card declined', { type: 'Declined' });
});

const cluster = new Worker({ addresses: ['http://n1:8233', 'http://n2:8233'], activityConcurrency: 8 });
cluster.workflow('Typed', async (ctx) => {
  const order = ctx.inputAs<{ id: string }>('json');
  const v: number = await ctx.getVersion('pricing', DEFAULT_VERSION, 1);
  const id: string = await ctx.sideEffect('id', () => `${order.id}-${v}`);
  const receipt = await ctx.activity<{ ok: boolean }>('charge', order, {
    retry: { maxAttempts: 3, initialBackoffMs: 100, nonRetryableErrorTypes: ['BadRequest'] },
    startToCloseTimeoutMs: 10_000,
    scheduleToCloseTimeoutMs: 60_000,
    as: 'json',
  });
  const blob: Uint8Array = await ctx.activity('render', new Uint8Array([1, 2]), { as: 'bytes' });
  const [a, b] = await Promise.all([ctx.activity('a', 1), ctx.activity('b', 2)]);
  const timedOut: boolean = (await ctx.waitAny([ctx.waitForSignal('late', 'x'), ctx.sleep('deadline', 1000)], { cancelRest: true })) === 1;
  const hedge = ctx.activity('hedge', 1);
  ctx.cancel(hedge, 'not needed');
  await hedge.catch((err: unknown) => (err instanceof CancelledFailure ? err.stepName : null));
  const approved = await ctx.waitForSignal<{ yes: boolean }>('wait', 'approve', { as: 'json' });
  void [a, b, timedOut];
  return { id, ok: receipt.ok && approved.yes, size: blob.length };
});
cluster.activity('render', (bytes: Uint8Array, task) => (task.signal.aborted ? bytes : bytes.reverse()), { as: 'bytes' });

async function client(): Promise<void> {
  const id: string = await worker.startWorkflow('Order', 'order-1');
  await worker.startWorkflow('Order', 'order-2', 'order-2', { executionTimeoutMs: 3_600_000 });
  await worker.signal(id, 'approved', 'yes');
  await worker.cancel(id, 'customer cancelled');
  const wf = await worker.describe(id);
  const decision = await decide(async (ctx) => ctx.input, { workflow_id: id, input: 'x', steps: [] });
  console.log(wf.status, decision.command?.type, decision.replayError);
  worker.stop();
  await worker.run();
}
void client;
