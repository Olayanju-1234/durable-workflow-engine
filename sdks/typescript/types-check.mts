// Compile-only check that workflowd.d.mts describes the SDK as the examples
// use it (CI: tsc --noEmit). Not shipped in the npm package.
import { ActivityFailure, decide, Worker, WorkflowFailure, type Context } from 'workflowd';

const worker = new Worker({ address: 'https://localhost:8233', workerId: 'types', token: 'tok' });

worker.workflow('Order', async (ctx: Context) => {
  const reserved: string = await ctx.activity('reserve', ctx.input);
  await ctx.sleep('settle', 300);
  const approval: string = await ctx.waitForSignal('await-approval', 'approved');
  if (approval !== 'yes') throw new WorkflowFailure(`rejected: ${approval}`);
  try {
    await ctx.activity('charge', reserved);
  } catch (err) {
    if (err instanceof ActivityFailure) await ctx.activity('release', err.stepName);
    throw err;
  }
  return ctx.activity('ship', reserved);
});
worker.activity('reserve', async (input: string) => `reserved(${input})`).activity('ship', (input) => input);

async function client(): Promise<void> {
  const id: string = await worker.startWorkflow('Order', 'order-1');
  await worker.signal(id, 'approved', 'yes');
  const wf = await worker.describe(id);
  const decision = await decide(async (ctx) => ctx.input, { workflow_id: id, input: 'x', steps: [] });
  console.log(wf.status, decision.command?.type, decision.replayError);
  worker.stop();
  await worker.run();
}
void client;
