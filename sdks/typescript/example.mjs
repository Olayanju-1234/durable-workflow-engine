/**
 * Runnable example: an order workflow written entirely in TypeScript/JavaScript,
 * orchestrated by a Go engine that has never seen this code.
 *
 *   1. start the engine:  ./workflowd --serve
 *   2. run this:          node example.mjs
 */

import { Worker, WorkflowFailure } from './workflowd.mjs';

const address = process.env.WORKFLOWD_ADDR ?? 'http://localhost:8233';
// Its own task queue, so these activities never meet another worker's of the
// same name (an engine with a demo worker runs reserve/charge/ship too).
const taskQueue = process.env.WORKFLOWD_TASK_QUEUE ?? 'ts-orders';
// For an engine with auth on; for a private CA, set NODE_EXTRA_CA_CERTS.
const worker = new Worker({ address, workerId: 'ts-example', token: process.env.WORKFLOWD_AUTH_TOKEN, taskQueue });

// ---- the workflow: ordinary async JavaScript ----
worker.workflow('TsOrder', async (ctx) => {
  const reserved = await ctx.activity('reserve', ctx.input);

  // Durable sleep — survives an engine restart.
  await ctx.sleep('settle', 300);

  // Park until someone approves. Could be seconds or days.
  const approval = await ctx.waitForSignal('await-approval', 'approved');
  if (approval !== 'yes') {
    await ctx.activity('release', reserved);
    throw new WorkflowFailure(`order rejected: ${approval}`);
  }

  try {
    await ctx.activity('charge', reserved);
  } catch (err) {
    await ctx.activity('release', reserved); // Saga compensation
    throw new WorkflowFailure(`charge failed, reservation released: ${err.message}`);
  }

  return await ctx.activity('ship', reserved);
});

// ---- the activities: where real side effects happen ----
worker.activity('reserve', async (input) => `reserved(${input})`);
worker.activity('charge', async (input) => `charged(${input})`);
worker.activity('ship', async (input) => `shipped(${input})`);
worker.activity('release', async (input) => `released(${input})`);

worker.run().catch((err) => {
  console.error('worker stopped:', err);
  process.exit(1);
});

console.log(`[ts] worker running against ${address}; workflow type "TsOrder" on task queue "${taskQueue}"`);
