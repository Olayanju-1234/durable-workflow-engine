// Compile-only check that the CommonJS entry point (require('workflowd'), as
// NestJS apps load it) has the same types (CI: tsc --noEmit). Not shipped.
import { ActivityFailure, CancelledFailure, NonRetryableError, Worker, type Context } from 'workflowd';

const worker = new Worker({ address: 'http://localhost:8233', token: 'tok' });

worker.workflow('liftoff.custom-step', async (ctx: Context) => {
  const input = ctx.inputAs<{ deadline: string }>('json');
  try {
    await ctx.activity('send', input, { retry: { maxAttempts: 6, initialBackoffMs: 5000 }, startToCloseTimeoutMs: 30_000 });
  } catch (err) {
    if (err instanceof ActivityFailure && err.nonRetryable) return 'not delivered';
    throw err;
  }
  const wait = Math.max(0, Date.parse(input.deadline) - ctx.now().getTime());
  const callback = await ctx.waitForSignal('await-callback', 'callback', { timeoutMs: wait });
  return callback === undefined ? 'deadline passed' : 'called back';
});
worker.activity('send', async (_input: unknown, task) => {
  if (task.signal.reason instanceof CancelledFailure) return null;
  throw new NonRetryableError(`rejected ${task.activityId}`, { type: 'HTTP4xx' });
});
void worker.cancel('id', 'no longer needed');
