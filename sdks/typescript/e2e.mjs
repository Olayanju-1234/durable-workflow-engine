// End-to-end check of the SDK against a live engine, run by sdks/sdk_e2e_test.go:
//   node e2e.mjs <follower-url> <leader-url>
// The worker is given only the follower, so everything below also proves it
// follows the leader hint. Prints PASS on success.
import { createInterface } from 'node:readline';
import { Worker, DEFAULT_VERSION, NonRetryableError } from './workflowd.mjs';

const [follower, leader] = process.argv.slice(2);
const fail = (msg) => {
  console.error(`[ts e2e] FAIL: ${msg}`);
  process.exit(1);
};
const lines = createInterface({ input: process.stdin })[Symbol.asyncIterator]();

const worker = new Worker({ addresses: [follower], workerId: 'ts-e2e' });
let flakyCalls = 0;

worker.workflow('TsE2E', async (ctx) => {
  const order = ctx.inputAs('json');
  const v = await ctx.getVersion('pricing-v2', DEFAULT_VERSION, 2);
  const id = await ctx.sideEffect('order-id', () => `id-${order.n}`);
  const flaky = await ctx.activity('ts-flaky', order, {
    retry: { maxAttempts: 3, initialBackoffMs: 10 },
    as: 'json',
  });
  const bin = await ctx.activity('ts-binary', new Uint8Array([0xff, 0x00, 0x7f]), { as: 'bytes' });
  const slow = await ctx.activity('ts-slow', 'x');
  await ctx.sleep('nap', 20);
  const sig = await ctx.waitForSignal('wait', 'go', { as: 'json' });
  return { v, id, flaky, bin: [...bin], slow, sig };
});
// Three activities at once (each waits until all three are running, so this
// hangs unless they really run in parallel), then a signal raced against a
// deadline that wins.
worker.workflow('TsParallel', async (ctx) => {
  const parts = await Promise.all([1, 2, 3].map((n) => ctx.activity('ts-part', n, { as: 'json' })));
  const reply = ctx.waitForSignal('reply', 'never-sent');
  const deadline = ctx.sleep('deadline', 200);
  const winner = await ctx.waitAny([reply, deadline]);
  return { parts, winner };
});
let partsRunning = 0;
worker.activity('ts-part', async (n) => {
  partsRunning++;
  while (partsRunning < 3) await new Promise((r) => setTimeout(r, 5));
  return n * 10;
}, { as: 'json' });
// Timeouts: an attempt that outlives its start-to-close fails (and the
// activity's signal aborts), and a workflow past its execution timeout ends.
worker.workflow('TsTimeouts', async (ctx) => {
  try {
    await ctx.activity('ts-stall', '', { startToCloseTimeoutMs: 300, scheduleToCloseTimeoutMs: 5000 });
    return 'finished';
  } catch (err) {
    return `failed: ${err.message}`;
  }
});
let stallAborted = false;
worker.activity('ts-stall', (_input, task) => new Promise((_resolve, reject) => {
  task.signal.addEventListener('abort', () => {
    stallAborted = true;
    reject(task.signal.reason);
  });
}));
worker.workflow('TsForever', async (ctx) => ctx.waitForSignal('never', 'never-sent'));
// Cancellation: liftoff's custom-step race with the loser cancelled, so a late
// callback reaches the next wait; and a cancelled workflow tells its activity.
worker.workflow('TsRace', async (ctx) => {
  const callback = ctx.waitForSignal('await-callback', 'callback');
  const deadline = ctx.sleep('deadline', 100);
  if ((await ctx.waitAny([callback, deadline], { cancelRest: true })) === 0) return 'called back';
  return `late: ${await ctx.waitForSignal('late-callback', 'callback')}`;
});
let cancelReason = null;
let provisionStarted;
const provisioning = new Promise((r) => { provisionStarted = r; });
worker.workflow('TsProvision', async (ctx) => ctx.activity('ts-provision'));
worker.activity('ts-provision', (_input, task) => new Promise((_resolve, reject) => {
  provisionStarted();
  task.signal.addEventListener('abort', () => {
    cancelReason = task.signal.reason;
    reject(task.signal.reason);
  });
}));
// Non-retryable failures: a flagged 404 and a policy-listed BadRequest fail at
// once, a 503 retries; against an older engine the failure still arrives.
class BadRequest extends Error {
  constructor(message) {
    super(message);
    this.name = 'BadRequest';
  }
}
const webhookCalls = {};
worker.workflow('TsWebhook', async (ctx) => {
  try {
    await ctx.activity(ctx.input === 'legacy' ? 'ts-legacy' : 'ts-webhook', ctx.input, {
      retry: { maxAttempts: ctx.input === 'legacy' ? 1 : 4, initialBackoffMs: 10, nonRetryableErrorTypes: ['BadRequest'] },
    });
    return 'delivered';
  } catch (err) {
    return `${err.type}|${err.nonRetryable}|${err.message}`;
  }
});
const webhook = async (code) => {
  webhookCalls[code] = (webhookCalls[code] ?? 0) + 1;
  if (code === '404') throw new NonRetryableError('webhook answered 404');
  if (code === '400' || code === 'legacy') throw new BadRequest('webhook answered 400');
  throw new Error('webhook answered 503');
};
worker.activity('ts-webhook', webhook);
worker.activity('ts-legacy', webhook);
// Ergonomics: a deterministic clock and random numbers (what history recorded
// at the first decision equals what the last replay computes), sleepUntil, a
// signal wait with a timeout, and attempt numbers with a stable activity id.
worker.workflow('TsErgonomics', async (ctx) => {
  const started = ctx.now().getTime();
  const roll = ctx.random();
  const seen = await ctx.activity('ts-attempts', { roll }, { as: 'json', retry: { maxAttempts: 2, initialBackoffMs: 10 } });
  const afterActivity = ctx.now().getTime();
  await ctx.sleepUntil('until', new Date(afterActivity + 150));
  // The timed-out wait is cancelled, so the signal sent later reaches the next wait.
  const missing = await ctx.waitForSignal('missing', 'late', { timeoutMs: 100 });
  const present = await ctx.waitForSignal('present', 'late', { timeoutMs: 60_000 });
  return { started, roll, seen, clockMoved: afterActivity > started, missing: missing ?? null, present };
});
const attemptsSeen = [];
worker.activity('ts-attempts', async (input, task) => {
  attemptsSeen.push({ attempt: task.attempt, id: task.activityId, roll: input.roll });
  if (task.attempt === 1) throw new Error('first attempt fails');
  return attemptsSeen;
}, { as: 'json' });
worker.workflow('TsAfterRestart', async (ctx) => ctx.activity('ts-flaky', { n: 41 }, { as: 'json' }));

worker.activity('ts-flaky', async (order) => {
  if (order.n === 1 && ++flakyCalls < 3) throw new Error('flaky');
  return { ok: order.n + 1 };
}, { as: 'json' });
worker.activity('ts-binary', async (bytes) => Buffer.from([...bytes].reverse()), { as: 'bytes' });
// Longer than the engine's 600 ms lease: it only survives by heartbeating.
worker.activity('ts-slow', async () => {
  await new Promise((r) => setTimeout(r, 1500));
  return 'slow-done';
});

async function waitDone(id) {
  for (let i = 0; i < 300; i++) {
    const wf = await worker.describe(id);
    if (wf.status === 'COMPLETED') return wf;
    if (wf.status !== 'RUNNING' && wf.status !== 'WAITING') fail(`workflow ${id} is ${wf.status}: ${wf.error}`);
    await new Promise((r) => setTimeout(r, 50));
  }
  fail(`workflow ${id} did not complete`);
}

// Registered before anything starts, so the first start can't race it.
await worker.register();
worker.run().catch((err) => fail(`worker: ${err.message}`));

const id = await worker.startWorkflow('TsE2E', { n: 1 });
if (worker.address !== leader) fail(`worker uses ${worker.address}, want the leader ${leader}`);
for (let i = 0; ; i++) {
  const wf = await worker.describe(id);
  if (wf.steps.some((s) => s.step_name === 'wait')) break;
  if (i > 300) fail('workflow never reached its signal wait');
  await new Promise((r) => setTimeout(r, 50));
}
await worker.signal(id, 'go', { go: true });
const wf = await waitDone(id);
const want = { v: 2, id: 'id-1', flaky: { ok: 2 }, bin: [0x7f, 0x00, 0xff], slow: 'slow-done', sig: { go: true } };
if (JSON.stringify(JSON.parse(wf.output)) !== JSON.stringify(want)) fail(`output ${wf.output}, want ${JSON.stringify(want)}`);
const flakyStep = wf.steps.find((s) => s.step_name === 'ts-flaky');
if (flakyStep.attempts !== 3) fail(`ts-flaky attempts ${flakyStep.attempts}, want 3`);
const slowStep = wf.steps.find((s) => s.step_name === 'ts-slow');
if (slowStep.attempts !== 1) fail(`ts-slow attempts ${slowStep.attempts}, want 1 (heartbeats kept the lease)`);

const par = await waitDone(await worker.startWorkflow('TsParallel'));
if (par.output !== '{"parts":[10,20,30],"winner":1}') fail(`parallel: output ${par.output}`);

const bounded = await waitDone(await worker.startWorkflow('TsTimeouts'));
if (!/^failed: .*start-to-close/.test(bounded.output) || !stallAborted) {
  fail(`timeouts: output ${bounded.output}, aborted=${stallAborted}; want the attempt to time out and abort`);
}
for (const [code, want, calls] of [
  ['404', 'NonRetryableError|true|webhook answered 404', 1],
  ['400', 'BadRequest|false|webhook answered 400', 1],
  ['503', '|false|webhook answered 503', 4],
  ['legacy', '|false|webhook answered 400', 1], // the older engine never saw the type
]) {
  const hook = await waitDone(await worker.startWorkflow('TsWebhook', code));
  if (hook.output !== want || webhookCalls[code] !== calls) {
    fail(`webhook ${code}: output ${hook.output} after ${webhookCalls[code]} calls, want ${want} after ${calls}`);
  }
}
const ergoId = await worker.startWorkflow('TsErgonomics');
for (let i = 0; ; i++) {
  const wf = await worker.describe(ergoId);
  if (wf.steps.some((s) => s.step_name === 'present')) break;
  if (i > 100) fail('ergonomics: never reached the second signal wait');
  await new Promise((r) => setTimeout(r, 50));
}
await worker.signal(ergoId, 'late', 'world');
const ergo = await waitDone(ergoId);
const out = JSON.parse(ergo.output);
const startedAt = Date.parse(ergo.started_at.replace(/(\.\d{3})\d+/, '$1'));
const ids = new Set(out.seen.map((s) => s.id));
if (out.started !== startedAt || !out.clockMoved || out.missing !== null || out.present !== 'world' ||
    out.seen.map((s) => s.attempt).join() !== '1,2' || ids.size !== 1 || ![...ids][0].startsWith(`${ergoId}/`) ||
    out.seen.some((s) => s.roll !== out.roll)) {
  fail(`ergonomics: ${ergo.output} (started_at ${ergo.started_at})`);
}
const ergoSteps = ergo.steps.map((s) => `${s.step_name}=${s.status}`).join(' ');
if (ergoSteps !== 'ts-attempts=COMPLETED until=COMPLETED missing=CANCELLED missing.timeout=COMPLETED present=COMPLETED present.timeout=CANCELLED') {
  fail(`ergonomics steps: ${ergoSteps}`);
}

const raceId = await worker.startWorkflow('TsRace');
for (let i = 0; ; i++) {
  const wf = await worker.describe(raceId);
  if (wf.steps.some((s) => s.step_name === 'late-callback')) break;
  if (i > 100) fail('race: never reached the second wait');
  await new Promise((r) => setTimeout(r, 50));
}
await worker.signal(raceId, 'callback', 'hello');
const race = await waitDone(raceId);
const raceSteps = race.steps.map((s) => `${s.step_name}=${s.status}`).join(' ');
if (race.output !== 'late: hello' || raceSteps !== 'await-callback=CANCELLED deadline=COMPLETED late-callback=COMPLETED') {
  fail(`race: output ${race.output}, steps ${raceSteps}`);
}
const provisionId = await worker.startWorkflow('TsProvision');
await provisioning;
await worker.cancel(provisionId, 'tenant deleted');
for (let i = 0; cancelReason === null; i++) {
  if (i > 100) fail('the cancelled activity was never told');
  await new Promise((r) => setTimeout(r, 50));
}
const provisioned = await worker.describe(provisionId);
if (provisioned.status !== 'CANCELLED' || cancelReason.name !== 'CancelledFailure') {
  fail(`cancel: status ${provisioned.status}, abort reason ${cancelReason?.name}: ${cancelReason?.message}`);
}
const foreverId = await worker.startWorkflow('TsForever', '', '', { executionTimeoutMs: 300 });
for (let i = 0; ; i++) {
  const wf = await worker.describe(foreverId);
  if (wf.status === 'TIMED_OUT') break;
  if (i > 100 || (wf.status !== 'RUNNING' && wf.status !== 'WAITING')) fail(`execution timeout: ${wf.status}`);
  await new Promise((r) => setTimeout(r, 50));
}

// A restarted engine has forgotten every registration: the worker must notice
// and register again before this workflow can be dispatched.
console.log('RESTART');
await lines.next();
let afterId;
for (let i = 0; !afterId; i++) {
  try {
    afterId = await worker.startWorkflow('TsAfterRestart');
  } catch (err) {
    // 400 until the worker has registered with the new engine.
    if (err.status !== 400 || i > 100) fail(`start after restart: ${err.message}`);
    await new Promise((r) => setTimeout(r, 50));
  }
}
const after = await waitDone(afterId);
if (after.output !== '{"ok":42}') fail(`after restart: output ${after.output}`);

console.log('PASS');
process.exit(0);
