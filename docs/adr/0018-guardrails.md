# ADR-0018: Engine guardrails: quotas, rate limits, read-only

- **Status:** Accepted
- **Date:** 2026-10-24

## Context

The public demo engine is open by design: visitors start workflows without a token. What keeps
it alive is two rules in the VPS's shared nginx config: a per-IP limit of 10 writes a minute
(burst 20) on everything but reads and task completions, and a 403 on any schedule change
(one `@every 1s` schedule would start 86,400 workflows a day). Those rules protect only an
engine behind that nginx, live in a file shared with three other apps, and know nothing of
namespaces (ADR-0015): one tenant's burst could crowd out another's, and nothing bounded how many
workflows a namespace runs at once.

Forces: the engine should protect itself wherever it runs; limits must hold under concurrency
(a check-then-start race must not let a namespace past its quota); behind a proxy the client's
address is only in `X-Forwarded-For`, which a client can also forge; refusals must say what was
refused and when to try again; nothing changes for a deployment that sets nothing.

## Decision

1. **Three guardrails**, engine-wide by flag and per namespace in `--namespaces` (a namespace's
   setting overrides the flag; a zero limit means no limit):
   - **Quotas** on starts: `--max-active-workflows` (running or waiting at once) and
     `--max-starts-per-minute`; `max_active_workflows`, `max_starts_per_minute`.
   - **Rate limits** on client writes: `--rate-limit` per minute per client, `--rate-limit-burst`;
     `rate_limit_per_minute`, `rate_limit_burst`.
   - **Read-only**: `--read-only` refuses every client write (starts, signals, cancels,
     terminations, schedule changes); `--read-only-schedules` refuses schedule changes only;
     `read_only`, `read_only_schedules`.
2. **Quotas live in the engine**, checked in `Start`, so every start obeys them: the HTTP and
   gRPC APIs, schedule fires and in-process callers alike. A continue-as-new is not a start.
   The active count is kept per namespace by the visibility index as summaries change; a start
   **reserves** its slot under a lock before it runs and releases it after, so concurrent starts
   see each other and a namespace never exceeds its limit. Starts per minute are a token bucket
   per namespace; a start that does not begin a run (an existing run returned, an id conflict)
   gives its token back.
3. **Rate limits and read-only live at the API**, in one `Guard` that HTTP and gRPC share after
   authorization. Limited calls are client writes and worker registration; reads, long-polls,
   task completions and heartbeats are never limited (they need a live lease, and limiting them
   only fails work). The engine token is not limited. A client is its namespace token, or else
   its address; one bucket per client per namespace.
4. **Client addresses behind a proxy**: `--trusted-proxies` lists proxy addresses or ranges. Only
   when a connection comes from one is `X-Forwarded-For` (or the gRPC `x-forwarded-for` key)
   believed, and then read from the right: the first address that is not a trusted proxy is the
   client. Anything a client puts in the header itself is to the left of that and is ignored.
5. **Refusals**: read-only answers 403 / `PERMISSION_DENIED`; rate limits and quotas answer 429 /
   `RESOURCE_EXHAUSTED` with a `Retry-After` (seconds) and a message naming the limit.
   `workflowd_requests_refused_total{reason}` and `workflowd_starts_refused_total{reason}` count them.
6. **Read-only does not stop the engine.** In-flight workflows keep running, workers keep
   completing tasks, and existing schedules keep firing; it refuses clients' changes only.

## Consequences

- The demo can run with `--rate-limit 10 --rate-limit-burst 20 --trusted-proxies 127.0.0.1,::1
  --read-only-schedules`, which is what its nginx rules do; the nginx rules become a second layer
  rather than the only one. (Removing them is a separate, shared-config change.)
- Namespaces get fair shares: each has its own buckets and quota.
- SDK workers treat a 429 on registration like an engine that is not up yet, and retry.
- Quotas count what the visibility index sees: a workflow whose history cannot be read is not
  counted as active.
- Limits are in memory: a restart refills every bucket.

## Alternatives considered

- **Keep it all in a proxy.** Works for the demo, protects nothing else, and cannot see
  namespaces or tokens.
- **Count active workflows by scanning the index at each start.** Simple, but O(n) per start and
  still racy without a reservation; counting as summaries change is O(1).
- **Limit reads too.** Reads are cheap (the index is in memory) and the dashboard polls; a read
  limit can come later if it is ever needed.
- **Trust the left-most `X-Forwarded-For` address.** That is the one a client controls.
