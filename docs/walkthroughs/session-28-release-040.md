# Session 28 walkthrough: 0.4.0, one engine for several apps

This session shipped workflowd 0.4.0 in seven PRs (#49 to #55) and deployed it to both engines on
the VPS. The theme: running more than one app on one engine without them getting in each
other's way, and running that engine without hand-written `curl`. Here is what each piece is, in
terms you know from Node, NestJS and Mongo.

## Why this release

A product review after 0.3.0 listed four things that had been awkward:

1. liftoff needed a whole second engine (its own unit, ports, token and deploy) just to stay off
   the public dashboard.
2. The demo's built-in worker and the SDK examples both implement activities called `reserve`,
   `charge` and `ship`, so pointing an example at the demo mixed their work up.
3. Every operation on the VPS was `curl` with hand-written JSON.
4. The demo stayed alive only because of rules in nginx, a config file shared with three other apps.

Each PR fixes one of these, plus two operator features: archiving, and schedule controls.

## 1. Namespaces (#49, ADR-0015)

**The idea.** A namespace is a tenant inside one engine, like a separate Mongo database on one
server: its own workflows, schedules, workers and settings, reached with its own token. The
`default` namespace is everything that existed before, so nothing changes for anyone who doesn't
configure one.

**How it is stored.** Each workflow still has one history file. Default-namespace files stay where
they were (`data/order-1.history`); another namespace's go in its own folder
(`data/namespaces/acme/order-1.history`). Inside the engine a workflow is named by a *key*: just
`order-1` in default, `acme/order-1` elsewhere. Workflow ids can never contain `/`, so a key always
has one meaning.

**The safety rule.** The engine has one door for the APIs: `engine.In("acme")` returns a `Scope`,
a bit like a NestJS request-scoped service bound to one tenant. Every id coming in through it is
checked to be a plain id (so a caller can never smuggle in `beta/order-1`), and every workflow
going out is named by its plain id again. Polling for tasks is filtered by namespace too: a worker
in `acme` asking for "any task" only ever sees acme's.

**Tokens.** `--namespaces namespaces.json` lists namespaces and their tokens, like an
`.env`-driven config file. One `Authorizer` decides every call, for HTTP and gRPC alike:

- the engine's own token (`--auth-token-file`) reaches every namespace, like an admin key;
- a namespace's token reaches only its namespace (anything else gets 403 Forbidden);
- no token reaches only the default namespace, and only if it's open.

Clients pick a namespace with a header (`X-Workflowd-Namespace`), like a tenant header in a
multi-tenant NestJS app. The SDKs got a `namespace` option, and the dashboard a namespace picker.

**The bug the tests found.** Continue-as-new would have written the namespaced key `acme/order-1`
into the next run's start event, so Go workflow code would have seen the wrong id for itself.

## 2. Named task queues (#50, ADR-0016)

**The idea.** Think of BullMQ queues. Before, the engine had effectively one queue per activity
name: every worker that implemented `charge` competed for every `charge` task. Now tasks go on a
named **task queue**, and a worker polls one queue. The demo worker stays on the `default` queue;
the SDK examples use `ts-orders`, `py-orders` and `java-orders`. Same activity names, no collision.

**Where a task goes.** A workflow runs on the queue it was started on, and so do its activities
unless the code says otherwise (`ctx.activity('send-email', msg, { taskQueue: 'email' })`). The
queue an activity resolves to is written into its history event, so a retry, a restart or a
continue-as-new sends it to the same place without having to work it out again.

## 3. `workflowd ctl` (#51, ADR-0017)

**The idea.** A command line for operators, like the `kubectl` or `mongosh` you would reach for
instead of hand-writing requests. It is a subcommand of the same binary (`workflowd ctl ...`),
so there is still only one thing to ship, and it talks to the engine over the same JSON API as
everything else, with the token from a file or an environment variable (never on the command line).

```sh
workflowd ctl list --status waiting
workflowd ctl describe order-7        # a table of the workflow and its steps
workflowd ctl history order-7         # the raw events, exactly as recorded
workflowd ctl start OrderSaga --id order-8 --input order-8
workflowd ctl backup today.tar.gz     # every history, as a tar.gz
```

`--json` prints the engine's JSON instead of a table, for scripts and `jq`. Exit codes follow Unix
conventions: 0 done, 1 the engine said no, 2 you typed it wrong.

**Backup and restore.** A backup is a `.tar.gz` of the history files. Because history files are
append-only, copying one while the engine writes gives a clean prefix of it. Restore is
deliberately cautious: it only writes into a new, empty folder, checks every file's checksums
first, and renames the folder into place at the end, so a half-finished restore can't leave a
broken data directory behind.

## 4. Guardrails (#52, ADR-0018)

**The idea.** Express-style rate limiting and quotas, but inside the engine instead of nginx.

- **Quotas** per namespace: at most N workflows running at once, and at most M starts a minute.
  These live in the engine itself, so a schedule or an in-process caller can't get around them.
- **Rate limits** per client: so many writes a minute per token, or per IP address without one.
  Behind nginx the engine only sees nginx's address, so it reads the real client from
  `X-Forwarded-For`, but only when the connection comes from a proxy you trust, and reading from
  the right, because anything to the left could have been typed by the client.
- **Read-only**: refuse every write, or just schedule changes. The demo uses that last one.

**The race that matters.** Two requests arriving at the same moment must not both squeeze under
the limit. Like a Mongo `findOneAndUpdate` with a condition instead of a read followed by a
write, each start *reserves* its slot under a lock before it runs. The test fires 60 starts at
once against a limit of 5 and checks that exactly 5 got through.

**The bug the tests found.** The rate limiter cleans up idle clients every minute. The clean-up ran
after a new client's bucket was created, deleted it, and so every client's first write was free.

## 5. Archiving (#53, ADR-0019)

**The idea.** Retention deletes finished workflows after a while (the demo keeps them 7 days).
With `--archive`, the engine first copies each one to a folder or an S3-compatible bucket
(Cloudflare R2, AWS S3), and deletes it only once the copy is confirmed. If the archive is down,
nothing is deleted, and the next pass tries again: nothing can be lost.

**S3 without the AWS SDK.** Talking to S3 means signing each request with AWS Signature V4: a
recipe of SHA-256 hashes and HMACs over the method, path, headers and body. It's about a hundred
lines, so instead of adding the whole AWS SDK as a dependency (the project has none beyond gRPC
and protobuf) it is written by hand and tested against AWS's own published test vectors. Each
upload also carries the body's MD5, so S3 refuses a corrupted upload.

## 6. Schedule controls (#54, ADR-0020)

Schedules (cron jobs that start workflows) got the controls you'd expect from a job scheduler:

- **pause / resume**, with a note saying why;
- **overlap policy**: what a fire does while the last run is still going: skip it, start it anyway,
  or *hold one* and start it as soon as the running one finishes;
- **jitter**: start each run a little after its fire time so schedules that all fire on the hour
  don't stampede. The delay is fixed per fire (a hash, not a random number), so a restart or the
  preview always picks the same one;
- **backfill**: start the runs a schedule missed over a past range. Each run's id is the
  schedule id plus the fire time, so backfilling twice does nothing the second time;
- **preview**: the next fire times, including daylight-saving shifts, before you save a schedule.

All of it is in the API, the CLI and the dashboard's new "Pattern cards" panel.

## How "safe to roll back" stays true

ADR-0011 (0.3.0) gave history files numbered formats. 0.4.0 adds format 6: anything an older
binary would silently get wrong (a namespace it can't see, a task queue it ignores, a paused
schedule it would fire) raises the data directory to format 6 the first time it is used, and an
older binary then refuses to start instead of misbehaving. Upgrading without using the new
features keeps the old format, so rolling back stays possible.

## How we know it works ("teeth")

Every load-bearing guarantee has a test, and each test is checked by **mutation testing**: break
the code on purpose (remove the namespace filter, make the rate limiter ignore the proxy rule,
delete before archiving) and confirm a test fails. 76 mutations across five of the PRs; every
one that changed behaviour was caught. The two survivors were checks that another check already
covers, and the PR descriptions say which.

## What was deployed

Both engines on the VPS run the 0.4.0 binary (`4d9ac4c`), and the SDKs are on npm and PyPI as
0.4.0. Three checks worth knowing about:

- **The demo now guards itself.** It runs with `--rate-limit 10 --rate-limit-burst 20
  --trusted-proxies 127.0.0.1,::1 --read-only-schedules`, the same numbers nginx enforces. Firing 30
  writes at the engine directly (skipping nginx) let 20 through and refused 10 with `429` and a
  `Retry-After` header, while a different client was still served. The nginx rules are still there:
  that file is shared with other apps, so they stay until someone decides to remove them.
- **The SDKs, fresh from the registries.** A brand-new `npm install workflowd` and
  `pip install workflowd` each ran a workflow whose `charge` activity sat on its own task queue,
  right next to the demo worker's `charge` on the default queue. Each got its own worker's answer,
  which is the collision from the top of this page, fixed.
- **liftoff didn't notice.** Its private engine had no workflows in flight, took the new binary,
  and liftoff's worker re-registered a fifth of a second later. A test webhook run, started with
  `workflowd ctl` on the server, completed.

The SESSION_LOG entry has the details and the rollback notes.
