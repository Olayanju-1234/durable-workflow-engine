# Session 25 — Publishing the SDKs

*Plain-English walkthrough for a Node/TS developer new to Go. Almost no Go changed this
session: it was about getting the TypeScript and Python SDKs onto npm and PyPI safely.*

## What happened

Anyone can now run `npm install workflowd` or `pip install workflowd`. Version 0.1.1 is
the current one on both registries. The Java SDK isn't on Maven Central yet, because that
needs a signing key and a Sonatype account, and those are yours to create.

## The idea that matters: trusted publishing

The usual way to publish from CI is a long-lived **token**: you create one on npmjs.com,
paste it into GitHub secrets, and the workflow uses it. It works, but the token is a
password that never expires on its own. If it leaks (a log line, a compromised action),
anyone can publish a malicious version under your name.

**Trusted publishing** removes the token entirely. It works like "Sign in with GitHub",
but for a CI job instead of a person:

1. You tell npm (and PyPI) once: "trust releases that come from the `release-sdks.yml`
   workflow in `Olayanju-1234/durable-workflow-engine`."
2. When that workflow runs, GitHub hands the job a short-lived, signed identity token
   (OIDC) that says exactly which repo and workflow it is. That's what
   `permissions: id-token: write` in the YAML allows.
3. npm checks the signature and the repo/workflow names, and accepts the upload. The
   token expires within minutes, and there's nothing stored anywhere to steal.

In NestJS terms: instead of an API key in `.env`, the caller presents a JWT signed by an
identity provider you've chosen to trust, and you verify its claims.

npm also attached a **provenance statement** to 0.1.1: a signed record, published to a
public log (Sigstore), saying "this tarball was built by this GitHub Actions run from this
commit." Users can check that the package really came from your repo.

## Why there are two versions

- **0.1.0** on npm had to be uploaded from your laptop. A brand-new npm package can't be
  created from CI; trusted publishing is configured per package, so the package has to
  exist first.
- **0.1.1** exists to *prove* the token-free path. npm marked the new trust setup
  "pending validation" and wanted one real CI publish within 48 hours. Versions can never
  be reused, so validating meant shipping 0.1.1. It has no behaviour changes, just the
  new release process. npm now shows the setup as "Valid".

## A bug in the safety check

The release workflow refuses to publish unless `CHANGELOG.md` has a heading for the
version, like `## [0.1.0] - 2026-10-22`. The check was a plain substring search (in JS,
`text.includes("## [0.1.0]")`). But the changelog's own instructions said "rename this
heading to `## [0.1.0] - YYYY-MM-DD`", so the check always passed. One real publish
attempt got past it, and stopped later only because no registry credentials existed yet.

The fix is to require the heading at the **start of a line** (a regex with `^` in
multiline mode), so prose that merely mentions it doesn't count. The lesson: a guard that
never fails isn't a guard. Test it with the bad input too.

## A flaky test

`TestReplayWorkflowSurvivesRestart` failed about once in ten runs. The test waits until
it sees a workflow paused on a 40 ms timer, then simulates a crash. It checks every 5 ms,
but each check reads the history file. Under the race detector and a busy machine, a
single check could take longer than 40 ms, so the timer fired, the workflow moved on, and
the test waited forever for a moment it had already missed.

The engine was fine. The test was racing a timer it didn't control. Giving that one test
a 750 ms timer makes the pause impossible to miss. It's the same class of bug as a Jest
test that `await`s a `setTimeout(…, 40)` while asserting on intermediate state.

## What stayed with you, and why

Some steps had to be yours even though you gave permission: creating the npm and PyPI
accounts, signing in, setting up 2FA, and approving with your security key. Those are
identity and credential actions, and they stay with the account owner. Everything around
them was automated: the forms, the workflow changes, the releases and the checks.

## Releasing next time

1. Bump the version in the six places `scripts/sdk-version.py` checks (it fails if they
   disagree), and add a dated `## [x.y.z] - YYYY-MM-DD` heading to `CHANGELOG.md`. Merge.
2. Run the release workflow once with `registries=npm` and once with `registries=pypi`.
   Not `all`: the Maven job would fail without its credentials.
3. Tag it: `sdks-vx.y.z`.

No tokens, no secrets, no laptop involved.
