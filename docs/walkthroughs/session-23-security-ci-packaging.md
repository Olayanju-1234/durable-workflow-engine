# Session 23: locks on the doors, a stricter CI, and packages ready to ship

*Plain-English walkthrough for a Node/TS developer who is new to Go. This session ran
in parallel with the engine-backlog session and covered four things around the
engine rather than inside it: TLS and auth on every port, `buf` for the protobuf
files, CI that runs the TypeScript, Python and Java SDKs against a real engine, and
publish-ready npm, PyPI and Maven packages. PRs #16, #17, #18 and #20.*

## What you can do now

```bash
openssl rand -hex 32 > token && chmod 600 token
mkdir -p certs && go run ./internal/tlsconfig/tlstest/gencert certs   # throwaway CA + localhost cert
go run ./cmd/workflowd --serve --auth-token-file token \
  --tls-cert certs/server.pem --tls-key certs/server-key.pem
```

Now `https://localhost:8233` serves the dashboard (it asks for the token once),
`curl --cacert certs/test-ca.pem https://localhost:8233/api/workflows` answers `401`,
and the same request with `-H "Authorization: Bearer $(cat token)"` answers `200`.
Workers send the same token. Leave every flag off and nothing changes: production
still runs plaintext behind nginx, which does TLS there.

## TLS in one paragraph

TLS is what turns `http://` into `https://`: the server proves who it is with a
**certificate**, and both sides agree on keys so nobody in between can read or change
the traffic. A certificate is trusted because a **certificate authority (CA)** signed
it. Browsers trust a list of public CAs. For a private network you can be your own CA,
which is what the tests do. In Node this is
`https.createServer({ key, cert }, app)`. In Go it's a `*tls.Config` value (an
options object) handed to the HTTP server, the gRPC server, or a client.

`internal/tlsconfig` builds those configs. One detail matters in production: Let's
Encrypt certificates expire every 90 days. Instead of loading the certificate once,
the config holds a function (`GetCertificate`) that the TLS library calls on every
new connection. It checks the file's modification time and reloads when it changed.
If a renewal is only half written, it keeps serving the old certificate. That's the
`TestCertificateReload` test.

## Bearer tokens: an API key header, on HTTP and gRPC

The JSON API already had a token check (Express-style middleware). This session:

- **Extended it to gRPC.** gRPC has *interceptors*, the equivalent of a NestJS guard:
  a function that runs before every call. Ours reads the `authorization` *metadata*
  (gRPC's name for headers) and answers `Unauthenticated` without the right token.
- **Compares tokens in constant time** with `crypto/subtle.ConstantTimeCompare`, the
  Go version of Node's `crypto.timingSafeEqual`. A plain `==` stops at the first wrong
  character, and timing that difference can leak the token one character at a time.
- **Added `--auth-public-reads`.** Anyone can list and look at workflows, but starting,
  signalling and terminating still need the token. Polls (`/api/activity-tasks`) are
  *not* reads, even though they are `GET`s: a poll hands the caller a task, which is
  how a stranger could steal work. The test table checks that.
- **Gave `/metrics` its own token**, so Prometheus never holds the key that can
  terminate workflows. Least privilege, same as giving a read-only DB user to a
  reporting job.

One thing turned out different from what the backlog said: the old `--auth-token`
already protected *reads* too (the docs said "mutating routes" only, which was wrong).
So the "read auth" option became the opposite switch, `--auth-public-reads`, and
the default stayed as strict as it already was.

## Why secrets come from a file, not the command line

Anything in a process's arguments is visible to every user on the machine via `ps`.
So the token comes from `--auth-token-file` or the `WORKFLOWD_AUTH_TOKEN` environment
variable (like `process.env.API_KEY` from a systemd `EnvironmentFile`). The old
`--auth-token` still works but logs a warning, and the daemon also warns when the token
file is readable by other users (`chmod 600`). `TestTokenSources` covers the trailing
newline that `echo > file` adds, empty files, both flags at once, and env vs flag.

## Mutual TLS between Raft nodes

Normal TLS is one-sided: the server shows a certificate and the client checks it.
**Mutual TLS** makes the client show one too. Picture a door where the guard checks
your badge *and* you check the guard's. Cluster nodes now do that with
`--raft-tls-cert/key/ca`: a node accepts Raft traffic only from a peer whose
certificate the cluster's CA signed. The test starts a real 3-node cluster over mTLS,
runs a workflow, and then knocks on the leader's Raft port three ways: plaintext,
TLS without a certificate, and a certificate from a different CA. All three are
refused before reaching the handler; a valid peer gets through.

## Go things you'll see in this diff

- **Functional options** (`server.WithPublicReads()`): instead of a big options
  object, each option is a small function that edits the config. It's how Go does
  `new Thing({ a, b })` while keeping defaults in one place.
- **Satisfying an interface by shape**: the Go client's token is a `string` type
  with two methods (`GetRequestMetadata`, `RequireTransportSecurity`). That makes it
  a gRPC `PerRPCCredentials`, with no `implements` keyword, like TypeScript's
  structural typing.
- **Sentinel errors** (`client.ErrUnauthenticated`): exported error values you check
  with `errors.Is(err, client.ErrUnauthenticated)`, like checking `err instanceof
  AuthError`.

## buf: ESLint plus a breaking-change detector for `.proto` files

Every history file on disk is protobuf, and protobuf identifies fields by **number**,
not name. Change a field's number or type and old history files decode into garbage.
That's a breaking change the Go compiler can't see. `buf` is like ESLint for `.proto`
files plus an OpenAPI diff:

- `buf lint` checks naming and style. The existing files break two layout rules (four
  packages in one folder), and moving them would rename every generated descriptor,
  so `buf.yaml` lists them as exceptions instead of moving files.
- `buf breaking` compares the PR's protos with the base branch and fails on anything
  that breaks the wire format or JSON names. Tried by hand: changing one field from
  `bytes` to `string` made it fail.

It runs in a new CI job, `proto`. buf is a downloaded tool with a checked checksum,
not a Go dependency, the way you'd `npx` a CLI instead of adding it to `dependencies`.

## CI that actually runs the SDKs (and the Java SDK compiles)

The Java SDK had been written in Session 13 but never compiled, because there was no
JDK on the laptop. `scripts/sdk-e2e.sh` now does, for each SDK: build the engine,
start it, run that SDK's example worker against it, then drive two orders through
the HTTP API: one approved (must complete with `shipped(reserved(…))`) and one
rejected (must fail *after* the `release` compensation ran exactly once). It's a
Playwright-style test against a real server instead of mocks.

The first CI run of the Java job failed, but not on real bugs: `-Werror` (treat
warnings as errors) tripped on `serialVersionUID` warnings, which don't matter for
exceptions that are never serialized. Turning that one lint category off made it
pass. So the Java SDK compiled for the first time with no code changes. Each SDK now
also runs a second time over TLS with a required token, which proves the new
`token` option and custom-CA support in all three languages.

## Packages, ready but not published

| | TypeScript | Python | Java |
|---|---|---|---|
| Manifest | `package.json` | `pyproject.toml` | `pom.xml` |
| Name | `workflowd` | `workflowd` | `io.github.olayanju-1234:workflowd` |
| Types | `workflowd.d.mts` (hand-written) | type hints in the code | the code is typed |

Three details:

- **`.d.mts`, not `.d.ts`.** The SDK is an `.mjs` file (an ES module), and TypeScript
  looks for `.d.mts` next to `.mjs`. A compile-only file, `types-check.mts`, uses the
  API like the example does, and CI runs `tsc --strict` on it, so the types can't
  drift silently.
- **The Java SDK got a `package` line.** Java can't import a class from the "default
  package" (no `package` line) into a named package, so a published library must
  have one. The file stays where it is, because the parallel session is rewriting it
  and moving it would have caused a painful merge. Maven copies it into the right
  folder layout at build time.
- **`scripts/sdk-version.py`** fails CI if the six version strings (three manifests,
  plus the version each SDK reports to the engine) disagree, much like a monorepo
  version check.

`.github/workflows/release-sdks.yml` publishes all three, and it only runs when you
start it by hand. It defaults to a **dry run**, which builds and validates everything
and uploads nothing. A real run needs registry tokens stored as repository secrets,
which only you can create. The exact steps (accounts, tokens, a GPG key for Maven
Central, then one `gh workflow run` command) are in `sdks/README.md` under
"Publishing the SDKs".

## What's still not there

- One shared API token: no per-user identities, roles or audit trail (that's the
  commercial layer on the roadmap).
- Client certificates are only checked on the Raft port, and any certificate the
  cluster CA signed can speak for any node, so give that CA to cluster nodes only.
- Nothing is published until you do the steps in `sdks/README.md`.
