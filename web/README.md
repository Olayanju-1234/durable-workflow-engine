# web/

Everything with a user interface. Two independent things live here.

```
web/
├── embed.go      Go package: go:embed of dashboard/ + the HTTP handler
├── dashboard/    operational webapp — COMPILED INTO the workflowd binary
└── site/         public landing page + developer docs — deployed separately
```

## dashboard/ — the embedded operational UI

Served by `workflowd --serve` on `--http-addr` (default `:8233`), alongside the
JSON API it talks to (`internal/server/httpapi.go`). The assets are baked into
the binary with `go:embed`, so there is still exactly one file to deploy.

- Workflow list with status filters, auto-refreshing every 2s
- Detail drawer: step timeline, attempt counts, per-step errors and outputs
- Actions: send a signal, terminate a workflow

Plain HTML/CSS/vanilla JS. **No build step, no npm, no bundler** — edit a file
and rebuild the Go binary. That constraint is deliberate: a dashboard that needs
a Node toolchain to ship would undermine the single-binary promise.

## site/ — the public website

A static site (landing page + `docs.html`). Not embedded in the binary and not
served by the engine — deploy it to any static host:

```sh
# preview locally
cd web/site && python3 -m http.server 4000
```

Both pages are self-contained: no external fonts, scripts, or CDNs, so they work
offline and leak nothing to third parties.

## Conventions

- Theme-aware: both follow the OS light/dark preference (the dashboard also has
  a manual toggle that persists to `localStorage`).
- Any value that came from a workflow — ids, types, errors, outputs — is escaped
  before it reaches `innerHTML`. Treat all of it as untrusted.
- Keep the dashboard dependency-free. If something needs a framework, it probably
  belongs in the future managed product instead.
