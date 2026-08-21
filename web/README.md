# web/

Everything with a user interface. Two independent things live here.

```
web/
├── embed.go      Go package: go:embed of dashboard/ + the HTTP handler
├── dashboard/    operational webapp — COMPILED INTO the workflowd binary
└── site/         public landing page + developer docs — deployed separately
```

## The design language — "The Loom"

Both UIs share one concept. In 1804 the Jacquard loom became the first
programmable machine: its instructions were punched cards, chained together,
read one after another and **never rewritten**. A weave could stop for a second
or a year; the cards held the pattern, and the loom resumed on the exact pick
it stopped at.

That is this engine, so the UI says so:

| Engine concept      | Loom concept        | Where you see it                                              |
|---------------------|---------------------|---------------------------------------------------------------|
| Workflow definition | The pattern         | Copy throughout both UIs                                      |
| History log         | The card chain      | Hero illustration (cards labelled with real event names), "Cards" count |
| One step            | One pick of the shuttle | Step timeline rendered as woven weft picks                |
| Crash + replay      | The weaver stepping away | Empty states, colophon copy                              |
| Signal              | Passing a shuttle   | Dashboard action button                                       |
| Terminate           | Cutting the warp    | Dashboard action button (madder red)                          |

**Palette — natural dyes, not screen-native colors.** Linen `#F3ECDF`, walnut
ink `#2C2318`, indigo `#3F5089` (running/accent), madder `#A63A2B` (failed),
weld ochre `#A97B24` (waiting), Lincoln green `#47663F` (completed). Dark mode
is "the dye vat": warm umber black `#161210`, never blue-black.

**Assets are hand-drawn inline SVG**, no icon fonts, no images: the 3×3
over-under weave mark (also the favicon), the working loom in the hero
(animated shuttle, punch-card chain), step-kind glyphs (shuttle / pick-counter /
flag), a line-art empty-state loom, and stitched/dashed borders that echo
machine stitching. Type is system stacks only: Iowan Old Style/Palatino for
display, system sans for body, ui-monospace for data.

The constraint that shapes all of it: **no webfonts, no frameworks, no build
step**. A UI that needs a Node toolchain to ship would undermine the
single-binary promise the whole project makes.

## dashboard/ — the embedded operational UI

Served by `workflowd --serve` on `--http-addr` (default `:8233`), alongside the
JSON API it talks to (`internal/server/httpapi.go`). The assets are baked into
the binary with `go:embed`, so there is still exactly one file to deploy.

- Stat tiles (total / active / completed / failed) with per-status thread rails
- Workflow list: status filter chips with live counts, client-side search over
  id + type, auto-refresh every 2s
- Detail drawer ("inspection bench"): ledger, input/output cards, steps as weft
  picks with kind glyphs, attempt counts, per-step errors and outputs
- Actions: start a workflow (mount a warp), send a signal (pass a shuttle),
  terminate (cut the warp)

Plain HTML/CSS/vanilla JS. Edit a file and rebuild the Go binary.

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
- Keep both UIs dependency-free. If something needs a framework or a webfont,
  it probably belongs in the future managed product instead.
- New colors must come from the dye palette (extend it deliberately); no
  off-palette hex values.
