# Session 14 — The UI revamp: "The Loom"

*Plain-English walkthrough for a Node/TS developer who is new to Go. No Go was
changed this session — this is all HTML/CSS/JS — but the *why* matters as much
as the *how*.*

## The problem

The dashboard and site worked, but they looked like every admin template ever
generated: Inter-ish font, indigo-on-white cards, rounded corners, a dark mode.
Nothing about the design said what this product actually *is*. You asked for a
design with a concept behind it — not variant #1 or #2 of a common template.

## The concept: the Jacquard loom

In 1804 Joseph-Marie Jacquard built a loom that read its instructions from
punched cards chained together. Three facts about those cards should sound
extremely familiar:

1. **They were append-only.** You never erased a card; the pattern was the
   whole chain, read in order.
2. **Restarting was free.** Stop the loom mid-weave — for a second or a year —
   put the cards back in, and it resumed on the exact pick it stopped at.
3. **The program outlived the machine.** Cards were copied between looms.

That is *literally* this engine's value proposition. So the UI stops being a
generic dashboard and becomes **the weaving room**:

| Engine thing      | Loom name          | Where you see it |
|-------------------|--------------------|------------------|
| History log       | the card chain     | hero illustration, "Cards" stat |
| One step          | one pick           | step timeline = woven weft threads |
| Crash + replay    | weaver steps away  | empty states, colophon |
| Signal            | passing the shuttle | action button |
| Terminate         | cutting the warp   | red action button |

A concept only works if everything obeys it. So:

- **Colors are natural dyes, not screen colors.** Linen `#F3ECDF`, walnut ink,
  indigo, madder red, weld ochre, Lincoln green. Dark mode is "the dye vat" —
  warm umber black, not the usual blue-black. No SaaS blue/purple anywhere.
- **Type is system stacks.** `"Iowan Old Style", Palatino, Georgia` for display
  serif; system sans for body; `ui-monospace` for data. Loading a webfont would
  mean shipping a font file inside the binary for questionable gain — the
  zero-dependency rule applies to the UI too.
- **Every asset is hand-drawn inline SVG.** The logo is a 3×3 over-under weave
  (drawn as rectangles with "twill patches" to fake over-crossing). The hero is
  a working loom whose punch cards are labelled with real event names
  (`STARTED`, `RESERVE · DONE`). Step kinds get glyphs: shuttle = activity,
  clock = timer, flag = signal.

## Things worth knowing (with TS analogies)

**Design tokens ≈ a theme object.** All colors live in CSS custom properties on
`:root` (`--indigo`, `--madder`…), like a `const theme = {...}` in TS. Dark mode
just swaps the values under `[data-theme="dark"]` / a `prefers-color-scheme`
media query — same trick as swapping a theme object behind one interface.

**The signature component: steps as weft picks.** In the drawer, each step is
rendered as a horizontal thread between two dashed "warp rails". Completed =
solid green thread with a woven stripe overlay (a `repeating-linear-gradient`),
scheduled = dashed ochre outline, failed = broken madder thread. It's still just
an `<ol>` — semantic list, styled — so screen readers see a normal list.

**Favicon as a data URI.** Instead of a file, the favicon is URL-encoded inline
SVG in the `<link>` tag (`%23` is an encoded `#`). One less asset to embed.

**Escaping still rules.** Every workflow id/type/error still goes through
`esc()` before touching `innerHTML` — same XSS discipline as before. New IDs
were cross-checked: a small Node script extracts every `$('id')` call from
`app.js` and asserts each exists in the HTML (44/44).

**Verification without a browser.** `node --check` (syntax), a Python
tag-balance parser over all three pages (structure), then the real thing:
built the binary, ran it on scratch ports, curl'd the dashboard, health
endpoint, started a workflow through the new dialog's exact API call, listed
it. Then `make vet` + full test suite.

## What changed, file by file

- `web/dashboard/index.html` — new structure, inline SVG assets, start-workflow modal
- `web/dashboard/styles.css` — the whole loom design system (~600 lines)
- `web/dashboard/app.js` — same API calls; new rendering (picks, chip counts),
  search box, start-workflow dialog, form-based signal/terminate
- `web/site/styles.css`, `index.html`, `docs.html` — same treatment; docs content
  unchanged except two new dashboard feature bullets
- `web/README.md` — the design language documented so future sessions keep it coherent

Go code untouched; `go:embed` picked up the new assets on rebuild automatically.

---

# Part 2 — the backend hardening pass (same session)

The second half of this session was a full read of every backend package
(history, state, timer, server engine/leases/retry/polyglot/visibility, HTTP +
gRPC layers, main) hunting for bugs. Four real issues found and fixed.

## 1. Wrong status codes (HTTP + gRPC)

Every error from "start workflow" came back as HTTP 400 / gRPC
`InvalidArgument` — even *disk failures* (a 500-class problem) and *duplicate
ids* (a 409-class conflict). Like returning 400 for both "your JSON is
malformed" and "our database is on fire". Fixed with sentinel errors
(`errors.Is` works through `%w` wrapping — like checking `err.cause` in a
typed error chain): duplicates → **409**/**`AlreadyExists`**, unknown type →
400, I/O → 500/**`Internal`**. Same treatment for signals: unknown workflow is
now **404**, terminal is **409**, and an *empty signal name* is rejected with
400 before it can be written into history forever.

## 2. Silent stranding on transient disk errors (the subtle one)

Timer callbacks run once: the timer pops off the heap, the callback reads
history and appends an event. If that append hits a full disk, the old code
logged an error and returned — but the event never landed and the timer is
gone, so the workflow waits forever. A silent permanent stall from a temporary
problem.

Fix: every callback now re-arms itself ~10s later on failure and retries until
it succeeds. One case needed care: if lease-expiry's failure event fails to
append, history still shows the attempt as scheduled with no lease and no queue
entry — so we re-enqueue the task instead (consistent, because the failed
attempt was never recorded). There's also a new retry service for the decision
loop itself: advancing is idempotent (pure replay), so a blind retry is safe.

## 3. Optional auth (`--auth-token`)

The documented gap. Now: pass `--auth-token sekrit` and every mutating `/api`
route requires `Authorization: Bearer sekrit` (constant-time compare).
`/api/health` stays open for liveness probes; dashboard static files stay open.
Empty flag = old behavior, plus a startup warning. Honest scope note in the
docs: gRPC and metrics remain unauthenticated; there's still no TLS.

## 4. Operator quality-of-life

- List endpoint gained `q=` substring search and `limit`/`offset` pagination
  with a `total` field.
- Health went from `{"status":"ok"}` to version + uptime + active workflows +
  queue depths — enough to wire uptime monitors without scraping Prometheus.
- Structured request logging (Info for mutations, Debug for reads).

## Verification habit worth copying

Unit tests updated + new tests (`httpapi_auth_test.go`: the whole auth matrix,
pagination math, duplicate 409), then the real binary run against scratch ports
with curl asserting each status code by hand. Tests prove logic; the live run
proves wiring — flags parse, middleware order is right, the dashboard still
loads without a token.
