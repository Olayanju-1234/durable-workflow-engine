# Contributing

## Prerequisites

- **Go 1.26+**
- **protoc 35+** and `protoc-gen-go` — only needed if you change `.proto` files.

```sh
# macOS
brew install go protobuf
make tools                      # installs protoc-gen-go into $(go env GOPATH)/bin
```

Ensure `$(go env GOPATH)/bin` is on your `PATH` so `protoc` can find `protoc-gen-go`.

## Everyday commands

```sh
make build     # go build ./...
make test      # go test ./... -race
make vet       # go vet ./...
make fmt       # gofmt -w .
make proto     # regenerate Go from proto/*.proto (commit the generated .pb.go)
```

Generated protobuf code is committed, so CI does not run `protoc`. If you edit a `.proto`,
run `make proto` and commit the result.

## Code conventions

- `gofmt` is mandatory (CI fails otherwise). Prefer `go vet`-clean code.
- Wrap errors with context: `fmt.Errorf("history: open: %w", err)`.
- Structured logging via `log/slog`; include `workflow_id` (and `step`/`worker_id` where relevant).
- Comments explain *why*, briefly. Let names carry the *what*.
- Every durability or concurrency guarantee gets a test (`t.TempDir()`, `-race`).
- **Workflow code must be deterministic** (Phase 2+): no `time.Now()`, no `rand`, no bare
  goroutines — use the SDK's `workflow.Now/Random/Go`. This is what makes replay correct.

## Commits & branches

- **Conventional Commits:** `type(scope): description`
  (`feat`, `fix`, `refactor`, `test`, `docs`, `chore`). Example: `feat(state): add decision loop`.
- Branch off `main`; open a PR; keep commits focused.
- Run `make fmt vet test` before pushing. Never use `--no-verify`.

## Architecture decisions

Significant decisions are recorded as ADRs in [`docs/adr/`](docs/adr/). Add a new ADR rather
than rewriting an old one.

## Session protocol (for AI-assisted sessions)

End each session by updating `docs/SESSION_LOG.md` and the **Status** line in
`docs/ROADMAP.md`. See [`CLAUDE.md`](CLAUDE.md).
