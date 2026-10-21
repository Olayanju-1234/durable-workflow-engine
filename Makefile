GOBIN   := $(shell go env GOPATH)/bin
MODULE  := github.com/olayanju-1234/durable-workflow-engine

.PHONY: all tools buf proto proto-check fmt fmtcheck vet build test cover run cluster ci

all: build

# Install codegen plugins (run once, or after Go upgrades).
tools:
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# buf lints the protos and checks wire compatibility (CI pins the same version).
BUF_VERSION := v1.73.0
buf:
	go install github.com/bufbuild/buf/cmd/buf@$(BUF_VERSION)

# Lint, and compare against main for wire-breaking changes. Needs `make buf`.
proto-check:
	PATH="$(GOBIN):$$PATH" buf lint
	PATH="$(GOBIN):$$PATH" buf breaking --against '.git#branch=main,subdir=proto'

# Regenerate Go bindings from proto/*.proto into their go_package dirs.
# event.proto -> message types; service.proto -> messages + gRPC stubs;
# raft.proto -> peer protocol; cluster.proto -> replicated history commands.
proto:
	PATH="$(GOBIN):$$PATH" protoc \
		--proto_path=proto \
		--go_out=. \
		--go_opt=module=$(MODULE) \
		--go-grpc_out=. \
		--go-grpc_opt=module=$(MODULE) \
		proto/event.proto proto/service.proto proto/raft.proto proto/cluster.proto

fmt:
	gofmt -w .

# Fail if anything is not gofmt-clean (used by CI).
fmtcheck:
	@unformatted="$$(gofmt -l .)"; \
	if [ -n "$$unformatted" ]; then \
		echo "Not gofmt-clean:"; echo "$$unformatted"; exit 1; \
	fi

vet:
	go vet ./...

build:
	go build ./...

test:
	go test ./... -race

cover:
	go test ./... -coverprofile=coverage.out && go tool cover -func=coverage.out

# Smoke test: append + read back from a real on-disk store.
run:
	go run ./cmd/workflowd --demo

# Three local nodes forming a Raft cluster (see scripts/cluster.sh for ports).
cluster:
	./scripts/cluster.sh

# What CI runs.
ci: fmtcheck vet build test
