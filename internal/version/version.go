// Package version exposes the engine's build version.
package version

// Version is the engine version. Override at build time with:
//
//	go build -ldflags "-X github.com/olayanju-1234/durable-workflow-engine/internal/version.Version=v0.1.0"
var Version = "0.0.0-dev"
