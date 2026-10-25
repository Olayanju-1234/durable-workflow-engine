package main

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/olayanju-1234/durable-workflow-engine/internal/tracing"
	"github.com/olayanju-1234/durable-workflow-engine/internal/version"
)

// newTraceExporter builds the OTLP/HTTP exporter for --otlp-endpoint.
func newTraceExporter(endpoint, headersFile, service string, logger *slog.Logger) (*tracing.Exporter, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("--otlp-endpoint %q: want an http(s) URL, e.g. http://localhost:4318/v1/traces", endpoint)
	}
	headers := map[string]string{}
	if headersFile != "" {
		data, err := os.ReadFile(headersFile)
		if err != nil {
			return nil, fmt.Errorf("--otlp-headers-file: %w", err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			k, v, ok := strings.Cut(line, ":")
			if !ok || strings.TrimSpace(k) == "" {
				return nil, fmt.Errorf("--otlp-headers-file: %q is not a \"Name: value\" line", line)
			}
			headers[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	host, _ := os.Hostname()
	return tracing.NewExporter(tracing.ExporterConfig{
		Endpoint: endpoint, Headers: headers, Logger: logger,
		ScopeName: "workflowd", ScopeVersion: version.Version,
		Resource: []tracing.Attr{
			{Key: "service.name", Value: service},
			{Key: "service.version", Value: version.Version},
			{Key: "host.name", Value: host},
		},
	}), nil
}
