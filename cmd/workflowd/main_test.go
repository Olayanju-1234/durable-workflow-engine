package main

import "testing"

func TestBrowseURL(t *testing.T) {
	tests := map[string]string{
		":8233":            "http://localhost:8233",
		"0.0.0.0:8233":     "http://localhost:8233",
		"[::]:8233":        "http://localhost:8233",
		"127.0.0.1:8233":   "http://127.0.0.1:8233",
		"[::1]:8233":       "http://[::1]:8233",
		"example.com:8233": "http://example.com:8233",
	}
	for addr, want := range tests {
		if got := browseURL(addr); got != want {
			t.Errorf("browseURL(%q) = %q, want %q", addr, got, want)
		}
	}
}
