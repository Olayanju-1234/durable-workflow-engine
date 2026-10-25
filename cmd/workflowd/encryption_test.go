package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
)

// newKeys makes random keys by id, base64 as a key file holds them.
func newKeys(t *testing.T, ids ...string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, id := range ids {
		k := make([]byte, 32)
		if _, err := rand.Read(k); err != nil {
			t.Fatal(err)
		}
		out[id] = base64.StdEncoding.EncodeToString(k)
	}
	return out
}

// keyFile writes a key file with some of keys and loads it as --encryption-keys does.
func keyFile(t *testing.T, keys map[string]string, active string, ids ...string) *history.Keyring {
	t.Helper()
	var entries []string
	for _, id := range ids {
		entries = append(entries, `{"id":"`+id+`","key":"`+keys[id]+`"}`)
	}
	path := filepath.Join(t.TempDir(), "keys.json")
	if err := os.WriteFile(path, []byte(`{"active":"`+active+`","keys":[`+strings.Join(entries, ",")+`]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	k, err := loadKeys(path)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func started(typ string) *history.HistoryEvent {
	return &history.HistoryEvent{Attributes: &history.HistoryEvent_WorkflowStarted{WorkflowStarted: &history.WorkflowStartedEvent{WorkflowType: typ, Input: []byte("secret-input")}}}
}

func writeSealed(t *testing.T, dir, id string, keys *history.Keyring, evs ...*history.HistoryEvent) {
	t.Helper()
	s, err := history.NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	s.SetKeyring(keys)
	for i, ev := range evs {
		ev.EventId = int64(i + 1)
		if err := s.Append(id, ev); err != nil {
			t.Fatal(err)
		}
	}
}

// --check-format says which keys seal how many events, refuses a directory
// whose keys it was not given, and says which keys nothing uses any more.
// --encrypt-all reseals everything with the active key so an old key can go,
// and --decrypt-all takes the directory back to plaintext and its format down.
func TestEncryptionTools(t *testing.T) {
	dir := t.TempDir()
	keys := newKeys(t, "k1", "k2")
	k1 := keyFile(t, keys, "k1", "k1")
	both := keyFile(t, keys, "k2", "k1", "k2")
	writeEvents(t, dir, "plain", started("Plain"))
	writeSealed(t, dir, "old", k1, started("Old"))
	writeSealed(t, dir, "new", both, started("New"))

	var out bytes.Buffer
	if err := checkFormat(&out, dir, false, nil); err == nil || !strings.Contains(out.String(), `encrypted with key "k1"`) {
		t.Fatalf("check without keys: %v\n%s", err, out.String())
	}
	out.Reset()
	if err := checkFormat(&out, dir, false, both); err != nil {
		t.Fatalf("check with keys: %v\n%s", err, out.String())
	}
	for _, want := range []string{"histories need:   7", "2 events sealed (key k1: 1, key k2: 1), 1 plaintext",
		"active key k2; keys no event uses (safe to retire): none", "OK:"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("check output lacks %q:\n%s", want, out.String())
		}
	}

	out.Reset()
	if err := rewriteEncryption(&out, dir, both, false); err != nil {
		t.Fatal(err)
	}
	k2 := keyFile(t, keys, "k2", "k2")
	out.Reset()
	if err := checkFormat(&out, dir, false, both); err != nil || !strings.Contains(out.String(), "3 events sealed (key k2: 3), 0 plaintext") ||
		!strings.Contains(out.String(), "safe to retire): k1") {
		t.Fatalf("after --encrypt-all: %v\n%s", err, out.String())
	}
	for _, f := range []string{"plain", "old", "new"} {
		data, _ := os.ReadFile(filepath.Join(dir, f+".history"))
		if bytes.Contains(data, []byte("secret-input")) {
			t.Fatalf("%s is still plaintext on disk", f)
		}
	}
	out.Reset()
	if err := checkFormat(&out, dir, false, k2); err != nil {
		t.Fatalf("k1 retired after --encrypt-all: %v\n%s", err, out.String())
	}

	out.Reset()
	if err := rewriteEncryption(&out, dir, k2, true); err != nil {
		t.Fatal(err)
	}
	if f, _ := history.ReadFormat(dir); f != history.FormatBase || !strings.Contains(out.String(), "recorded format set to 1") {
		t.Fatalf("after --decrypt-all the format is %d:\n%s", f, out.String())
	}
	out.Reset()
	if err := checkFormat(&out, dir, false, nil); err != nil || !strings.Contains(out.String(), "OK:") {
		t.Fatalf("plaintext again, no keys needed: %v\n%s", err, out.String())
	}
	if err := rewriteEncryption(&out, dir, nil, true); err == nil {
		t.Fatal("--decrypt-all without keys should refuse")
	}
}
