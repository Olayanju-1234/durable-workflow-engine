package history

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
)

func newKey(t *testing.T) []byte {
	t.Helper()
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		t.Fatal(err)
	}
	return k
}

func keyring(t *testing.T, active string, keys map[string][]byte) *Keyring {
	t.Helper()
	k, err := NewKeyring(active, keys)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

const secret = "4242-4242-4242-4242"

func secretEvent(id int64) *HistoryEvent {
	return &HistoryEvent{EventId: id, Timestamp: 1000 + id, RaftIndex: 7, Attributes: &HistoryEvent_StepScheduled{
		StepScheduled: &StepScheduledEvent{StepName: "charge-" + secret, Sequence: id, Input: []byte(`{"card":"` + secret + `"}`)},
	}}
}

func TestSealHidesTheAttributesAndOpensBack(t *testing.T) {
	k := keyring(t, "k1", map[string][]byte{"k1": newKey(t)})
	ev := secretEvent(3)
	sealed, err := k.Seal(ev)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := proto.Marshal(sealed)
	if bytes.Contains(raw, []byte(secret)) || bytes.Contains(raw, []byte("charge")) {
		t.Fatal("the sealed event shows its step name or payload")
	}
	if sealed.GetAttributes() != nil || sealed.GetKeyId() != "k1" || sealed.GetMinReaderFormat() != FormatEncrypted ||
		sealed.GetEventId() != 3 || sealed.GetTimestamp() != 1003 || sealed.GetRaftIndex() != 7 {
		t.Fatalf("sealed header %v", sealed)
	}
	if ev.GetSealed() != nil || ev.GetAttributes() == nil {
		t.Fatal("Seal changed the event it was given")
	}
	again, _ := k.Seal(ev)
	if bytes.Equal(again.GetSealed(), sealed.GetSealed()) {
		t.Fatal("two seals of one event are identical: the nonce is not fresh")
	}
	opened, err := k.Open(sealed)
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(opened.GetStepScheduled(), ev.GetStepScheduled()) || opened.GetEventId() != 3 || opened.GetRaftIndex() != 7 ||
		opened.GetMinReaderFormat() != FormatEncrypted || IsSealed(opened) {
		t.Fatalf("opened %v, want %v", opened, ev)
	}
	if same, _ := k.Open(ev); same != ev {
		t.Fatal("opening a plaintext event should return it as it is")
	}
	if same, _ := k.Seal(sealed); same != sealed {
		t.Fatal("sealing a sealed event should return it as it is")
	}
}

func TestSealedEventsResistTampering(t *testing.T) {
	k := keyring(t, "k1", map[string][]byte{"k1": newKey(t), "k2": newKey(t)})
	sealed, err := k.Seal(secretEvent(3))
	if err != nil {
		t.Fatal(err)
	}
	mutate := func(fn func(*HistoryEvent)) *HistoryEvent {
		c := proto.Clone(sealed).(*HistoryEvent)
		fn(c)
		return c
	}
	for name, ev := range map[string]*HistoryEvent{
		"a flipped ciphertext byte": mutate(func(e *HistoryEvent) { e.Sealed[len(e.Sealed)-5] ^= 1 }),
		"a flipped nonce byte":      mutate(func(e *HistoryEvent) { e.Sealed[3] ^= 1 }),
		"moved to another position": mutate(func(e *HistoryEvent) { e.EventId = 4 }),
		"relabelled as another key": mutate(func(e *HistoryEvent) { e.KeyId = "k2" }),
		"an unknown seal version":   mutate(func(e *HistoryEvent) { e.Sealed[0] = 9 }),
		"truncated":                 mutate(func(e *HistoryEvent) { e.Sealed = e.Sealed[:8] }),
	} {
		if _, err := k.Open(ev); !errors.Is(err, ErrSealBroken) {
			t.Errorf("%s: %v, want ErrSealBroken", name, err)
		}
	}
	other := keyring(t, "k3", map[string][]byte{"k3": newKey(t)})
	if _, err := other.Open(sealed); !errors.Is(err, ErrKeyMissing) || !strings.Contains(err.Error(), `"k1"`) {
		t.Errorf("a keyring without k1: %v", err)
	}
	var none *Keyring
	if _, err := none.Open(sealed); !errors.Is(err, ErrKeyMissing) || !strings.Contains(err.Error(), "no --encryption-keys") {
		t.Errorf("no keyring: %v", err)
	}
	// A seal is bound to its key id: the same key material under another id
	// does not open it.
	material := newKey(t)
	twins := keyring(t, "a", map[string][]byte{"a": material, "b": material})
	byA, err := twins.Seal(secretEvent(3))
	if err != nil {
		t.Fatal(err)
	}
	byA.KeyId = "b"
	if _, err := twins.Open(byA); !errors.Is(err, ErrSealBroken) {
		t.Errorf("relabelled to a twin key: %v", err)
	}
	// The same id with different key material does not authenticate either.
	impostor := keyring(t, "k1", map[string][]byte{"k1": newKey(t)})
	if _, err := impostor.Open(sealed); !errors.Is(err, ErrSealBroken) {
		t.Errorf("another key under the same id: %v", err)
	}
}

func TestParseKeyring(t *testing.T) {
	good := base64.StdEncoding.EncodeToString(make([]byte, 32))
	short := base64.StdEncoding.EncodeToString(make([]byte, 16))
	for _, c := range []struct {
		name, file, err string
	}{
		{"valid", `{"active":"a","keys":[{"id":"a","key":"` + good + `"},{"id":"b-2026.10","key":"` + good + `"}]}`, ""},
		{"no active", `{"keys":[{"id":"a","key":"` + good + `"}]}`, "no \"active\""},
		{"active not listed", `{"active":"z","keys":[{"id":"a","key":"` + good + `"}]}`, `"z" is not in the file`},
		{"not base64", `{"active":"a","keys":[{"id":"a","key":"!!"}]}`, "not base64"},
		{"too short", `{"active":"a","keys":[{"id":"a","key":"` + short + `"}]}`, "16 bytes"},
		{"listed twice", `{"active":"a","keys":[{"id":"a","key":"` + good + `"},{"id":"a","key":"` + good + `"}]}`, "twice"},
		{"bad id", `{"active":"a b","keys":[{"id":"a b","key":"` + good + `"}]}`, "key id"},
		{"unknown field", `{"active":"a","primary":"a","keys":[]}`, "not a key file"},
	} {
		_, err := ParseKeyring([]byte(c.file))
		switch {
		case c.err == "" && err != nil:
			t.Errorf("%s: %v", c.name, err)
		case c.err != "" && (err == nil || !strings.Contains(err.Error(), c.err)):
			t.Errorf("%s: %v, want an error containing %q", c.name, err, c.err)
		}
	}
}

// Encryption at rest: nothing of what happened is on disk in the clear, the
// directory records format 7 before the first sealed event, and the store
// reads its own histories back.
func TestStoreEncryptsAtRest(t *testing.T) {
	dir := t.TempDir()
	k := keyring(t, "k1", map[string][]byte{"k1": newKey(t)})
	s, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	s.SetKeyring(k)
	for i := int64(1); i <= 3; i++ {
		if err := s.Append("wf", secretEvent(i)); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	raw, err := os.ReadFile(filepath.Join(dir, "wf.history"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte(secret)) || bytes.Contains(raw, []byte("charge")) {
		t.Fatal("the history file shows its payloads")
	}
	if f, _ := ReadFormat(dir); f != FormatEncrypted {
		t.Fatalf("recorded format %d, want %d", f, FormatEncrypted)
	}

	s2, err := NewHistoryStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	if _, err := s2.ReadAll("wf"); !errors.Is(err, ErrKeyMissing) || !strings.Contains(err.Error(), "wf") {
		t.Fatalf("read without keys: %v, want ErrKeyMissing naming the history", err)
	}
	if raw, err := s2.ReadSealed("wf"); err != nil || len(raw) != 3 || raw[2].GetKeyId() != "k1" {
		t.Fatalf("ReadSealed without keys: %v %v", raw, err)
	}
	s2.SetKeyring(k)
	evs, err := s2.ReadAll("wf")
	if err != nil || len(evs) != 3 {
		t.Fatalf("read: %v %v", evs, err)
	}
	for i, ev := range evs {
		if !proto.Equal(ev.GetStepScheduled(), secretEvent(int64(i+1)).GetStepScheduled()) {
			t.Fatalf("event %d read back as %v", i+1, ev)
		}
	}
}

// Turning encryption on keeps every plaintext event readable: a history can
// hold both, and only events written from then on are sealed.
func TestPlaintextAndSealedEventsLiveTogether(t *testing.T) {
	s, err := NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	for i := int64(1); i <= 2; i++ {
		if err := s.Append("wf", secretEvent(i)); err != nil {
			t.Fatal(err)
		}
	}
	s.SetKeyring(keyring(t, "k1", map[string][]byte{"k1": newKey(t)}))
	if err := s.Append("wf", secretEvent(3)); err != nil {
		t.Fatal(err)
	}
	raw, _ := s.ReadSealed("wf")
	if IsSealed(raw[0]) || IsSealed(raw[1]) || !IsSealed(raw[2]) {
		t.Fatal("want the first two in plaintext and the third sealed")
	}
	evs, err := s.ReadAll("wf")
	if err != nil || len(evs) != 3 || evs[2].GetStepScheduled().GetSequence() != 3 {
		t.Fatalf("mixed history: %v %v", evs, err)
	}
}

// Rotation: a new active key seals new events, the old key still opens the
// old ones, and dropping the old key makes its events unreadable, by name.
func TestKeyRotation(t *testing.T) {
	s, err := NewHistoryStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	k1, k2 := newKey(t), newKey(t)
	s.SetKeyring(keyring(t, "k1", map[string][]byte{"k1": k1}))
	if err := s.Append("wf", secretEvent(1)); err != nil {
		t.Fatal(err)
	}
	s.SetKeyring(keyring(t, "k2", map[string][]byte{"k1": k1, "k2": k2}))
	if err := s.Append("wf", secretEvent(2)); err != nil {
		t.Fatal(err)
	}
	// A new run (continue-as-new, a reset) seals every event it starts with.
	if err := s.NewRun("wf", "__workflowd-run~1~r~wf", secretEvent(1), secretEvent(2)); err != nil {
		t.Fatal(err)
	}
	raw, _ := s.ReadSealed("wf")
	for _, ev := range raw {
		if ev.GetKeyId() != "k2" {
			t.Fatalf("new run event %d sealed with %q", ev.GetEventId(), ev.GetKeyId())
		}
	}
	old, _ := s.ReadSealed("__workflowd-run~1~r~wf")
	if old[0].GetKeyId() != "k1" || old[1].GetKeyId() != "k2" {
		t.Fatalf("earlier run sealed with %q and %q", old[0].GetKeyId(), old[1].GetKeyId())
	}
	if _, err := s.ReadAll("__workflowd-run~1~r~wf"); err != nil {
		t.Fatalf("both keys in the file: %v", err)
	}
	s.SetKeyring(keyring(t, "k2", map[string][]byte{"k2": k2}))
	if _, err := s.ReadAll("__workflowd-run~1~r~wf"); !errors.Is(err, ErrKeyMissing) || !strings.Contains(err.Error(), `"k1"`) {
		t.Fatalf("k1 retired too early: %v, want ErrKeyMissing naming k1", err)
	}
	if _, err := s.ReadAll("wf"); err != nil {
		t.Fatalf("the new run, all under k2: %v", err)
	}
}
