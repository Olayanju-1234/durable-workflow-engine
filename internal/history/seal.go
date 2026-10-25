package history

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
)

// Encryption at rest (ADR-0022). An encrypted event keeps its id, timestamp,
// Raft index and format stamp in the clear, and carries its attributes (the
// kind of event, with every name, payload and error) in Sealed: AES-256-GCM
// under the key named KeyId, bound to that key id and the event's id.

// Errors opening a sealed event.
var (
	// ErrKeyMissing: the event was sealed with a key the keyring does not have.
	ErrKeyMissing = errors.New("history: encryption key missing")
	// ErrSealBroken: the sealed bytes do not authenticate: changed on disk,
	// moved from another position, or sealed with a different key of that id.
	ErrSealBroken = errors.New("history: sealed event does not authenticate")
)

const (
	sealVersion = 1
	keyBytes    = 32 // AES-256
	nonceBytes  = 12
)

var keyIDPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,64}$`)

// Keyring is the set of keys that open sealed events, and the one that seals
// new ones. It is immutable: reloading builds a new one.
type Keyring struct {
	active string
	keys   map[string]cipher.AEAD
}

// keyFile is the JSON form of --encryption-keys.
type keyFile struct {
	Active string `json:"active"`
	Keys   []struct {
		ID  string `json:"id"`
		Key string `json:"key"`
	} `json:"keys"`
}

// LoadKeyring reads a key file: {"active": "<id>", "keys": [{"id": "<id>",
// "key": "<32 random bytes, base64>"}, ...]}.
func LoadKeyring(path string) (*Keyring, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("history: encryption keys: %w", err)
	}
	k, err := ParseKeyring(data)
	if err != nil {
		return nil, fmt.Errorf("history: encryption keys %s: %w", path, err)
	}
	return k, nil
}

// ParseKeyring reads a key file's contents (see LoadKeyring).
func ParseKeyring(data []byte) (*Keyring, error) {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var f keyFile
	if err := dec.Decode(&f); err != nil {
		return nil, fmt.Errorf("not a key file: %w", err)
	}
	keys := make(map[string][]byte, len(f.Keys))
	for _, k := range f.Keys {
		if _, dup := keys[k.ID]; dup {
			return nil, fmt.Errorf("key %q is listed twice", k.ID)
		}
		raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(k.Key))
		if err != nil {
			return nil, fmt.Errorf("key %q is not base64: %w", k.ID, err)
		}
		keys[k.ID] = raw
	}
	return NewKeyring(f.Active, keys)
}

// NewKeyring builds a keyring from raw 32-byte keys by id; active seals new
// events and must be one of them.
func NewKeyring(active string, keys map[string][]byte) (*Keyring, error) {
	k := &Keyring{active: active, keys: make(map[string]cipher.AEAD, len(keys))}
	for id, raw := range keys {
		if !keyIDPattern.MatchString(id) {
			return nil, fmt.Errorf("key id %q: use 1 to 64 letters, digits, dots, underscores and hyphens", id)
		}
		if len(raw) != keyBytes {
			return nil, fmt.Errorf("key %q is %d bytes; an AES-256 key is %d (openssl rand -base64 32)", id, len(raw), keyBytes)
		}
		block, err := aes.NewCipher(raw)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", id, err)
		}
		aead, err := cipher.NewGCM(block)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", id, err)
		}
		k.keys[id] = aead
	}
	if active == "" {
		return nil, errors.New(`no "active" key: name the key that seals new events`)
	}
	if _, ok := k.keys[active]; !ok {
		return nil, fmt.Errorf("the active key %q is not in the file", active)
	}
	return k, nil
}

// Active is the id of the key that seals new events ("" for a nil keyring).
func (k *Keyring) Active() string {
	if k == nil {
		return ""
	}
	return k.active
}

// IDs lists every key id the keyring opens, sorted.
func (k *Keyring) IDs() []string {
	if k == nil {
		return nil
	}
	ids := make([]string, 0, len(k.keys))
	for id := range k.keys {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// Has reports whether the keyring opens events sealed with key id.
func (k *Keyring) Has(id string) bool {
	if k == nil {
		return false
	}
	_, ok := k.keys[id]
	return ok
}

// IsSealed reports whether an event's attributes are encrypted.
func IsSealed(ev *HistoryEvent) bool { return len(ev.GetSealed()) > 0 }

// sealAAD binds a seal to its key id and its event's position.
func sealAAD(keyID string, eventID int64) []byte {
	aad := make([]byte, 0, 32+len(keyID))
	aad = append(aad, "workflowd-event/v1\x00"...)
	aad = append(aad, keyID...)
	aad = append(aad, 0)
	return binary.BigEndian.AppendUint64(aad, uint64(eventID))
}

// Seal returns ev with its attributes encrypted under the active key, as a
// new event; ev is not changed. A nil keyring, or an event already sealed,
// returns ev itself.
func (k *Keyring) Seal(ev *HistoryEvent) (*HistoryEvent, error) {
	if k == nil || IsSealed(ev) {
		return ev, nil
	}
	plain, err := proto.Marshal(&HistoryEvent{Attributes: ev.GetAttributes()})
	if err != nil {
		return nil, fmt.Errorf("history: seal event %d: %w", ev.GetEventId(), err)
	}
	nonce := make([]byte, nonceBytes, nonceBytes+len(plain)+32)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("history: seal event %d: %w", ev.GetEventId(), err)
	}
	box := append([]byte{sealVersion}, k.keys[k.active].Seal(nonce, nonce, plain, sealAAD(k.active, ev.GetEventId()))...)
	return &HistoryEvent{
		EventId: ev.GetEventId(), Timestamp: ev.GetTimestamp(), RaftIndex: ev.GetRaftIndex(),
		MinReaderFormat: max(ev.GetMinReaderFormat(), FormatEncrypted),
		Sealed:          box, KeyId: k.active,
	}, nil
}

// Open returns a sealed event with its attributes decrypted, as a new event
// that keeps its header (format stamp included); an event that is not sealed
// is returned as it is.
func (k *Keyring) Open(ev *HistoryEvent) (*HistoryEvent, error) {
	if !IsSealed(ev) {
		return ev, nil
	}
	id := ev.GetKeyId()
	aead, ok := k.lookup(id)
	if !ok {
		hint := "the --encryption-keys file does not have it"
		if k == nil {
			hint = "no --encryption-keys file was given"
		}
		return nil, fmt.Errorf("%w: event %d is encrypted with key %q, and %s", ErrKeyMissing, ev.GetEventId(), id, hint)
	}
	box := ev.GetSealed()
	if len(box) < 1+nonceBytes || box[0] != sealVersion {
		return nil, fmt.Errorf("%w: event %d is not a sealed event this binary knows", ErrSealBroken, ev.GetEventId())
	}
	nonce, ct := box[1:1+nonceBytes], box[1+nonceBytes:]
	plain, err := aead.Open(nil, nonce, ct, sealAAD(id, ev.GetEventId()))
	if err != nil {
		return nil, fmt.Errorf("%w: event %d (key %q)", ErrSealBroken, ev.GetEventId(), id)
	}
	inner := &HistoryEvent{}
	if err := proto.Unmarshal(plain, inner); err != nil {
		return nil, fmt.Errorf("%w: event %d: %v", ErrSealBroken, ev.GetEventId(), err)
	}
	return &HistoryEvent{
		EventId: ev.GetEventId(), Timestamp: ev.GetTimestamp(), RaftIndex: ev.GetRaftIndex(),
		MinReaderFormat: ev.GetMinReaderFormat(), Attributes: inner.GetAttributes(),
	}, nil
}

func (k *Keyring) lookup(id string) (cipher.AEAD, bool) {
	if k == nil {
		return nil, false
	}
	aead, ok := k.keys[id]
	return aead, ok
}

// OpenAll opens every sealed event of a history read without its keys (a
// backup, an archive copy).
func (k *Keyring) OpenAll(events []*HistoryEvent) ([]*HistoryEvent, error) {
	out := make([]*HistoryEvent, len(events))
	for i, ev := range events {
		o, err := k.Open(ev)
		if err != nil {
			return nil, err
		}
		out[i] = o
	}
	return out, nil
}
