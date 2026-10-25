package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"google.golang.org/protobuf/proto"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// loadKeys reads --encryption-keys, warning (on stderr) when other users can
// read it: whoever can read the file can read every encrypted history.
func loadKeys(path string) (*history.Keyring, error) {
	keys, err := history.LoadKeyring(path)
	if err != nil {
		return nil, err
	}
	if st, err := os.Stat(path); err == nil && st.Mode().Perm()&0o077 != 0 {
		fmt.Fprintf(os.Stderr, "workflowd: warning: the key file %s is readable by other users (mode %s); chmod 600 or 640\n",
			path, st.Mode().Perm())
	}
	return keys, nil
}

// rewriteEncryption is --encrypt-all (every history sealed with the active
// key, so older keys can be retired) and --decrypt-all (every history in
// plaintext, then the recorded format lowered to what they need, so an older
// binary can read the directory again). The engine must be stopped.
func rewriteEncryption(w io.Writer, dir string, keys *history.Keyring, decrypt bool) error {
	if keys == nil {
		return errors.New("--encrypt-all and --decrypt-all need --encryption-keys: the keys that open the histories (and, to encrypt, the active one)")
	}
	recorded, err := history.ReadFormat(dir)
	if err != nil {
		return err
	}
	if recorded > history.FormatVersion {
		return history.TooNewError(recorded, "data directory "+dir)
	}
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		return err
	}
	defer store.Close()
	store.SetKeyring(keys)
	ids, err := store.ListWorkflows()
	if err != nil {
		return err
	}
	rewritten, events := 0, 0
	for _, id := range ids {
		evs, err := store.ReadAll(id)
		if err != nil {
			return fmt.Errorf("%s: %w (nothing after it was rewritten; fix the key file and run again)", id, err)
		}
		out := make([]*history.HistoryEvent, len(evs))
		fold := state.NewFolder()
		for i, ev := range evs {
			if decrypt {
				// The encryption stamp goes; what the event itself needs stays.
				c := proto.Clone(ev).(*history.HistoryEvent)
				c.MinReaderFormat = 0
				if need := fold.FormatOf(c); need > history.FormatBase {
					c.MinReaderFormat = need
				}
				_ = fold.Apply(c)
				out[i] = c
				continue
			}
			if out[i], err = keys.Seal(ev); err != nil {
				return err
			}
		}
		if err := store.Rewrite(id, out); err != nil {
			return err
		}
		rewritten++
		events += len(out)
	}
	how := fmt.Sprintf("sealed with key %s", keys.Active())
	if decrypt {
		how = "in plaintext"
	}
	fmt.Fprintf(w, "rewrote %d histories (%d events) %s\n", rewritten, events, how)
	if !decrypt {
		return nil
	}
	r, err := scanFormat(dir, nil)
	if err != nil {
		return err
	}
	if !r.ok() {
		return fmt.Errorf("after decrypting, some histories are unreadable: %v", r.unreadable)
	}
	if err := history.WriteFormat(dir, r.needed); err != nil {
		return err
	}
	fmt.Fprintf(w, "recorded format set to %d (%s)\n", r.needed, history.FormatRelease[r.needed])
	return nil
}
