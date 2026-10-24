package main

import (
	"fmt"
	"io"
	"os"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// formatReport is what --check-format prints: the format the data directory
// records, the one its histories actually need, and whether this binary can
// serve it. It is how an operator decides whether a rollback is safe.
type formatReport struct {
	recorded, needed uint32
	histories        int
	unreadable       []string
}

// scanFormat reads every history in dir (workflows, archived runs and the
// engine's own) and works out the newest format any of them needs. It never
// writes. A directory recorded as newer than this binary is not scanned:
// this binary cannot tell what such histories contain.
func scanFormat(dir string) (*formatReport, error) {
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("data directory: %w", err)
	}
	recorded, err := history.ReadFormat(dir)
	if err != nil {
		return nil, err
	}
	r := &formatReport{recorded: recorded, needed: history.FormatBase}
	if recorded > history.FormatVersion {
		return r, nil
	}
	store, err := history.NewHistoryStore(dir)
	if err != nil {
		return nil, err
	}
	defer store.Close()
	ids, err := store.ListWorkflows()
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		r.histories++
		events, err := store.ReadAll(id)
		if err != nil {
			r.unreadable = append(r.unreadable, fmt.Sprintf("%s: %v", id, err))
			continue
		}
		f := state.NewFolder()
		for _, ev := range events {
			if f.Apply(ev) != nil {
				break
			}
		}
		if err := f.Err(); err != nil {
			r.unreadable = append(r.unreadable, fmt.Sprintf("%s: %v", id, err))
			continue
		}
		r.needed = max(r.needed, f.Format())
		if history.Namespaced(id) {
			// A binary before 0.4.0 would not see this history at all.
			r.needed = max(r.needed, history.FormatNamespaces)
		}
	}
	return r, nil
}

// ok reports whether this binary can serve the directory.
func (r *formatReport) ok() bool {
	return r.recorded <= history.FormatVersion && len(r.unreadable) == 0
}

func (r *formatReport) print(w io.Writer, dir string) {
	release := func(f uint32) string {
		if s := history.FormatRelease[f]; s != "" {
			return s
		}
		return "a newer workflowd"
	}
	fmt.Fprintf(w, "data directory:   %s\n", dir)
	if r.recorded == 0 {
		fmt.Fprintf(w, "recorded format:  none (written only by workflowd before 0.3.0)\n")
	} else {
		fmt.Fprintf(w, "recorded format:  %d (needs %s)\n", r.recorded, release(r.recorded))
	}
	if r.recorded <= history.FormatVersion {
		fmt.Fprintf(w, "histories need:   %d (needs %s; %d histories scanned)\n", r.needed, release(r.needed), r.histories)
	}
	fmt.Fprintf(w, "this binary:      reads up to format %d\n", history.FormatVersion)
	for _, u := range r.unreadable {
		fmt.Fprintf(w, "unreadable:       %s\n", u)
	}
	switch {
	case r.recorded > history.FormatVersion:
		fmt.Fprintf(w, "REFUSED: this directory needs %s; this binary would not start on it.\n", release(r.recorded))
	case len(r.unreadable) > 0:
		fmt.Fprintln(w, "REFUSED: some histories could not be read (see above).")
	default:
		fmt.Fprintln(w, "OK: this binary can serve this data directory.")
	}
}

// checkFormat is --check-format (and, with rescan, --rescan-format, which also
// records the scanned format, lowering it once the histories that needed a
// newer one are gone). Run it with the engine stopped.
func checkFormat(w io.Writer, dir string, rescan bool) error {
	r, err := scanFormat(dir)
	if err != nil {
		return err
	}
	r.print(w, dir)
	if !r.ok() {
		return fmt.Errorf("data directory %s is not readable by this binary", dir)
	}
	if rescan && r.needed != r.recorded {
		if err := history.WriteFormat(dir, r.needed); err != nil {
			return err
		}
		fmt.Fprintf(w, "recorded format set to %d\n", r.needed)
	}
	return nil
}
