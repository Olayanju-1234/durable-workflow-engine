package server

import (
	"container/list"
	"fmt"
	"sync"

	"github.com/olayanju-1234/durable-workflow-engine/internal/history"
	"github.com/olayanju-1234/durable-workflow-engine/internal/state"
)

// defaultStateCacheSize bounds how many idle workflows keep their history in
// memory. A workflow in use is always cached; one that finishes is dropped.
const defaultStateCacheSize = 4096

// WithStateCacheSize sets how many idle, unfinished workflows keep their
// history and replayed state in memory between operations (0 caches none).
// Larger means fewer history reads; each entry costs its history's size.
func WithStateCacheSize(n int) Option {
	return func(e *Engine) { e.cacheSize = n }
}

// wfState is one workflow's lock plus its history and replayed state, kept in
// memory so an operation folds in the events it appends instead of reading and
// replaying the whole file again.
//
// Everything that reads a workflow's history and then appends to it holds mu,
// so a workflow's events are recorded one at a time and its ids stay
// contiguous, while different workflows proceed in parallel. Lock order: a
// wfState's mu, then Engine.mu; never two wfStates at once.
type wfState struct {
	id string
	mu sync.Mutex

	// Guarded by Engine.wfMu.
	refs int
	idle *list.Element // in Engine.idle while refs == 0

	// Guarded by mu. The engine is the only writer of its histories, so once
	// loaded these stay in step with the store; a failed append drops them.
	loaded bool
	events []*history.HistoryEvent
	fold   *state.Folder
}

// lockWorkflow returns the workflow's wfState, locked.
func (e *Engine) lockWorkflow(id string) *wfState {
	e.wfMu.Lock()
	w, ok := e.wfs[id]
	if !ok {
		w = &wfState{id: id}
		e.wfs[id] = w
	}
	w.refs++
	if w.idle != nil {
		e.idle.Remove(w.idle)
		w.idle = nil
	}
	e.wfMu.Unlock()
	w.mu.Lock()
	return w
}

// unlockWorkflow releases w. An unfinished workflow stays cached (least
// recently used first out); a finished, deleted or unloaded one is forgotten.
func (e *Engine) unlockWorkflow(w *wfState) {
	keep := e.cacheSize > 0 && w.loaded && len(w.events) > 0 && w.fold.Err() == nil &&
		!w.fold.State().Status.IsTerminal()
	w.mu.Unlock()

	e.wfMu.Lock()
	defer e.wfMu.Unlock()
	if w.refs--; w.refs > 0 {
		return
	}
	if !keep {
		delete(e.wfs, w.id)
		return
	}
	w.idle = e.idle.PushFront(w)
	for e.idle.Len() > e.cacheSize {
		old := e.idle.Remove(e.idle.Back()).(*wfState)
		old.idle = nil
		delete(e.wfs, old.id)
	}
}

// load reads the history from the store unless it is already in memory.
func (w *wfState) load(store *history.HistoryStore) error {
	if w.loaded {
		return nil
	}
	events, err := store.ReadAll(w.id)
	if err != nil {
		return err
	}
	w.events, w.fold = events, state.NewFolder()
	for _, ev := range events {
		if w.fold.Apply(ev) != nil {
			break // the folder keeps the error; the history is invalid
		}
	}
	w.loaded = true
	return nil
}

// forget drops the cached history, so the next operation reads the store.
func (w *wfState) forget() {
	w.loaded, w.events, w.fold = false, nil, nil
}

// historyLocked returns the workflow's events and replayed state. The events
// slice is safe to keep; the state is updated in place by later appends, so
// copy what must outlive one. The caller must hold w.mu.
func (e *Engine) historyLocked(w *wfState) ([]*history.HistoryEvent, *state.State, error) {
	if err := w.load(e.store); err != nil {
		return nil, nil, fmt.Errorf("server: read history %s: %w", w.id, err)
	}
	events := w.events[:len(w.events):len(w.events)]
	if err := w.fold.Err(); err != nil {
		return events, nil, fmt.Errorf("server: reconstruct %s: %w", w.id, err)
	}
	return events, w.fold.State(), nil
}

// recordLocked adds a just-appended event to the cached history.
func (w *wfState) recordLocked(ev *history.HistoryEvent) {
	if !w.loaded {
		return
	}
	w.events = append(w.events, ev)
	_ = w.fold.Apply(ev) // an invalid history is reported by the next read
}
