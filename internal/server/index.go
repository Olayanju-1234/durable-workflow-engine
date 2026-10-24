package server

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"
)

// visibilityIndex holds one summary per workflow, so listing and filtering
// never read or replay history files. Recover builds it in the pass it already
// makes over the data directory (or the first List does, on an engine that was
// never recovered); every append and purge keeps it current, and a write whose
// outcome is unknown marks the workflow to be re-read. It is in memory only:
// the history files stay the source of truth, so a restart rebuilds it.
type visibilityIndex struct {
	mu      sync.Mutex
	entries map[string]*WorkflowSummary // nil until a build starts
	stale   map[string]struct{}         // ids whose entry must be re-read
	ready   chan struct{}               // closed once the first build finished
}

// reservedPrefix marks history files the engine keeps for itself (schedules),
// which are not workflows: recovery, listing and retention skip them.
const reservedPrefix = "__workflowd"

func isReservedID(id string) bool { return strings.HasPrefix(id, reservedPrefix) }

// indexLocked records w's current summary. The caller holds w.mu.
func (e *Engine) indexLocked(w *wfState) {
	if isReservedID(w.id) {
		return
	}
	var sum *WorkflowSummary
	switch {
	case !w.loaded:
		e.idx.mu.Lock()
		if e.idx.entries != nil {
			e.idx.stale[w.id] = struct{}{}
		}
		e.idx.mu.Unlock()
		return
	case len(w.events) == 0:
		e.indexDelete(w.id)
		return
	case w.fold.Err() != nil:
		sum = &WorkflowSummary{WorkflowId: w.id, Error: fmt.Sprintf("history invalid: %v", w.fold.Err())}
	default:
		sum = summaryFrom(w.id, w.events, w.fold.State(), w.bytes)
	}
	e.idx.mu.Lock()
	defer e.idx.mu.Unlock()
	if e.idx.entries != nil {
		e.idx.entries[w.id] = sum
		delete(e.idx.stale, w.id)
	}
}

func (e *Engine) indexDelete(id string) {
	e.idx.mu.Lock()
	defer e.idx.mu.Unlock()
	if e.idx.entries != nil {
		delete(e.idx.entries, id)
		delete(e.idx.stale, id)
	}
}

// startIndex makes the index live (empty) and reports whether it was not
// already: whoever gets true must add every workflow on disk, then call
// finishIndex. Anyone else gets the channel that closes when that is done.
func (e *Engine) startIndex() (bool, chan struct{}) {
	e.idx.mu.Lock()
	defer e.idx.mu.Unlock()
	if e.idx.entries != nil {
		return false, e.idx.ready
	}
	e.idx.entries = make(map[string]*WorkflowSummary)
	e.idx.stale = make(map[string]struct{})
	e.idx.ready = make(chan struct{})
	return true, e.idx.ready
}

// finishIndex ends a build; a failed one is discarded so the next List retries.
func (e *Engine) finishIndex(err error) {
	e.idx.mu.Lock()
	defer e.idx.mu.Unlock()
	if err != nil {
		e.idx.entries, e.idx.stale = nil, nil
	}
	close(e.idx.ready)
}

// indexWorkflow adds one workflow from the store (or its cache) to the index.
// Taking the workflow's lock orders it against concurrent appends, which
// update the live index themselves.
func (e *Engine) indexWorkflow(id string) {
	w := e.lockWorkflow(id)
	defer e.unlockWorkflow(w)
	if err := w.load(e.store, e.limits); err != nil {
		e.idx.mu.Lock()
		if e.idx.entries != nil {
			e.idx.entries[id] = &WorkflowSummary{WorkflowId: id, Error: fmt.Sprintf("history unreadable: %v", err)}
		}
		e.idx.mu.Unlock()
		return
	}
	e.indexLocked(w)
}

// ensureIndex builds the index by a full scan if nothing has yet, or waits for
// the build under way.
func (e *Engine) ensureIndex() error {
	first, ready := e.startIndex()
	if !first {
		<-ready
		e.idx.mu.Lock()
		built := e.idx.entries != nil
		e.idx.mu.Unlock()
		if !built {
			return e.ensureIndex() // that build failed; try again
		}
		return nil
	}
	ids, err := e.store.ListWorkflows()
	if err != nil {
		err = fmt.Errorf("server: build visibility index: %w", err)
		e.finishIndex(err)
		return err
	}
	for _, id := range ids {
		if !isReservedID(id) {
			e.indexWorkflow(id)
		}
	}
	e.finishIndex(nil)
	return nil
}

// indexed returns copies of the summaries that match, sorted by id, after
// re-reading any entry a failed write left in doubt.
func (e *Engine) indexed(match func(*WorkflowSummary) bool) ([]*WorkflowSummary, error) {
	if err := e.ensureIndex(); err != nil {
		return nil, err
	}
	e.idx.mu.Lock()
	stale := make([]string, 0, len(e.idx.stale))
	for id := range e.idx.stale {
		stale = append(stale, id)
	}
	e.idx.mu.Unlock()
	for _, id := range stale {
		e.indexWorkflow(id)
	}

	e.idx.mu.Lock()
	out := make([]*WorkflowSummary, 0, len(e.idx.entries))
	for _, s := range e.idx.entries {
		if match(s) {
			out = append(out, proto.Clone(s).(*WorkflowSummary))
		}
	}
	e.idx.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].GetWorkflowId() < out[j].GetWorkflowId() })
	return out, nil
}
