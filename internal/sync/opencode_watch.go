package sync

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sort"

	"go.kenn.io/agentsview/internal/parser"
)

const (
	openCodeJournalBatchLimit = 256
	openCodePendingLimit      = 128
)

// openCodeWatchState is disposable watcher optimization state. Startup and
// periodic full sync remain authoritative; this state never acknowledges
// durable correctness and intentionally owns no retry timer.
type openCodeWatchState struct {
	rowID   int64
	pending map[string]struct{}
}

func (e *Engine) classifyOpenCodeJournalPath(
	ctx context.Context,
	path, watchRoot string,
	provider parser.Provider,
) ([]parser.SourceRef, bool) {
	root := filepath.Clean(watchRoot)
	dbPath := filepath.Join(root, "opencode.db")
	cleanPath := filepath.Clean(path)
	if cleanPath != dbPath && cleanPath != dbPath+"-wal" {
		return nil, false
	}
	if cleanPath == dbPath+"-wal" {
		info, err := os.Stat(cleanPath)
		if os.IsNotExist(err) || (err == nil && (info == nil || !info.Mode().IsRegular() || info.Size() <= 32)) {
			return nil, true
		}
	}
	if info, err := os.Stat(dbPath); err != nil || info == nil || !info.Mode().IsRegular() {
		return nil, true
	}
	targeted, ok := provider.(parser.OpenCodeTargetedSourceProvider)
	if !ok {
		return nil, true
	}

	e.openCodeWatchMu.Lock()
	state, initialized := e.openCodeWatch[dbPath]
	if !initialized {
		high, supported, err := parser.OpenCodeJournalHighWater(ctx, dbPath)
		if err == nil && supported {
			e.openCodeWatch[dbPath] = openCodeWatchState{
				rowID: high, pending: make(map[string]struct{}),
			}
		}
		e.openCodeWatchMu.Unlock()
		if err != nil && ctx.Err() == nil {
			log.Printf("opencode watcher baseline: %v", err)
		}
		return nil, true
	}
	batch, err := parser.ReadOpenCodeJournal(
		ctx, dbPath, state.rowID, openCodeJournalBatchLimit,
	)
	if err != nil {
		// A later event establishes a fresh baseline instead of retrying.
		delete(e.openCodeWatch, dbPath)
		e.openCodeWatchMu.Unlock()
		if ctx.Err() == nil {
			log.Printf("opencode watcher journal: %v", err)
		}
		return nil, true
	}
	if !batch.Supported {
		delete(e.openCodeWatch, dbPath)
		e.openCodeWatchMu.Unlock()
		return nil, true
	}
	state.rowID = batch.HighRowID
	if !batch.Safe || batch.Overflow {
		state.pending = make(map[string]struct{})
		e.openCodeWatch[dbPath] = state
		e.openCodeWatchMu.Unlock()
		return nil, true
	}
	ready := make(map[string]struct{})
	for _, event := range batch.Events {
		if event.Settlement {
			ready[event.SessionID] = struct{}{}
			delete(state.pending, event.SessionID)
			continue
		}
		state.pending[event.SessionID] = struct{}{}
		if len(state.pending) > openCodePendingLimit {
			state.pending = make(map[string]struct{})
			e.openCodeWatch[dbPath] = state
			e.openCodeWatchMu.Unlock()
			return nil, true
		}
	}
	e.openCodeWatch[dbPath] = state
	e.openCodeWatchMu.Unlock()
	if len(ready) == 0 {
		return nil, true
	}

	ids := make([]string, 0, len(ready))
	for id := range ready {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	if parser.ResolveOpenCodeSource(root).Mode == parser.OpenCodeSourceStorage {
		filtered := ids[:0]
		for _, id := range ids {
			virtualPath := parser.OpenCodeSQLiteVirtualPath(dbPath, id)
			storedPath := e.db.GetSessionFilePath("opencode:" + id)
			if storedPath == virtualPath {
				filtered = append(filtered, id)
			}
			// A storage-backed canonical source is ignored. A new hybrid
			// session is deferred until full discovery proves its canonical source.
		}
		ids = filtered
	}
	if len(ids) == 0 {
		return nil, true
	}
	sources, err := targeted.OpenCodeSourcesForSessionIDs(ctx, dbPath, ids)
	if err != nil {
		if ctx.Err() == nil {
			log.Printf("opencode watcher metadata: %v", err)
		}
		return nil, true
	}
	return sources, true
}

func (e *Engine) rebaselineOpenCodeWatch(ctx context.Context) {
	baselines := make(map[string]openCodeWatchState)
	for _, root := range e.agentDirs[parser.AgentOpenCode] {
		if err := ctx.Err(); err != nil {
			return
		}
		dbPath := parser.ResolveOpenCodeSource(filepath.Clean(root)).DBPath
		if dbPath == "" {
			continue
		}
		high, supported, err := parser.OpenCodeJournalHighWater(ctx, dbPath)
		if err != nil || !supported {
			continue
		}
		baselines[dbPath] = openCodeWatchState{
			rowID: high, pending: make(map[string]struct{}),
		}
	}
	e.openCodeWatchMu.Lock()
	for dbPath := range e.openCodeWatch {
		delete(e.openCodeWatch, dbPath)
	}
	for dbPath, state := range baselines {
		e.openCodeWatch[dbPath] = state
	}
	e.openCodeWatchMu.Unlock()
}
