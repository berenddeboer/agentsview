package parser

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenCodeProviderStorageSourceMethods(t *testing.T) {

	root := t.TempDir()
	sessionPath := writeOpenCodeProviderStorageSession(
		t, root, "session", "ses_provider", "opencode-app", "Provider Session",
	)

	provider, ok := NewProvider(AgentOpenCode, ProviderConfig{
		Roots:   []string{root},
		Machine: "devbox",
	})
	require.True(t, ok)

	plan, err := provider.WatchPlan(context.Background())
	require.NoError(t, err)
	require.Len(t, plan.Roots, 1)
	assert.Equal(t, filepath.Join(root, "storage"), plan.Roots[0].Path)
	assert.True(t, plan.Roots[0].Recursive)

	discovered, err := provider.Discover(context.Background())
	require.NoError(t, err)
	require.Len(t, discovered, 1)
	source := discovered[0]
	assert.Equal(t, AgentOpenCode, source.Provider)
	assert.Equal(t, sessionPath, source.DisplayPath)
	assert.Equal(t, sessionPath, source.FingerprintKey)
	assert.Equal(t, "opencode_app", source.ProjectHint)

	found, ok, err := provider.FindSource(context.Background(), FindSourceRequest{
		FullSessionID: "remote~opencode:ses_provider",
	})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, sessionPath, found.DisplayPath)

	messagePath := filepath.Join(
		root, "storage", "message", "ses_provider", "msg_1.json",
	)
	partPath := filepath.Join(root, "storage", "part", "msg_1", "prt_1.json")
	for _, tc := range []struct {
		name string
		path string
	}{
		{name: "session", path: sessionPath},
		{name: "message", path: messagePath},
		{name: "part", path: partPath},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed, err := provider.SourcesForChangedPath(
				context.Background(),
				ChangedPathRequest{
					Path:      tc.path,
					EventKind: "write",
					WatchRoot: filepath.Join(root, "storage"),
				},
			)
			require.NoError(t, err)
			require.Len(t, changed, 1)
			assert.Equal(t, sessionPath, changed[0].DisplayPath)
		})
	}

	fingerprint, err := provider.Fingerprint(context.Background(), found)
	require.NoError(t, err)
	assert.Equal(t, sessionPath, fingerprint.Key)
	assert.Positive(t, fingerprint.Size)
	assert.Positive(t, fingerprint.MTimeNS)
	// Storage-mode Fingerprint is stat-only: the content fingerprint is
	// computed by Parse, and hashing here would re-read every message and
	// part file on each fingerprint call.
	assert.Empty(t, fingerprint.Hash)

	outcome, err := provider.Parse(context.Background(), ParseRequest{
		Source:      found,
		Fingerprint: fingerprint,
	})
	require.NoError(t, err)
	require.True(t, outcome.ResultSetComplete)
	require.Len(t, outcome.Results, 1)
	result := outcome.Results[0]
	assert.Equal(t, DataVersionCurrent, result.DataVersion)
	assert.Equal(t, "opencode:ses_provider", result.Result.Session.ID)
	assert.Equal(t, AgentOpenCode, result.Result.Session.Agent)
	assert.Equal(t, "opencode_app", result.Result.Session.Project)
	assert.Equal(t, "devbox", result.Result.Session.Machine)
	assert.True(t,
		HasOpenCodeStorageFingerprint(result.Result.Session.File.Hash),
		"Parse must compute the storage content fingerprint itself")
	assert.Len(t, result.Result.Messages, 1)

	require.NoError(t, os.Remove(sessionPath), "remove storage session")
	removed, err := provider.SourcesForChangedPath(
		context.Background(),
		ChangedPathRequest{
			Path:      sessionPath,
			EventKind: "remove",
			WatchRoot: filepath.Join(root, "storage"),
		},
	)
	require.NoError(t, err)
	require.Len(t, removed, 1)
	assert.Equal(t, sessionPath, removed[0].DisplayPath)
	assert.Equal(t, "global", removed[0].ProjectHint)
}

func TestOpenCodeProviderSQLiteSourceMethods(t *testing.T) {

	fixture := openCodeSQLiteProviderReadFixture(t)
	root := fixture.Root
	dbPath := fixture.DBPath
	virtualPath := fixture.SQLiteVirtualPath

	provider, ok := NewProvider(AgentOpenCode, ProviderConfig{
		Roots:   []string{root},
		Machine: "devbox",
	})
	require.True(t, ok)

	plan, err := provider.WatchPlan(context.Background())
	require.NoError(t, err)
	require.Len(t, plan.Roots, 1)
	assert.Equal(t, root, plan.Roots[0].Path)
	assert.True(t, plan.Roots[0].Recursive)
	assert.Equal(t, []string{
		"*.json", "opencode.db", "opencode.db-wal",
	}, plan.Roots[0].IncludeGlobs)

	discovered, err := provider.Discover(context.Background())
	require.NoError(t, err)
	requireSourcePathsMatch(t, discovered, fixture.AllVirtualPaths)
	requireContainsSourcePath(t, discovered, virtualPath)

	changed, err := provider.SourcesForChangedPath(
		context.Background(),
		ChangedPathRequest{Path: dbPath, EventKind: "write", WatchRoot: root},
	)
	require.NoError(t, err)
	requireSourcePathsMatch(t, changed, fixture.AllVirtualPaths)

	found, ok, err := provider.FindSource(context.Background(), FindSourceRequest{
		FullSessionID: "host~opencode:" + fixture.TargetSessionID,
	})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, virtualPath, found.DisplayPath)

	fingerprint, err := provider.Fingerprint(context.Background(), found)
	require.NoError(t, err)
	assert.Equal(t, virtualPath, fingerprint.Key)
	assert.Equal(t, int64(1700000060000)*1_000_000, fingerprint.MTimeNS)

	outcome, err := provider.Parse(context.Background(), ParseRequest{
		Source:      found,
		Fingerprint: fingerprint,
	})
	require.NoError(t, err)
	require.True(t, outcome.ResultSetComplete)
	require.Len(t, outcome.Results, 1)
	result := outcome.Results[0]
	assert.Equal(t, DataVersionCurrent, result.DataVersion)
	assert.Equal(t, "opencode:ses_sqlite", result.Result.Session.ID)
	assert.Equal(t, "sqlite_app", result.Result.Session.Project)
	assert.Equal(t, "devbox", result.Result.Session.Machine)
	assert.Equal(t, "Hello from sqlite", result.Result.Messages[0].Content)

	removedRoot, removedDBPath := newRemovedOpenCodeDBPath(t)
	removedProvider, ok := NewProvider(AgentOpenCode, ProviderConfig{
		Roots: []string{removedRoot},
	})
	require.True(t, ok)
	removed, err := removedProvider.SourcesForChangedPath(
		context.Background(),
		ChangedPathRequest{Path: removedDBPath, EventKind: "remove", WatchRoot: removedRoot},
	)
	require.NoError(t, err)
	assert.Empty(t, removed, "removed sqlite DBs have no stateless virtual source list")
}

func TestOpenCodeProviderSQLiteEventDeltaIsBoundedByChangedSessions(t *testing.T) {
	for _, sessionCount := range []int{10, 5000} {
		t.Run(fmt.Sprintf("sessions_%d", sessionCount), func(t *testing.T) {
			root := t.TempDir()
			dbPath, _, db := newTestDBAt(t, filepath.Join(root, "opencode.db"))
			defer db.Close()
			seedOpenCodeEventJournal(t, db)
			_, err := db.Exec(
				"INSERT INTO project (id, worktree) VALUES (?, ?)",
				"prj_scale", "/tmp/scale",
			)
			require.NoError(t, err)
			tx, err := db.Begin()
			require.NoError(t, err)
			stmt, err := tx.Prepare(`
				INSERT INTO session
					(id, project_id, time_created, time_updated)
				VALUES (?, 'prj_scale', 1, 2)
			`)
			require.NoError(t, err)
			for i := range sessionCount {
				_, err = stmt.Exec(fmt.Sprintf("ses_%06d", i))
				require.NoError(t, err)
			}
			require.NoError(t, stmt.Close())
			require.NoError(t, tx.Commit())

			target := fmt.Sprintf("ses_%06d", sessionCount/2)
			addOpenCodeEvent(t, db, "evt_001", target, "session.created.1", 1)
			cursor, supported, err := OpenCodeEventCursor(dbPath)
			require.NoError(t, err)
			require.True(t, supported)
			addOpenCodeEvent(t, db, "evt_002", target, "message.part.updated.1", 2)

			provider, ok := NewProvider(AgentOpenCode, ProviderConfig{Roots: []string{root}})
			require.True(t, ok)
			cursorProvider, ok := provider.(ChangedPathCursorProvider)
			require.True(t, ok)
			result, err := cursorProvider.SourcesForChangedPathCursor(
				context.Background(),
				ChangedPathRequest{
					Path:          dbPath,
					EventKind:     "write",
					WatchRoot:     root,
					ChangeCursors: map[string]string{dbPath: cursor},
				},
			)
			require.NoError(t, err)
			require.Len(t, result.Sources, 1)
			assert.Equal(t, dbPath+"#"+target, result.Sources[0].DisplayPath)
			require.Len(t, result.Cursors, 1)
			decoded, err := decodeOpenCodeEventCursor(result.Cursors[0].Value)
			require.NoError(t, err)
			assert.Equal(t, int64(2), decoded.RowID)
		})
	}
}

func TestOpenCodeProviderSQLiteFallbackIncludesRetryDescriptor(t *testing.T) {
	root := t.TempDir()
	dbPath, seeder, db := newTestDBAt(t, filepath.Join(root, "opencode.db"))
	defer db.Close()
	seedOpenCodeEventJournal(t, db)
	seeder.AddProject("prj_fallback", "/tmp/fallback")
	seeder.AddSession("ses_fallback", "prj_fallback", "", "Fallback", 1, 2)
	addOpenCodeEvent(
		t, db, "evt_001", "ses_fallback", "session.created.1", 1,
	)

	provider, ok := NewProvider(AgentOpenCode, ProviderConfig{Roots: []string{root}})
	require.True(t, ok)
	cursorProvider, ok := provider.(ChangedPathCursorProvider)
	require.True(t, ok)
	result, err := cursorProvider.SourcesForChangedPathCursor(
		context.Background(),
		ChangedPathRequest{
			Path:      dbPath,
			EventKind: "write",
			WatchRoot: root,
		},
	)
	require.NoError(t, err)
	require.Len(t, result.Sources, 1)
	require.Equal(t, []ChangedPathRetry{{Key: dbPath}}, result.Retries)
	require.Len(t, result.Cursors, 1)
	assert.Equal(t, dbPath, result.Cursors[0].Key)
}

func TestOpenCodeProviderSQLiteEventDeltaDoesNotListStorageProjects(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory read permissions are not enforced on Windows")
	}

	for _, projectCount := range []int{10, 1000} {
		t.Run(fmt.Sprintf("projects_%d", projectCount), func(t *testing.T) {
			root := t.TempDir()
			dbPath, seeder, db := newTestDBAt(t, filepath.Join(root, "opencode.db"))
			defer db.Close()
			seedOpenCodeEventJournal(t, db)
			seeder.AddProject("prj_target", "/tmp/target")
			seeder.AddSession("ses_target", "prj_target", "", "Changed", 1, 2)
			addOpenCodeEvent(t, db, "evt_001", "ses_target", "session.created.1", 1)
			cursor, supported, err := OpenCodeEventCursor(dbPath)
			require.NoError(t, err)
			require.True(t, supported)

			sessionRoot := filepath.Join(root, "storage", "session")
			for i := range projectCount {
				require.NoError(t, os.MkdirAll(
					filepath.Join(sessionRoot, fmt.Sprintf("prj_%06d", i)),
					0o755,
				))
			}
			targetDir := filepath.Join(sessionRoot, "prj_target")
			require.NoError(t, os.MkdirAll(targetDir, 0o755))
			require.NoError(t, os.WriteFile(
				filepath.Join(targetDir, "ses_target.json"), []byte("{}"), 0o600,
			))
			require.NoError(t, os.Chmod(sessionRoot, 0o111))
			t.Cleanup(func() { _ = os.Chmod(sessionRoot, 0o755) })
			if _, err := os.ReadDir(sessionRoot); err == nil {
				t.Skip("filesystem does not enforce directory read permissions")
			}
			addOpenCodeEvent(t, db, "evt_002", "ses_target", "message.part.updated.1", 2)

			provider, ok := NewProvider(AgentOpenCode, ProviderConfig{Roots: []string{root}})
			require.True(t, ok)
			cursorProvider := provider.(ChangedPathCursorProvider)
			result, err := cursorProvider.SourcesForChangedPathCursor(
				context.Background(),
				ChangedPathRequest{
					Path:          dbPath,
					EventKind:     "write",
					WatchRoot:     root,
					ChangeCursors: map[string]string{dbPath: cursor},
				},
			)
			require.NoError(t, err)
			assert.Empty(t, result.Sources,
				"the exact storage duplicate should suppress the SQLite source")
		})
	}
}

func TestOpenCodeProviderIgnoresNonDataSQLiteSidecars(t *testing.T) {
	tests := []struct {
		name      string
		suffix    string
		create    bool
		size      int
		remove    bool
		eventKind string
	}{
		{name: "missing WAL", suffix: "-wal", eventKind: "remove"},
		{name: "empty WAL", suffix: "-wal", create: true, eventKind: "write"},
		{name: "partial WAL", suffix: "-wal", create: true, size: 3, eventKind: "write"},
		{name: "header-only WAL", suffix: "-wal", create: true, size: 32, eventKind: "write"},
		{name: "removed WAL", suffix: "-wal", create: true, size: 64, remove: true, eventKind: "remove"},
		{name: "SHM", suffix: "-shm", create: true, size: 32 * 1024, eventKind: "write"},
		{name: "unknown sidecar", suffix: "-backup", create: true, size: 64, eventKind: "write"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			fixture := openCodeSQLiteProviderReadFixture(t)
			path := fixture.DBPath + tc.suffix
			if tc.create {
				require.NoError(t, os.WriteFile(path, make([]byte, tc.size), 0o600))
			}
			if tc.remove {
				require.NoError(t, os.Remove(path))
			}

			provider, ok := NewProvider(AgentOpenCode, ProviderConfig{
				Roots: []string{fixture.Root},
			})
			require.True(t, ok)
			changed, err := provider.SourcesForChangedPath(
				context.Background(),
				ChangedPathRequest{
					Path:      path,
					EventKind: tc.eventKind,
					WatchRoot: fixture.Root,
				},
			)
			require.NoError(t, err)
			assert.Empty(t, changed)
		})
	}
}

func TestSQLiteWALHasFramesFailsOpenOnStatError(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("directory-permission stat failures are not portable to Windows")
	}
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}

	locked := filepath.Join(t.TempDir(), "locked")
	require.NoError(t, os.Mkdir(locked, 0o700))
	walPath := filepath.Join(locked, "opencode.db-wal")
	require.NoError(t, os.WriteFile(walPath, make([]byte, 64), 0o600))
	require.NoError(t, os.Chmod(locked, 0o000))
	t.Cleanup(func() {
		require.NoError(t, os.Chmod(locked, 0o700))
	})

	assert.True(t, sqliteWALHasFrames(walPath),
		"stat errors other than not-exist must fail open so real WAL updates are not dropped")
}

func TestOpenCodeProviderReadsLiveSQLiteWAL(t *testing.T) {
	dbPath, seeder, writer := newTestDB(t)
	defer writer.Close()

	var journalMode string
	require.NoError(t, writer.QueryRow("PRAGMA journal_mode=WAL").Scan(&journalMode))
	require.Equal(t, "wal", journalMode)
	_, err := writer.Exec("PRAGMA wal_autocheckpoint=0")
	require.NoError(t, err)
	seedStandardSession(t, seeder)

	walPath := dbPath + "-wal"
	walInfo, err := os.Stat(walPath)
	require.NoError(t, err)
	require.Greater(t, walInfo.Size(), sqliteWALHeaderSize)

	provider, ok := NewProvider(AgentOpenCode, ProviderConfig{
		Roots:   []string{filepath.Dir(dbPath)},
		Machine: "devbox",
	})
	require.True(t, ok)
	changed, err := provider.SourcesForChangedPath(
		context.Background(),
		ChangedPathRequest{
			Path:      walPath,
			EventKind: "write",
			WatchRoot: filepath.Dir(dbPath),
		},
	)
	require.NoError(t, err)
	require.Len(t, changed, 1)

	outcome, err := provider.Parse(context.Background(), ParseRequest{
		Source: changed[0],
	})
	require.NoError(t, err)
	require.Len(t, outcome.Results, 1)
	assert.Equal(t, "opencode:ses_abc", outcome.Results[0].Result.Session.ID)
	assert.Equal(t, "Sure, I can help with Go.",
		outcome.Results[0].Result.Messages[1].Content)
}

// TestOpenCodeProviderSQLiteDiscoversAllListedSessions guards the refactor that
// builds SourceRefs directly from the listed SQLite metadata instead of
// reopening the DB per row via OpenCodeSQLiteSessionExists. Every row read from
// the DB must surface as a discoverable source with its dbPath#id virtual path.
func TestOpenCodeProviderSQLiteDiscoversAllListedSessions(t *testing.T) {

	fixture := openCodeSQLiteProviderReadFixture(t)
	provider, ok := NewProvider(AgentOpenCode, ProviderConfig{
		Roots:   []string{fixture.Root},
		Machine: "devbox",
	})
	require.True(t, ok)

	discovered, err := provider.Discover(context.Background())
	require.NoError(t, err)
	requireSourcePathsMatch(t, discovered, fixture.AllVirtualPaths)
	for _, src := range discovered {
		assert.Equal(t, src.DisplayPath, src.FingerprintKey)
	}
}

// TestOpenCodeProviderSQLiteFingerprintUsesDiscoveryMeta pins that
// fingerprinting a discovered SQLite-backed session reuses the time_updated
// already listed during discovery instead of reopening the shared DB once per
// session. Replacing the DB with unreadable bytes after discovery makes any
// reopen fail, so a successful fingerprint proves the metadata was carried on
// the source.
func TestOpenCodeProviderSQLiteFingerprintUsesDiscoveryMeta(t *testing.T) {

	root := t.TempDir()
	dbPath, seeder, db := newTestDBAt(t, filepath.Join(root, "opencode.db"))
	seeder.AddProject("prj_1", "/home/user/code/sqlite-app")
	seeder.AddSession(
		"ses_meta", "prj_1", "", "Meta", 1700000000000, 1700000010000,
	)
	require.NoError(t, db.Close())

	provider, ok := NewProvider(AgentOpenCode, ProviderConfig{Roots: []string{root}})
	require.True(t, ok)
	discovered, err := provider.Discover(context.Background())
	require.NoError(t, err)
	require.Len(t, discovered, 1)

	garbage := []byte("not a sqlite database")
	require.NoError(t, os.WriteFile(dbPath, garbage, 0o644))

	fp, err := provider.Fingerprint(context.Background(), discovered[0])
	require.NoError(t, err,
		"fingerprint must not reopen the SQLite DB for a discovered source")
	assert.Equal(t, OpenCodeSQLiteVirtualPath(dbPath, "ses_meta"), fp.Key)
	assert.Equal(t, int64(1700000010000000000), fp.MTimeNS,
		"fingerprint mtime must be the discovered time_updated in ns")
	assert.Equal(t, int64(len(garbage)), fp.Size,
		"fingerprint size stays the shared container file size")
}

func TestOpenCodeProviderHybridDiscoveryFiltersSQLiteDuplicate(t *testing.T) {

	root := t.TempDir()
	storagePath := writeOpenCodeProviderStorageSession(
		t, root, "session", "ses_dup", "storage-app", "Storage Session",
	)
	dbPath, seeder, db := newTestDBAt(t, filepath.Join(root, "opencode.db"))
	defer db.Close()
	seeder.AddProject("prj_1", "/home/user/code/sqlite-app")
	seeder.AddSession("ses_dup", "prj_1", "", "Duplicate", 1700000000000, 1700000010000)
	seeder.AddSession("ses_db_only", "prj_1", "", "DB Only", 1700000000000, 1700000020000)
	virtualOnly := OpenCodeSQLiteVirtualPath(dbPath, "ses_db_only")

	provider, ok := NewProvider(AgentOpenCode, ProviderConfig{Roots: []string{root}})
	require.True(t, ok)

	discovered, err := provider.Discover(context.Background())
	require.NoError(t, err)
	require.Len(t, discovered, 2)
	assert.ElementsMatch(t, []string{storagePath, virtualOnly}, []string{
		discovered[0].DisplayPath,
		discovered[1].DisplayPath,
	})

	found, ok, err := provider.FindSource(context.Background(), FindSourceRequest{
		StoredFilePath: OpenCodeSQLiteVirtualPath(dbPath, "ses_dup"),
		FullSessionID:  "opencode:ses_dup",
	})
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, storagePath, found.DisplayPath)
}

func TestOpenCodeProviderDiscoveryToleratesCorruptSQLiteDB(t *testing.T) {

	root := t.TempDir()
	storagePath := writeOpenCodeProviderStorageSession(
		t, root, "session", "ses_valid", "storage-app", "Valid Session",
	)
	// A present-but-corrupt optional DB must not abort discovery of the valid
	// storage-backed session that lives in the same root.
	require.NoError(t, os.WriteFile(
		filepath.Join(root, "opencode.db"), []byte("not a sqlite database"), 0o644,
	))

	provider, ok := NewProvider(AgentOpenCode, ProviderConfig{Roots: []string{root}})
	require.True(t, ok)

	discovered, err := provider.Discover(context.Background())
	require.NoError(t, err)
	require.Len(t, discovered, 1)
	assert.Equal(t, storagePath, discovered[0].DisplayPath)
}

func TestOpenCodeFamilyProviderRelabelsForks(t *testing.T) {

	for _, tc := range []struct {
		agent         AgentType
		sessionSubdir string
		prefix        string
		project       string
	}{
		{agent: AgentKilo, sessionSubdir: "session", prefix: "kilo:", project: "kilo-app"},
		{agent: AgentMiMoCode, sessionSubdir: "session_diff", prefix: "mimocode:", project: "mimo-app"},
	} {
		t.Run(string(tc.agent), func(t *testing.T) {

			root := t.TempDir()
			sessionPath := writeOpenCodeProviderStorageSession(
				t, root, tc.sessionSubdir, "ses_provider", tc.project, "Provider Session",
			)
			provider, ok := NewProvider(tc.agent, ProviderConfig{
				Roots:   []string{root},
				Machine: "devbox",
			})
			require.True(t, ok)
			source, ok, err := provider.FindSource(context.Background(), FindSourceRequest{
				FullSessionID: "host~" + tc.prefix + "ses_provider",
			})
			require.NoError(t, err)
			require.True(t, ok)
			assert.Equal(t, sessionPath, source.DisplayPath)

			outcome, err := provider.Parse(context.Background(), ParseRequest{
				Source: source,
			})
			require.NoError(t, err)
			require.True(t, outcome.ResultSetComplete)
			require.Len(t, outcome.Results, 1)
			result := outcome.Results[0].Result
			assert.Equal(t, tc.prefix+"ses_provider", result.Session.ID)
			assert.Equal(t, tc.agent, result.Session.Agent)
			assert.Equal(t, strings.ReplaceAll(tc.project, "-", "_"), result.Session.Project)

			require.NoError(t, os.Remove(sessionPath), "remove storage session")
			removed, err := provider.SourcesForChangedPath(
				context.Background(),
				ChangedPathRequest{
					Path:      sessionPath,
					EventKind: "rename",
					WatchRoot: filepath.Join(root, "storage"),
				},
			)
			require.NoError(t, err)
			require.Len(t, removed, 1)
			assert.Equal(t, sessionPath, removed[0].DisplayPath)
		})
	}
}

func writeOpenCodeProviderStorageSession(
	t *testing.T,
	root, sessionSubdir, sessionID, project, title string,
) string {
	t.Helper()
	sessionPath := filepath.Join(
		root, "storage", sessionSubdir, "global", sessionID+".json",
	)
	writeOpenCodeStorageFile(t, sessionPath, map[string]any{
		"id":        sessionID,
		"directory": filepath.Join("/home/user/code", project),
		"title":     title,
		"time": map[string]any{
			"created": int64(1700000000000),
			"updated": int64(1700000060000),
		},
	})
	writeOpenCodeStorageFile(t, filepath.Join(
		root, "storage", "message", sessionID, "msg_1.json",
	), map[string]any{
		"id":        "msg_1",
		"sessionID": sessionID,
		"role":      "user",
		"time": map[string]any{
			"created": int64(1700000000000),
		},
	})
	writeOpenCodeStorageFile(t, filepath.Join(
		root, "storage", "part", "msg_1", "prt_1.json",
	), map[string]any{
		"id":        "prt_1",
		"sessionID": sessionID,
		"messageID": "msg_1",
		"type":      "text",
		"text":      "Hello from storage",
		"time": map[string]any{
			"created": int64(1700000000000),
		},
	})
	return sessionPath
}

func newTestDBAt(
	t *testing.T,
	dbPath string,
) (string, *OpenCodeSeeder, *sql.DB) {
	t.Helper()
	copyOpenCodeSchemaTemplate(t, dbPath)
	db, err := sql.Open("sqlite3", dbPath)
	require.NoError(t, err, "open test db")
	return dbPath, &OpenCodeSeeder{db: db, t: t}, db
}
