# OpenCode Storage, From the AgentsView Perspective

How opencode stores sessions on disk, why most real installs are "hybrid",
and what agentsview actually reads. Companion to the provenance entry in
`session-format-sources.md`.

## Three generations, one data model

opencode has persisted sessions three ways. All three express the same
session → message → part model; only the medium changed.

1. **Per-project JSON tree** (oldest):
   `project/<dir>/storage/session/info/<sid>.json`. Migrated away by
   opencode itself; agentsview never sees this.
1. **Flat JSON tree** ("old style"): one JSON file per record under
   `~/.local/share/opencode/storage/`.
1. **SQLite** ("new style", current): everything in
   `~/.local/share/opencode/opencode.db` (WAL mode). Non-stable channels
   write `opencode-<channel>.db`, which agentsview does not detect.

## What "hybrid" means

Hybrid is not a mode opencode supports — it is a leftover. The JSON → SQLite
importer copied every record into the database but never deleted the JSON
files, and the importer itself was removed from opencode in June 2026. So:

- Installs predating the SQLite switch keep the full `storage/` JSON tree
  next to `opencode.db`, forever. Only `storage/session_diff/` still
  receives new writes; sessions, messages, and parts are SQLite-only now.
- Fresh installs have no `storage/session/` directory at all.

agentsview therefore sees three shapes of the same root:

| On disk                       | agentsview mode | Sessions come from        |
| ----------------------------- | --------------- | ------------------------- |
| `storage/session/` only       | storage         | JSON tree                 |
| `storage/session/` + `opencode.db` | hybrid (storage mode + DB) | union of both; JSON shadows same-ID DB rows |
| `opencode.db` only            | sqlite          | database                  |

The hybrid union is the expensive case: discovery must enumerate the whole
JSON tree *and* every `session` row, and the watcher must cover the whole
root recursively.

## Old style: flat JSON tree layout

```text
~/.local/share/opencode/storage/
  project/<projectID>.json
  session/<projectID>/<sessionID>.json
  message/<sessionID>/<messageID>.json
  part/<messageID>/<partID>.json          # sharded by message, not session
  todo/, session_share/, session_diff/    # ignored by agentsview
```

Reading one session means statting/reading its session file, every message
file, and every part file of every message.

## The data model (Xplain schema)

Both formats carry this model. Keys are prefixed ids (`ses_*`, `msg_*`,
`prt_*`, `evt_*`); ascending ids embed a millisecond timestamp. Unlisted
attributes are bases: `title`, `role`, `directory`, `worktree` are text;
`data` is a JSON blob; `time_*` are epoch milliseconds.

```text
database opencode.

type project (A30) = worktree, time_created, time_updated.
type session (A30) = project, parent, title, directory,
                     time_created, time_updated,
                     cost, tokens_input, tokens_output,
                     tokens_cache_read, tokens_cache_write.
type message (A30) = session, time_created, time_updated, data.
type part    (A30) = message, time_created, time_updated, data.
type event   (I10) = aggregate, seq, type, data.

end.
```

Notes:

- `part` also stores `session_id` directly (denormalized for indexing);
  semantically session is reachable via message.
- `message.data` holds the real message: `role`, `modelID`/`providerID`,
  `time{created,completed}`, `error`, `tokens{input,output,cache{read,write}}`.
  `part.data` holds `type` (`text`, `reasoning`, `tool`, `step-finish`, ...)
  and its payload. agentsview parses both blobs with gjson/`json.Unmarshal`;
  blobs can be tens of megabytes.
- `event` is an append-only journal (`rowid` is a natural cursor;
  `aggregate` is the session id; `type` like `message.part.updated.1`).
  It is the only table that records *what changed* — the session/message/
  part tables are projections of it.
- Session `cost` and token columns exist but agentsview recomputes tokens
  from message data and `step-finish` parts instead.

## Write behavior: why large archives hurt

opencode is event-sourced. Every persisted message/part update writes one
`event` row, upserts the projection row, and updates session token/cost
columns — each a WAL commit, so each a filesystem event on
`opencode.db-wal`. Streaming text deltas are in-memory only, but one
assistant turn still commits on the order of tens of events (part start/end,
tool state transitions, step boundaries).

For agentsview the consequences are:

- A single `opencode.db-wal` event carries no session id, so today it fans
  out to *every* session row (`SELECT id, time_updated FROM session`), and
  in hybrid roots additionally re-lists the entire JSON tree.
- Hybrid forces a recursive watch of the whole root (which also contains
  `log/`, `repos/`, caches); on large installs this exhausts the recursive
  watch budget, dropping opencode to the 2-minute unwatched poller, whose
  full reconcile enumerates the entire archive per pass. This is the #1208
  failure loop.
- The `event` journal is the only bounded change signal opencode offers:
  work proportional to new journal rows instead of total archive size. Any
  fix that does not read it ends up rediscovering the archive.
