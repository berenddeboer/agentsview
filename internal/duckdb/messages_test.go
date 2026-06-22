//go:build !(windows && arm64)

package duckdb

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGetAllMessagesSkipsNegativeCallIndex guards against a panic when the
// DuckDB mirror holds a tool_calls or tool_result_events row with a negative
// call_index (a corrupt or malformed mirror row). Such a row would skip the
// grow loop / pass the upper-bound check and index ToolCalls[-1], crashing
// message loading with "index out of range [-1]". The Postgres store already
// guards callIndex < 0; the DuckDB store must behave the same way.
func TestGetAllMessagesSkipsNegativeCallIndex(t *testing.T) {
	ctx := context.Background()
	store, fixture := newSyncedStore(t)

	// alpha message ordinal 1 has exactly one real tool call ("search").
	// Inject malformed rows with call_index = -1 for that same message.
	_, err := store.duck.ExecContext(ctx, `
		INSERT INTO tool_calls (
			id, message_id, session_id, tool_name, category,
			call_index, tool_use_id
		)
		SELECT 90001, m.id, m.session_id, 'bad', 'other', -1, 'bad-tool'
		FROM messages m
		WHERE m.session_id = ? AND m.ordinal = 1`, fixture.alphaID)
	require.NoError(t, err)

	_, err = store.duck.ExecContext(ctx, `
		INSERT INTO tool_result_events (
			id, session_id, tool_call_message_ordinal, call_index,
			source, status, content, content_length, event_index
		) VALUES (90002, ?, 1, -1, 'tool', 'complete', 'bad', 3, 0)`,
		fixture.alphaID)
	require.NoError(t, err)

	// Must not panic; the negative-index rows are simply skipped.
	msgs, err := store.GetAllMessages(ctx, fixture.alphaID)
	require.NoError(t, err)
	require.Len(t, msgs, 2)

	// The valid tool call and its result event are preserved intact.
	require.Len(t, msgs[1].ToolCalls, 1)
	assert.Equal(t, "search", msgs[1].ToolCalls[0].ToolName)
	require.Len(t, msgs[1].ToolCalls[0].ResultEvents, 1)
	assert.Equal(t, "duck result", msgs[1].ToolCalls[0].ResultEvents[0].Content)
}
