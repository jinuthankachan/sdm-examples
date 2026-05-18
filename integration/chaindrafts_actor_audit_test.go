//go:build !noaudit && chaindrafts

// Actor tests under chain-drafts that depend on the audit table.
// Companion to chaindrafts_actor_test.go (which covers actor flow into
// PII / chain). Compiled by default; skip with `go test -tags noaudit`
// against an audit-off generation.

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"demo/models/user"
)

// Companion to TestChainDrafts_Actor_CreatedBy_SurvivesUpsert (in
// chaindrafts_actor_test.go): pins that the audit table captures the
// latest updater in the same scenario. Split out because the audit-off
// generation doesn't emit AuditLog.
func TestChainDrafts_Actor_AuditCapturesLastUpdater(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newDraftUser("audit_upsert_actor_cd")
	aliceCtx := user.WithActor(context.Background(), "alice")
	require.NoError(t, repo.Create(aliceCtx, u))
	require.NoError(t, repo.CommitChain(aliceCtx, u.UserId, ""))

	bobCtx := user.WithActor(context.Background(), "bob")
	u.Name = "Bob's Edit"
	require.NoError(t, repo.Upsert(bobCtx, u))

	rows, err := repo.AuditLog(context.Background(), u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1, "exactly one UPDATE was logged by the trigger")
	require.Equal(t, "UPDATE", rows[0].ChangeType)
	require.Equal(t, "bob", rows[0].ChangedBy,
		"latest audit row identifies the most recent updater")
}
