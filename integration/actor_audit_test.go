//go:build !noaudit && !chaindrafts

// Actor tests that depend on the audit table. Compiled by default; skip
// with `go test -tags noaudit` against an audit-off generation, or with
// `go test -tags chaindrafts` against a drafts-on generation.

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"demo/models/user"
)

// Companion to TestActor_CreatedBy_SurvivesUpsert (in actor_test.go): pins
// that the audit table captures the latest updater in the same scenario.
// Split out because the audit-off generation doesn't emit AuditLog.
func TestActor_AuditCapturesLastUpdater(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newUser("audit_upsert_actor")
	aliceCtx := user.WithActor(context.Background(), "alice")
	require.NoError(t, repo.SaveAll(aliceCtx, u, true))

	bobCtx := user.WithActor(context.Background(), "bob")
	u.Name = "Bob's Edit"
	require.NoError(t, repo.SaveAll(bobCtx, u, true))

	rows, err := repo.AuditLog(context.Background(), u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1, "exactly one UPDATE was logged by the trigger")
	require.Equal(t, "UPDATE", rows[0].ChangeType)
	require.Equal(t, "bob", rows[0].ChangedBy,
		"latest audit row identifies the most recent updater")
}
