//go:build chaindrafts

// Chain-drafts × audit interaction. The audit trigger is PII-table-level
// and fires regardless of chain status, so these tests pin: INSERT not
// logged, Upsert (which becomes an UPDATE on conflict) IS logged, the
// actor on ctx surfaces as changed_by, and soft/hard deletes still produce
// the expected audit rows.

package integration

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"demo/models/user"
)

// ────────────────────────────────────────────────────────────────────────
// INSERT path → no audit row
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Audit_Save_NoAuditRow(t *testing.T) {
	// Save is a strict INSERT on PII. The AFTER UPDATE/DELETE trigger
	// doesn't fire on INSERTs, so the audit table stays empty even after
	// the chained DraftChain step (chain writes don't touch the audit
	// trigger; that's PII-only).
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_insert")
	require.NoError(t, repo.Save(ctx, u))

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Empty(t, rows, "pure INSERT must not produce an audit row")
}

// ────────────────────────────────────────────────────────────────────────
// Upsert UPDATE path → audit row
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Audit_Upsert_OnConflict_LogsRow(t *testing.T) {
	// First Save inserts (no audit). Second Upsert hits the conflict path
	// and UPDATEs — trigger fires, audit row written with the OLD row's
	// snapshot.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_upsert")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	u.Email = "renamed@example.com"
	require.NoError(t, repo.Upsert(ctx, u))

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1, "Upsert that triggered ON CONFLICT DO UPDATE → 1 audit row")
	require.Equal(t, "UPDATE", rows[0].ChangeType)

	var snapshot map[string]any
	require.NoError(t, json.Unmarshal(rows[0].LastValue, &snapshot))
	require.Equal(t, "audit_upsert@example.com", snapshot["email"],
		"audit must capture the row BEFORE the update")
}

// ────────────────────────────────────────────────────────────────────────
// Actor on ctx → audit.changed_by
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Audit_WithActor_PopulatesChangedBy(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newDraftUser("audit_who")
	require.NoError(t, repo.Save(context.Background(), u))
	require.NoError(t, repo.CommitChain(context.Background(), u.UserId, ""))

	// Second Upsert with WithActor → trigger records "alice".
	ctx := user.WithActor(context.Background(), "alice@example.com")
	u.Name = "Renamed"
	require.NoError(t, repo.Upsert(ctx, u))

	rows, err := repo.AuditLog(context.Background(), u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "alice@example.com", rows[0].ChangedBy,
		"WithActor must propagate via SET LOCAL → trigger → changed_by")
}

func TestChainDrafts_Audit_NoActor_RecordsEmptyChangedBy(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_anon")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	u.Email = "renamed@anon.com"
	require.NoError(t, repo.Upsert(ctx, u))

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "", rows[0].ChangedBy)
}

// ────────────────────────────────────────────────────────────────────────
// Hard delete → audit row with change_type=DELETE
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Audit_HardDelete_LogsAsDelete(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_hard_del")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	require.NoError(t, testDB.Unscoped().Delete(&user.UserPii{Id: u.Id}).Error)

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "DELETE", rows[0].ChangeType)

	var snap map[string]any
	require.NoError(t, json.Unmarshal(rows[0].LastValue, &snap))
	require.Equal(t, "audit_hard_del", snap["user_id"])
}

// ────────────────────────────────────────────────────────────────────────
// Soft delete via gorm.DeletedAt → audit row as UPDATE
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Audit_SoftDelete_LogsAsUpdate(t *testing.T) {
	// GORM soft-delete UPDATE-sets deleted_at; trigger sees TG_OP='UPDATE'.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_soft_del")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	require.NoError(t, testDB.Delete(&user.UserPii{Id: u.Id}).Error)

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "UPDATE", rows[0].ChangeType,
		"GORM soft-delete is an UPDATE; trigger sees UPDATE")
}

// ────────────────────────────────────────────────────────────────────────
// Audit table + trigger schema invariants
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Audit_Schema_HasTriggerAndTable(t *testing.T) {
	// Independent of chain-drafts: the audit table + trigger come from
	// create-audit-tables: true. Pin them here to catch regressions when
	// regenerating in chaindrafts mode.
	var tableCount int64
	require.NoError(t, testDB.Raw(
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_name IN ('audit_pii_users', 'audit_pii_invoices')`,
	).Scan(&tableCount).Error)
	require.Equal(t, int64(2), tableCount)

	type triggerRow struct{ EventManipulation string }
	var triggers []triggerRow
	require.NoError(t, testDB.Raw(
		`SELECT event_manipulation FROM information_schema.triggers
		 WHERE trigger_name = 'audit_pii_users_log_trigger'`,
	).Scan(&triggers).Error)
	require.Len(t, triggers, 2, "trigger fires on both UPDATE and DELETE")
}
