//go:build !noaudit && chaindrafts

// Chain-drafts × audit interaction. Tagged !noaudit because every test
// here calls AuditLog (which is only emitted when create-audit-tables is
// true). The audit trigger is PII-table-level and fires regardless of
// chain status, so these tests pin: INSERT not logged, Upsert (which
// becomes an UPDATE on conflict) IS logged, the actor on ctx surfaces as
// changed_by, and soft/hard deletes still produce the expected audit rows.

package integration

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"demo/models/invoice"
	"demo/models/user"
)

// ────────────────────────────────────────────────────────────────────────
// INSERT path → no audit row
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Audit_Create_NoAuditRow(t *testing.T) {
	// Create is a strict INSERT on PII. The AFTER UPDATE/DELETE trigger
	// doesn't fire on INSERTs, so the audit table stays empty even after
	// the chained DraftChain step (chain writes don't touch the audit
	// trigger; that's PII-only).
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_insert")
	require.NoError(t, repo.Create(ctx, u))

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Empty(t, rows, "pure INSERT must not produce an audit row")
}

// ────────────────────────────────────────────────────────────────────────
// Upsert UPDATE path → audit row
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Audit_Upsert_OnConflict_LogsRow(t *testing.T) {
	// First Create inserts (no audit). Second Upsert hits the conflict path
	// and UPDATEs — trigger fires, audit row written with the OLD row's
	// snapshot.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_upsert")
	require.NoError(t, repo.Create(ctx, u))
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
	require.NoError(t, repo.Create(context.Background(), u))
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
	require.NoError(t, repo.Create(ctx, u))
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
	require.NoError(t, repo.Create(ctx, u))
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
	require.NoError(t, repo.Create(ctx, u))
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

// ────────────────────────────────────────────────────────────────────────
// AuditLog ordering / string-PK / actor scoping / last_value bytes
// (mirror of the four OFF-mode audit tests with no ON equivalent)
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Audit_OrderIsChronological(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_order_cd")
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	for i, email := range []string{"a@a.com", "b@b.com", "c@c.com"} {
		u.Email = email
		require.NoError(t, repo.Upsert(ctx, u), "iteration %d", i)
		require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))
	}

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 3, "3 Upsert updates → 3 audit rows")

	// Each row's last_value is the email that WAS there before each update.
	wantEmails := []string{
		"audit_order_cd@example.com", // before first Upsert-update
		"a@a.com",                    // before second
		"b@b.com",                    // before third
	}
	for i, want := range wantEmails {
		var snap map[string]any
		require.NoError(t, json.Unmarshal(rows[i].LastValue, &snap))
		require.Equal(t, want, snap["email"],
			"row %d last_value.email", i)
	}

	// Strict non-decreasing timestamps.
	for i := 1; i < len(rows); i++ {
		require.False(t, rows[i].ChangedAt.Before(rows[i-1].ChangedAt),
			"audit rows must be chronological; row %d predates %d", i, i-1)
	}
}

func TestChainDrafts_Audit_Invoice_StringPK(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := invoice.WithActor(context.Background(), "bob")

	inv := newDraftInvoice("audit_inv_cd", sellerID, buyerID)
	inv.Amount = 100
	require.NoError(t, repo.Create(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	inv.Amount = 200
	require.NoError(t, repo.Upsert(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	rows, err := repo.AuditLog(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Len(t, rows, 1, "second Upsert on PII-backed Invoice → 1 audit row")
	require.Equal(t, "bob", rows[0].ChangedBy)
	require.Equal(t, "UPDATE", rows[0].ChangeType)
	require.Equal(t, inv.InvoiceId, rows[0].RefId,
		"ref_id stores the (text-cast) PK")
}

func TestChainDrafts_Audit_WithActor_ScopedToTransaction(t *testing.T) {
	// `sdm.actor` is set with is_local=true → transaction-scoped. A follow-up
	// Upsert in a fresh transaction without WithActor must record empty.
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newDraftUser("scoped_a_cd")
	require.NoError(t, repo.Create(
		user.WithActor(context.Background(), "alice"), u))
	require.NoError(t, repo.CommitChain(
		user.WithActor(context.Background(), "alice"), u.UserId, ""))

	// First Upsert was effectively the initial Create (INSERT → no audit).
	// Make a follow-up Upsert without WithActor to confirm the previous
	// transaction's session var didn't leak.
	u.Country = "DE"
	require.NoError(t, repo.Upsert(context.Background(), u))

	rows, err := repo.AuditLog(context.Background(), u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "", rows[0].ChangedBy,
		"SET LOCAL is transaction-scoped; bare ctx must record empty changed_by")
}

func TestChainDrafts_Audit_LastValue_IsJSONBytes(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_bytes_cd")
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	u.Name = "Bytes"
	require.NoError(t, repo.Upsert(ctx, u))

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.True(t, strings.HasPrefix(string(rows[0].LastValue), "{"),
		"last_value should be a JSONB object; got %q", string(rows[0].LastValue))
}

// ────────────────────────────────────────────────────────────────────────
// ON-only — Update / DraftChain / CommitChain / DropChain audit shape
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Audit_Update_LogsRow(t *testing.T) {
	// Update() is the strict-UPDATE path (no insert-on-miss). It writes
	// directly to pii_<name>s → trigger fires → audit row.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_update_cd")
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	u.Name = "Updated by Update()"
	require.NoError(t, repo.Update(ctx, u))

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1, "Update() must produce exactly one audit row")
	require.Equal(t, "UPDATE", rows[0].ChangeType)

	var snap map[string]any
	require.NoError(t, json.Unmarshal(rows[0].LastValue, &snap))
	require.Equal(t, "Name audit_update_cd", snap["name"],
		"audit must capture the row BEFORE Update()")
}

func TestChainDrafts_Audit_DraftChain_NoAuditOnPii(t *testing.T) {
	// DraftChain touches only chain_<name>s rows; no PII mutation, so no
	// audit_pii_* row should fire.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_draft_no_pii")
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	// Audit table is empty after the initial Create+Commit (INSERT-only path).
	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Empty(t, rows)

	// Now a pure DraftChain — should also not append an audit row.
	u.Country = "DE"
	require.NoError(t, repo.DraftChain(ctx, u))

	rows, err = repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Empty(t, rows, "DraftChain must not produce an audit row (chain-only mutation)")
}

func TestChainDrafts_Audit_CommitChain_NoAuditOnPii(t *testing.T) {
	// CommitChain mutates chain rows' status, not the PII row. The audit
	// trigger is PII-table-only, so no audit row.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_commit_no_pii")
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, "tx1"))

	// After Create (INSERT) + CommitChain (chain UPDATE) the audit_pii_users
	// table should still be empty.
	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Empty(t, rows, "CommitChain must not produce an audit row (chain-only mutation)")
}

func TestChainDrafts_Audit_DropChain_NoAuditOnPii(t *testing.T) {
	// DropChain mutates chain rows' status to DROPPED. Same as CommitChain —
	// no PII mutation → no audit row.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_drop_no_pii")
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.DropChain(ctx, u.UserId))

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Empty(t, rows, "DropChain must not produce an audit row (chain-only mutation)")
}

func TestChainDrafts_Audit_Upsert_AfterCreate_LogsSecondTx(t *testing.T) {
	// Create (INSERT — no audit) → CommitChain (chain UPDATE only — no audit),
	// then Upsert that changes a PII column → ON CONFLICT DO UPDATE → audit
	// row captures the pre-update PII snapshot. Validates the audit trigger
	// fires only on the conflict-UPDATE path, not the initial INSERT, and
	// not on CommitChain's chain-only mutation.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("audit_create_then_upsert")
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	// After Create+Commit (INSERT + chain UPDATE): no audit rows.
	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Empty(t, rows)

	// Second pass via Upsert with a changed PII field — ON CONFLICT DO UPDATE.
	u.Name = "Renamed"
	require.NoError(t, repo.Upsert(ctx, u))

	rows, err = repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1, "Upsert hit the UPDATE branch → 1 audit row")
	require.Equal(t, "UPDATE", rows[0].ChangeType)

	var snap map[string]any
	require.NoError(t, json.Unmarshal(rows[0].LastValue, &snap))
	require.Equal(t, "Name audit_create_then_upsert", snap["name"],
		"audit captures the row BEFORE the Upsert UPDATE")
}
