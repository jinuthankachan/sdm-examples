//go:build !noaudit && !chaindrafts

// Audit tests — only compile when audit tables are emitted (default) AND
// chain-drafts is OFF (default). Skip with `go test -tags noaudit` against
// an audit-off generation, or `go test -tags chaindrafts` against a drafts-on
// generation (whose repo surface differs).

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

// audit_pii_<name>s + the AFTER UPDATE/DELETE trigger are emitted alongside
// every PII table. These tests pin:
//   - INSERTs are NOT audited (audit table empty after pure inserts)
//   - UPDATEs (including soft-delete via gorm.DeletedAt) ARE audited as UPDATE
//   - hard DELETEs ARE audited as DELETE
//   - last_value is the OLD row as JSON (deleted_at = NULL before soft delete)
//   - changed_by reflects WithActor(ctx, actorID); empty when context is bare
//   - AuditLog repo method returns rows in chronological order

func TestAudit_PureInsert_NoAuditRow(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("audit_insert")
	require.NoError(t, repo.SaveAll(ctx, u, true))

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Empty(t, rows, "pure INSERT must not produce an audit row")

	// Sanity at the raw level too.
	var n int64
	require.NoError(t, testDB.Table("audit_pii_users").Count(&n).Error)
	require.Zero(t, n, "audit_pii_users empty after only-insert SaveAll")
}

func TestAudit_SaveAll_Update_LogsRow(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("audit_update")
	require.NoError(t, repo.SaveAll(ctx, u, true))

	u.Email = "renamed@example.com"
	require.NoError(t, repo.SaveAll(ctx, u, true))

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1, "second SaveAll triggered ON CONFLICT DO UPDATE → 1 audit row")

	r := rows[0]
	require.Equal(t, "UPDATE", r.ChangeType)
	require.Equal(t, "", r.ChangedBy, "no WithActor → empty changed_by")

	// last_value should be the OLD row — original email, deleted_at null.
	var snapshot map[string]any
	require.NoError(t, json.Unmarshal(r.LastValue, &snapshot))
	require.Equal(t, "audit_update@example.com", snapshot["email"],
		"last_value must capture the row BEFORE the update")
	require.Equal(t, "audit_update", snapshot["user_id"])
	require.Nil(t, snapshot["deleted_at"], "deleted_at was NULL before the change")
}

func TestAudit_WithActor_PopulatesUser(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newUser("audit_who")
	require.NoError(t, repo.SaveAll(context.Background(), u, true))

	// Second SaveAll with WithActor → trigger should record "alice".
	ctx := user.WithActor(context.Background(), "alice@example.com")
	u.Name = "Renamed"
	require.NoError(t, repo.SaveAll(ctx, u, true))

	rows, err := repo.AuditLog(context.Background(), u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "alice@example.com", rows[0].ChangedBy,
		"WithActor must propagate via SET LOCAL → trigger → changed_by")
}

func TestAudit_HardDelete_LogsAsDelete(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("audit_hard_del")
	require.NoError(t, repo.SaveAll(ctx, u, true))

	// Unscoped().Delete bypasses GORM's soft-delete scope → real SQL DELETE.
	require.NoError(t, testDB.Unscoped().Delete(&user.UserPii{Id: u.Id}).Error)

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "DELETE", rows[0].ChangeType)
	// last_value preserves the deleted row's snapshot.
	var snap map[string]any
	require.NoError(t, json.Unmarshal(rows[0].LastValue, &snap))
	require.Equal(t, "audit_hard_del", snap["user_id"])
}

func TestAudit_SoftDelete_LogsAsUpdate(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("audit_soft_del")
	require.NoError(t, repo.SaveAll(ctx, u, true))

	// gorm.DeletedAt makes this an UPDATE setting deleted_at = NOW(),
	// not a real DELETE — so TG_OP is 'UPDATE'.
	require.NoError(t, testDB.Delete(&user.UserPii{Id: u.Id}).Error)

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "UPDATE", rows[0].ChangeType,
		"GORM soft-delete is an UPDATE under the hood; trigger sees UPDATE")
	var snap map[string]any
	require.NoError(t, json.Unmarshal(rows[0].LastValue, &snap))
	require.Nil(t, snap["deleted_at"],
		"OLD row's deleted_at was NULL before the soft-delete UPDATE")
}

func TestAudit_OrderIsChronological(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("audit_order")
	require.NoError(t, repo.SaveAll(ctx, u, true))

	for i, email := range []string{"a@a.com", "b@b.com", "c@c.com"} {
		u.Email = email
		require.NoError(t, repo.SaveAll(ctx, u, true), "iteration %d", i)
	}

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 3, "3 SaveAll updates → 3 audit rows")

	// Each row's last_value is the email that WAS there before each update.
	wantEmails := []string{
		"audit_order@example.com", // before first SaveAll-update
		"a@a.com",                 // before second
		"b@b.com",                 // before third
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

func TestAudit_Invoice_StringPK(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := invoice.WithActor(context.Background(), "bob")

	inv := newInvoice("audit_inv", sellerID, buyerID)
	inv.Amount = 100
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	inv.Amount = 200
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	rows, err := repo.AuditLog(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Len(t, rows, 1, "second SaveAll on PII-backed Invoice → 1 audit row")
	require.Equal(t, "bob", rows[0].ChangedBy)
	require.Equal(t, "UPDATE", rows[0].ChangeType)
	require.Equal(t, inv.InvoiceId, rows[0].RefId,
		"ref_id stores the (text-cast) PK")
}

func TestAudit_Schema_HasTriggerAndTable(t *testing.T) {
	// Pin: the audit table exists and the trigger is attached AFTER UPDATE OR
	// DELETE. Catches regressions in the generated SQL DDL.
	var tableCount int64
	require.NoError(t, testDB.Raw(
		`SELECT count(*) FROM information_schema.tables
		 WHERE table_name IN ('audit_pii_users', 'audit_pii_invoices')`,
	).Scan(&tableCount).Error)
	require.Equal(t, int64(2), tableCount,
		"audit_pii_users and audit_pii_invoices must both exist")

	type triggerRow struct {
		EventManipulation string
	}
	var triggers []triggerRow
	require.NoError(t, testDB.Raw(
		`SELECT event_manipulation FROM information_schema.triggers
		 WHERE trigger_name = 'audit_pii_users_log_trigger'`,
	).Scan(&triggers).Error)
	require.Len(t, triggers, 2, "trigger fires on both UPDATE and DELETE")

	events := []string{triggers[0].EventManipulation, triggers[1].EventManipulation}
	require.Contains(t, events, "UPDATE")
	require.Contains(t, events, "DELETE")
}

func TestAudit_WithActor_ScopedToTransaction(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u1 := newUser("scoped_a")
	require.NoError(t, repo.SaveAll(
		user.WithActor(context.Background(), "alice"), u1, true,
	))

	// First SaveAll for u1 was an INSERT → no audit row. Make a follow-up
	// SaveAll WITHOUT WithActor to confirm the previous transaction's
	// session var didn't leak.
	u1.Country = "DE"
	require.NoError(t, repo.SaveAll(context.Background(), u1, true))

	rows, err := repo.AuditLog(context.Background(), u1.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "", rows[0].ChangedBy,
		"SET LOCAL is transaction-scoped; bare ctx must record empty changed_by")
}

// json.Unmarshal helper sanity — last_value is real JSONB bytes, not a string.
func TestAudit_LastValue_IsJSONBytes(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("audit_bytes")
	require.NoError(t, repo.SaveAll(ctx, u, true))
	u.Name = "Bytes"
	require.NoError(t, repo.SaveAll(ctx, u, true))

	rows, err := repo.AuditLog(ctx, u.Id)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.True(t, strings.HasPrefix(string(rows[0].LastValue), "{"),
		"last_value should be a JSONB object; got %q", string(rows[0].LastValue))
}
