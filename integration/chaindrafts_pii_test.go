//go:build chaindrafts

// Chain-drafts PII operations — Save/Upsert/Update/Exists/FetchBy* under the
// draft workflow, plus soft-delete and the hashed sidecar.

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"demo/models/user"
)

// ────────────────────────────────────────────────────────────────────────
// AutoIncrement / ChainIdentifierKey
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_AutoIncrement_IdAssigned(t *testing.T) {
	// Save is strict INSERT; the BIGSERIAL `id` should be populated on the
	// model after the call returns, same as OFF mode.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u1 := newDraftUser("auto_1")
	u2 := newDraftUser("auto_2")
	require.NoError(t, repo.Save(ctx, u1))
	require.NoError(t, repo.Save(ctx, u2))
	require.Equal(t, int64(1), u1.Id)
	require.Equal(t, int64(2), u2.Id, "BIGSERIAL should hand out sequential ids")
}

func TestChainDrafts_ChainIdentifierKey_IsUserId(t *testing.T) {
	// chain_users.key uses the chain_identifier_key field (user_id), not
	// the BIGSERIAL `id`. Stable across redeploys; same as OFF mode.
	resetTables(t)
	u := newDraftUser("chain_key_test")
	commitUser(t, u)

	var keys []string
	require.NoError(t, testDB.Table("chain_users").
		Distinct("key").
		Order("key").
		Pluck("key", &keys).Error)
	require.Equal(t, []string{u.UserId}, keys)
}

// ────────────────────────────────────────────────────────────────────────
// Strict-insert conflict surfaces
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Save_UniqueEmailViolation(t *testing.T) {
	// Save is strict INSERT on PII — a UNIQUE collision surfaces as a
	// driver error, before any chain rows get drafted. The first row's
	// drafts remain pending.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u1 := newDraftUser("dup_a")
	u2 := newDraftUser("dup_b")
	u2.Email = u1.Email // collide on UNIQUE(email)

	require.NoError(t, repo.Save(ctx, u1))
	err := repo.Save(ctx, u2)
	require.Error(t, err)
	lower := strings.ToLower(err.Error())
	require.True(t,
		strings.Contains(lower, "duplicate") || strings.Contains(lower, "unique"),
		"unexpected error: %v", err)
}

// ────────────────────────────────────────────────────────────────────────
// Hashed sidecar — drafted alongside the main field, committed together
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_HashedEmail_DraftedAndCommitted(t *testing.T) {
	// The hashed_<field> sidecar is staged DRAFTED alongside the main
	// chain row by DraftChain, then promoted by CommitChain in lockstep.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("hash_test")
	require.NoError(t, repo.Save(ctx, u))

	// Before commit: both email-related chain rows are DRAFTED.
	var statuses []string
	require.NoError(t, testDB.Raw(
		`SELECT status FROM chain_users WHERE key = ? AND field_name LIKE 'hashed%'`,
		u.UserId,
	).Scan(&statuses).Error)
	require.NotEmpty(t, statuses)
	for _, s := range statuses {
		require.Equal(t, "DRAFTED", s, "hashed sidecar must be DRAFTED pre-commit")
	}

	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	sum := sha256.Sum256([]byte(u.Email))
	expected := hex.EncodeToString(sum[:])

	view, err := repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.Equal(t, expected, view.HashedEmail,
		"committed hashed_email must surface on the view after CommitChain")
}

// ────────────────────────────────────────────────────────────────────────
// Audit fields on the PII row (created_at / updated_at / deleted_at / created_by)
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_AuditFields_Populated(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	before := time.Now().UTC().Add(-time.Second)
	u := newDraftUser("audit_fields")
	require.NoError(t, repo.Save(ctx, u))

	// Use overlay so chain fields don't bother us — we just want PII audit cols.
	view, err := repo.Fetch(ctx, u.Id, true)
	require.NoError(t, err)
	require.False(t, view.DeletedAt.Valid, "fresh row has NULL deleted_at")
	require.True(t, view.CreatedAt.After(before),
		"CreatedAt %v should be after %v", view.CreatedAt, before)
	require.WithinDuration(t, view.CreatedAt, view.UpdatedAt, time.Second)
}

// ────────────────────────────────────────────────────────────────────────
// FetchBy{Unique} with drafted flag
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_FetchByEmail_AfterCommit(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("fetchby_email")
	commitUser(t, u)

	view, err := repo.FetchByEmail(ctx, u.Email, false)
	require.NoError(t, err)
	require.Equal(t, u.Id, view.Id)
	require.Equal(t, u.UserId, view.UserId)
	require.False(t, view.HasPendingDrafts, "no pending drafts after commit")
}

func TestChainDrafts_FetchByPan_OnlyVisibleViaOverlayBeforeCommit(t *testing.T) {
	// `pan` is a chain field (not in PII). Before commit it lives only as
	// a DRAFTED row, so the committed view's c_pan join returns nothing.
	// FetchByPan against the committed view returns ErrRecordNotFound.
	// The overlay view sees the DRAFTED value and resolves the lookup.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("pan_via_overlay")
	require.NoError(t, repo.Save(ctx, u))

	// Committed view doesn't see drafted pan → no row matches.
	_, err := repo.FetchByPan(ctx, u.Pan, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"committed view shouldn't surface DRAFTED chain values")

	// Overlay view sees the DRAFTED pan → returns the row.
	view, err := repo.FetchByPan(ctx, u.Pan, true)
	require.NoError(t, err)
	require.Equal(t, u.Id, view.Id)
	require.True(t, view.HasPendingDrafts)
}

// ────────────────────────────────────────────────────────────────────────
// Exists semantics — PII-based; not draft-aware
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Exists_AfterSaveTrueBeforeCommit(t *testing.T) {
	// Exists checks the PII table, which is committed at Save time
	// regardless of chain status. Returns true immediately after Save.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("exists_immediate")
	require.NoError(t, repo.Save(ctx, u))

	exists, err := repo.Exists(ctx, u.Id)
	require.NoError(t, err)
	require.True(t, exists, "PII row exists immediately after Save")

	existsByEmail, err := repo.ExistsByEmail(ctx, u.Email)
	require.NoError(t, err)
	require.True(t, existsByEmail)
}

func TestChainDrafts_Exists_Missing(t *testing.T) {
	resetTables(t)
	exists, err := user.NewUserRepo(testDB).Exists(context.Background(), 999999)
	require.NoError(t, err)
	require.False(t, exists)
}

// ────────────────────────────────────────────────────────────────────────
// Fetch missing
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Fetch_Missing(t *testing.T) {
	resetTables(t)
	_, err := user.NewUserRepo(testDB).Fetch(context.Background(), 999999, false)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"missing row must return ErrRecordNotFound from committed view, got %v", err)

	_, err = user.NewUserRepo(testDB).Fetch(context.Background(), 999999, true)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"missing row must return ErrRecordNotFound from overlay view, got %v", err)
}

// ────────────────────────────────────────────────────────────────────────
// Soft-delete
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_SoftDelete_HidesFromFetch_BothViews(t *testing.T) {
	// deleted_at IS NULL filter applies to both views; a soft-deleted row
	// disappears from both committed and overlay Fetch.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("soft_delete_fetch")
	commitUser(t, u)

	require.NoError(t, testDB.Exec(
		`UPDATE pii_users SET deleted_at = NOW() WHERE id = ?`, u.Id,
	).Error)

	_, err := repo.Fetch(ctx, u.Id, false)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"committed view must hide soft-deleted rows, got %v", err)
	_, err = repo.Fetch(ctx, u.Id, true)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"overlay view must hide soft-deleted rows too, got %v", err)
}

func TestChainDrafts_SoftDelete_HidesFromFetchByEmail(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("soft_delete_email")
	commitUser(t, u)

	require.NoError(t, testDB.Exec(
		`UPDATE pii_users SET deleted_at = NOW() WHERE id = ?`, u.Id,
	).Error)

	_, err := repo.FetchByEmail(ctx, u.Email, false)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound))
}

func TestChainDrafts_SoftDelete_ExistsReturnsFalse(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("soft_delete_exists")
	commitUser(t, u)

	require.NoError(t, testDB.Exec(
		`UPDATE pii_users SET deleted_at = NOW() WHERE id = ?`, u.Id,
	).Error)

	exists, err := repo.Exists(ctx, u.Id)
	require.NoError(t, err)
	require.False(t, exists, "Exists must respect deleted_at filter")
}

func TestChainDrafts_SoftDelete_ViaGormDelete(t *testing.T) {
	// db.Delete on the PII struct (which carries gorm.DeletedAt) performs
	// the same soft-delete UPDATE as direct SQL.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("gorm_soft_delete")
	commitUser(t, u)

	require.NoError(t, testDB.Delete(&user.UserPii{Id: u.Id}).Error)

	_, err := repo.Fetch(ctx, u.Id, false)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound))
}

// ────────────────────────────────────────────────────────────────────────
// Schema invariants — status column, partial unique index, trigger exist
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Schema_StatusColumnAndConstraints(t *testing.T) {
	// Pin the DDL artifacts so a generator regression is caught at the
	// schema level (independent of any data-path test).
	var colExists int64
	require.NoError(t, testDB.Raw(
		`SELECT count(*) FROM information_schema.columns
		 WHERE table_name = 'chain_users' AND column_name = 'status'`,
	).Scan(&colExists).Error)
	require.Equal(t, int64(1), colExists, "chain_users.status must exist")

	var idxExists int64
	require.NoError(t, testDB.Raw(
		`SELECT count(*) FROM pg_indexes
		 WHERE tablename = 'chain_users' AND indexname = 'chain_users_one_draft'`,
	).Scan(&idxExists).Error)
	require.Equal(t, int64(1), idxExists, "partial unique index must exist")

	var triggerExists int64
	require.NoError(t, testDB.Raw(
		`SELECT count(*) FROM information_schema.triggers
		 WHERE event_object_table = 'chain_users'
		   AND trigger_name = 'chain_users_status_guard_trigger'`,
	).Scan(&triggerExists).Error)
	require.True(t, triggerExists > 0, "state-machine trigger must be attached")
}

func TestChainDrafts_Schema_CheckConstraintRejectsBogusStatus(t *testing.T) {
	// The CHECK constraint blocks INSERTs of unknown status values.
	resetTables(t)
	err := testDB.Exec(
		`INSERT INTO chain_users (key, field_name, version, field_value, status)
		 VALUES ('check_test', 'pan', 1, 'XYZ', 'WHATEVER')`,
	).Error
	require.Error(t, err, "CHECK constraint must reject unknown status")
}
