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

func newUser(userID string) *user.User {
	return &user.User{
		UserId:  userID,
		Email:   userID + "@example.com",
		Name:    "Name " + userID,
		Pan:     "PAN" + userID,
		Country: "IN",
	}
}

func TestUser_Save_RoundTrip(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("u1")
	require.NoError(t, repo.Save(ctx, u))
	require.NotZero(t, u.Id, "auto_increment should populate Id")

	view, err := repo.Fetch(ctx, u.Id)
	require.NoError(t, err)
	require.Equal(t, u.Id, view.Id)
	require.Equal(t, u.UserId, view.UserId)
	require.Equal(t, u.Email, view.Email)
	require.Equal(t, u.Name, view.Name)
	require.Equal(t, u.Pan, view.Pan)
	require.Equal(t, u.Country, view.Country)
}

func TestUser_SavePii_Only(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("u_pii_only")
	require.NoError(t, repo.SavePii(ctx, u))

	var piiCount, chainCount int64
	require.NoError(t, testDB.Table("pii_users").Count(&piiCount).Error)
	require.NoError(t, testDB.Table("chain_users").Count(&chainCount).Error)
	require.Equal(t, int64(1), piiCount)
	require.Equal(t, int64(0), chainCount, "SavePii must not touch chain table")
}

func TestUser_SaveChain_Only(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("u_chain_only")
	require.NoError(t, repo.SaveChain(ctx, u))

	var piiCount, chainCount int64
	require.NoError(t, testDB.Table("pii_users").Count(&piiCount).Error)
	require.NoError(t, testDB.Table("chain_users").Count(&chainCount).Error)
	require.Equal(t, int64(0), piiCount, "SaveChain must not touch PII table")
	// chain_users gets one row per non-PK, non-PII field plus one row per hashed
	// field: hashed_email, pan, country → 3 rows for user.
	require.Equal(t, int64(3), chainCount)
}

func TestUser_AutoIncrement_IdAssigned(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u1 := newUser("auto_1")
	u2 := newUser("auto_2")
	require.NoError(t, repo.Save(ctx, u1))
	require.NoError(t, repo.Save(ctx, u2))
	require.Equal(t, int64(1), u1.Id)
	require.Equal(t, int64(2), u2.Id, "BIGSERIAL should hand out sequential ids")
}

func TestUser_ChainIdentifierKey_IsUserId(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("chain_key_test")
	require.NoError(t, repo.Save(ctx, u))

	// chain_users.key should be the user_id (chain_identifier_key), not the
	// numeric BIGSERIAL id. This keeps the chain key stable across deployments
	// where the BIGSERIAL might restart.
	var keys []string
	require.NoError(t, testDB.Table("chain_users").
		Distinct("key").
		Order("key").
		Pluck("key", &keys).Error)
	require.Equal(t, []string{u.UserId}, keys)
}

func TestUser_HashedEmail_Stored(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("hash_test")
	require.NoError(t, repo.Save(ctx, u))

	sum := sha256.Sum256([]byte(u.Email))
	expected := hex.EncodeToString(sum[:])

	var stored string
	require.NoError(t, testDB.Table("chain_users").
		Where("key = ? AND field_name = ?", u.UserId, "hashed_email").
		Select("field_value").
		Scan(&stored).Error)
	require.Equal(t, expected, stored)

	// And the view should expose the same value as HashedEmail.
	view, err := repo.Fetch(ctx, u.Id)
	require.NoError(t, err)
	require.Equal(t, expected, view.HashedEmail)
}

func TestUser_UniqueEmail_Violation_SavePii(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u1 := newUser("dup_a")
	u2 := newUser("dup_b")
	u2.Email = u1.Email // force conflict on the UNIQUE constraint

	// SavePii does not use ON CONFLICT, so a UNIQUE collision propagates as an error.
	require.NoError(t, repo.SavePii(ctx, u1))
	err := repo.SavePii(ctx, u2)
	require.Error(t, err, "SavePii with duplicate email should violate UNIQUE")
	lower := strings.ToLower(err.Error())
	require.True(t,
		strings.Contains(lower, "duplicate") || strings.Contains(lower, "unique"),
		"unexpected error: %v", err)
}

func TestUser_Save_IdempotentOnDuplicateEmail(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u1 := newUser("save_dup_a")
	u2 := newUser("save_dup_b")
	u2.Email = u1.Email // would violate UNIQUE on a raw insert

	// Save wraps the PII insert in ON CONFLICT DO NOTHING. PG applies that to
	// any unique constraint violation, so the second Save is a silent no-op
	// for the PII row. Chain rows, however, are still appended.
	require.NoError(t, repo.Save(ctx, u1))
	require.NoError(t, repo.Save(ctx, u2),
		"Save must absorb the unique-constraint conflict via OnConflict DoNothing")

	// pii_users should still have one row — the first save wins.
	var piiCount int64
	require.NoError(t, testDB.Table("pii_users").Count(&piiCount).Error)
	require.Equal(t, int64(1), piiCount)

	// View should reflect u1, not u2.
	view, err := repo.FetchByEmail(ctx, u1.Email)
	require.NoError(t, err)
	require.Equal(t, u1.UserId, view.UserId)
}

func TestUser_FetchByEmail_RoundTrip(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("by_email")
	require.NoError(t, repo.Save(ctx, u))

	view, err := repo.FetchByEmail(ctx, u.Email)
	require.NoError(t, err)
	require.Equal(t, u.UserId, view.UserId)
	require.Equal(t, u.Email, view.Email)
}

func TestUser_FetchByPan_RoundTrip(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("by_pan")
	require.NoError(t, repo.Save(ctx, u))

	// pan is unique + chain-only. The view sources pan from the chain table,
	// so FetchByPan should still return the latest value.
	view, err := repo.FetchByPan(ctx, u.Pan)
	require.NoError(t, err)
	require.Equal(t, u.UserId, view.UserId)
	require.Equal(t, u.Pan, view.Pan)
}

func TestUser_ExistsByEmail(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("exists_email")
	require.NoError(t, repo.Save(ctx, u))

	got, err := repo.ExistsByEmail(ctx, u.Email)
	require.NoError(t, err)
	require.True(t, got)

	got, err = repo.ExistsByEmail(ctx, "nobody@example.com")
	require.NoError(t, err)
	require.False(t, got)
}

func TestUser_Fetch_Missing(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	_, err := repo.Fetch(context.Background(), 999999)
	require.Error(t, err)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"expected ErrRecordNotFound, got %v", err)
}

func TestUser_Exists_Missing(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	got, err := repo.Exists(context.Background(), 999999)
	require.NoError(t, err)
	require.False(t, got)
}

// TestUser_ExistsByPan_KnownIssue documents that ExistsByPan currently SQL-errors
// because pan is not in pii_users — see the suite's "Known SDM quirks" note.
// The test pins the current behavior so a future fix is detected as a change.
func TestUser_ExistsByPan_KnownIssue(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("pan_quirk")
	require.NoError(t, repo.Save(ctx, u))

	_, err := repo.ExistsByPan(ctx, u.Pan)
	require.Error(t, err, "ExistsByPan should fail until pan is in PII table")
	require.Contains(t, strings.ToLower(err.Error()), "pan")
}

func TestUser_AuditFields_Populated(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	before := time.Now().UTC().Add(-time.Second)
	u := newUser("audit_fields")
	require.NoError(t, repo.Save(ctx, u))

	view, err := repo.Fetch(ctx, u.Id)
	require.NoError(t, err)
	require.False(t, view.DeletedAt.Valid,
		"a freshly inserted row has NULL deleted_at; gorm.DeletedAt.Valid should be false")
	require.True(t, view.CreatedAt.After(before),
		"CreatedAt %v should be after %v", view.CreatedAt, before)
	// On INSERT, GORM sets both timestamps to the same now(); ON CONFLICT DO
	// NOTHING means no UPDATE ever fires from generated code, so UpdatedAt
	// equals CreatedAt for the lifetime of a row today.
	require.WithinDuration(t, view.CreatedAt, view.UpdatedAt, time.Second)
}

func TestUser_SoftDelete_HidesFromFetch(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("soft_delete_fetch")
	require.NoError(t, repo.Save(ctx, u))

	require.NoError(t, testDB.Exec(
		`UPDATE pii_users SET deleted_at = NOW() WHERE id = ?`, u.Id,
	).Error)

	_, err := repo.Fetch(ctx, u.Id)
	require.Error(t, err)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"soft-deleted row must be invisible to Fetch, got %v", err)
}

func TestUser_SoftDelete_HidesFromFetchByEmail(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("soft_delete_email")
	require.NoError(t, repo.Save(ctx, u))

	require.NoError(t, testDB.Exec(
		`UPDATE pii_users SET deleted_at = NOW() WHERE id = ?`, u.Id,
	).Error)

	_, err := repo.FetchByEmail(ctx, u.Email)
	require.Error(t, err)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound))
}

func TestUser_SoftDelete_ExistsReturnsFalse(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("soft_delete_exists")
	require.NoError(t, repo.Save(ctx, u))

	require.NoError(t, testDB.Exec(
		`UPDATE pii_users SET deleted_at = NOW() WHERE id = ?`, u.Id,
	).Error)

	got, err := repo.Exists(ctx, u.Id)
	require.NoError(t, err)
	require.False(t, got, "Exists must return false for soft-deleted row")

	got, err = repo.ExistsByEmail(ctx, u.Email)
	require.NoError(t, err)
	require.False(t, got, "ExistsByEmail must return false for soft-deleted row")
}

func TestUser_SoftDelete_ViaGormDelete(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("gorm_delete")
	require.NoError(t, repo.Save(ctx, u))

	// GORM's `.Delete()` on a model with gorm.DeletedAt soft-deletes (sets
	// deleted_at = NOW()) instead of issuing a real DELETE. This proves the
	// PII struct's DeletedAt field is wired correctly for the GORM API path.
	require.NoError(t, testDB.Delete(&user.UserPii{Id: u.Id}).Error)

	// The row should still exist on disk (raw count with .Unscoped).
	var rawCount int64
	require.NoError(t, testDB.Unscoped().Model(&user.UserPii{}).
		Where("id = ?", u.Id).Count(&rawCount).Error)
	require.Equal(t, int64(1), rawCount, "soft delete must keep the row physically")

	// But the generated read methods should hide it.
	_, err := repo.Fetch(ctx, u.Id)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"Fetch must not return a soft-deleted row, got %v", err)
	got, err := repo.Exists(ctx, u.Id)
	require.NoError(t, err)
	require.False(t, got)
}

func TestTimestamps_AreTimestampTZ(t *testing.T) {
	resetTables(t)
	// Pin that the generator emits TIMESTAMP WITH TIME ZONE for all audit
	// columns. Plain TIMESTAMP loses offset information and silently shifts
	// values across host/server tz drift.
	for _, tbl := range []string{"pii_users", "pii_invoices"} {
		for _, col := range []string{"created_at", "updated_at", "deleted_at"} {
			var dataType string
			err := testDB.Raw(
				`SELECT data_type FROM information_schema.columns
				 WHERE table_name = ? AND column_name = ?`,
				tbl, col,
			).Scan(&dataType).Error
			require.NoError(t, err)
			require.Equal(t, "timestamp with time zone", dataType,
				"%s.%s should be timestamptz, got %q", tbl, col, dataType)
		}
	}
}
