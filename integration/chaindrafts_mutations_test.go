//go:build chaindrafts

// Chain-drafts mutation semantics — Upsert/Update behaviour, chain
// skip-if-unchanged across multiple commit cycles, ChangeLog history,
// AsBaseModel repeated-message round-trip, and chain schema invariants.
// Mirrors OFF-mode mutations_test.go.

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"demo/models/invoice"
	"demo/models/user"
)

func sha256HexMut(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// ─────────────────────────────────────────────────────────────────────────────
// Upsert / Update
// ─────────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Upsert_InsertsNewWithAutoIncrement(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("upsert_new")
	require.NoError(t, repo.Upsert(ctx, u))
	require.NotZero(t, u.Id, "Upsert must copy back the BIGSERIAL id on insert")
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	view, err := repo.FetchByUserId(ctx, u.UserId, false)
	require.NoError(t, err)
	require.Equal(t, u.Email, view.Email)
	require.Equal(t, u.Name, view.Name)
}

func TestChainDrafts_Upsert_UpdatesExistingPii(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("upsert_existing")
	require.NoError(t, repo.Upsert(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))
	originalID := u.Id

	// Mutate then upsert again — conflict on user_id triggers DO UPDATE.
	u.Email = "renamed@example.com"
	u.Name = "Renamed " + u.UserId
	require.NoError(t, repo.Upsert(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	view, err := repo.FetchByUserId(ctx, u.UserId, false)
	require.NoError(t, err)
	require.Equal(t, "renamed@example.com", view.Email)
	require.Equal(t, "Renamed upsert_existing", view.Name)
	require.Equal(t, originalID, view.Id, "Upsert must reuse the same PII row")
	require.True(t, view.UpdatedAt.After(view.CreatedAt),
		"updated_at (%v) must move past created_at (%v) on overwrite",
		view.UpdatedAt, view.CreatedAt)
}

func TestChainDrafts_Update_AppendsChainVersion(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("update_chain") // initial Country=IN
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	u.Country = "US"
	require.NoError(t, repo.Update(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	u.Country = "FR"
	require.NoError(t, repo.Update(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	// country: 3 distinct values → 3 CREATED versions.
	var rows []struct {
		Version    int64
		FieldValue string
		Status     string
	}
	require.NoError(t, testDB.Table("chain_users").
		Select("version, field_value, status").
		Where("key = ? AND field_name = ?", u.UserId, "country").
		Order("version").
		Scan(&rows).Error)
	require.Len(t, rows, 3)
	require.Equal(t, []string{"IN", "US", "FR"},
		[]string{rows[0].FieldValue, rows[1].FieldValue, rows[2].FieldValue})
	require.Equal(t, []int64{1, 2, 3},
		[]int64{rows[0].Version, rows[1].Version, rows[2].Version})
	for _, r := range rows {
		require.Equal(t, "CREATED", r.Status)
	}
}

func TestChainDrafts_Invoice_Upsert_UpdatesExisting(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("upsert_inv", sellerID, buyerID)
	inv.Amount = 100
	require.NoError(t, repo.Create(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	inv.Amount = 200
	inv.SellerGst = "27NEWGST1234B1Zupsert_inv"
	require.NoError(t, repo.Upsert(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Equal(t, int64(200), view.Amount)
	require.Equal(t, "27NEWGST1234B1Zupsert_inv", view.SellerGst,
		"PII column should reflect Upsert change")
}

// ─────────────────────────────────────────────────────────────────────────────
// Chain skip-if-unchanged across multiple commit cycles
// ─────────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Chain_SkipsUnchangedAcrossUpsertCycles(t *testing.T) {
	// Re-running Upsert+CommitChain with identical chain values must not
	// append new CREATED chain rows. The skip-if-unchanged guard compares
	// against the latest CREATED, so an unchanged second cycle produces
	// zero new chain rows.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("cycle_skip")
	require.NoError(t, repo.Upsert(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	// Two more identical Upsert+Commit cycles — should be no-ops on chain.
	require.NoError(t, repo.Upsert(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))
	require.NoError(t, repo.Upsert(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	var countryRows int64
	require.NoError(t, testDB.Table("chain_users").
		Where("key = ? AND field_name = ?", u.UserId, "country").
		Count(&countryRows).Error)
	require.Equal(t, int64(1), countryRows,
		"country unchanged across 3 Upsert+Commit cycles → still 1 chain row")

	var hashedEmailRows int64
	require.NoError(t, testDB.Table("chain_users").
		Where("key = ? AND field_name = ?", u.UserId, "hashed_email").
		Count(&hashedEmailRows).Error)
	require.Equal(t, int64(1), hashedEmailRows,
		"hashed_email unchanged → still 1 chain row")
}

func TestChainDrafts_Chain_OnlyChangedFieldsGetNewVersion(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("partial_cd")
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	// Change only country; pan + email stay the same.
	u.Country = "DE"
	require.NoError(t, repo.Update(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	count := func(field string) int64 {
		var n int64
		require.NoError(t, testDB.Table("chain_users").
			Where("key = ? AND field_name = ?", u.UserId, field).
			Count(&n).Error)
		return n
	}
	require.Equal(t, int64(2), count("country"), "country changed → v1 + v2")
	require.Equal(t, int64(1), count("pan"), "pan unchanged → still v1")
	require.Equal(t, int64(1), count("hashed_email"), "hashed_email unchanged → still v1")
}

func TestChainDrafts_Upsert_RepeatIsNoOp(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("inv_idem_cd", sellerID, buyerID)
	require.NoError(t, repo.Create(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	var chainBefore int64
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ?", inv.InvoiceId).Count(&chainBefore).Error)

	// Second Upsert+Commit with unchanged data: PII conflict triggers DO UPDATE
	// (touches updated_at) but every chain field is unchanged, so the
	// skip-if-unchanged guard means no new chain rows are appended.
	require.NoError(t, repo.Upsert(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	var chainAfter int64
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ?", inv.InvoiceId).Count(&chainAfter).Error)
	require.Equal(t, chainBefore, chainAfter,
		"unchanged Upsert+Commit must not append new chain versions")

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Equal(t, inv.Amount, view.Amount)
}

// ─────────────────────────────────────────────────────────────────────────────
// ChangeLog
// ─────────────────────────────────────────────────────────────────────────────

func TestChainDrafts_User_ChangeLog_TracksChangedFields(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("changelog_cd")
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	u.Email = "second@example.com"
	require.NoError(t, repo.Upsert(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	u.Email = "third@example.com"
	require.NoError(t, repo.Upsert(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	log, err := repo.ChangeLog(ctx, u.UserId)
	require.NoError(t, err)

	hashedEmail := log["hashed_email"]
	require.Len(t, hashedEmail, 3, "hashed_email should track all 3 email changes")
	require.Equal(t, sha256HexMut("changelog_cd@example.com"), hashedEmail[1].Value)
	require.Equal(t, sha256HexMut("second@example.com"), hashedEmail[2].Value)
	require.Equal(t, sha256HexMut("third@example.com"), hashedEmail[3].Value)

	// Timestamps strictly non-decreasing across versions.
	require.False(t, hashedEmail[2].Timestamp.Before(hashedEmail[1].Timestamp))
	require.False(t, hashedEmail[3].Timestamp.Before(hashedEmail[2].Timestamp))

	// Country / pan didn't change — skip-if-unchanged keeps them at v1 only.
	require.Len(t, log["country"], 1, "country unchanged → 1 version")
	require.Equal(t, "IN", log["country"][1].Value)
	require.Len(t, log["pan"], 1, "pan unchanged → 1 version")
}

func TestChainDrafts_Invoice_ChangeLog_TracksAmount(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("changelog_inv_cd", sellerID, buyerID)
	inv.Amount = 10
	require.NoError(t, repo.Create(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	inv.Amount = 20
	require.NoError(t, repo.Upsert(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	inv.Amount = 30
	require.NoError(t, repo.Upsert(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	log, err := repo.ChangeLog(ctx, inv.InvoiceId)
	require.NoError(t, err)

	amount := log["amount"]
	require.Len(t, amount, 3)
	require.Equal(t, "10", amount[1].Value)
	require.Equal(t, "20", amount[2].Value)
	require.Equal(t, "30", amount[3].Value)

	// Hashed sidecars present from the initial Create.
	require.Contains(t, log, "hashed_seller_gst")
}

func TestChainDrafts_Invoice_ChangeLog_TimestampsAreTZAware(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("changelog_ts_cd", sellerID, buyerID)
	require.NoError(t, repo.Create(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	log, err := repo.ChangeLog(ctx, inv.InvoiceId)
	require.NoError(t, err)

	amount := log["amount"]
	require.Len(t, amount, 1)
	now := time.Now()
	require.True(t, now.Sub(amount[1].Timestamp) < time.Minute,
		"ChangeLog timestamp (%v) should be within a minute of now (%v)",
		amount[1].Timestamp, now)
	require.True(t, amount[1].Timestamp.Before(now.Add(time.Minute)))
}

// ─────────────────────────────────────────────────────────────────────────────
// AsBaseModel with repeated messages
// ─────────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_AsBaseModel_RepeatedMessages(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	src := newDraftInvoice("asbase_items_cd", sellerID, buyerID)
	src.Items = []*invoice.Money{
		{Value: 1000, Unit: "INR"},
		{Value: 500, Unit: "INR"},
		{Value: 250, Unit: "USD"},
	}
	src.Tags = []string{"a", "b", "c"}
	require.NoError(t, repo.Create(ctx, src))
	require.NoError(t, repo.CommitChain(ctx, src.InvoiceId, ""))

	view, err := repo.Fetch(ctx, src.InvoiceId, false)
	require.NoError(t, err)

	got := view.AsBaseModel()
	require.Len(t, got.Items, 3)
	require.Equal(t, int64(1000), got.Items[0].Value)
	require.Equal(t, "INR", got.Items[0].Unit)
	require.Equal(t, int64(500), got.Items[1].Value)
	require.Equal(t, int64(250), got.Items[2].Value)
	require.Equal(t, "USD", got.Items[2].Unit)
	require.Equal(t, []string{"a", "b", "c"}, got.Tags)
}

// ─────────────────────────────────────────────────────────────────────────────
// Chain schema invariants
// ─────────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Chain_CreatedAt_IsTimestampTZ(t *testing.T) {
	for _, tbl := range []string{"chain_users", "chain_invoices"} {
		var dataType string
		err := testDB.Raw(
			`SELECT data_type FROM information_schema.columns
			 WHERE table_name = ? AND column_name = 'created_at'`,
			tbl,
		).Scan(&dataType).Error
		require.NoError(t, err)
		require.Equal(t, "timestamp with time zone", dataType,
			"%s.created_at should be timestamptz, got %q", tbl, dataType)
	}
}

func TestChainDrafts_Chain_PrimaryKey_IsKeyFieldNameVersion(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("pk_test_cd")
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	// Trying to insert a duplicate (key, field_name, version) — the
	// generated chain PK is the three-column composite, so this must
	// raise a unique/duplicate violation regardless of status.
	err := testDB.Exec(
		`INSERT INTO chain_users (key, field_name, version, field_value, status)
		 VALUES (?, 'country', 1, 'PIRATE', 'CREATED')`,
		u.UserId,
	).Error
	require.Error(t, err, "duplicate (key, field_name, version) must violate PK")
	require.True(t,
		strings.Contains(strings.ToLower(err.Error()), "duplicate") ||
			strings.Contains(strings.ToLower(err.Error()), "unique"),
		"expected duplicate/unique error, got %v", err)
}
