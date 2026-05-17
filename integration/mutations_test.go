//go:build !chaindrafts

// OFF-mode mutation tests; not compiled when -tags chaindrafts is in effect.

package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	"demo/models/invoice"
	"demo/models/user"
)

// ─────────────────────────────────────────────────────────────────────────────
// SaveAll
// ─────────────────────────────────────────────────────────────────────────────

func TestUser_SaveAll_InsertNew(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("saveall_new")
	require.NoError(t, repo.SaveAll(ctx, u, true))
	require.NotZero(t, u.Id, "SaveAll must copy back the BIGSERIAL id on insert")

	view, err := repo.FetchByUserId(ctx, u.UserId)
	require.NoError(t, err)
	require.Equal(t, u.Email, view.Email)
	require.Equal(t, u.Name, view.Name)
}

func TestUser_SaveAll_UpdatesExistingPii(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("saveall_existing")
	require.NoError(t, repo.SaveAll(ctx, u, true))
	originalID := u.Id

	// Mutate then save again — the conflict on user_id triggers DO UPDATE.
	u.Email = "renamed@example.com"
	u.Name = "Renamed " + u.UserId
	require.NoError(t, repo.SaveAll(ctx, u, true))

	view, err := repo.FetchByUserId(ctx, u.UserId)
	require.NoError(t, err)
	require.Equal(t, "renamed@example.com", view.Email)
	require.Equal(t, "Renamed saveall_existing", view.Name)
	require.Equal(t, originalID, view.Id, "SaveAll must reuse the same PII row")
	require.True(t, view.UpdatedAt.After(view.CreatedAt),
		"updated_at (%v) must move past created_at (%v) on overwrite",
		view.UpdatedAt, view.CreatedAt)
}

func TestUser_SaveAll_AppendsChainVersion(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("saveall_chain") // initial Country=IN
	require.NoError(t, repo.SaveAll(ctx, u, true))

	u.Country = "US"
	require.NoError(t, repo.SaveAll(ctx, u, true))

	u.Country = "FR"
	require.NoError(t, repo.SaveAll(ctx, u, true))

	// country: 3 distinct values → 3 versions.
	var rows []struct {
		Version    int64
		FieldValue string
	}
	require.NoError(t, testDB.Table("chain_users").
		Select("version, field_value").
		Where("key = ? AND field_name = ?", u.UserId, "country").
		Order("version").
		Scan(&rows).Error)
	require.Len(t, rows, 3)
	require.Equal(t, []string{"IN", "US", "FR"},
		[]string{rows[0].FieldValue, rows[1].FieldValue, rows[2].FieldValue})
	require.Equal(t, []int64{1, 2, 3},
		[]int64{rows[0].Version, rows[1].Version, rows[2].Version})
}

func TestUser_SaveAll_WithChainFalse_LeavesChainUntouched(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("saveall_no_chain")
	require.NoError(t, repo.SaveAll(ctx, u, false))

	// PII inserted …
	var piiCount int64
	require.NoError(t, testDB.Table("pii_users").Count(&piiCount).Error)
	require.Equal(t, int64(1), piiCount)

	// … but no chain rows.
	var chainCount int64
	require.NoError(t, testDB.Table("chain_users").Count(&chainCount).Error)
	require.Zero(t, chainCount, "withChain=false must not touch chain_users")
}

func TestInvoice_SaveAll_UpdatesExisting(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("saveall_inv", sellerID, buyerID)
	inv.Amount = 100
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	inv.Amount = 200
	inv.SellerGst = "27NEWGST1234B1Zsaveall_inv"
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Equal(t, int64(200), view.Amount)
	require.Equal(t, "27NEWGST1234B1Zsaveall_inv", view.SellerGst,
		"PII column should reflect SaveAll change")
}

func TestInvoice_SaveAll_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("saveall_rt", sellerID, buyerID)
	inv.Amount = 500
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	inv.Amount = 999
	inv.Price = &invoice.Money{Value: 99900, Unit: "INR"}
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Equal(t, int64(999), view.Amount)
	require.NotNil(t, view.Price)
	require.Equal(t, int64(99900), view.Price.Value)
}

// ─────────────────────────────────────────────────────────────────────────────
// Chain skip-if-unchanged
// ─────────────────────────────────────────────────────────────────────────────

func TestChain_SkipsUnchangedValue(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("skip_unchanged")
	require.NoError(t, repo.SaveAll(ctx, u, true))

	// Re-save identical model — every chain field is unchanged, so no new
	// versions should be appended.
	require.NoError(t, repo.SaveAll(ctx, u, true))
	require.NoError(t, repo.SaveAll(ctx, u, true))

	var countryRows int64
	require.NoError(t, testDB.Table("chain_users").
		Where("key = ? AND field_name = ?", u.UserId, "country").
		Count(&countryRows).Error)
	require.Equal(t, int64(1), countryRows,
		"country unchanged across 3 SaveAlls → still 1 chain row")

	var hashedEmailRows int64
	require.NoError(t, testDB.Table("chain_users").
		Where("key = ? AND field_name = ?", u.UserId, "hashed_email").
		Count(&hashedEmailRows).Error)
	require.Equal(t, int64(1), hashedEmailRows,
		"hashed_email unchanged → still 1 chain row")
}

func TestChain_OnlyChangedFieldsGetNewVersion(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("partial_change")
	require.NoError(t, repo.SaveAll(ctx, u, true))

	// Change only country; pan and email stay the same.
	u.Country = "DE"
	require.NoError(t, repo.SaveAll(ctx, u, true))

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

func TestChain_SaveAll_AlsoSkipsUnchanged(t *testing.T) {
	// Skip-if-unchanged also applies to repeated SaveAll calls with the
	// chain flag — repeated saves with unchanged values produce no extra
	// chain rows. (Equivalent of the old SaveChain-only test; SaveChain was
	// removed when chain-drafts became opt-in.)
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("savechain_skip")
	require.NoError(t, repo.SaveAll(ctx, u, true))
	require.NoError(t, repo.SaveAll(ctx, u, true))
	require.NoError(t, repo.SaveAll(ctx, u, true))

	var rowCount int64
	require.NoError(t, testDB.Table("chain_users").
		Where("key = ?", u.UserId).
		Count(&rowCount).Error)
	// User has 3 chain fields (hashed_email, pan, country) → 3 rows total
	// across all 3 SaveAll calls because subsequent calls are no-ops on the
	// chain side.
	require.Equal(t, int64(3), rowCount,
		"unchanged SaveAll calls must not append chain rows; got %d", rowCount)
}

// ─────────────────────────────────────────────────────────────────────────────
// ChangeLog
// ─────────────────────────────────────────────────────────────────────────────

func TestUser_ChangeLog_TracksChangedFields(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("changelog_user")
	require.NoError(t, repo.SaveAll(ctx, u, true))

	u.Email = "second@example.com"
	require.NoError(t, repo.SaveAll(ctx, u, true))

	u.Email = "third@example.com"
	require.NoError(t, repo.SaveAll(ctx, u, true))

	log, err := repo.ChangeLog(ctx, u.UserId)
	require.NoError(t, err)

	// Email changed 3 times → hashed_email tracks all 3 versions.
	hashedEmail := log["hashed_email"]
	require.Len(t, hashedEmail, 3, "hashed_email should track all 3 email changes")
	require.Equal(t, sha256Hex("changelog_user@example.com"), hashedEmail[1].Value)
	require.Equal(t, sha256Hex("second@example.com"), hashedEmail[2].Value)
	require.Equal(t, sha256Hex("third@example.com"), hashedEmail[3].Value)

	// Timestamps strictly non-decreasing across versions.
	require.False(t, hashedEmail[2].Timestamp.Before(hashedEmail[1].Timestamp),
		"v2 timestamp must not predate v1")
	require.False(t, hashedEmail[3].Timestamp.Before(hashedEmail[2].Timestamp))

	// Country / pan didn't change — skip-if-unchanged keeps them at v1 only.
	require.Len(t, log["country"], 1, "country unchanged → 1 version")
	require.Equal(t, "IN", log["country"][1].Value)
	require.Len(t, log["pan"], 1, "pan unchanged → 1 version")
}

func TestUser_ChangeLog_Missing_ReturnsErrNotFound(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	_, err := repo.ChangeLog(ctx, "does_not_exist")
	require.Error(t, err)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"expected ErrRecordNotFound, got %v", err)
}

func TestInvoice_ChangeLog_TracksAmount(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("changelog_inv", sellerID, buyerID)
	inv.Amount = 10
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	inv.Amount = 20
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	inv.Amount = 30
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	log, err := repo.ChangeLog(ctx, inv.InvoiceId)
	require.NoError(t, err)

	amount := log["amount"]
	require.Len(t, amount, 3)
	require.Equal(t, "10", amount[1].Value)
	require.Equal(t, "20", amount[2].Value)
	require.Equal(t, "30", amount[3].Value)

	// Hashed sidecars present from the initial SaveAll.
	require.Contains(t, log, "hashed_seller_gst")
}

func TestInvoice_ChangeLog_TimestampsAreTZAware(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("changelog_ts", sellerID, buyerID)
	require.NoError(t, repo.SaveAll(ctx, inv, true))

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
// View.AsBaseModel
// ─────────────────────────────────────────────────────────────────────────────

func TestUser_View_AsBaseModel(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	src := newUser("asbase_user")
	require.NoError(t, repo.SaveAll(ctx, src, true))

	view, err := repo.Fetch(ctx, src.Id)
	require.NoError(t, err)

	got := view.AsBaseModel()
	require.NotNil(t, got)
	require.Equal(t, src.UserId, got.UserId)
	require.Equal(t, src.Email, got.Email)
	require.Equal(t, src.Name, got.Name)
	require.Equal(t, src.Pan, got.Pan)
	require.Equal(t, src.Country, got.Country)
	require.Equal(t, src.Id, got.Id)
}

func TestInvoice_View_AsBaseModel_RoundTripScalarsAndTimestamp(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	src := newInvoice("asbase_inv", sellerID, buyerID)
	src.Amount = 7777
	src.TransferDate = timestamppb.New(time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC))
	require.NoError(t, repo.SaveAll(ctx, src, true))

	view, err := repo.Fetch(ctx, src.InvoiceId)
	require.NoError(t, err)

	got := view.AsBaseModel()
	require.Equal(t, src.InvoiceId, got.InvoiceId)
	require.Equal(t, src.SellerGst, got.SellerGst)
	require.Equal(t, src.BuyerGst, got.BuyerGst)
	require.Equal(t, src.SellerId, got.SellerId)
	require.Equal(t, src.BuyerId, got.BuyerId)
	require.Equal(t, int64(7777), got.Amount)
	// Metadata is stored as JSONB; Postgres normalizes whitespace + key order,
	// so the round-trip is JSON-equal but not byte-equal.
	require.JSONEq(t, src.Metadata, got.Metadata)
	require.NotNil(t, got.Price)
	require.Equal(t, src.Price.Value, got.Price.Value)
	require.NotNil(t, got.TransferDate)
	require.True(t, src.TransferDate.AsTime().Equal(got.TransferDate.AsTime()),
		"TransferDate must round-trip; got %v want %v",
		got.TransferDate.AsTime(), src.TransferDate.AsTime())
}

func TestInvoice_View_AsBaseModel_RepeatedMessages(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	src := newInvoice("asbase_items", sellerID, buyerID)
	src.Items = []*invoice.Money{
		{Value: 1000, Unit: "INR"},
		{Value: 500, Unit: "INR"},
		{Value: 250, Unit: "USD"},
	}
	src.Tags = []string{"a", "b", "c"}
	require.NoError(t, repo.SaveAll(ctx, src, true))

	view, err := repo.Fetch(ctx, src.InvoiceId)
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

func TestInvoice_View_AsBaseModel_SaveAllReplay(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	// Round-trip pattern: Save → Fetch → AsBaseModel → SaveAll (no-op via skip).
	src := newInvoice("asbase_replay", sellerID, buyerID)
	require.NoError(t, repo.SaveAll(ctx, src, true))

	view, err := repo.Fetch(ctx, src.InvoiceId)
	require.NoError(t, err)
	base := view.AsBaseModel()

	require.NoError(t, repo.SaveAll(ctx, base, true))

	// Chain rows should still be exactly one per field — replay was a no-op
	// because every value matches what's already in chain.
	var amountRows int64
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ?", src.InvoiceId, "amount").
		Count(&amountRows).Error)
	require.Equal(t, int64(1), amountRows,
		"replay via AsBaseModel → SaveAll must not append new chain versions")
}

// ─────────────────────────────────────────────────────────────────────────────
// Chain schema invariants
// ─────────────────────────────────────────────────────────────────────────────

func TestChain_CreatedAt_IsTimestampTZ(t *testing.T) {
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

func TestChain_PrimaryKey_IsKeyFieldNameVersion(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newUser("pk_test")
	require.NoError(t, repo.SaveAll(ctx, u, true))

	err := testDB.Exec(
		`INSERT INTO chain_users (key, field_name, version, field_value)
		 VALUES (?, 'country', 1, 'PIRATE')`,
		u.UserId,
	).Error
	require.Error(t, err, "duplicate (key, field_name, version) must violate PK")
	require.True(t,
		strings.Contains(strings.ToLower(err.Error()), "duplicate") ||
			strings.Contains(strings.ToLower(err.Error()), "unique"),
		"expected duplicate/unique error, got %v", err)
}

// ─────────────────────────────────────────────────────────────────────────────
// PII-stored repeated fields
// ─────────────────────────────────────────────────────────────────────────────

func TestInvoice_PII_RepeatedString_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("pii_strs", sellerID, buyerID)
	inv.PiiTags = []string{"alpha", "beta", "gamma"}
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	// Read back via view → []string preserved.
	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Equal(t, []string{"alpha", "beta", "gamma"}, []string(view.PiiTags))

	// Stored as text[] in pii_invoices, not in chain at all.
	var rawTags []string
	require.NoError(t, testDB.Raw(
		`SELECT pii_tags FROM pii_invoices WHERE invoice_id = ?`, inv.InvoiceId,
	).Row().Scan(pq.Array(&rawTags)))
	require.Equal(t, []string{"alpha", "beta", "gamma"}, rawTags)

	var chainTagsCount int64
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ?", inv.InvoiceId, "pii_tags").
		Count(&chainTagsCount).Error)
	require.Zero(t, chainTagsCount, "PII repeated string must NOT be chain-stored")

	// information_schema should confirm text[] column type.
	var colType string
	require.NoError(t, testDB.Raw(
		`SELECT data_type FROM information_schema.columns
		 WHERE table_name = 'pii_invoices' AND column_name = 'pii_tags'`,
	).Scan(&colType).Error)
	require.Equal(t, "ARRAY", colType, "pii_tags should be a Postgres array column")
}

func TestInvoice_PII_RepeatedString_Empty(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("pii_strs_empty", sellerID, buyerID)
	require.Nil(t, inv.PiiTags)
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Empty(t, view.PiiTags, "nil PiiTags must round-trip empty")
}

func TestInvoice_PII_RepeatedMessage_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("pii_msgs", sellerID, buyerID)
	inv.PiiItems = []*invoice.Money{
		{Value: 200, Unit: "USD"},
		{Value: 75, Unit: "INR"},
	}
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Len(t, view.PiiItems, 2)
	require.Equal(t, int64(200), view.PiiItems[0].Value)
	require.Equal(t, "USD", view.PiiItems[0].Unit)
	require.Equal(t, int64(75), view.PiiItems[1].Value)
	require.Equal(t, "INR", view.PiiItems[1].Unit)

	// Stored as jsonb in pii_invoices.
	var colType string
	require.NoError(t, testDB.Raw(
		`SELECT data_type FROM information_schema.columns
		 WHERE table_name = 'pii_invoices' AND column_name = 'pii_items'`,
	).Scan(&colType).Error)
	require.Equal(t, "jsonb", colType, "pii_items should be jsonb")

	// Not in chain.
	var chainCount int64
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ?", inv.InvoiceId, "pii_items").
		Count(&chainCount).Error)
	require.Zero(t, chainCount, "PII repeated message must NOT be chain-stored")
}

func TestInvoice_PII_RepeatedMessage_Empty(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("pii_msgs_empty", sellerID, buyerID)
	require.Nil(t, inv.PiiItems)
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Empty(t, view.PiiItems, "nil PiiItems must round-trip empty")
}

func TestInvoice_PII_RepeatedMessage_UpsertOverwrites(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("pii_msgs_overwrite", sellerID, buyerID)
	inv.PiiItems = []*invoice.Money{{Value: 1, Unit: "EUR"}}
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	// SaveAll with a different PiiItems → PII update column list includes
	// pii_items, so the new value overwrites the row.
	inv.PiiItems = []*invoice.Money{
		{Value: 10, Unit: "GBP"},
		{Value: 20, Unit: "GBP"},
	}
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Len(t, view.PiiItems, 2)
	require.Equal(t, "GBP", view.PiiItems[0].Unit)
	require.Equal(t, int64(20), view.PiiItems[1].Value)
}

// sha256Hex is defined in invoice_test.go.
