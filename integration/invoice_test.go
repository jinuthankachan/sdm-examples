package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"demo/models/invoice"
	"demo/models/user"
)

// seedTwoUsers inserts the seller and buyer that invoice tests reference via
// foreign keys, then returns their user_id strings. invoice.SellerId/BuyerId
// reference pii_users.user_id (a unique TEXT column).
func seedTwoUsers(t *testing.T) (sellerID, buyerID string) {
	t.Helper()
	seller := newUser("seller")
	buyer := newUser("buyer")
	mustSaveUser(t, seller)
	mustSaveUser(t, buyer)
	return seller.UserId, buyer.UserId
}

func newInvoice(id, sellerID, buyerID string) *invoice.Invoice {
	return &invoice.Invoice{
		InvoiceId: id,
		SellerGst: "27AABCS1429B1Z" + id,
		BuyerGst:  "27AABCB2345B1Z" + id,
		SellerId:  sellerID,
		BuyerId:   buyerID,
		Amount:    10000,
		Metadata:  `{"source":"test","tags":["a","b"]}`,
		Price: &invoice.Money{
			Value: 10000,
			Unit:  "INR",
		},
	}
}

// minimalInvoice has the bare fields needed to satisfy FK + PK but no chain
// data — used by FK-violation tests that don't need to round-trip values.
func minimalInvoice(id, sellerID, buyerID string) *invoice.Invoice {
	return &invoice.Invoice{
		InvoiceId: id,
		SellerId:  sellerID,
		BuyerId:   buyerID,
		Price:     &invoice.Money{Value: 1, Unit: "INR"},
	}
}

func TestInvoice_Save_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv1", sellerID, buyerID)
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Equal(t, inv.InvoiceId, view.InvoiceId)
	require.Equal(t, inv.SellerGst, view.SellerGst)
	require.Equal(t, inv.BuyerGst, view.BuyerGst)
	require.Equal(t, inv.SellerId, view.SellerId)
	require.Equal(t, inv.BuyerId, view.BuyerId)
	require.Equal(t, inv.Amount, view.Amount)
}

func TestInvoice_HashedGsts_InView(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_hash", sellerID, buyerID)
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Equal(t, sha256Hex(inv.SellerGst), view.HashedSellerGst)
	require.Equal(t, sha256Hex(inv.BuyerGst), view.HashedBuyerGst)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func TestInvoice_PII_PriceMessage_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_price", sellerID, buyerID)
	inv.Price = &invoice.Money{Value: 12345, Unit: "USD"}
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.NotNil(t, view.Price, "protojson serializer should decode JSONB into *Money")
	require.Equal(t, int64(12345), view.Price.Value)
	require.Equal(t, "USD", view.Price.Unit)

	// Verify the underlying column is actually JSONB (not TEXT) in pii_invoices.
	var dataType string
	require.NoError(t, testDB.Raw(
		`SELECT data_type FROM information_schema.columns
		 WHERE table_name = 'pii_invoices' AND column_name = 'price'`,
	).Scan(&dataType).Error)
	require.Equal(t, "jsonb", dataType)
}

func TestInvoice_Chain_MetadataString_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	expected := `{"trace_id":"abc-123","retries":3}`
	inv := newInvoice("inv_meta", sellerID, buyerID)
	inv.Metadata = expected
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)

	// View column is c_metadata.field_value::jsonb. Postgres re-serializes JSONB
	// without whitespace, so byte-identical comparison would fail; instead, the
	// JSON should be semantically equivalent. require.JSONEq compares structures.
	require.NotEmpty(t, view.Metadata)
	require.JSONEq(t, expected, string(view.Metadata))

	// Sanity: the column should be readable as datatypes.JSON bytes.
	require.IsType(t, datatypes.JSON{}, view.Metadata)
}

func TestInvoice_FK_Valid(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_fk_ok", sellerID, buyerID)
	require.NoError(t, repo.Save(ctx, inv))
}

func TestInvoice_FK_Violation_SellerMissing(t *testing.T) {
	resetTables(t)
	// Only seed buyer; reference a seller that doesn't exist.
	buyer := newUser("buyer_only")
	mustSaveUser(t, buyer)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := minimalInvoice("inv_fk_bad", "user_does_not_exist", buyer.UserId)
	err := repo.Save(ctx, inv)
	require.Error(t, err, "save with non-existent seller_id should violate FK")
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
}

func TestInvoice_View_Combined(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_view", sellerID, buyerID)
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	// PII fields
	require.Equal(t, inv.SellerGst, view.SellerGst)
	require.Equal(t, inv.BuyerGst, view.BuyerGst)
	require.NotNil(t, view.Price)
	// Chain fields
	require.Equal(t, inv.SellerId, view.SellerId)
	require.Equal(t, inv.BuyerId, view.BuyerId)
	require.Equal(t, inv.Amount, view.Amount)
	// Hashed sidecar columns
	require.Equal(t, sha256Hex(inv.SellerGst), view.HashedSellerGst)
	require.Equal(t, sha256Hex(inv.BuyerGst), view.HashedBuyerGst)
}

func TestInvoice_OnConflict_Idempotent(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_idem", sellerID, buyerID)
	require.NoError(t, repo.Save(ctx, inv))

	var chainBefore int64
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ?", inv.InvoiceId).Count(&chainBefore).Error)

	// Second Save with same id: PII insert is a no-op (OnConflict DoNothing),
	// but the chain entries are appended unconditionally, so chain count grows.
	require.NoError(t, repo.Save(ctx, inv))

	var chainAfter int64
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ?", inv.InvoiceId).Count(&chainAfter).Error)
	require.Equal(t, chainBefore*2, chainAfter,
		"each Save appends a new chain version of every chain-stored field")

	// Fetch still works and returns the same logical values.
	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Equal(t, inv.Amount, view.Amount)
}

func TestInvoice_Fetch_Missing(t *testing.T) {
	resetTables(t)
	repo := invoice.NewInvoiceRepo(testDB)

	_, err := repo.Fetch(context.Background(), "ghost")
	require.Error(t, err)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"expected ErrRecordNotFound, got %v", err)
}

// Compile-time guard to make sure the user import isn't dropped by goimports.
var _ = (*user.User)(nil)
