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
	"google.golang.org/protobuf/types/known/timestamppb"
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

func TestInvoice_AuditFields_Populated(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	before := time.Now().UTC().Add(-time.Second)
	inv := newInvoice("inv_audit", sellerID, buyerID)
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.False(t, view.DeletedAt.Valid,
		"a freshly inserted row has NULL deleted_at; gorm.DeletedAt.Valid should be false")
	require.True(t, view.CreatedAt.After(before),
		"CreatedAt %v should be after %v", view.CreatedAt, before)
	require.WithinDuration(t, view.CreatedAt, view.UpdatedAt, time.Second)
}

func TestInvoice_SoftDelete_HidesFromFetch(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_soft_delete", sellerID, buyerID)
	require.NoError(t, repo.Save(ctx, inv))

	require.NoError(t, testDB.Exec(
		`UPDATE pii_invoices SET deleted_at = NOW() WHERE invoice_id = ?`, inv.InvoiceId,
	).Error)

	_, err := repo.Fetch(ctx, inv.InvoiceId)
	require.Error(t, err)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"soft-deleted invoice must be invisible to Fetch, got %v", err)

	got, err := repo.Exists(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.False(t, got, "Exists must return false for soft-deleted invoice")
}

// ── Repeated proto field (pg array) support ──────────────────────────────────
// These exercise the pq.StringArray view field + pgArrayLiteral chain writer
// for `repeated string tags = 9` on Invoice. The view does NOT cast the chain's
// TEXT field_value to text[]; pq.StringArray.Scan parses the {a,b,c} literal
// from raw text regardless, which is what makes the round-trip work today.

func TestInvoice_RepeatedField_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_tags", sellerID, buyerID)
	inv.Tags = []string{"urgent", "paid", "Q4"}
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Equal(t, []string{"urgent", "paid", "Q4"}, []string(view.Tags))
}

func TestInvoice_RepeatedField_SingleElement(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_tags_one", sellerID, buyerID)
	inv.Tags = []string{"solo"}
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Equal(t, []string{"solo"}, []string(view.Tags))
}

func TestInvoice_RepeatedField_Empty(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_tags_empty", sellerID, buyerID)
	inv.Tags = []string{}
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Empty(t, view.Tags, "empty slice should round-trip as an empty array")

	// Confirm the chain row stores the literal `{}`.
	var stored string
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ?", inv.InvoiceId, "tags").
		Select("field_value").Scan(&stored).Error)
	require.Equal(t, "{}", stored)
}

func TestInvoice_RepeatedField_Nil(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	// Tags left at the proto zero-value (nil). pgArrayLiteral(nil) yields "{}"
	// — same wire format as an empty slice — so Fetch returns an empty array.
	inv := newInvoice("inv_tags_nil", sellerID, buyerID)
	require.Nil(t, inv.Tags)
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Empty(t, view.Tags)
}

func TestInvoice_RepeatedField_ChainStorage(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_tags_chain", sellerID, buyerID)
	inv.Tags = []string{"a", "b", "c"}
	require.NoError(t, repo.Save(ctx, inv))

	// pgArrayLiteral emits "{a,b,c}" — the Postgres array text literal.
	// Asserted directly against chain_invoices so any change to that helper
	// (escaping, quoting) is caught here.
	var stored string
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ?", inv.InvoiceId, "tags").
		Select("field_value").Scan(&stored).Error)
	require.Equal(t, "{a,b,c}", stored)
}

// ── Repeated MessageType (chain-stored JSON array) support ───────────────────
// These exercise the `repeated Money items = 10` field added to invoice.proto.
// View column is datatypes.JSON (raw JSON-array bytes); chain rows store the
// same bytes pre-marshaled element-wise via protojson, then string-joined.

func TestInvoice_RepeatedMessage_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_items", sellerID, buyerID)
	inv.Items = []*invoice.Money{
		{Value: 100, Unit: "USD"},
		{Value: 50, Unit: "INR"},
	}
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.NotEmpty(t, view.Items)
	// protojson renders int64 as string. Compare semantically.
	expected := `[{"value":"100","unit":"USD"},{"value":"50","unit":"INR"}]`
	require.JSONEq(t, expected, string(view.Items))
}

func TestInvoice_RepeatedMessage_Empty(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	// Items at zero-value (nil). The generator initialises the local JSON var
	// to the empty array literal "[]", so the chain row's field_value is a
	// valid JSON array and the view's ::jsonb cast succeeds.
	inv := newInvoice("inv_items_empty", sellerID, buyerID)
	require.Nil(t, inv.Items)
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.JSONEq(t, "[]", string(view.Items),
		"empty Items must round-trip as the JSON empty array")
}

func TestInvoice_RepeatedMessage_ChainStorage(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_items_chain", sellerID, buyerID)
	inv.Items = []*invoice.Money{
		{Value: 1, Unit: "EUR"},
		{Value: 2, Unit: "GBP"},
	}
	require.NoError(t, repo.Save(ctx, inv))

	// Chain row stores the JSON array literal verbatim — confirms the
	// element-wise protojson.Marshal + strings.Join path in
	// emitMessageJsonMarshals (generator.go:687-697).
	var stored string
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ?", inv.InvoiceId, "items").
		Select("field_value").Scan(&stored).Error)
	require.JSONEq(t,
		`[{"value":"1","unit":"EUR"},{"value":"2","unit":"GBP"}]`,
		stored)
}

// ── google.protobuf.Timestamp support ────────────────────────────────────────
// `transfer_date` on Invoice is a chain-stored google.protobuf.Timestamp. The
// generator maps it to time.Time in the model, TIMESTAMP WITH TIME ZONE in
// the DB, and serializes through the chain as RFC3339Nano text cast back to
// timestamptz by the view.

func TestInvoice_Timestamp_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	// Postgres timestamptz has microsecond precision (6 digits), so the
	// input has to use μs to round-trip exactly. RFC3339Nano emits up to
	// nanoseconds but Postgres truncates anything past μs.
	expected := time.Date(2026, time.March, 15, 14, 30, 45, 123456000, time.UTC)
	inv := newInvoice("inv_ts", sellerID, buyerID)
	inv.TransferDate = timestamppb.New(expected)
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.IsType(t, time.Time{}, view.TransferDate,
		"View should expose google.protobuf.Timestamp as time.Time")
	require.True(t, expected.Equal(view.TransferDate.UTC()),
		"transfer_date round-trip mismatch: got %v, want %v",
		view.TransferDate.UTC(), expected)
}

func TestInvoice_Timestamp_ColumnIsTimestampTZ(t *testing.T) {
	resetTables(t)
	// The chain field_value is TEXT, but the VIEW projects it as
	// `c_transfer_date.field_value::timestamptz AS transfer_date`. Pin both
	// the cast (via the view's data type) and the chain serialization format.
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("inv_ts_col", sellerID, buyerID)
	inv.TransferDate = timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	require.NoError(t, repo.Save(ctx, inv))

	// The view exposes the column as timestamptz.
	var viewType string
	require.NoError(t, testDB.Raw(
		`SELECT data_type FROM information_schema.columns
		 WHERE table_name = 'invoices' AND column_name = 'transfer_date'`,
	).Scan(&viewType).Error)
	require.Equal(t, "timestamp with time zone", viewType)

	// And the chain row stores the raw RFC3339Nano text.
	var stored string
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ?", inv.InvoiceId, "transfer_date").
		Select("field_value").Scan(&stored).Error)
	require.Equal(t, "2026-01-02T03:04:05Z", stored)
}

// Compile-time guard to make sure the user import isn't dropped by goimports.
var _ = (*user.User)(nil)
