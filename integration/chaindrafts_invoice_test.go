//go:build chaindrafts

// Chain-drafts invoice tests — FK enforcement, repeated fields (PII +
// chain), nested message + timestamp round-trips, string-PK draft flow.

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"demo/models/invoice"
)

func sha256HexInv(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// minimalDraftInvoice mirrors minimalInvoice from OFF mode; the smallest
// payload that satisfies invoice's required references.
func minimalDraftInvoice(id, sellerID, buyerID string) *invoice.Invoice {
	return &invoice.Invoice{
		InvoiceId: id,
		SellerId:  sellerID,
		BuyerId:   buyerID,
		Price:     &invoice.Money{Value: 1, Unit: "INR"},
	}
}

// ────────────────────────────────────────────────────────────────────────
// Create+CommitChain workflow on the string-PK invoice
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_Create_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)

	inv := newDraftInvoice("draft_inv_1", sellerID, buyerID)
	inv.Tags = []string{"first", "draft"}
	commitInvoice(t, inv)

	view, err := invoice.NewInvoiceRepo(testDB).Fetch(context.Background(), inv.InvoiceId, false)
	require.NoError(t, err)
	require.Equal(t, inv.InvoiceId, view.InvoiceId)
	require.Equal(t, int64(10000), view.Amount)
	require.Equal(t, []string{"first", "draft"}, []string(view.Tags))
	require.False(t, view.HasPendingDrafts, "fully committed → no drafts pending")
}

// ────────────────────────────────────────────────────────────────────────
// FK enforcement
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_FK_Valid(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)

	inv := newDraftInvoice("fk_ok", sellerID, buyerID)
	commitInvoice(t, inv)
}

func TestChainDrafts_Invoice_FK_Violation_SellerMissing(t *testing.T) {
	resetTables(t)
	buyer := newDraftUser("buyer_only")
	mustSaveUser(t, buyer)
	repo := invoice.NewInvoiceRepo(testDB)

	inv := minimalDraftInvoice("fk_bad", "user_does_not_exist", buyer.UserId)
	err := repo.Create(context.Background(), inv)
	require.Error(t, err, "Create with non-existent seller_id must violate FK")
	require.Contains(t, strings.ToLower(err.Error()), "foreign key")
}

// ────────────────────────────────────────────────────────────────────────
// Repeated scalar chain field (Tags) — draft → commit
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_RepeatedString_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)

	inv := newDraftInvoice("rep_str", sellerID, buyerID)
	inv.Tags = []string{"urgent", "paid", "Q4"}
	commitInvoice(t, inv)

	view, err := invoice.NewInvoiceRepo(testDB).Fetch(context.Background(), inv.InvoiceId, false)
	require.NoError(t, err)
	require.Equal(t, []string{"urgent", "paid", "Q4"}, []string(view.Tags))
}

func TestChainDrafts_Invoice_RepeatedString_Empty_StoresLiteralBraces(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)

	inv := newDraftInvoice("rep_str_empty", sellerID, buyerID)
	inv.Tags = []string{}
	commitInvoice(t, inv)

	view, err := invoice.NewInvoiceRepo(testDB).Fetch(context.Background(), inv.InvoiceId, false)
	require.NoError(t, err)
	require.Empty(t, view.Tags)

	var stored string
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ? AND status = ?", inv.InvoiceId, "tags", "CREATED").
		Select("field_value").Scan(&stored).Error)
	require.Equal(t, "{}", stored)
}

// ────────────────────────────────────────────────────────────────────────
// PII-stored repeated message (PiiItems) — drafted alongside other PII
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_PII_RepeatedMessage_RoundTrip(t *testing.T) {
	// Repeated *Message fields annotated (sdm.pii) = true go into the PII
	// table via the protojsonArray serializer — independent of chain
	// drafts. The save path writes them in the PII INSERT, not as chain
	// rows. They should be visible from the committed view immediately.
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)

	inv := newDraftInvoice("rep_msg", sellerID, buyerID)
	inv.PiiItems = []*invoice.Money{
		{Value: 100, Unit: "USD"},
		{Value: 50, Unit: "INR"},
	}
	commitInvoice(t, inv)

	view, err := invoice.NewInvoiceRepo(testDB).Fetch(context.Background(), inv.InvoiceId, false)
	require.NoError(t, err)
	require.Len(t, view.PiiItems, 2)
	require.Equal(t, int64(100), view.PiiItems[0].Value)
	require.Equal(t, "USD", view.PiiItems[0].Unit)
	require.Equal(t, int64(50), view.PiiItems[1].Value)
	require.Equal(t, "INR", view.PiiItems[1].Unit)
}

func TestChainDrafts_Invoice_PII_RepeatedMessage_UpsertOverwrites(t *testing.T) {
	// Upsert path: items get replaced by the new array, not appended.
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("rep_msg_upsert", sellerID, buyerID)
	inv.PiiItems = []*invoice.Money{{Value: 1, Unit: "INR"}}
	commitInvoice(t, inv)

	inv.PiiItems = []*invoice.Money{
		{Value: 999, Unit: "USD"},
		{Value: 1, Unit: "EUR"},
	}
	require.NoError(t, repo.Upsert(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Len(t, view.PiiItems, 2)
	require.Equal(t, int64(999), view.PiiItems[0].Value)
	require.Equal(t, "USD", view.PiiItems[0].Unit)
}

// ────────────────────────────────────────────────────────────────────────
// Nested message in chain (Price ~ value type) and PII (Price as
// PII-stored *Message) — both should survive the draft → commit cycle.
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_PriceMessage_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)

	inv := newDraftInvoice("price_round", sellerID, buyerID)
	inv.Price = &invoice.Money{Value: 12345, Unit: "USD"}
	commitInvoice(t, inv)

	view, err := invoice.NewInvoiceRepo(testDB).Fetch(context.Background(), inv.InvoiceId, false)
	require.NoError(t, err)
	require.NotNil(t, view.Price)
	require.Equal(t, int64(12345), view.Price.Value)
	require.Equal(t, "USD", view.Price.Unit)
}

// ────────────────────────────────────────────────────────────────────────
// Update method on the string-PK invoice
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_Update_StrictErrorsWhenMissing(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)

	// Build an invoice but don't Create it first.
	inv := newDraftInvoice("never_inserted", sellerID, buyerID)
	err := repo.Update(context.Background(), inv)
	require.Error(t, err)
	require.Contains(t, strings.ToLower(err.Error()), "record not found")
}

func TestChainDrafts_Invoice_Update_PromotesExisting(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("update_existing", sellerID, buyerID)
	commitInvoice(t, inv)

	inv.Amount = 99999
	require.NoError(t, repo.Update(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, "tx-update"))

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Equal(t, int64(99999), view.Amount)
	require.Equal(t, "tx-update", view.TxHash)
}

// ────────────────────────────────────────────────────────────────────────
// Multiple invoices, only one with pending drafts
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_HasPendingDrafts_IsolatesPerRecord(t *testing.T) {
	// HasPendingDrafts is per-record, not global — committed records show
	// false even while a sibling record has pending drafts.
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	committed := newDraftInvoice("committed_inv", sellerID, buyerID)
	commitInvoice(t, committed)

	pending := newDraftInvoice("pending_inv", sellerID, buyerID)
	require.NoError(t, repo.Create(ctx, pending)) // drafts but no commit

	a, err := repo.Fetch(ctx, committed.InvoiceId, false)
	require.NoError(t, err)
	require.False(t, a.HasPendingDrafts)

	b, err := repo.Fetch(ctx, pending.InvoiceId, false)
	require.NoError(t, err)
	require.True(t, b.HasPendingDrafts)
}

// ────────────────────────────────────────────────────────────────────────
// Hashed sidecar columns visible in the committed view
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_HashedGsts_InView(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("inv_hash_cd", sellerID, buyerID)
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Equal(t, sha256HexInv(inv.SellerGst), view.HashedSellerGst)
	require.Equal(t, sha256HexInv(inv.BuyerGst), view.HashedBuyerGst)
}

// ────────────────────────────────────────────────────────────────────────
// Chain-stored JSON string round-trip
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_Chain_MetadataString_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	expected := `{"trace_id":"abc-123","retries":3}`
	inv := newDraftInvoice("inv_meta_cd", sellerID, buyerID)
	inv.Metadata = expected
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)

	// View column is c_metadata.field_value::jsonb. Postgres re-serializes JSONB
	// without whitespace, so byte-identical comparison would fail; instead,
	// the JSON should be semantically equivalent.
	require.NotEmpty(t, view.Metadata)
	require.JSONEq(t, expected, string(view.Metadata))
	require.IsType(t, datatypes.JSON{}, view.Metadata)
}

// ────────────────────────────────────────────────────────────────────────
// Composite view (PII + chain + hashed) round-trip
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_View_Combined(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("inv_view_cd", sellerID, buyerID)
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
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
	require.Equal(t, sha256HexInv(inv.SellerGst), view.HashedSellerGst)
	require.Equal(t, sha256HexInv(inv.BuyerGst), view.HashedBuyerGst)
}

// ────────────────────────────────────────────────────────────────────────
// Missing / soft-delete on invoice
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_Fetch_Missing(t *testing.T) {
	resetTables(t)
	repo := invoice.NewInvoiceRepo(testDB)

	_, err := repo.Fetch(context.Background(), "ghost_cd", false)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"committed view must return ErrRecordNotFound for missing key, got %v", err)

	_, err = repo.Fetch(context.Background(), "ghost_cd", true)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"overlay view must return ErrRecordNotFound for missing key, got %v", err)
}

func TestChainDrafts_Invoice_AuditFields_Populated(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	before := time.Now().UTC().Add(-time.Second)
	inv := newDraftInvoice("inv_audit_cd", sellerID, buyerID)
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.False(t, view.DeletedAt.Valid,
		"freshly inserted row has NULL deleted_at; gorm.DeletedAt.Valid should be false")
	require.True(t, view.CreatedAt.After(before),
		"CreatedAt %v should be after %v", view.CreatedAt, before)
	require.WithinDuration(t, view.CreatedAt, view.UpdatedAt, time.Second)
}

func TestChainDrafts_Invoice_SoftDelete_HidesFromFetch_BothViews(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("inv_soft_delete_cd", sellerID, buyerID)
	commitInvoice(t, inv)

	require.NoError(t, testDB.Exec(
		`UPDATE pii_invoices SET deleted_at = NOW() WHERE invoice_id = ?`, inv.InvoiceId,
	).Error)

	_, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"committed view must hide soft-deleted invoice, got %v", err)
	_, err = repo.Fetch(ctx, inv.InvoiceId, true)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"overlay view must hide soft-deleted invoice, got %v", err)

	got, err := repo.Exists(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.False(t, got, "Exists must return false for soft-deleted invoice")
}

// ────────────────────────────────────────────────────────────────────────
// Repeated scalar (Tags) — chain-stored — additional edge cases
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_RepeatedString_SingleElement(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("inv_tags_one_cd", sellerID, buyerID)
	inv.Tags = []string{"solo"}
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Equal(t, []string{"solo"}, []string(view.Tags))
}

func TestChainDrafts_Invoice_RepeatedString_Nil(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	// Tags left at the proto zero-value (nil). pgArrayLiteral(nil) yields "{}"
	// — same wire format as an empty slice — so Fetch returns an empty array.
	inv := newDraftInvoice("inv_tags_nil_cd", sellerID, buyerID)
	require.Nil(t, inv.Tags)
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Empty(t, view.Tags)
}

func TestChainDrafts_Invoice_RepeatedString_ChainStorage(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)

	inv := newDraftInvoice("inv_tags_chain_cd", sellerID, buyerID)
	inv.Tags = []string{"a", "b", "c"}
	commitInvoice(t, inv)

	// pgArrayLiteral emits "{a,b,c}" — Postgres array text literal. Asserted
	// against the CREATED chain row so any change to that helper is caught.
	var stored string
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ? AND status = ?", inv.InvoiceId, "tags", "CREATED").
		Select("field_value").Scan(&stored).Error)
	require.Equal(t, "{a,b,c}", stored)
}

// ────────────────────────────────────────────────────────────────────────
// Repeated message (Items) — chain-stored — full coverage
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_RepeatedMessage_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("inv_items_cd", sellerID, buyerID)
	inv.Items = []*invoice.Money{
		{Value: 100, Unit: "USD"},
		{Value: 50, Unit: "INR"},
	}
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Len(t, view.Items, 2)
	require.Equal(t, int64(100), view.Items[0].Value)
	require.Equal(t, "USD", view.Items[0].Unit)
	require.Equal(t, int64(50), view.Items[1].Value)
	require.Equal(t, "INR", view.Items[1].Unit)
}

func TestChainDrafts_Invoice_RepeatedMessage_Empty(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	// Items at zero-value (nil). The generator initialises the local JSON var
	// to the empty array literal "[]", so the chain row's field_value is a
	// valid JSON array and the view's ::jsonb cast succeeds.
	inv := newDraftInvoice("inv_items_empty_cd", sellerID, buyerID)
	require.Nil(t, inv.Items)
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Empty(t, view.Items, "empty Items must round-trip as a nil/empty slice")
}

func TestChainDrafts_Invoice_RepeatedMessage_ChainStorage(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)

	inv := newDraftInvoice("inv_items_chain_cd", sellerID, buyerID)
	inv.Items = []*invoice.Money{
		{Value: 1, Unit: "EUR"},
		{Value: 2, Unit: "GBP"},
	}
	commitInvoice(t, inv)

	// Chain row stores the JSON array literal verbatim — confirms the
	// element-wise protojson.Marshal + strings.Join path in
	// emitMessageJsonMarshals.
	var stored string
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ? AND status = ?", inv.InvoiceId, "items", "CREATED").
		Select("field_value").Scan(&stored).Error)
	require.JSONEq(t,
		`[{"value":"1","unit":"EUR"},{"value":"2","unit":"GBP"}]`,
		stored)
}

// ────────────────────────────────────────────────────────────────────────
// PII-stored repeated scalar (PiiTags) + repeated message empty case
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_PII_RepeatedString_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("pii_strs_cd", sellerID, buyerID)
	inv.PiiTags = []string{"alpha", "beta", "gamma"}
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
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

func TestChainDrafts_Invoice_PII_RepeatedString_Empty(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("pii_strs_empty_cd", sellerID, buyerID)
	require.Nil(t, inv.PiiTags)
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Empty(t, view.PiiTags, "nil PiiTags must round-trip empty")
}

func TestChainDrafts_Invoice_PII_RepeatedMessage_Empty(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("pii_msgs_empty_cd", sellerID, buyerID)
	require.Nil(t, inv.PiiItems)
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Empty(t, view.PiiItems, "nil PiiItems must round-trip empty")
}

// ────────────────────────────────────────────────────────────────────────
// google.protobuf.Timestamp — chain-stored — round-trip + column shape
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_Timestamp_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	// Postgres timestamptz has microsecond precision (6 digits), so the input
	// has to use μs to round-trip exactly. RFC3339Nano emits up to nanoseconds
	// but Postgres truncates anything past μs.
	expected := time.Date(2026, time.March, 15, 14, 30, 45, 123456000, time.UTC)
	inv := newDraftInvoice("inv_ts_cd", sellerID, buyerID)
	inv.TransferDate = timestamppb.New(expected)
	commitInvoice(t, inv)

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.IsType(t, time.Time{}, view.TransferDate,
		"View should expose google.protobuf.Timestamp as time.Time")
	require.True(t, expected.Equal(view.TransferDate.UTC()),
		"transfer_date round-trip mismatch: got %v, want %v",
		view.TransferDate.UTC(), expected)
}

func TestChainDrafts_Invoice_Timestamp_ColumnIsTimestampTZ(t *testing.T) {
	resetTables(t)
	// The chain field_value is TEXT, but the VIEW projects it as
	// `c_transfer_date.field_value::timestamptz AS transfer_date`. Pin both
	// the cast (via the view's data type) and the chain serialization format.
	sellerID, buyerID := seedTwoUsers(t)

	inv := newDraftInvoice("inv_ts_col_cd", sellerID, buyerID)
	inv.TransferDate = timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC))
	commitInvoice(t, inv)

	// The view exposes the column as timestamptz.
	var viewType string
	require.NoError(t, testDB.Raw(
		`SELECT data_type FROM information_schema.columns
		 WHERE table_name = 'invoices' AND column_name = 'transfer_date'`,
	).Scan(&viewType).Error)
	require.Equal(t, "timestamp with time zone", viewType)

	// And the chain row stores the raw RFC3339Nano text in the CREATED row.
	var stored string
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ? AND status = ?", inv.InvoiceId, "transfer_date", "CREATED").
		Select("field_value").Scan(&stored).Error)
	require.Equal(t, "2026-01-02T03:04:05Z", stored)
}
