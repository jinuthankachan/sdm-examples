//go:build chaindrafts

// Chain-drafts invoice tests — FK enforcement, repeated fields (PII +
// chain), nested message + timestamp round-trips, string-PK draft flow.

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"demo/models/invoice"
)

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
// Save+CommitChain workflow on the string-PK invoice
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Invoice_Save_RoundTrip(t *testing.T) {
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
	err := repo.Save(context.Background(), inv)
	require.Error(t, err, "Save with non-existent seller_id must violate FK")
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

	// Build an invoice but don't Save it first.
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
	require.NoError(t, repo.Save(ctx, pending)) // drafts but no commit

	a, err := repo.Fetch(ctx, committed.InvoiceId, false)
	require.NoError(t, err)
	require.False(t, a.HasPendingDrafts)

	b, err := repo.Fetch(ctx, pending.InvoiceId, false)
	require.NoError(t, err)
	require.True(t, b.HasPendingDrafts)
}
