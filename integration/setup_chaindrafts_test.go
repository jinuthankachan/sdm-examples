//go:build chaindrafts

// ON-mode helpers — used when the demo was generated with chain-drafts: true.
// Mirrors the OFF-mode helpers under !chaindrafts so tests share semantics
// across the two API surfaces.

package integration

import (
	"context"
	"testing"

	"demo/models/invoice"
	"demo/models/user"
)

// newDraftUser is the ON-mode equivalent of newUser. The "Draft" prefix is
// a reminder that any chain field set here will be staged DRAFTED until
// committed.
func newDraftUser(userID string) *user.User {
	return &user.User{
		UserId:  userID,
		Email:   userID + "@example.com",
		Name:    "Name " + userID,
		Pan:     "PAN" + userID,
		Country: "IN",
	}
}

// newDraftInvoice mirrors newInvoice from OFF-mode tests.
func newDraftInvoice(id, sellerID, buyerID string) *invoice.Invoice {
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

// mustSaveUser inserts a user (PII + chain) atomically by upserting the
// PII row + drafting chain rows + committing the drafts in a single Go
// flow. Mirrors the OFF-mode helper's "all in one" semantics so test
// authors can seed FK dependencies without thinking about drafts.
func mustSaveUser(t *testing.T, u *user.User) {
	t.Helper()
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()
	if err := repo.Upsert(ctx, u); err != nil {
		t.Fatalf("seed user %s upsert: %v", u.UserId, err)
	}
	if err := repo.CommitChain(ctx, u.UserId, ""); err != nil {
		t.Fatalf("seed user %s commit: %v", u.UserId, err)
	}
}

// seedTwoUsers inserts a seller and buyer for invoice FK tests.
func seedTwoUsers(t *testing.T) (sellerID, buyerID string) {
	t.Helper()
	seller := newDraftUser("seller")
	buyer := newDraftUser("buyer")
	mustSaveUser(t, seller)
	mustSaveUser(t, buyer)
	return seller.UserId, buyer.UserId
}

// commitInvoice runs Save followed by CommitChain — the chain-drafts
// equivalent of a single SaveAll(_, true).
func commitInvoice(t *testing.T, inv *invoice.Invoice) {
	t.Helper()
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()
	if err := repo.Save(ctx, inv); err != nil {
		t.Fatalf("save invoice %s: %v", inv.InvoiceId, err)
	}
	if err := repo.CommitChain(ctx, inv.InvoiceId, ""); err != nil {
		t.Fatalf("commit invoice %s: %v", inv.InvoiceId, err)
	}
}

// commitUser runs Save followed by CommitChain — the chain-drafts
// equivalent of a single SaveAll(_, true).
func commitUser(t *testing.T, u *user.User) {
	t.Helper()
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()
	if err := repo.Save(ctx, u); err != nil {
		t.Fatalf("save user %s: %v", u.UserId, err)
	}
	if err := repo.CommitChain(ctx, u.UserId, ""); err != nil {
		t.Fatalf("commit user %s: %v", u.UserId, err)
	}
}
