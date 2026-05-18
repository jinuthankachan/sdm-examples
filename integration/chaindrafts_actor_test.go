//go:build chaindrafts

// Chain-drafts actor wiring tests. Mirror actor_test.go's OFF-mode
// coverage of WithActor → created_by propagation, with adaptations for
// the Create / Upsert / Update / CommitChain surface. The existing
// TestChainDrafts_ActorFlowsIntoDraftAndCommit (in chaindrafts_test.go)
// covers basic propagation; these tests fill out the rest of the
// scenarios.

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"demo/models/invoice"
	"demo/models/user"
)

// The actor (who introduced / who changed a row) flows through a single
// channel — the request context — and is recorded in two places:
//
//   • pii_<name>s.created_by   — set at INSERT, preserved across upserts
//   • chain_<name>s.created_by — set on each new chain row
//   • audit_pii_<name>s.changed_by — written by the AFTER UPDATE/DELETE
//     trigger for every PII mutation. This is the source of truth for
//     "who last updated this row" — no row-level updated_by exists.
//
// API surface:
//
//   ctx := user.WithActor(ctx, "alice")  → all subsequent repo calls on
//                                          this ctx attribute writes to alice
//
// Bare ctx → '' recorded (no attribution).

func TestChainDrafts_Actor_CreatedBy_SurvivesUpsert(t *testing.T) {
	// created_by is immutable across upserts. (For "who last updated?" the
	// audit-tagged sibling test checks audit_pii_<name>s.changed_by.)
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newDraftUser("upsert_actor_cd")
	aliceCtx := user.WithActor(context.Background(), "alice")
	require.NoError(t, repo.Create(aliceCtx, u))
	require.NoError(t, repo.CommitChain(aliceCtx, u.UserId, ""))

	bobCtx := user.WithActor(context.Background(), "bob")
	u.Name = "Bob's Edit"
	require.NoError(t, repo.Upsert(bobCtx, u))
	require.NoError(t, repo.CommitChain(bobCtx, u.UserId, ""))

	var pii user.UserPii
	require.NoError(t, testDB.First(&pii, "id = ?", u.Id).Error)
	require.Equal(t, "alice", pii.CreatedBy, "created_by must survive upsert")
}

func TestChainDrafts_Actor_NoActor_RecordsEmptyOnPii(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("no_actor_cd")
	require.NoError(t, repo.Create(ctx, u))

	var pii user.UserPii
	require.NoError(t, testDB.First(&pii, "id = ?", u.Id).Error)
	require.Equal(t, "", pii.CreatedBy)

	var chainRow user.UserChain
	require.NoError(t, testDB.Where("key = ?", u.UserId).First(&chainRow).Error)
	require.Equal(t, "", chainRow.CreatedBy)
}

func TestChainDrafts_Actor_View_SurfacesCreatedBy(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newDraftUser("view_actor_cd")
	require.NoError(t, repo.Create(
		user.WithActor(context.Background(), "alice"), u))
	require.NoError(t, repo.CommitChain(context.Background(), u.UserId, ""))

	u.Name = "Renamed by Bob"
	require.NoError(t, repo.Upsert(
		user.WithActor(context.Background(), "bob"), u))
	require.NoError(t, repo.CommitChain(context.Background(), u.UserId, ""))

	view, err := repo.Fetch(context.Background(), u.Id, false)
	require.NoError(t, err)
	require.Equal(t, "alice", view.CreatedBy,
		"view exposes the original creator; for 'who last updated' query audit_pii_<name>s")
}

func TestChainDrafts_Actor_Invoice_StringPK_RoundTrip(t *testing.T) {
	// Pins that the actor wiring works for a PII-backed message with a
	// string primary key (not BIGSERIAL) and a different conflict column.
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := invoice.WithActor(context.Background(), "billing-svc")

	inv := newDraftInvoice("inv_actor_cd", sellerID, buyerID)
	require.NoError(t, repo.Create(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	var pii invoice.InvoicePii
	require.NoError(t, testDB.First(&pii, "invoice_id = ?", inv.InvoiceId).Error)
	require.Equal(t, "billing-svc", pii.CreatedBy)

	var chainRow invoice.InvoiceChain
	require.NoError(t, testDB.Where("key = ?", inv.InvoiceId).First(&chainRow).Error)
	require.Equal(t, "billing-svc", chainRow.CreatedBy)
}

func TestChainDrafts_Actor_CommitChainPreservesDraftActor(t *testing.T) {
	// CommitChain mutates chain row STATUS via an UPDATE, not an INSERT, so
	// the created_by column on those rows is frozen at draft time. A
	// different actor running CommitChain doesn't rewrite it.
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newDraftUser("commit_preserves_actor")
	require.NoError(t, repo.Create(
		user.WithActor(context.Background(), "alice"), u))

	// Bob commits — chain rows already exist; their created_by stays alice.
	require.NoError(t, repo.CommitChain(
		user.WithActor(context.Background(), "bob"), u.UserId, ""))

	var createdBys []string
	require.NoError(t, testDB.Raw(
		`SELECT DISTINCT created_by FROM chain_users WHERE key = ? AND status = 'CREATED'`,
		u.UserId,
	).Scan(&createdBys).Error)
	require.Equal(t, []string{"alice"}, createdBys,
		"created_by on chain rows is frozen at draft insert time; CommitChain doesn't rewrite it")
}
