//go:build !chaindrafts

// OFF-mode (chain-drafts disabled) actor tests. The on-mode workflow has a
// different repo surface (no SaveAll; Fetch takes drafted bool), so these
// tests don't compile against an ON-mode generation. Run with
// `go test -tags chaindrafts` against an ON-mode demo to skip these.

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

func TestActor_WithActor_PopulatesPiiAndChain(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := user.WithActor(context.Background(), "alice@example.com")

	u := newUser("actor_user")
	require.NoError(t, repo.SaveAll(ctx, u, true))

	// PII row: created_by set on initial INSERT.
	var pii user.UserPii
	require.NoError(t, testDB.First(&pii, "id = ?", u.Id).Error)
	require.Equal(t, "alice@example.com", pii.CreatedBy)

	// Every chain row appended by that SaveAll carries the same actor.
	var chainRows []user.UserChain
	require.NoError(t, testDB.Where("key = ?", u.UserId).Find(&chainRows).Error)
	require.NotEmpty(t, chainRows, "SaveAll(_, true) must append chain rows")
	for _, c := range chainRows {
		require.Equal(t, "alice@example.com", c.CreatedBy,
			"chain row %s/v%d must record the actor", c.FieldName, c.Version)
	}
}

// TestActor_CreatedBy_SurvivesUpsert lives in actor_test.go (no audit dep);
// the companion assertion that the audit table captures the last updater is
// in actor_audit_test.go behind `//go:build !noaudit`.
func TestActor_CreatedBy_SurvivesUpsert(t *testing.T) {
	// created_by is immutable across upserts. (For "who last updated?" the
	// audit-tagged sibling test checks audit_pii_<name>s.changed_by.)
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newUser("upsert_actor")
	aliceCtx := user.WithActor(context.Background(), "alice")
	require.NoError(t, repo.SaveAll(aliceCtx, u, true))

	bobCtx := user.WithActor(context.Background(), "bob")
	u.Name = "Bob's Edit"
	require.NoError(t, repo.SaveAll(bobCtx, u, true))

	var pii user.UserPii
	require.NoError(t, testDB.First(&pii, "id = ?", u.Id).Error)
	require.Equal(t, "alice", pii.CreatedBy, "created_by must survive upsert")
}

func TestActor_NoActor_RecordsEmptyString(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newUser("no_actor")
	require.NoError(t, repo.SaveAll(context.Background(), u, true))

	var pii user.UserPii
	require.NoError(t, testDB.First(&pii, "id = ?", u.Id).Error)
	require.Equal(t, "", pii.CreatedBy)

	var chainRow user.UserChain
	require.NoError(t, testDB.Where("key = ?", u.UserId).First(&chainRow).Error)
	require.Equal(t, "", chainRow.CreatedBy)
}

func TestActor_View_SurfacesCreatedBy(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newUser("view_actor")
	require.NoError(t, repo.SaveAll(user.WithActor(context.Background(), "alice"), u, true))
	u.Name = "Renamed by Bob"
	require.NoError(t, repo.SaveAll(user.WithActor(context.Background(), "bob"), u, true))

	view, err := repo.Fetch(context.Background(), u.Id)
	require.NoError(t, err)
	require.Equal(t, "alice", view.CreatedBy,
		"view exposes the original creator; for 'who last updated' query audit_pii_<name>s")
}

// TestActor_SaveChain_OnlyPath_PopulatesCreatedBy — removed. The SaveChain
// method (chain-only writes on a PII-backed message) was retired when
// chain-drafts became opt-in. Actor flow into chain rows under the
// remaining ingestion paths is already covered by
// TestActor_WithActor_PopulatesPiiAndChain (via SaveAll).

func TestActor_Create_StrictInsert_PopulatesCreatedBy(t *testing.T) {
	// Create is PII-only strict INSERT; verify the actor column is set
	// without going through SaveAll.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := user.WithActor(context.Background(), "strict-insert")

	u := newUser("strict")
	require.NoError(t, repo.Create(ctx, u))

	var pii user.UserPii
	require.NoError(t, testDB.First(&pii, "id = ?", u.Id).Error)
	require.Equal(t, "strict-insert", pii.CreatedBy)
}

func TestActor_Invoice_StringPK_RoundTrip(t *testing.T) {
	// Pins that the actor wiring works for a PII-backed message with a
	// string primary key (not BIGSERIAL) and a different conflict column.
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := invoice.WithActor(context.Background(), "billing-svc")

	inv := newInvoice("inv_actor", sellerID, buyerID)
	require.NoError(t, repo.SaveAll(ctx, inv, true))

	var pii invoice.InvoicePii
	require.NoError(t, testDB.First(&pii, "invoice_id = ?", inv.InvoiceId).Error)
	require.Equal(t, "billing-svc", pii.CreatedBy)

	var chainRow invoice.InvoiceChain
	require.NoError(t, testDB.Where("key = ?", inv.InvoiceId).First(&chainRow).Error)
	require.Equal(t, "billing-svc", chainRow.CreatedBy)
}
