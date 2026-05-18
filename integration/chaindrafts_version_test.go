//go:build chaindrafts

// Chain-drafts version trigger tests. The BEFORE INSERT trigger that
// assigns chain.version = MAX(version)+1 per (key, field_name) is the
// same one OFF mode exercises in version_test.go; these tests mirror
// the OFF coverage and add ON-only scenarios that involve DRAFTED and
// DROPPED rows participating in the version sequence.

package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"demo/models/invoice"
)

// loadChainVersionsCD returns (version, field_value) rows for the given
// key+field in chain_invoices, ordered ASC. Local copy of the OFF-mode
// helper (which is !chaindrafts-tagged so not visible here).
func loadChainVersionsCD(t *testing.T, key, field string) []struct {
	Version    int64
	FieldValue string
} {
	t.Helper()
	var rows []struct {
		Version    int64
		FieldValue string
	}
	err := testDB.Table("chain_invoices").
		Select("version, field_value").
		Where("key = ? AND field_name = ?", key, field).
		Order("version ASC").
		Scan(&rows).Error
	require.NoError(t, err)
	return rows
}

func TestChainDrafts_Version_SameKeyField_Increments(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("cd_ver_same", sellerID, buyerID)
	inv.Amount = 100
	require.NoError(t, repo.Create(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	inv.Amount = 200
	require.NoError(t, repo.Upsert(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	inv.Amount = 300
	require.NoError(t, repo.Upsert(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	rows := loadChainVersionsCD(t, inv.InvoiceId, "amount")
	require.Len(t, rows, 3)
	require.Equal(t, int64(1), rows[0].Version)
	require.Equal(t, "100", rows[0].FieldValue)
	require.Equal(t, int64(2), rows[1].Version)
	require.Equal(t, "200", rows[1].FieldValue)
	require.Equal(t, int64(3), rows[2].Version)
	require.Equal(t, "300", rows[2].FieldValue)
}

func TestChainDrafts_Version_DifferentFields_StartAtOne(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("cd_ver_first", sellerID, buyerID)
	require.NoError(t, repo.Create(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	// Every chain row for a brand-new record should be at version=1, regardless
	// of how many distinct field_names there are.
	var versions []int64
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ?", inv.InvoiceId).
		Pluck("version", &versions).Error)
	require.NotEmpty(t, versions)
	for _, v := range versions {
		require.Equal(t, int64(1), v, "fresh chain rows must start at version=1")
	}
}

func TestChainDrafts_Version_AcrossKeys_Independent(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	a := newDraftInvoice("cd_ver_a", sellerID, buyerID)
	b := newDraftInvoice("cd_ver_b", sellerID, buyerID)
	require.NoError(t, repo.Create(ctx, a))
	require.NoError(t, repo.CommitChain(ctx, a.InvoiceId, ""))
	require.NoError(t, repo.Create(ctx, b))
	require.NoError(t, repo.CommitChain(ctx, b.InvoiceId, ""))

	rowsA := loadChainVersionsCD(t, a.InvoiceId, "amount")
	rowsB := loadChainVersionsCD(t, b.InvoiceId, "amount")
	require.Len(t, rowsA, 1)
	require.Len(t, rowsB, 1)
	require.Equal(t, int64(1), rowsA[0].Version)
	require.Equal(t, int64(1), rowsB[0].Version)
}

func TestChainDrafts_Version_FetchReturnsLatest(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("cd_ver_latest", sellerID, buyerID)
	inv.Amount = 1
	require.NoError(t, repo.Create(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))
	inv.Amount = 2
	require.NoError(t, repo.Upsert(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))
	inv.Amount = 42
	require.NoError(t, repo.Upsert(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	view, err := repo.Fetch(ctx, inv.InvoiceId, false)
	require.NoError(t, err)
	require.Equal(t, int64(42), view.Amount,
		"committed view should join the highest CREATED version per field_name")
}

// ────────────────────────────────────────────────────────────────────────
// ON-only — DRAFTED and DROPPED rows participate in the version sequence
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Version_DraftedRowGetsNextVersion(t *testing.T) {
	// The version trigger is status-agnostic — it assigns the next sequential
	// number to whatever's being inserted, DRAFTED or CREATED. So after a
	// committed v1, the next draft of that field lands at v2 (still DRAFTED).
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("cd_ver_draft_next", sellerID, buyerID)
	inv.Amount = 10
	require.NoError(t, repo.Create(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	inv.Amount = 20
	require.NoError(t, repo.DraftChain(ctx, inv))

	// Now we should have v1 CREATED and v2 DRAFTED for `amount`.
	rows := loadChainVersionsCD(t, inv.InvoiceId, "amount")
	require.Len(t, rows, 2)
	require.Equal(t, int64(1), rows[0].Version)
	require.Equal(t, "10", rows[0].FieldValue)
	require.Equal(t, int64(2), rows[1].Version)
	require.Equal(t, "20", rows[1].FieldValue)

	var status string
	require.NoError(t, testDB.Table("chain_invoices").
		Where("key = ? AND field_name = ? AND version = 2", inv.InvoiceId, "amount").
		Select("status").Scan(&status).Error)
	require.Equal(t, "DRAFTED", status)
}

func TestChainDrafts_Version_DroppedDoesntBreakSequence(t *testing.T) {
	// After commit (v=1), draft+drop (v=2 DROPPED), the next commit should
	// land at v=3 — the trigger picks MAX(version)+1 regardless of status, so
	// a DROPPED row still occupies its version slot.
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newDraftInvoice("cd_ver_dropped_seq", sellerID, buyerID)
	inv.Amount = 100
	require.NoError(t, repo.Create(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	inv.Amount = 200
	require.NoError(t, repo.DraftChain(ctx, inv))
	require.NoError(t, repo.DropChain(ctx, inv.InvoiceId))

	inv.Amount = 300
	require.NoError(t, repo.DraftChain(ctx, inv))
	require.NoError(t, repo.CommitChain(ctx, inv.InvoiceId, ""))

	rows := loadChainVersionsCD(t, inv.InvoiceId, "amount")
	require.Len(t, rows, 3, "v1 CREATED + v2 DROPPED + v3 CREATED = 3 rows")
	require.Equal(t, int64(1), rows[0].Version)
	require.Equal(t, "100", rows[0].FieldValue)
	require.Equal(t, int64(2), rows[1].Version)
	require.Equal(t, "200", rows[1].FieldValue)
	require.Equal(t, int64(3), rows[2].Version)
	require.Equal(t, "300", rows[2].FieldValue)

	// Statuses match the lifecycle.
	var statuses []string
	require.NoError(t, testDB.Table("chain_invoices").
		Select("status").
		Where("key = ? AND field_name = ?", inv.InvoiceId, "amount").
		Order("version ASC").
		Scan(&statuses).Error)
	require.Equal(t, []string{"CREATED", "DROPPED", "CREATED"}, statuses)
}
