package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"demo/models/invoice"
)

// loadChainVersions returns the (version, field_value) rows in chain_invoices
// matching the given key and field_name, ordered by version ascending.
func loadChainVersions(t *testing.T, key, field string) []struct {
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

func TestVersion_SameKeyField_Increments(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("ver_same", sellerID, buyerID)
	inv.Amount = 100
	require.NoError(t, repo.Save(ctx, inv))

	inv.Amount = 200
	require.NoError(t, repo.Save(ctx, inv))

	inv.Amount = 300
	require.NoError(t, repo.Save(ctx, inv))

	rows := loadChainVersions(t, inv.InvoiceId, "amount")
	require.Len(t, rows, 3)
	require.Equal(t, int64(1), rows[0].Version)
	require.Equal(t, "100", rows[0].FieldValue)
	require.Equal(t, int64(2), rows[1].Version)
	require.Equal(t, "200", rows[1].FieldValue)
	require.Equal(t, int64(3), rows[2].Version)
	require.Equal(t, "300", rows[2].FieldValue)
}

func TestVersion_DifferentFields_StartAtOne(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("ver_first", sellerID, buyerID)
	require.NoError(t, repo.Save(ctx, inv))

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

func TestVersion_AcrossKeys_Independent(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	a := newInvoice("ver_a", sellerID, buyerID)
	b := newInvoice("ver_b", sellerID, buyerID)
	require.NoError(t, repo.Save(ctx, a))
	require.NoError(t, repo.Save(ctx, b))

	// Each invoice's "amount" chain row should be at version=1; the trigger
	// scopes MAX(version)+1 by (key, field_name), so the two keys are
	// independent counters.
	rowsA := loadChainVersions(t, a.InvoiceId, "amount")
	rowsB := loadChainVersions(t, b.InvoiceId, "amount")
	require.Len(t, rowsA, 1)
	require.Len(t, rowsB, 1)
	require.Equal(t, int64(1), rowsA[0].Version)
	require.Equal(t, int64(1), rowsB[0].Version)
}

func TestVersion_FetchReturnsLatest(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	inv := newInvoice("ver_latest", sellerID, buyerID)
	inv.Amount = 1
	require.NoError(t, repo.Save(ctx, inv))
	inv.Amount = 2
	require.NoError(t, repo.Save(ctx, inv))
	inv.Amount = 42
	require.NoError(t, repo.Save(ctx, inv))

	view, err := repo.Fetch(ctx, inv.InvoiceId)
	require.NoError(t, err)
	require.Equal(t, int64(42), view.Amount,
		"view should join the chain row with the highest version per field_name")
}
