//go:build chaindrafts

// Chain-drafts view + helpers — ChangeLog (returns full history including
// drafted/dropped rows), View.AsBaseModel under the draft workflow,
// HasPendingDrafts accuracy across the lifecycle, and replay
// (committed view → AsBaseModel → Upsert + CommitChain).

package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"gorm.io/gorm"

	"demo/models/invoice"
	"demo/models/user"
)

func sha256HexCD(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// ────────────────────────────────────────────────────────────────────────
// ChangeLog — returns ALL versions, regardless of status
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_ChangeLog_IncludesAllStatuses(t *testing.T) {
	// ChangeLog is unfiltered — it surfaces history (CREATED) and
	// in-flight drafts (DRAFTED) and dropped ones (DROPPED), since
	// they're all chain rows. Callers can filter on `Value` semantics if
	// they need only committed history.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("changelog_all")
	require.NoError(t, repo.Create(ctx, u))                     // → drafts
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))   // v1 CREATED for each chain field
	u.Country = "DE"
	require.NoError(t, repo.Upsert(ctx, u))                   // → 'country' v2 DRAFTED
	require.NoError(t, repo.CommitChain(ctx, u.UserId, "tx")) // v2 CREATED
	u.Country = "FR"
	require.NoError(t, repo.DraftChain(ctx, u))               // → 'country' v3 DRAFTED
	require.NoError(t, repo.DropChain(ctx, u.UserId))         // v3 DROPPED

	log, err := repo.ChangeLog(ctx, u.UserId)
	require.NoError(t, err)
	country := log["country"]
	require.Len(t, country, 3, "ChangeLog returns every appended version")
	require.Equal(t, "IN", country[1].Value, "v1 = original commit")
	require.Equal(t, "DE", country[2].Value, "v2 = committed update")
	require.Equal(t, "FR", country[3].Value, "v3 = drafted-then-dropped (still in history)")
}

func TestChainDrafts_ChangeLog_HashedTracksAllEmailChanges(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("changelog_user")
	commitUser(t, u)

	u.Email = "second@example.com"
	require.NoError(t, repo.Upsert(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	u.Email = "third@example.com"
	require.NoError(t, repo.Upsert(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	log, err := repo.ChangeLog(ctx, u.UserId)
	require.NoError(t, err)

	hashedEmail := log["hashed_email"]
	require.Len(t, hashedEmail, 3, "hashed_email should track all 3 email changes")
	require.Equal(t, sha256HexCD("changelog_user@example.com"), hashedEmail[1].Value)
	require.Equal(t, sha256HexCD("second@example.com"), hashedEmail[2].Value)
	require.Equal(t, sha256HexCD("third@example.com"), hashedEmail[3].Value)
}

func TestChainDrafts_ChangeLog_Missing_ReturnsErrNotFound(t *testing.T) {
	resetTables(t)
	_, err := user.NewUserRepo(testDB).ChangeLog(context.Background(), "never_existed")
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"got %v, want errors.Is(err, gorm.ErrRecordNotFound)", err)
}

// ────────────────────────────────────────────────────────────────────────
// View.AsBaseModel under the draft workflow
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_AsBaseModel_FromCommittedView(t *testing.T) {
	// Round-trip: Create+Commit → Fetch(_, false) → AsBaseModel → match.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	src := newDraftUser("asbase_user")
	commitUser(t, src)

	view, err := repo.Fetch(ctx, src.Id, false)
	require.NoError(t, err)

	got := view.AsBaseModel()
	require.Equal(t, src.UserId, got.UserId)
	require.Equal(t, src.Email, got.Email)
	require.Equal(t, src.Name, got.Name)
	require.Equal(t, src.Pan, got.Pan)
	require.Equal(t, src.Country, got.Country)
	require.Equal(t, src.Id, got.Id)
}

func TestChainDrafts_AsBaseModel_FromOverlayView(t *testing.T) {
	// Overlay variant: even when fetched before commit, AsBaseModel
	// surfaces the DRAFTED chain values.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	src := newDraftUser("asbase_overlay")
	require.NoError(t, repo.Create(ctx, src)) // chain fields DRAFTED, not committed

	view, err := repo.Fetch(ctx, src.Id, true) // overlay
	require.NoError(t, err)

	got := view.AsBaseModel()
	require.Equal(t, src.Pan, got.Pan, "overlay AsBaseModel must include DRAFTED values")
	require.Equal(t, src.Country, got.Country)
}

func TestChainDrafts_AsBaseModel_Invoice_RoundTrip(t *testing.T) {
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	src := newDraftInvoice("asbase_inv", sellerID, buyerID)
	src.Amount = 7777
	src.TransferDate = timestamppb.New(time.Date(2026, 4, 1, 12, 0, 0, 0, time.UTC))
	commitInvoice(t, src)

	view, err := repo.Fetch(ctx, src.InvoiceId, false)
	require.NoError(t, err)

	got := view.AsBaseModel()
	require.Equal(t, src.InvoiceId, got.InvoiceId)
	require.Equal(t, src.SellerId, got.SellerId)
	require.Equal(t, src.BuyerId, got.BuyerId)
	require.Equal(t, int64(7777), got.Amount)
	require.JSONEq(t, src.Metadata, got.Metadata,
		"datatypes.JSON normalises whitespace and key order; compare JSON-equal")
	require.NotNil(t, got.Price)
	require.Equal(t, src.Price.Value, got.Price.Value)
	require.NotNil(t, got.TransferDate)
	require.True(t, got.TransferDate.AsTime().Equal(src.TransferDate.AsTime()))
}

func TestChainDrafts_AsBaseModel_Upsert_Replay(t *testing.T) {
	// Fetch → AsBaseModel → mutate → Upsert + CommitChain replay loop.
	// Mirrors the OFF-mode SaveAll-replay pattern.
	resetTables(t)
	sellerID, buyerID := seedTwoUsers(t)
	repo := invoice.NewInvoiceRepo(testDB)
	ctx := context.Background()

	src := newDraftInvoice("replay_inv", sellerID, buyerID)
	commitInvoice(t, src)

	view, err := repo.Fetch(ctx, src.InvoiceId, false)
	require.NoError(t, err)
	base := view.AsBaseModel()
	base.Amount = 8888

	require.NoError(t, repo.Upsert(ctx, base))
	require.NoError(t, repo.CommitChain(ctx, base.InvoiceId, "tx-replay"))

	final, err := repo.Fetch(ctx, src.InvoiceId, false)
	require.NoError(t, err)
	require.Equal(t, int64(8888), final.Amount)
	require.Equal(t, "tx-replay", final.TxHash)
}

// ────────────────────────────────────────────────────────────────────────
// HasPendingDrafts lifecycle accuracy
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_HasPendingDrafts_LifecycleFlips(t *testing.T) {
	// HasPendingDrafts walks through true (after Create) → false (after
	// CommitChain) → true (after a follow-up DraftChain) → false (after
	// DropChain). One signal, four transitions.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("hpd_lifecycle")
	require.NoError(t, repo.Create(ctx, u))

	v, err := repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.True(t, v.HasPendingDrafts, "post-Create: drafts pending")

	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))
	v, err = repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.False(t, v.HasPendingDrafts, "post-Commit: drafts gone")

	u.Country = "DE"
	require.NoError(t, repo.DraftChain(ctx, u))
	v, err = repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.True(t, v.HasPendingDrafts, "post-DraftChain: drafts pending again")

	require.NoError(t, repo.DropChain(ctx, u.UserId))
	v, err = repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.False(t, v.HasPendingDrafts, "post-DropChain: drafts cleared")
}

// ────────────────────────────────────────────────────────────────────────
// Committed view tx_hash reflects the latest commit (not stale)
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_View_TxHash_TracksLatestCommit(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("txhash_track")
	require.NoError(t, repo.Create(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, "tx-1"))

	v, err := repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.Equal(t, "tx-1", v.TxHash)

	u.Country = "DE"
	require.NoError(t, repo.Upsert(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, "tx-2"))

	v, err = repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.Equal(t, "tx-2", v.TxHash, "view tx_hash must reflect latest commit")
}

// ────────────────────────────────────────────────────────────────────────
// MultiField commit semantics — one CommitChain promotes all drafts for the key
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_CommitChain_PromotesAllDraftedForKeyAtomically(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("multi_field_commit")
	require.NoError(t, repo.Create(ctx, u))

	// Before commit: every chain field is DRAFTED.
	var draftedCount int64
	require.NoError(t, testDB.Table("chain_users").
		Where("key = ? AND status = ?", u.UserId, "DRAFTED").
		Count(&draftedCount).Error)
	require.True(t, draftedCount >= 3, "expected at least 3 drafted chain fields, got %d", draftedCount)

	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	// After commit: zero DRAFTED, all promoted to CREATED.
	require.NoError(t, testDB.Table("chain_users").
		Where("key = ? AND status = ?", u.UserId, "DRAFTED").
		Count(&draftedCount).Error)
	require.Zero(t, draftedCount, "CommitChain must promote every DRAFTED row")

	var createdCount int64
	require.NoError(t, testDB.Table("chain_users").
		Where("key = ? AND status = ?", u.UserId, "CREATED").
		Count(&createdCount).Error)
	require.True(t, createdCount >= 3, "all drafts must end as CREATED, got %d", createdCount)
}
