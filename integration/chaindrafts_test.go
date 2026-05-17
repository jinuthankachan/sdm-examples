//go:build chaindrafts

// Chain-drafts workflow tests. Compile only when the demo was generated
// with chain-drafts: true and the build is invoked with -tags chaindrafts.

package integration

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"gorm.io/gorm"

	"demo/models/user"
)

// newDraftUser, newDraftInvoice, mustSaveUser, seedTwoUsers, commitUser,
// commitInvoice live in setup_chaindrafts_test.go so all chain-drafts test
// files share the same helpers.

// ────────────────────────────────────────────────────────────────────────
// Save → DraftChain wiring (the half-state under feature ON)
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Save_StagesChainAsDrafted(t *testing.T) {
	// Save commits PII immediately but stages chain rows as DRAFTED. The
	// committed view sees PII columns populated and chain columns NULL
	// until CommitChain runs.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("save_drafts")
	require.NoError(t, repo.Save(ctx, u))

	// Committed view: chain columns hidden, HasPendingDrafts=true.
	committed, err := repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.Equal(t, u.UserId, committed.UserId)
	require.Empty(t, committed.Pan, "chain field hidden in committed view")
	require.Empty(t, committed.Country, "chain field hidden in committed view")
	require.True(t, committed.HasPendingDrafts, "committed view must signal pending drafts")

	// Overlay view: chain columns visible.
	overlay, err := repo.Fetch(ctx, u.Id, true)
	require.NoError(t, err)
	require.Equal(t, "PANsave_drafts", overlay.Pan)
	require.Equal(t, "IN", overlay.Country)
	require.True(t, overlay.HasPendingDrafts)
}

func TestChainDrafts_CommitChain_PromotesDraftedToCreated(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("commit_workflow")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, "tx-abc-123"))

	v, err := repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.Equal(t, "PANcommit_workflow", v.Pan, "chain field visible after commit")
	require.Equal(t, "tx-abc-123", v.TxHash, "tx_hash recorded on promoted rows")
	require.False(t, v.HasPendingDrafts, "no more pending drafts after commit")
}

func TestChainDrafts_DropChain_DiscardsDrafted(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("drop_workflow")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.DropChain(ctx, u.UserId))

	// Committed view: chain values stay NULL (the drafts were dropped, not
	// promoted), HasPendingDrafts becomes false (no DRAFTED rows left).
	v, err := repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.Empty(t, v.Pan, "dropped drafts must not become committed")
	require.False(t, v.HasPendingDrafts)

	// Raw chain probe: rows exist but are all DROPPED.
	var statuses []string
	require.NoError(t, testDB.Raw(
		"SELECT status FROM chain_users WHERE key = ?", u.UserId,
	).Scan(&statuses).Error)
	require.NotEmpty(t, statuses)
	for _, s := range statuses {
		require.Equal(t, "DROPPED", s)
	}
}

// ────────────────────────────────────────────────────────────────────────
// Partial unique index — at-most-one-DRAFTED guarantee
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_DraftTwice_ReturnsErrPendingDraft(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("double_draft")
	require.NoError(t, repo.Save(ctx, u))

	// Second draft of a different value for the same key+field collides
	// with the partial unique index.
	u.Country = "DE"
	err := repo.DraftChain(ctx, u)
	require.Error(t, err)
	require.True(t, errors.Is(err, user.ErrPendingDraftExists),
		"got %v, want errors.Is(err, ErrPendingDraftExists)", err)
}

func TestChainDrafts_DropThenRedraft_Succeeds(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("redraft_after_drop")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.DropChain(ctx, u.UserId))

	// After DropChain there are no DRAFTED rows; re-drafting is allowed.
	u.Country = "DE"
	require.NoError(t, repo.DraftChain(ctx, u))

	overlay, err := repo.Fetch(ctx, u.Id, true)
	require.NoError(t, err)
	require.Equal(t, "DE", overlay.Country)
}

// ────────────────────────────────────────────────────────────────────────
// Upsert + Update + skip-if-unchanged
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Upsert_PromotesAndRedrafts(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("upsert_cycle")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, "tx1"))

	// Second pass: change a field. Upsert path drafts the new value.
	u.Country = "DE"
	require.NoError(t, repo.Upsert(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, "tx2"))

	v, err := repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.Equal(t, "DE", v.Country)
	require.Equal(t, "tx2", v.TxHash)
}

func TestChainDrafts_Update_ErrorsWhenMissing(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	// No prior Save — Update must error rather than insert.
	u := newDraftUser("strict_update_missing")
	u.Id = 99999 // a row that doesn't exist
	err := repo.Update(context.Background(), u)
	require.Error(t, err)
	require.True(t, errors.Is(err, gorm.ErrRecordNotFound),
		"got %v, want errors.Is(err, gorm.ErrRecordNotFound)", err)
}

func TestChainDrafts_SkipIfUnchanged_AgainstCreatedBaseline(t *testing.T) {
	// DraftChain compares the proposed value against the latest CREATED,
	// not the latest of any status. Re-drafting the same committed value
	// is a no-op (no new DRAFTED row produced).
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("skip_unchanged")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	// Re-saving the same values: DraftChain should produce zero new rows.
	require.NoError(t, repo.DraftChain(ctx, u))

	var draftedCount int64
	require.NoError(t, testDB.Table("chain_users").
		Where("key = ? AND status = ?", u.UserId, "DRAFTED").
		Count(&draftedCount).Error)
	require.Zero(t, draftedCount,
		"identical re-draft against latest CREATED must be a no-op")
}

// ────────────────────────────────────────────────────────────────────────
// State-machine trigger — DB-side enforcement
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_Trigger_BlocksCreatedToDropped(t *testing.T) {
	// The BEFORE UPDATE trigger rejects any transition that isn't
	// DRAFTED→CREATED or DRAFTED→DROPPED, regardless of how it's issued.
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("trigger_guard")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	// Now attempt CREATED → DROPPED via raw SQL.
	err := testDB.Exec(
		`UPDATE chain_users SET status = 'DROPPED' WHERE key = ? AND status = 'CREATED'`,
		u.UserId,
	).Error
	require.Error(t, err, "trigger must reject CREATED→DROPPED")
	require.Contains(t, err.Error(), "illegal chain status transition")
}

func TestChainDrafts_Trigger_BlocksDroppedToCreated(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("trigger_revival")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.DropChain(ctx, u.UserId))

	err := testDB.Exec(
		`UPDATE chain_users SET status = 'CREATED' WHERE key = ? AND status = 'DROPPED'`,
		u.UserId,
	).Error
	require.Error(t, err, "trigger must reject DROPPED→CREATED")
}

// ────────────────────────────────────────────────────────────────────────
// View structure — two views, both expose HasPendingDrafts
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_BothViewsExist(t *testing.T) {
	var n int64
	require.NoError(t, testDB.Raw(
		`SELECT count(*) FROM information_schema.views
		 WHERE table_name IN ('users', 'users_with_drafts')`,
	).Scan(&n).Error)
	require.Equal(t, int64(2), n,
		"expected both users and users_with_drafts views to exist")
}

func TestChainDrafts_OverlayShowsLatestNonDropped(t *testing.T) {
	// After CommitChain, then a new DraftChain that supersedes a field,
	// the overlay view shows the DRAFTED value (latest non-DROPPED).
	resetTables(t)
	repo := user.NewUserRepo(testDB)
	ctx := context.Background()

	u := newDraftUser("overlay_supersede")
	require.NoError(t, repo.Save(ctx, u))
	require.NoError(t, repo.CommitChain(ctx, u.UserId, ""))

	u.Country = "DE"
	require.NoError(t, repo.DraftChain(ctx, u))

	committed, err := repo.Fetch(ctx, u.Id, false)
	require.NoError(t, err)
	require.Equal(t, "IN", committed.Country, "committed view shows latest CREATED")

	overlay, err := repo.Fetch(ctx, u.Id, true)
	require.NoError(t, err)
	require.Equal(t, "DE", overlay.Country, "overlay shows latest DRAFTED")
	require.True(t, overlay.HasPendingDrafts)
}

// ────────────────────────────────────────────────────────────────────────
// Actor flow into draft rows
// ────────────────────────────────────────────────────────────────────────

func TestChainDrafts_ActorFlowsIntoDraftAndCommit(t *testing.T) {
	resetTables(t)
	repo := user.NewUserRepo(testDB)

	u := newDraftUser("actor_in_drafts")
	require.NoError(t, repo.Save(user.WithActor(context.Background(), "alice"), u))

	// CreatedBy is set on the DRAFTED chain rows.
	var draftedBy []string
	require.NoError(t, testDB.Raw(
		`SELECT DISTINCT created_by FROM chain_users WHERE key = ? AND status = 'DRAFTED'`,
		u.UserId,
	).Scan(&draftedBy).Error)
	require.Equal(t, []string{"alice"}, draftedBy)

	// CommitChain with a different actor doesn't rewrite created_by — that's
	// frozen at draft time. The commit's actor only affects audit (via the
	// session var) and shows up on any newly drafted rows in this tx.
	require.NoError(t, repo.CommitChain(user.WithActor(context.Background(), "bob"), u.UserId, ""))

	var committedBy []string
	require.NoError(t, testDB.Raw(
		`SELECT DISTINCT created_by FROM chain_users WHERE key = ? AND status = 'CREATED'`,
		u.UserId,
	).Scan(&committedBy).Error)
	require.Equal(t, []string{"alice"}, committedBy,
		"created_by is preserved across the DRAFTED→CREATED promotion")
}
