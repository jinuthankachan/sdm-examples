//go:build !chaindrafts

// OFF-mode helpers — used when the demo was generated with
// chain-drafts: false (default).

package integration

import (
	"context"
	"testing"

	"demo/models/user"
)

// mustSaveUser inserts a user (PII + chain) atomically via SaveAll. Used to
// seed FK dependencies for invoice tests; uses SaveAll(true) so the chain
// table also has rows for any ChangeLog assertions downstream.
func mustSaveUser(t *testing.T, u *user.User) {
	t.Helper()
	if err := user.NewUserRepo(testDB).SaveAll(context.Background(), u, true); err != nil {
		t.Fatalf("seed user %s: %v", u.UserId, err)
	}
}
