//go:build noaudit

package integration

import "testing"

// resetTables (audit-off variant) skips audit_pii_* — those tables don't
// exist when the demo is generated with create-audit-tables: false. The
// audit-tagged sibling includes them.
func resetTables(t *testing.T) {
	t.Helper()
	err := testDB.Exec(
		`TRUNCATE TABLE pii_invoices, chain_invoices,
		                pii_users, chain_users
		 RESTART IDENTITY CASCADE`,
	).Error
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}
