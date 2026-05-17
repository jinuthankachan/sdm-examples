//go:build !noaudit

package integration

import "testing"

// resetTables (audit-on variant) clears every SDM table between tests,
// including the audit_pii_* tables. RESTART IDENTITY resets BIGSERIAL
// counters so auto_increment assertions are deterministic.
//
// Compiled by default. The noaudit-tagged sibling drops audit_pii_* from
// the TRUNCATE list (those tables don't exist when the demo is generated
// with create-audit-tables: false).
func resetTables(t *testing.T) {
	t.Helper()
	err := testDB.Exec(
		`TRUNCATE TABLE pii_invoices, chain_invoices, audit_pii_invoices,
		                pii_users, chain_users, audit_pii_users
		 RESTART IDENTITY CASCADE`,
	).Error
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}
