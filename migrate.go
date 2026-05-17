package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"gorm.io/gorm"
)

// applySchemas executes every *.sql file in dir against db. Files run in
// alphabetic order; any that error (typically a FOREIGN KEY referencing a
// not-yet-created table) are retried on a subsequent pass once their deps
// exist. The loop terminates when no file makes progress on a full pass.
//
// Each file runs in its own transaction — Postgres rolls back partially-
// applied DDL on error so retries start clean.
//
// The generated `*_sdm_schema.sql` files use `CREATE TABLE IF NOT EXISTS` and
// `CREATE OR REPLACE FUNCTION/VIEW/TRIGGER`, so re-running this against an
// already-migrated database is a no-op.
func applySchemas(db *gorm.DB, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read dir %s: %w", dir, err)
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, filepath.Join(dir, e.Name()))
		}
	}
	if len(files) == 0 {
		return fmt.Errorf("no .sql files in %s", dir)
	}
	sort.Strings(files)

	remaining := files
	var lastErr error
	for {
		var failed []string
		progress := false
		for _, f := range remaining {
			b, err := os.ReadFile(f)
			if err != nil {
				return fmt.Errorf("read %s: %w", f, err)
			}
			if err := db.Transaction(func(tx *gorm.DB) error {
				return tx.Exec(string(b)).Error
			}); err != nil {
				lastErr = err
				failed = append(failed, f)
				continue
			}
			progress = true
		}
		if len(failed) == 0 {
			return nil
		}
		if !progress {
			return fmt.Errorf("unable to apply %v after retries: %w", failed, lastErr)
		}
		remaining = failed
	}
}
