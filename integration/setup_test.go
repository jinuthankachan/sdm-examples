// setup_test.go contains mode-agnostic test scaffolding (TestMain, schema
// loading) that compiles in any config. mode-specific helpers — most
// notably mustSaveUser, which uses methods that differ across configs —
// live in setup_off_test.go (!chaindrafts) and setup_chaindrafts_test.go
// (chaindrafts).

package integration

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
	gormpg "gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var testDB *gorm.DB

func TestMain(m *testing.M) {
	ctx := context.Background()

	container, err := tcpostgres.Run(ctx,
		"postgres:16-alpine",
		tcpostgres.WithDatabase("sdmdemo"),
		tcpostgres.WithUsername("sdm"),
		tcpostgres.WithPassword("sdm"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		log.Fatalf("postgres container: %v", err)
	}
	defer func() { _ = container.Terminate(ctx) }()

	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		log.Fatalf("connection string: %v", err)
	}
	db, err := gorm.Open(gormpg.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Warn),
	})
	if err != nil {
		log.Fatalf("gorm open: %v", err)
	}
	testDB = db

	if err := applySchemaFile(db, "../models/sql/user_sdm_schema.sql"); err != nil {
		log.Fatalf("apply user schema: %v", err)
	}
	if err := applySchemaFile(db, "../models/sql/invoice_sdm_schema.sql"); err != nil {
		log.Fatalf("apply invoice schema: %v", err)
	}

	os.Exit(m.Run())
}

func applySchemaFile(db *gorm.DB, relPath string) error {
	abs, err := filepath.Abs(relPath)
	if err != nil {
		return err
	}
	b, err := os.ReadFile(abs)
	if err != nil {
		return err
	}
	return db.Exec(string(b)).Error
}

// resetTables is defined in two build-tagged files:
//   - setup_audit_test.go   (default; tag: !noaudit) — truncates audit tables too
//   - setup_noaudit_test.go (tag: noaudit)           — base tables only
// Switching depends on whether the demo was generated with
// create-audit-tables: true (default) or false. Run with `-tags noaudit`
// against an audit-off generation.
//
// mustSaveUser is also split by tag:
//   - setup_off_test.go        (tag: !chaindrafts) — uses SaveAll
//   - setup_chaindrafts_test.go (tag: chaindrafts) — uses Upsert+CommitChain
