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

	"demo/models/user"
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

// resetTables clears every SDM table between tests. RESTART IDENTITY resets
// BIGSERIAL counters so auto_increment assertions are deterministic.
func resetTables(t *testing.T) {
	t.Helper()
	err := testDB.Exec(
		`TRUNCATE TABLE pii_invoices, chain_invoices, pii_users, chain_users RESTART IDENTITY CASCADE`,
	).Error
	if err != nil {
		t.Fatalf("truncate: %v", err)
	}
}

// mustSaveUser inserts a user and t.Fatals on error. Used to seed FK
// dependencies for invoice tests.
func mustSaveUser(t *testing.T, u *user.User) {
	t.Helper()
	if err := user.NewUserRepo(testDB).Save(context.Background(), u); err != nil {
		t.Fatalf("seed user %s: %v", u.UserId, err)
	}
}
