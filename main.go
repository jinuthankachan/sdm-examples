package main

import (
	"context"
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

	"demo/models/invoice"
	"demo/models/user"
)

func main() {
	// 1. Setup DB
	dsn := "host=localhost user=admin password=password dbname=demo port=5432 sslmode=disable TimeZone=Asia/Kolkata"
	db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		panic("Failed to connect to database")
	}

	// 2. Migrate Schema — apply every *_sdm_schema.sql under models/sql.
	// Re-running is safe (CREATE TABLE IF NOT EXISTS / CREATE OR REPLACE).
	if err := applySchemas(db, "models/sql"); err != nil {
		panic(fmt.Errorf("apply schemas: %w", err))
	}

	// 3. Initialize Repo
	userRepo := user.NewUserRepo(db)
	repo := invoice.NewInvoiceRepo(db)

	ctx := context.Background()
	ctx = invoice.WithActor(ctx, "tester_1")
	u1 := &user.User{
		UserId:  "user_3",
		Email:   "joh1n@doe.com",
		Name:    "John Doe",
		Pan:     "123456789",
		Country: "US",
	}
	if err := userRepo.Create(ctx, u1); err != nil {
		panic(fmt.Errorf("save user_1: %w", err))
	}
	if err := userRepo.CommitChain(ctx, u1.UserId, ""); err != nil {
		panic(fmt.Errorf("commit chain: %w", err))
	}

	u2 := &user.User{
		UserId:  "user_4",
		Email:   "jan1e@doe.com",
		Name:    "Jane Doe",
		Pan:     "987654321",
		Country: "US",
	}
	if err := userRepo.Create(ctx, u2); err != nil {
		panic(fmt.Errorf("save user_2: %w", err))
	}
	if err := userRepo.CommitChain(ctx, u2.UserId, ""); err != nil {
		panic(fmt.Errorf("commit chain: %w", err))
	}

	i := &invoice.Invoice{
		InvoiceId: "inv_2",
		SellerId:  u1.UserId,
		BuyerId:   u2.UserId,
		Amount:    100,
		Price: &invoice.Money{
			Value: 100,
			Unit:  "INR",
		},
	}
	if err := repo.Create(ctx, i); err != nil {
		panic(fmt.Errorf("save invoice: %w", err))
	}
	if err := repo.CommitChain(ctx, i.InvoiceId, ""); err != nil {
		panic(fmt.Errorf("commit chain: %w", err))
	}

	// Mutate + SaveAll → upserts the PII row and appends a new chain version
	// for the changed field (amount). Price is unchanged, so skip-if-unchanged
	// keeps it at the original version.
	i.Amount = 200
	if err := repo.Update(ctx, i); err != nil {
		panic(fmt.Errorf("save invoice: %w", err))
	}
	if err := repo.CommitChain(ctx, i.InvoiceId, ""); err != nil {
		panic(fmt.Errorf("commit chain: %w", err))
	}

	changeLog, err := repo.ChangeLog(ctx, i.InvoiceId)
	if err != nil {
		panic(fmt.Errorf("change log: %w", err))
	}
	fmt.Printf("amount history: %+v\n", changeLog["amount"])

	fmt.Println("Successfully ran SDM demo!")
}
