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
	userRepo := user.NewUserRepo(db)

	// 2. Initialize Repo
	repo := invoice.NewInvoiceRepo(db)

	// 3. Migrate Schema
	// In a real app, you would run the generated SQL.
	// For this demo, we can just AutoMigrate the underlying tables if the generated code exports them,
	// OR we can execute the generated SQL.
	// Since generated code splits structs, we'll just use AutoMigrate on the internal structs if likely visible,
	// or more simply, just print that we are ready.
	// NOTE: The generated code creates `PiiUser` and `ChainUser`.
	// Let's assume for this demo we just rely on the fact that it compiles.
	// To actually run it, we need the tables.
	// Let's try to AutoMigrate the internal models if we can access them, or just skip execution logic
	// if access is restricted. For now, let's just attempt to compile a save call.

	// We will just verify compilation and basic repo creation for now.
	ctx := context.Background()
	_ = ctx

	u1 := &user.User{
		UserId:  "user_1",
		Email:   "john@doe.com",
		Name:    "John Doe",
		Pan:     "123456789",
		Country: "US",
	}
	userRepo.Save(ctx, u1)

	u2 := &user.User{
		UserId:  "user_2",
		Email:   "jane@doe.com",
		Name:    "Jane Doe",
		Pan:     "123456789",
		Country: "US",
	}
	userRepo.Save(ctx, u2)

	// Attempt a Save (this will fail at runtime if tables don't exist, but proves compilation)
	i := &invoice.Invoice{
		InvoiceId: "inv_1",
		SellerId:  u1.UserId,
		BuyerId:   u2.UserId,
		Amount:    100,
		Price: &invoice.Money{
			Value: 100,
			Unit:  "INR",
		},
	}
	fmt.Printf("Created Invoice object: %+v\n", i)
	fmt.Printf("Repo initialized: %+v\n", repo)

	// Uncomment to test runtime if tables existed
	err = repo.Save(ctx, i)
	if err != nil {
		panic(fmt.Errorf("failed to save invoice: %s", err.Error()))
	}

	fmt.Println("Successfully compiled and initialized SDM example!")
}
