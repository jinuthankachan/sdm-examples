package invoice

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"gorm.io/gorm"
)

type InvoiceRepo struct {
	db *gorm.DB
}

func NewInvoiceRepo(db *gorm.DB) *InvoiceRepo {
	return &InvoiceRepo{db: db}
}

func (r *InvoiceRepo) Save(ctx context.Context, model *Invoice) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		pii := InvoicePii{
			Id:            model.Id,
			InvoiceNumber: model.InvoiceNumber,
			SellerGst:     model.SellerGst,
			BuyerGst:      model.BuyerGst,
			SellerName:    model.SellerName,
			BuyerName:     model.BuyerName,
		}
		if err := tx.Create(&pii).Error; err != nil {
			return err
		}

		// Save Chain Fields
		if err := tx.Create(&InvoiceChain{
			Key:        model.Id,
			FieldName:  "id",
			FieldValue: fmt.Sprintf("%v", model.Id),
		}).Error; err != nil {
			return err
		}
		// Hash SellerGst
		h_SellerGst := sha256.Sum256([]byte(fmt.Sprintf("%v", model.SellerGst)))
		hashed_SellerGst := hex.EncodeToString(h_SellerGst[:])
		if err := tx.Create(&InvoiceChain{
			Key:        model.Id,
			FieldName:  "hashed_seller_gst",
			FieldValue: hashed_SellerGst,
		}).Error; err != nil {
			return err
		}
		// Hash BuyerGst
		h_BuyerGst := sha256.Sum256([]byte(fmt.Sprintf("%v", model.BuyerGst)))
		hashed_BuyerGst := hex.EncodeToString(h_BuyerGst[:])
		if err := tx.Create(&InvoiceChain{
			Key:        model.Id,
			FieldName:  "hashed_buyer_gst",
			FieldValue: hashed_BuyerGst,
		}).Error; err != nil {
			return err
		}
		if err := tx.Create(&InvoiceChain{
			Key:        model.Id,
			FieldName:  "seller",
			FieldValue: fmt.Sprintf("%v", model.Seller),
		}).Error; err != nil {
			return err
		}
		if err := tx.Create(&InvoiceChain{
			Key:        model.Id,
			FieldName:  "buyer",
			FieldValue: fmt.Sprintf("%v", model.Buyer),
		}).Error; err != nil {
			return err
		}
		if err := tx.Create(&InvoiceChain{
			Key:        model.Id,
			FieldName:  "amount",
			FieldValue: fmt.Sprintf("%v", model.Amount),
		}).Error; err != nil {
			return err
		}
		return nil
	})
}

func (r *InvoiceRepo) Fetch(ctx context.Context, id string) (*InvoiceView, error) {
	var view InvoiceView
	// GORM might not support querying Views directly with First if it doesn't know it's a table.
	// But we defined TableName() to return the view name, so it should work.
	if err := r.db.WithContext(ctx).Where("id = ?", id).First(&view).Error; err != nil {
		return nil, err
	}
	return &view, nil
}
