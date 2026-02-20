CREATE TABLE IF NOT EXISTS pii_invoices (
  id TEXT,
  invoice_number BIGINT,
  seller_gst TEXT,
  buyer_gst TEXT,
  seller_name TEXT,
  buyer_name TEXT,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS chain_invoices (
  key TEXT NOT NULL,
  field_name TEXT NOT NULL,
  version BIGSERIAL,
  tx_hash TEXT,
  field_value TEXT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (key, field_name, version)
);

CREATE OR REPLACE VIEW invoices AS
  SELECT
    p.id,
    p.invoice_number,
    p.seller_gst,
    c_hashed_seller_gst.field_value AS hashed_seller_gst,
    p.buyer_gst,
    c_hashed_buyer_gst.field_value AS hashed_buyer_gst,
    p.seller_name,
    p.buyer_name,
    c_seller.field_value AS seller,
    c_buyer.field_value AS buyer,
    c_amount.field_value AS amount
  FROM pii_invoices p
  LEFT JOIN (SELECT DISTINCT ON (key, field_name) field_value, key FROM chain_invoices WHERE field_name='hashed_seller_gst' ORDER BY key, field_name, version DESC) c_hashed_seller_gst ON p.id = c_hashed_seller_gst.key
  LEFT JOIN (SELECT DISTINCT ON (key, field_name) field_value, key FROM chain_invoices WHERE field_name='hashed_buyer_gst' ORDER BY key, field_name, version DESC) c_hashed_buyer_gst ON p.id = c_hashed_buyer_gst.key
  LEFT JOIN (SELECT DISTINCT ON (key, field_name) field_value, key FROM chain_invoices WHERE field_name='seller' ORDER BY key, field_name, version DESC) c_seller ON p.id = c_seller.key
  LEFT JOIN (SELECT DISTINCT ON (key, field_name) field_value, key FROM chain_invoices WHERE field_name='buyer' ORDER BY key, field_name, version DESC) c_buyer ON p.id = c_buyer.key
  LEFT JOIN (SELECT DISTINCT ON (key, field_name) field_value, key FROM chain_invoices WHERE field_name='amount' ORDER BY key, field_name, version DESC) c_amount ON p.id = c_amount.key
;

