-- Seed schema for the simulated ERP source database (CDC source).
-- Debezium captures row-level changes from these tables via logical replication.
CREATE SCHEMA IF NOT EXISTS commerce;

CREATE TABLE commerce.suppliers (
    supplier_id    text PRIMARY KEY,
    name           text NOT NULL,
    country        text,
    lead_time_days int,
    on_time_rate   numeric(5,3),
    risk_score     numeric(6,2),
    updated_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE commerce.purchase_orders (
    po_id        text PRIMARY KEY,
    supplier_id  text REFERENCES commerce.suppliers(supplier_id),
    sku          text NOT NULL,
    quantity     int  NOT NULL CHECK (quantity > 0),
    status       text NOT NULL DEFAULT 'open',
    created_at   timestamptz NOT NULL DEFAULT now()
);

INSERT INTO commerce.suppliers (supplier_id, name, country, lead_time_days, on_time_rate, risk_score) VALUES
  ('SUP-001', 'Acme Components', 'US', 7,  0.965, 12.5),
  ('SUP-002', 'Globex Materials', 'DE', 21, 0.880, 34.0),
  ('SUP-003', 'Initech Parts',   'CN', 35, 0.790, 58.2);

INSERT INTO commerce.purchase_orders (po_id, supplier_id, sku, quantity, status) VALUES
  ('PO-1001', 'SUP-001', 'SKU-00042', 500, 'open'),
  ('PO-1002', 'SUP-002', 'SKU-00118', 120, 'confirmed'),
  ('PO-1003', 'SUP-003', 'SKU-00200', 800, 'open');

-- REPLICA IDENTITY FULL so UPDATE/DELETE events carry the full "before" row.
ALTER TABLE commerce.suppliers REPLICA IDENTITY FULL;
ALTER TABLE commerce.purchase_orders REPLICA IDENTITY FULL;
