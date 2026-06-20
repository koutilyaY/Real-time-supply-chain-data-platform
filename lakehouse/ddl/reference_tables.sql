-- Reference DDL for the Iceberg lakehouse (Trino dialect).
-- NOTE: in the running platform these Bronze/Silver tables are created by the
-- Flink SQL jobs (CREATE TABLE IF NOT EXISTS). This file documents the schemas
-- and lets you (re)create them manually via `trino` if needed. Gold tables are
-- managed by dbt (transformations/dbt).

CREATE SCHEMA IF NOT EXISTS iceberg.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg.silver;
CREATE SCHEMA IF NOT EXISTS iceberg.gold;

-- ---- Bronze (validated raw) ----
CREATE TABLE IF NOT EXISTS iceberg.bronze.orders_raw (
  event_id varchar, order_id varchar, customer_id varchar, sku varchar,
  quantity integer, unit_price double, currency varchar, status varchar,
  region varchar, order_ts timestamp(6), ingest_ts timestamp(6)
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.inventory_raw (
  event_id varchar, warehouse_id varchar, sku varchar, on_hand integer,
  reserved integer, reorder_point integer, region varchar,
  inventory_ts timestamp(6), ingest_ts timestamp(6)
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.shipments_raw (
  event_id varchar, shipment_id varchar, order_id varchar, carrier varchar,
  origin varchar, destination varchar, status varchar,
  ship_ts timestamp(6), ingest_ts timestamp(6)
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.iot_raw (
  event_id varchar, sensor_id varchar, asset_id varchar, warehouse_id varchar,
  metric varchar, value double, unit varchar,
  reading_ts timestamp(6), ingest_ts timestamp(6)
);

-- ---- Silver (streaming aggregates) ----
CREATE TABLE IF NOT EXISTS iceberg.silver.orders_revenue_1m (
  region varchar, window_start timestamp(6), window_end timestamp(6),
  order_count bigint, gross_revenue double
);

CREATE TABLE IF NOT EXISTS iceberg.silver.inventory_alerts (
  warehouse_id varchar, sku varchar, on_hand integer, reorder_point integer,
  shortfall integer, region varchar, detected_ts timestamp(6)
);

CREATE TABLE IF NOT EXISTS iceberg.silver.shipment_delays_5m (
  carrier varchar, window_start timestamp(6), window_end timestamp(6),
  total_shipments bigint, delayed_shipments bigint, delay_rate double
);

CREATE TABLE IF NOT EXISTS iceberg.silver.iot_metric_1m (
  warehouse_id varchar, metric varchar, window_start timestamp(6),
  window_end timestamp(6), reading_count bigint, avg_value double,
  min_value double, max_value double
);
