-- INVENTORY pipeline: Kafka -> Bronze (Iceberg) + low-stock alerts (Silver)

CREATE TABLE kafka_inventory (
  event_id      STRING,
  warehouse_id  STRING,
  sku           STRING,
  on_hand       INT,
  reserved      INT,
  reorder_point INT,
  region        STRING,
  inventory_ts  STRING,
  event_time    AS TO_TIMESTAMP(REPLACE(REPLACE(inventory_ts, 'T', ' '), 'Z', '')),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'inventory',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-inventory',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.inventory_raw (
  event_id STRING, warehouse_id STRING, sku STRING, on_hand INT, reserved INT,
  reorder_point INT, region STRING, inventory_ts TIMESTAMP(3), ingest_ts TIMESTAMP(3)
);

CREATE TABLE IF NOT EXISTS iceberg.silver.inventory_alerts (
  warehouse_id STRING, sku STRING, on_hand INT, reorder_point INT,
  shortfall INT, region STRING, detected_ts TIMESTAMP(3)
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO iceberg.bronze.inventory_raw
    SELECT event_id, warehouse_id, sku, on_hand, reserved, reorder_point,
           region, event_time, CURRENT_TIMESTAMP
    FROM kafka_inventory
    WHERE sku IS NOT NULL AND on_hand >= 0;

  INSERT INTO iceberg.silver.inventory_alerts
    SELECT warehouse_id, sku, on_hand, reorder_point,
           (reorder_point - on_hand) AS shortfall, region, CURRENT_TIMESTAMP
    FROM kafka_inventory
    WHERE sku IS NOT NULL AND on_hand >= 0 AND on_hand < reorder_point;
END;
