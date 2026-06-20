-- ORDERS pipeline: Kafka -> validate -> Bronze (Iceberg) + 1-min revenue (Silver)
--                                     -> invalid rows to DLQ (Kafka topic orders.dlq)

CREATE TABLE kafka_orders (
  event_id    STRING,
  order_id    STRING,
  customer_id STRING,
  sku         STRING,
  quantity    INT,
  unit_price  DOUBLE,
  currency    STRING,
  status      STRING,
  region      STRING,
  order_ts    STRING,
  event_time  AS TO_TIMESTAMP(REPLACE(REPLACE(order_ts, 'T', ' '), 'Z', '')),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-orders',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE kafka_orders_dlq (
  order_id STRING,
  reason   STRING,
  payload  STRING,
  dlq_ts   TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'topic' = 'orders.dlq',
  'properties.bootstrap.servers' = 'kafka:9092',
  'format' = 'json'
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.orders_raw (
  event_id     STRING,
  order_id     STRING,
  customer_id  STRING,
  sku          STRING,
  quantity     INT,
  unit_price   DOUBLE,
  currency     STRING,
  status       STRING,
  region       STRING,
  order_ts     TIMESTAMP(3),
  ingest_ts    TIMESTAMP(3)
);

-- Valid-rows view feeding the windowed aggregation. The window TVF assigns
-- windows BEFORE a WHERE filter runs, so rows with a null event_time (malformed
-- timestamps) must be excluded here or the window operator throws on null rowtime.
CREATE TEMPORARY VIEW orders_valid AS
  SELECT * FROM kafka_orders
  WHERE order_id IS NOT NULL AND sku IS NOT NULL AND quantity > 0
    AND event_time IS NOT NULL;

CREATE TABLE IF NOT EXISTS iceberg.silver.orders_revenue_1m (
  region        STRING,
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  order_count   BIGINT,
  gross_revenue DOUBLE
);

-- One source scan fans out to three sinks.
EXECUTE STATEMENT SET
BEGIN
  INSERT INTO iceberg.bronze.orders_raw
    SELECT event_id, order_id, customer_id, sku, quantity, unit_price,
           currency, status, region, event_time, CURRENT_TIMESTAMP
    FROM kafka_orders
    WHERE order_id IS NOT NULL AND sku IS NOT NULL AND quantity > 0;

  INSERT INTO iceberg.silver.orders_revenue_1m
    SELECT region, window_start, window_end,
           COUNT(*) AS order_count,
           SUM(quantity * unit_price) AS gross_revenue
    FROM TABLE(TUMBLE(TABLE orders_valid, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY region, window_start, window_end;

  INSERT INTO kafka_orders_dlq
    SELECT order_id,
           CASE WHEN order_id IS NULL THEN 'null_order_id'
                WHEN sku IS NULL THEN 'null_sku'
                WHEN quantity IS NULL OR quantity <= 0 THEN 'bad_quantity'
                ELSE 'unknown' END AS reason,
           CONCAT_WS('|', COALESCE(event_id, ''), COALESCE(sku, ''),
                          COALESCE(CAST(quantity AS STRING), 'null')) AS payload,
           CURRENT_TIMESTAMP
    FROM kafka_orders
    WHERE NOT (order_id IS NOT NULL AND sku IS NOT NULL AND quantity > 0);
END;
