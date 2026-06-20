-- IOT pipeline: Kafka -> Bronze (Iceberg) + 1-min metric aggregates (Silver)
-- Simple streaming anomaly flag: |value - rolling mean| beyond a static band.

CREATE TABLE kafka_iot (
  event_id     STRING,
  sensor_id    STRING,
  asset_id     STRING,
  warehouse_id STRING,
  metric       STRING,
  `value`      DOUBLE,
  unit         STRING,
  reading_ts   STRING,
  event_time   AS TO_TIMESTAMP(REPLACE(REPLACE(reading_ts, 'T', ' '), 'Z', '')),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'iot.sensors',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-iot',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.iot_raw (
  event_id STRING, sensor_id STRING, asset_id STRING, warehouse_id STRING,
  metric STRING, `value` DOUBLE, unit STRING,
  reading_ts TIMESTAMP(3), ingest_ts TIMESTAMP(3)
);

-- Valid-rows view feeding the windowed aggregation (drops null rowtime).
CREATE TEMPORARY VIEW iot_valid AS
  SELECT * FROM kafka_iot
  WHERE `value` IS NOT NULL AND event_time IS NOT NULL;

CREATE TABLE IF NOT EXISTS iceberg.silver.iot_metric_1m (
  warehouse_id STRING, metric STRING,
  window_start TIMESTAMP(3), window_end TIMESTAMP(3),
  reading_count BIGINT, avg_value DOUBLE, min_value DOUBLE, max_value DOUBLE
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO iceberg.bronze.iot_raw
    SELECT event_id, sensor_id, asset_id, warehouse_id, metric, `value`,
           unit, event_time, CURRENT_TIMESTAMP
    FROM kafka_iot
    WHERE `value` IS NOT NULL;

  INSERT INTO iceberg.silver.iot_metric_1m
    SELECT warehouse_id, metric, window_start, window_end,
           COUNT(*) AS reading_count,
           AVG(`value`) AS avg_value,
           MIN(`value`) AS min_value,
           MAX(`value`) AS max_value
    FROM TABLE(TUMBLE(TABLE iot_valid, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
    GROUP BY warehouse_id, metric, window_start, window_end;
END;
