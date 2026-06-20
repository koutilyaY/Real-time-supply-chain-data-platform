-- SHIPMENTS pipeline: Kafka -> Bronze (Iceberg) + 5-min delay rate per carrier (Silver)

CREATE TABLE kafka_shipments (
  event_id    STRING,
  shipment_id STRING,
  order_id    STRING,
  carrier     STRING,
  origin      STRING,
  destination STRING,
  status      STRING,
  ship_ts     STRING,
  eta_ts      STRING,
  event_time  AS TO_TIMESTAMP(REPLACE(REPLACE(ship_ts, 'T', ' '), 'Z', '')),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'shipments',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'flink-shipments',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'avro-confluent',
  'avro-confluent.url' = 'http://apicurio:8080/apis/ccompat/v7'
);

CREATE TABLE IF NOT EXISTS iceberg.bronze.shipments_raw (
  event_id STRING, shipment_id STRING, order_id STRING, carrier STRING,
  origin STRING, destination STRING, status STRING,
  ship_ts TIMESTAMP(3), ingest_ts TIMESTAMP(3)
);

-- Valid-rows view feeding the windowed aggregation (drops null rowtime).
CREATE TEMPORARY VIEW shipments_valid AS
  SELECT * FROM kafka_shipments
  WHERE shipment_id IS NOT NULL AND event_time IS NOT NULL;

CREATE TABLE IF NOT EXISTS iceberg.silver.shipment_delays_5m (
  carrier STRING, window_start TIMESTAMP(3), window_end TIMESTAMP(3),
  total_shipments BIGINT, delayed_shipments BIGINT, delay_rate DOUBLE
);

EXECUTE STATEMENT SET
BEGIN
  INSERT INTO iceberg.bronze.shipments_raw
    SELECT event_id, shipment_id, order_id, carrier, origin, destination,
           status, event_time, CURRENT_TIMESTAMP
    FROM kafka_shipments
    WHERE shipment_id IS NOT NULL;

  INSERT INTO iceberg.silver.shipment_delays_5m
    SELECT carrier, window_start, window_end,
           COUNT(*) AS total_shipments,
           SUM(CASE WHEN status IN ('delayed', 'customs_hold') THEN 1 ELSE 0 END) AS delayed_shipments,
           CAST(SUM(CASE WHEN status IN ('delayed', 'customs_hold') THEN 1 ELSE 0 END) AS DOUBLE)
             / COUNT(*) AS delay_rate
    FROM TABLE(TUMBLE(TABLE shipments_valid, DESCRIPTOR(event_time), INTERVAL '5' MINUTE))
    GROUP BY carrier, window_start, window_end;
END;
