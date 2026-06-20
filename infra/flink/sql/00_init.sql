-- Shared init: passed to sql-client via `-i`. Creates the Iceberg REST catalog
-- and the lakehouse namespaces. The current catalog stays default_catalog so
-- Kafka source/sink tables remain in-memory; Iceberg tables are addressed by
-- their fully-qualified name (iceberg.bronze.*, iceberg.silver.*).

CREATE CATALOG iceberg WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3://warehouse/',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://minio:9000',
  's3.path-style-access' = 'true'
);

CREATE DATABASE IF NOT EXISTS iceberg.bronze;
CREATE DATABASE IF NOT EXISTS iceberg.silver;

SET 'pipeline.name' = 'scp-streaming';
