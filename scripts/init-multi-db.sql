-- Bootstraps the side databases that share the single Postgres instance.
-- Runs once on first Postgres startup (docker-entrypoint-initdb.d).
CREATE DATABASE superset;
CREATE DATABASE dagster;
CREATE DATABASE mlflow;
CREATE DATABASE iceberg;
