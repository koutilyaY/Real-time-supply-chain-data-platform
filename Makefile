# Real-Time Supply-Chain Data Platform - developer entrypoints
.DEFAULT_GOAL := help
SHELL := /bin/bash

COMPOSE := docker compose

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_.-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
	  awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.PHONY: env
env: ## Create .env from .env.example if missing
	@test -f .env || (cp .env.example .env && echo "created .env")

.PHONY: up
up: env ## Bring up the vertical slice (core services)
	$(COMPOSE) up -d --build
	@echo "Core slice starting. UIs: Flink :8081  MinIO :9001  Trino :8080  API :8000"

.PHONY: up-full
up-full: env ## Bring up every layer (bi, orchestration, ml, rag, observability)
	$(COMPOSE) --profile bi --profile orchestration --profile ml --profile rag --profile observability up -d --build

.PHONY: bi
bi: env ## Add Superset
	$(COMPOSE) --profile bi up -d --build

.PHONY: obs
obs: env ## Add Prometheus + Grafana
	$(COMPOSE) --profile observability up -d

.PHONY: down
down: ## Stop all services (keep volumes)
	$(COMPOSE) --profile bi --profile orchestration --profile ml --profile rag --profile observability down

.PHONY: clean
clean: ## Stop all services and DELETE volumes (data loss)
	$(COMPOSE) --profile bi --profile orchestration --profile ml --profile rag --profile observability down -v

.PHONY: lake-init
lake-init: ## Create Iceberg namespaces + Bronze/Silver/Gold tables (via Trino)
	./scripts/lake_init.sh

.PHONY: flink-jobs
flink-jobs: ## Submit the Flink SQL streaming jobs
	./scripts/submit_flink_jobs.sh

.PHONY: cdc
cdc: env ## Start Debezium CDC (erp-db + kafka-connect) and register the connector
	$(COMPOSE) --profile cdc up -d
	./scripts/register_debezium.sh

.PHONY: dbt-run
dbt-run: ## Build dbt models (Silver views + Gold marts) in a throwaway container
	docker run --rm --network platform \
	  -v "$(CURDIR)/transformations/dbt":/dbt -w /dbt \
	  -e DBT_PROFILES_DIR=/dbt -e TRINO_HOST=trino \
	  python:3.11-slim bash -lc "pip install -q dbt-trino==1.8.1 && dbt deps && dbt build"

.PHONY: maintain
maintain: ## Iceberg maintenance: compact + expire snapshots + remove orphans (arg: RETENTION=7d)
	./scripts/iceberg_maintain.sh $(or $(RETENTION),7d)

.PHONY: partition
partition: ## Apply Iceberg day-partitioning (run flink-jobs after to honor the spec)
	./scripts/apply_partitioning.sh

.PHONY: backup
backup: ## Back up all Postgres databases (catalog + metadata) to ./backups
	./scripts/backup_postgres.sh

.PHONY: demo
demo: ## End-to-end: up -> lake-init -> flink-jobs (waits between steps)
	$(MAKE) up
	@echo "Waiting 45s for services to become healthy..."; sleep 45
	$(MAKE) lake-init
	$(MAKE) flink-jobs
	@echo "Demo running. Query Trino: make trino-cli"

.PHONY: trino-cli
trino-cli: ## Open a Trino SQL shell
	$(COMPOSE) exec trino trino

.PHONY: smoke
smoke: ## Run smoke tests against the running stack
	python3 tests/integration/smoke_test.py

.PHONY: contracts
contracts: ## Validate generator events against the JSON data contracts
	pip install -q -r tests/contracts/requirements.txt && python3 tests/contracts/validate_contracts.py

.PHONY: dq
dq: ## Run dbt data-quality tests + source freshness (throwaway container)
	docker run --rm --network platform \
	  -v "$(CURDIR)/transformations/dbt":/dbt -w /dbt \
	  -e DBT_PROFILES_DIR=/dbt -e TRINO_HOST=trino \
	  python:3.11-slim bash -lc "pip install -q dbt-trino==1.8.1 && dbt deps && dbt test && dbt source freshness"

.PHONY: chaos
chaos: ## Run chaos experiments (kill broker / taskmanager / minio)
	./tests/chaos/run_chaos.sh

.PHONY: validate
validate: ## Validate compose + python syntax (no daemon needed)
	$(COMPOSE) config -q && echo "compose: OK"
	python3 -m compileall -q ingestion serving ml rag digital_twin tests && echo "python: OK"

.PHONY: ps
ps: ## Show running services
	$(COMPOSE) ps
