# Run Guide — start the platform from scratch

A copy-paste terminal walkthrough. Almost everything runs inside Docker, so the
only thing you need installed is **Docker Desktop** (with the daemon running).

---

## TL;DR (the whole thing in 6 commands)

```bash
cd "/Users/koutilyayenumula/Real‑Time Supply‑Chain Data Platform"
make up           # start the core stack (build images first time: several minutes)
make lake-init    # create the lakehouse layers (bronze/silver/gold)
make flink-jobs   # start the 4 real-time streaming jobs
sleep 90          # let the first 1-minute windows close
make dbt-run      # build the business-ready tables
make smoke        # verify everything end to end  -> "SMOKE OK"
```

Then open the API docs at **http://localhost:8000/docs** and Flink at **http://localhost:8081**.

> Tip: `make demo` runs `up → lake-init → flink-jobs` for you in one shot.

---

## 0. Prerequisites

1. **Docker Desktop is installed and running.** Check:
   ```bash
   docker info >/dev/null 2>&1 && echo "Docker OK" || echo "Start Docker Desktop first"
   ```
   If it's not running, open Docker Desktop (`open -a Docker` on macOS) and wait ~30s.
2. **~10 GB of free Docker disk.** Check with `docker system df`. If low, free space
   (see *Stopping & cleanup* below).

---

## 1. Go to the project folder

```bash
cd "/Users/koutilyayenumula/Real‑Time Supply‑Chain Data Platform"
```
(The folder name uses special hyphen characters — copy the line exactly.)

---

## 2. Create your settings file

```bash
make env          # copies .env.example -> .env if it doesn't exist
```
`.env` holds local settings (passwords, ports, the API key). Defaults work out of
the box. On this machine some host ports are remapped to avoid clashing with other
projects (see the URL table below).

---

## 3. Start the core stack

```bash
make up
```
This builds and starts the always-on core: object storage (MinIO), the event bus
(Kafka), the schema registry (Apicurio), the catalog (Postgres), the lakehouse
catalog service, the stream processor (Flink), the query engine (Trino), the data
generator, and the API. First run pulls/builds images (a few minutes); later runs
are fast.

Watch them come up:
```bash
make ps
```
Wait until `kafka`, `minio`, `postgres`, `iceberg-rest`, `trino` show **healthy**.

---

## 4. Create the lakehouse layers

```bash
make lake-init
```
Creates the `bronze` (raw), `silver` (cleaned), and `gold` (business-ready) namespaces.

---

## 5. Start the real-time streaming jobs

```bash
make flink-jobs
```
Submits 4 Flink jobs (orders, inventory, shipments, IoT). They read the live event
stream, validate it, route bad records to a dead-letter holding area, and write
clean data into the lakehouse. Watch them at **http://localhost:8081**.

> **Important:** Flink jobs do not survive a Flink restart (or a Docker restart).
> If you ever restart, just run `make flink-jobs` again.

Give the 1-minute windows ~90 seconds to produce their first results:
```bash
sleep 90
```

---

## 6. Build the business-ready tables

```bash
make dbt-run
```
Transforms the cleaned data into the polished Gold tables leaders use (hourly
revenue, carrier performance, inventory health, etc.), running automatic quality
tests. Runs in a throwaway container — no extra installs needed.

---

## 7. Verify it all works

```bash
make smoke
```
Expected: a list of `[PASS]` lines ending in **`SMOKE OK`**.

Try the API directly (it requires an access key — the default is `dev-secret-key`):
```bash
# health needs no key
curl -s localhost:8000/health

# data endpoints need the key
curl -s localhost:8000/carriers          -H "X-API-Key: dev-secret-key"
curl -s "localhost:8000/revenue/hourly?limit=5" -H "X-API-Key: dev-secret-key"
```

Or query the lakehouse directly:
```bash
make trino-cli
# then, at the trino> prompt:
#   SELECT * FROM iceberg.gold.fct_revenue_hourly ORDER BY revenue_hour DESC LIMIT 10;
#   quit
```

---

## Service URLs (this machine's ports)

| Service | URL | Login |
|---|---|---|
| API (Swagger docs) | http://localhost:8000/docs | key: `dev-secret-key` |
| Flink (streaming jobs) | http://localhost:8081 | — |
| Trino (SQL engine) | http://localhost:8080 | — |
| MinIO console (storage) | http://localhost:9111 | admin / password |
| Schema registry (Apicurio) | http://localhost:8085 | — |
| Superset (dashboards)* | http://localhost:8090 | admin / admin |
| Dagster (orchestration)* | http://localhost:3000 | — |
| MLflow (ML tracking)* | http://localhost:5001 | — |
| Grafana (monitoring)* | http://localhost:3001 | anon / admin |
| Prometheus (metrics)* | http://localhost:9090 | — |

\* These come up only when you start their optional layer (next section).
Ports are set in `.env`; on a clean machine they fall back to standard defaults
(e.g. MinIO 9000/9001, Superset 8088, MLflow 5000).

---

## Optional layers (start only what you need)

```bash
make bi                              # Superset dashboards        (http://localhost:8090)
make obs                             # Prometheus + Grafana       (http://localhost:3001)
docker compose --profile ml up -d            # MLflow tracking server (http://localhost:5001)
docker compose --profile orchestration up -d # Dagster scheduler      (http://localhost:3000)
docker compose --profile rag up -d           # Q&A assistant (Qdrant + local LLM)
make cdc                             # Debezium change-data-capture from a sample ERP database
make up-full                         # everything at once
```

For the Q&A assistant, pull a small local model once and load the sample docs:
```bash
docker compose exec ollama ollama pull qwen2.5:0.5b
docker compose exec rag-api python ingest/ingest_docs.py
curl -s localhost:8100/ask -H 'content-type: application/json' \
  -d '{"question":"What temperature must vaccines stay within?"}'
```

---

## Day-2 operations (housekeeping)

```bash
make partition    # apply day-partitioning to the lakehouse tables (run flink-jobs after)
make maintain     # compact small files + expire old snapshots (RETENTION=7d default)
make backup       # back up all databases to ./backups
make contracts    # check generated events obey the data contracts
make dq           # run data-quality tests + freshness checks
```

---

## Useful commands

```bash
make ps                          # what's running
make validate                    # sanity-check configs without Docker
docker compose logs -f flink-jobmanager   # follow a service's logs
docker compose logs -f generator          # watch events being produced
```

---

## Stopping & cleanup

```bash
make down     # stop everything, KEEP all data (volumes)
make clean    # stop everything and DELETE data volumes (fresh start)
```

To reclaim image disk when you're not using the project (data is preserved; images
re-pull/rebuild next `make up`):
```bash
docker rmi $(docker images 'scp/*' -q)   # remove this project's built images
docker image prune -af                    # remove other unused images
```

---

## Troubleshooting

- **`make up` fails with "port is already allocated"** — another project is using
  that port. Edit the port values in `.env` and re-run `make up`.
- **"no space left on device"** — Docker's disk is full. Run the cleanup commands
  above, or increase the disk size in Docker Desktop → Settings → Resources.
- **No data in Gold tables** — the streaming windows need ~1 minute to close.
  Wait, then re-run `make dbt-run`.
- **Flink shows 0 jobs after a restart** — expected; re-run `make flink-jobs`.
- **API returns 401** — add the header `-H "X-API-Key: dev-secret-key"` (or your
  `API_KEY` from `.env`).
- **Check overall health any time** — `make smoke`.
```
