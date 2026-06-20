# Digital Supply-Chain Twin

A discrete-event simulation (SimPy) of a fulfillment node wrapped in a Monte
Carlo loop. Use it to rehearse disruptions before they happen — e.g. "what does a
10% supplier-outage probability do to our service level and stockout hours?"

## Run
```bash
pip install -r digital_twin/requirements.txt

# baseline
python digital_twin/sim/simulate.py --runs 300 --days 14

# what-if: frequent supplier outages
python digital_twin/sim/simulate.py --runs 300 --days 14 --supplier-outage-prob 0.25
```

Outputs mean / p05 / p95 for **service level**, **average order wait (h)**, and
**stockout hours**.

## Integrating with live data
Seed `--initial-on-hand` (and, in a fuller build, `--order-rate-per-h` and
`--lead-time-h`) from the lakehouse so the twin starts from current reality:
query `iceberg.gold.agg_inventory_health` / `fct_revenue_hourly` via Trino (see
`ml/common.py` for the connection pattern) and feed the values in. Simulation
summaries can be written back to a Gold table and surfaced in Superset alongside
actuals.
