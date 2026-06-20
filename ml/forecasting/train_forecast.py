"""
Demand/revenue forecasting.

Reads hourly revenue from the Gold mart, fits a simple per-region time-series
model (Holt-Winters exponential smoothing), forecasts the next 24 hours, and
logs params/metrics/model to MLflow. Designed to run on the host or as a Dagster
op. Falls back gracefully when there is too little history.

Usage:
  TRINO_HOST=localhost MLFLOW_TRACKING_URI=http://localhost:5000 \
    python ml/forecasting/train_forecast.py
"""
import sys

import mlflow
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing

sys.path.insert(0, __file__.rsplit("/ml/", 1)[0] + "/ml")
from common import MLFLOW_URI, read_sql  # noqa: E402

HORIZON = 24


def main() -> int:
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("demand_forecasting")

    # Use the minute-level Silver series so a meaningful forecast is possible
    # within minutes of starting the stream (hourly Gold needs hours of history).
    df = read_sql(
        "SELECT region, window_start AS ts, gross_revenue "
        "FROM iceberg.silver.orders_revenue_1m ORDER BY region, window_start"
    )
    if df.empty:
        print("No revenue history yet; let the stream run. Skipping.")
        return 0

    for region, g in df.groupby("region"):
        series = g.sort_values("ts")["gross_revenue"].astype(float).reset_index(drop=True)
        if len(series) < 6:
            print(f"[{region}] only {len(series)} points; skipping")
            continue
        with mlflow.start_run(run_name=f"hw_{region}"):
            mlflow.log_params({"region": region, "model": "holt_winters", "horizon": HORIZON})
            model = ExponentialSmoothing(series, trend="add", seasonal=None).fit()
            fitted = model.fittedvalues
            mae = float(np.mean(np.abs(series.values - fitted.values)))
            forecast = model.forecast(HORIZON)
            mlflow.log_metric("train_mae", mae)
            mlflow.log_metric("forecast_next", float(forecast.iloc[0]))
            print(f"[{region}] MAE={mae:.2f} next={forecast.iloc[0]:.2f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
