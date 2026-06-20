"""
IoT anomaly detection.

Trains an IsolationForest over hourly IoT metric aggregates (per metric type) to
flag abnormal sensor behavior (cold-chain excursions, equipment vibration).
Logs the model + contamination rate to MLflow.

Usage:
  TRINO_HOST=localhost MLFLOW_TRACKING_URI=http://localhost:5000 \
    python ml/anomaly/train_anomaly.py
"""
import sys

import mlflow
import mlflow.sklearn
from sklearn.ensemble import IsolationForest

sys.path.insert(0, __file__.rsplit("/ml/", 1)[0] + "/ml")
from common import MLFLOW_URI, read_sql  # noqa: E402


def main() -> int:
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("iot_anomaly")

    df = read_sql(
        "SELECT metric, avg_value, min_value, max_value, readings "
        "FROM iceberg.gold.fct_iot_hourly"
    )
    if df.empty:
        print("No IoT history yet; skipping.")
        return 0

    for metric, g in df.groupby("metric"):
        feats = g[["avg_value", "min_value", "max_value", "readings"]].astype(float)
        if len(feats) < 10:
            print(f"[{metric}] only {len(feats)} rows; skipping")
            continue
        with mlflow.start_run(run_name=f"iforest_{metric}"):
            contamination = 0.05
            model = IsolationForest(contamination=contamination, random_state=42)
            model.fit(feats)
            n_anom = int((model.predict(feats) == -1).sum())
            mlflow.log_params({"metric": metric, "contamination": contamination})
            mlflow.log_metric("anomalies_flagged", n_anom)
            mlflow.sklearn.log_model(model, artifact_path=f"iforest_{metric}")
            print(f"[{metric}] flagged {n_anom}/{len(feats)} as anomalous")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
