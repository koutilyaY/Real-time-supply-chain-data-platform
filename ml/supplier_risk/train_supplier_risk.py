"""
Supplier risk scoring.

Derives a risk score per carrier/supplier from on-time performance in the Gold
carrier-performance mart and logs a simple gradient-boosted classifier that
predicts "high risk" (on_time_rate below threshold). In a fuller build this would
also join supplier master data, lead-time variance, and external risk feeds.

Usage:
  TRINO_HOST=localhost MLFLOW_TRACKING_URI=http://localhost:5000 \
    python ml/supplier_risk/train_supplier_risk.py
"""
import sys

import mlflow
import mlflow.sklearn
import numpy as np
from sklearn.ensemble import GradientBoostingClassifier

sys.path.insert(0, __file__.rsplit("/ml/", 1)[0] + "/ml")
from common import MLFLOW_URI, read_sql  # noqa: E402

RISK_THRESHOLD = 0.9  # on_time_rate below this -> high risk


def main() -> int:
    mlflow.set_tracking_uri(MLFLOW_URI)
    mlflow.set_experiment("supplier_risk")

    df = read_sql(
        "SELECT carrier, total_shipments, delayed_shipments, delay_rate, on_time_rate "
        "FROM iceberg.gold.fct_carrier_performance"
    )
    if len(df) < 5:
        print("Not enough carrier history yet; skipping.")
        return 0

    X = df[["total_shipments", "delayed_shipments", "delay_rate"]].astype(float).values
    y = (df["on_time_rate"].astype(float) < RISK_THRESHOLD).astype(int).values
    if len(np.unique(y)) < 2:
        print("Only one risk class present; logging heuristic scores instead.")
    with mlflow.start_run(run_name="supplier_risk_gbc"):
        mlflow.log_param("risk_threshold", RISK_THRESHOLD)
        if len(np.unique(y)) == 2:
            model = GradientBoostingClassifier(random_state=42)
            model.fit(X, y)
            acc = float(model.score(X, y))
            mlflow.log_metric("train_accuracy", acc)
            mlflow.sklearn.log_model(model, artifact_path="supplier_risk")
            print(f"trained classifier, train_acc={acc:.3f}")
        else:
            mlflow.log_metric("high_risk_carriers", int(y.sum()))
            print(f"high-risk carriers: {int(y.sum())}/{len(y)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
