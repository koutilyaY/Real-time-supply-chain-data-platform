import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change-me")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "DATABASE_URL", "postgresql+psycopg2://platform:platform@postgres:5432/superset"
)
SQLALCHEMY_TRACK_MODIFICATIONS = False
FEATURE_FLAGS = {"DASHBOARD_NATIVE_FILTERS": True, "ALERT_REPORTS": True}

# Trino connection string to register in the UI:
#   trino://api@trino:8080/iceberg
