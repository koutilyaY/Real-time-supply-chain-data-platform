FROM python:3.11-slim
RUN pip install --no-cache-dir mlflow==2.16.2 psycopg2-binary==2.9.9 boto3==1.35.24
EXPOSE 5000
# BACKEND_URI / ARTIFACT_ROOT / MLFLOW_S3_ENDPOINT_URL supplied by compose.
CMD ["sh", "-c", "mlflow server --host 0.0.0.0 --port 5000 --backend-store-uri \"$BACKEND_URI\" --artifacts-destination \"$ARTIFACT_ROOT\""]
