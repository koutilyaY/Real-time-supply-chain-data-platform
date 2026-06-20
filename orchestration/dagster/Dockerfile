FROM python:3.11-slim
WORKDIR /opt/dagster/app
RUN apt-get update && apt-get install -y --no-install-recommends git && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
# The dbt project is bind-mounted at /opt/dagster/dbt by docker-compose so the
# build_marts asset can invoke it (kept out of the image to avoid a stale copy).
ENV DAGSTER_HOME=/opt/dagster/home PYTHONUNBUFFERED=1
RUN mkdir -p /opt/dagster/home && cp dagster.yaml /opt/dagster/home/dagster.yaml
EXPOSE 3000
CMD ["dagster", "dev", "-h", "0.0.0.0", "-p", "3000", "-m", "supply_chain_dagster"]
