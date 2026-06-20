FROM apache/superset:4.0.2
USER root
# Trino driver + Postgres metadata driver
RUN pip install --no-cache-dir trino sqlalchemy-trino psycopg2-binary
COPY superset_config.py /app/pythonpath/superset_config.py
COPY bootstrap.sh /app/bootstrap.sh
RUN chmod +x /app/bootstrap.sh
ENV SUPERSET_CONFIG_PATH=/app/pythonpath/superset_config.py
USER superset
ENTRYPOINT ["/app/bootstrap.sh"]
