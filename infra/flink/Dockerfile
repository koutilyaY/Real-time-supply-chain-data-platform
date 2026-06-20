FROM apache/flink:1.19.1-scala_2.12-java11

# Connector jars for Iceberg (REST catalog + S3FileIO) and Kafka.
# Pinned versions known to be mutually compatible with Flink 1.19.
ENV ICEBERG_VERSION=1.8.1
ENV KAFKA_CONNECTOR_VERSION=3.2.0-1.19
ENV HADOOP_UBER_VERSION=2.8.3-10.0
ENV MAVEN=https://repo1.maven.org/maven2

RUN set -eux; \
    cd /opt/flink/lib; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-flink-runtime-1.19/${ICEBERG_VERSION}/iceberg-flink-runtime-1.19-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fsSLO ${MAVEN}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar; \
    # Iceberg's Flink catalog requires Hadoop classes on the classpath (even for
    # the REST catalog + S3FileIO). The shaded uber jar provides them.
    curl -fsSLO ${MAVEN}/org/apache/flink/flink-shaded-hadoop-2-uber/${HADOOP_UBER_VERSION}/flink-shaded-hadoop-2-uber-${HADOOP_UBER_VERSION}.jar

# Enable the Prometheus metrics reporter (scraped on :9249) as a plugin.
RUN mkdir -p /opt/flink/plugins/metrics-prometheus && \
    cp /opt/flink/opt/flink-metrics-prometheus-*.jar /opt/flink/plugins/metrics-prometheus/

# Enable checkpointing so Iceberg commits happen on a regular interval, and
# wire up the Prometheus reporter.
RUN echo "execution.checkpointing.interval: 10s" >> /opt/flink/conf/flink-conf.yaml; \
    echo "state.backend: rocksdb" >> /opt/flink/conf/flink-conf.yaml; \
    echo "table.exec.sink.upsert-materialize: NONE" >> /opt/flink/conf/flink-conf.yaml; \
    echo "metrics.reporter.prom.factory.class: org.apache.flink.metrics.prometheus.PrometheusReporterFactory" >> /opt/flink/conf/flink-conf.yaml; \
    echo "metrics.reporter.prom.port: 9249" >> /opt/flink/conf/flink-conf.yaml
