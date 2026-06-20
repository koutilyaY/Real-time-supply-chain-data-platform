# Iceberg REST catalog (adapter) + PostgreSQL JDBC driver.
# The stock fixture only bundles SQLite, which cannot handle concurrent commits
# from multiple Flink jobs (SQLITE_BUSY_SNAPSHOT under WAL). Adding the Postgres
# driver lets us back the JDBC catalog with Postgres for real multi-writer use.
FROM apache/iceberg-rest-fixture:1.8.1
USER root
ADD https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar \
    /usr/lib/iceberg-rest/postgresql.jar
WORKDIR /usr/lib/iceberg-rest
# Launch with an explicit classpath (the adapter is a flat shaded jar) so the
# Postgres driver is visible. Main-Class: org.apache.iceberg.rest.RESTCatalogServer
CMD ["java", "-cp", "iceberg-rest-adapter.jar:postgresql.jar", "org.apache.iceberg.rest.RESTCatalogServer"]
