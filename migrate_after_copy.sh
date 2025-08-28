#!/bin/bash
# ---- set your versions and paths ----
ICEBERG_VER=1.9.2
SQLITE_JDBC_VER=3.50.3.0
SQLITE_DB="/Users/jwj/data/iceberg/new/iceberg.db"
WAREHOUSE="/Users/jwj/data/iceberg/new/warehouse"
TABLE_IDENTIFIER="local.my_table"
TARGET_TABLE_DIR="$WAREHOUSE/local.db/my_table"
LATEST_JSON=$(ls -1 "$TARGET_TABLE_DIR/metadata"/*.metadata.json | sort -V | tail -1)
# ------------------------------------

# Run spark-sql on the script, with the same configs as your SparkSession.builder
SPARK_LOCAL_IP=127.0.0.1 \
spark-sql \
  --packages "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_VER},org.xerial:sqlite-jdbc:${SQLITE_JDBC_VER}" \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.ice=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.ice.type=jdbc \
  --conf spark.sql.catalog.ice.uri="jdbc:sqlite:${SQLITE_DB}" \
  --conf spark.sql.catalog.ice.driver=org.sqlite.JDBC \
  --conf spark.sql.catalog.ice.warehouse="file://${WAREHOUSE}" \
  --conf spark.sql.catalog.ice.jdbc.schema-version=V1 \
  --conf spark.driver.bindAddress=127.0.0.1 \
  --conf spark.driver.host=localhost \
  -e "CALL ice.system.register_table(
    table => '${TABLE_IDENTIFIER}',
    metadata_file => 'file://${LATEST_JSON}'
  );"
