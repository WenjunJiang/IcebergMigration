#!/bin/bash
set -u
# ---- set your versions and paths ----
#ICEBERG_VER=1.9.2
SQLITE_JDBC_VER=3.50.3.0
SQLITE_DB="/Users/jwj/data/iceberg/new/iceberg.db"
WAREHOUSE="/Users/jwj/data/iceberg/new/warehouse"
TABLE_IDENTIFIER="local.my_table"
TARGET_TABLE_DIR="$WAREHOUSE/local.db/my_table"
JARS_PATH="/Users/jwj/Workspace/Git/DatabaseMigration/jars"

LATEST_JSON=$(
  find "$TARGET_TABLE_DIR/metadata" -type f -name "*.metadata.json" -print |
    sort -V |
    tail -n 1
)
echo "$LATEST_JSON"
# ------------------------------------
# Build jar path relative to script directory
ICE_RC_JAR="$JARS_PATH/iceberg-spark-runtime-4.0_2.13-1.11.0-20250910.002902-24.jar"
# Run spark-sql on the script, with the same configs as your SparkSession.builder
SPARK_LOCAL_IP=127.0.0.1 \
spark-sql \
  --jars "$ICE_RC_JAR" \
  --packages "org.xerial:sqlite-jdbc:${SQLITE_JDBC_VER}" \
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