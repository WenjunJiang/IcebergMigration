#!/bin/bash
set -euo pipefail
# ---- set your versions and paths ----
#ICEBERG_VER=1.9.2
SQLITE_JDBC_VER=3.50.3.0
SQLITE_DB="/Users/jwj/data/iceberg/old/iceberg.db"
WAREHOUSE="/Users/jwj/data/iceberg/old/warehouse"
TABLE_IDENTIFIER="local.my_table"
NAMESPACE="local"
SOURCE_PREFIX="/Users/jwj/data/iceberg/old/warehouse/local.db/my_table"
TARGET_PREFIX="/Users/jwj/data/iceberg/new/warehouse/local.db/my_table"
STAGE_PATH="/Users/jwj/data/iceberg/tmp/"

# Ensure stage directory exists locally; later convert to file:// URI
mkdir -p "$STAGE_PATH"

LATEST_JSON=$(
  find "$SOURCE_PREFIX/metadata" -type f -name "*.metadata.json" -print |
    sort -V |
    tail -n 1
)
echo "$LATEST_JSON"
# ------------------------------------
pushd "$(dirname "${BASH_SOURCE[0]}")" > /dev/null

# Build jar path relative to script directory
ICE_RC_JAR="$(pwd)/jar/iceberg-spark-runtime-4.0_2.13-1.11.0-20250910.002902-24.jar"
run_sql() {
  SPARK_LOCAL_IP=127.0.0.1 spark-sql \
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
    -S -e "$1"
}

# 1) Ensure namespace exists (idempotent)
run_sql "CREATE NAMESPACE IF NOT EXISTS ice.${NAMESPACE};" || true

# 2) Does table exist? (Correctly scope to catalog.namespace)
TABLE_MATCH=$(run_sql "SHOW TABLES IN ice.${NAMESPACE} LIKE 'my_table';" | awk 'NF{print $1}' | tail -n1)

if [[ "${TABLE_MATCH:-}" == "my_table" ]]; then
  echo "Table ice.${TABLE_IDENTIFIER} already exists; skipping register_table."
else
  echo "Registering table ice.${TABLE_IDENTIFIER} with metadata_file=${LATEST_JSON}"
  run_sql "
    CALL ice.system.register_table(
      table => '${TABLE_IDENTIFIER}',
      metadata_file => 'file://${LATEST_JSON}'
    );
  "
fi

# 3) Rewrite paths (safe to re-run; no-op if already done)
run_sql "
  CALL ice.system.rewrite_table_path(
    table => '${TABLE_IDENTIFIER}',
    source_prefix => 'file://${SOURCE_PREFIX}',
    target_prefix => 'file://${TARGET_PREFIX}',
    staging_location => 'file://${STAGE_PATH}'
  );
"
# Restore original directory
popd > /dev/null