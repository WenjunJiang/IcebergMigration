#!/usr/bin/env python3
"""
Migrate all Iceberg tables from an old warehouse path to a new one,
so they appear exactly like the old PyIceberg catalog (no "ice." prefix).

Directory layout assumption:
  <OLD_PARENT>/
    ├── iceberg.db
    └── warehouse/

Usage:
  spark-submit --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
    --conf spark.sql.catalog.spark_catalog.type=hadoop \
    migrate_warehouse.py <OLD_PARENT_DIR> <NEW_WAREHOUSE_PATH>
"""

import sys
import os
import sqlite3
from urllib.parse import urlparse
from pyspark.sql import SparkSession

if len(sys.argv) != 3:
    print("Usage: migrate_warehouse.py <OLD_PARENT_DIR> <NEW_WAREHOUSE_PATH>")
    sys.exit(1)

old_parent = os.path.abspath(sys.argv[1]).rstrip("/")
new_wh = os.path.abspath(sys.argv[2]).rstrip("/")

sqlite_path = os.path.join(old_parent, "iceberg.db")
old_wh = os.path.join(old_parent, "warehouse")

if not os.path.exists(sqlite_path):
    print(f"[ERROR] iceberg.db not found at {sqlite_path}")
    sys.exit(1)
if not os.path.exists(old_wh):
    print(f"[ERROR] warehouse dir not found at {old_wh}")
    sys.exit(1)

def fs2uri(p: str) -> str:
    return "file://" + os.path.abspath(p)

# Build Spark (pointing at the *new* warehouse)
spark = (
    SparkSession.builder
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hadoop")
    .config("spark.sql.catalog.spark_catalog.warehouse", "file:///tmp/warehouse")
    .getOrCreate()
)

print(f"[INFO] Old parent dir: {old_parent}")
print(f"[INFO] Old warehouse: {old_wh}")
print(f"[INFO] New warehouse: {new_wh}")
print(f"[INFO] SQLite catalog: {sqlite_path}")

# Read old catalog
con = sqlite3.connect(sqlite_path)
cur = con.cursor()
cur.execute("SELECT table_identifier, metadata_location FROM iceberg_tables")
rows = cur.fetchall()
con.close()

for table_identifier, metadata_location in rows:
    table_identifier = table_identifier.strip()
    metadata_location = metadata_location.strip()

    print(f"\n=== Migrating {table_identifier} ===")

    # 1. Register table using metadata file (from old path)
    spark.sql(f"""
      CALL spark_catalog.system.register_table(
        '{table_identifier}',
        '{metadata_location}'
      )
    """).show(truncate=False)

    # 2. Compute new table directory
    meta_path = urlparse(metadata_location).path
    ns, tbl = table_identifier.split(".", 1) if "." in table_identifier else ("default", table_identifier)
    new_table_dir = os.path.join(new_wh, ns, tbl)

    # 3. Set LOCATION to new warehouse
    spark.sql(f"""
      ALTER TABLE `{table_identifier}`
      SET LOCATION '{fs2uri(new_table_dir)}'
    """)

    # 4. Rewrite manifests under new location
    spark.sql(f"CALL spark_catalog.system.rewrite_manifests('{table_identifier}')").show(truncate=False)

    # 5. Basic test
    spark.sql(f"SELECT COUNT(*) FROM `{table_identifier}`").show()
    spark.sql(f"SELECT * FROM `{table_identifier}` LIMIT 5").show(truncate=False)

print("\n[OK] Migration complete. Query tables as local.db.my_table (no catalog prefix).")
