#!/usr/bin/env python3
"""
Migrate Iceberg tables from an old PyIceberg SqlCatalog (SQLite) to a new warehouse,
using Python only (no spark-submit flags). After migration, you can query tables as
`local.db.my_table` with no catalog prefix.

Layout assumption:
  <OLD_PARENT_DIR>/
    ├── iceberg.db
    └── warehouse/

Usage:
  python migrate_warehouse.py <OLD_PARENT_DIR> <NEW_WAREHOUSE_PATH>
"""

import os
import sys
import sqlite3
from urllib.parse import urlparse
from pyspark.sql import SparkSession

ICEBERG_COORD = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"

def fs2uri(p: str) -> str:
    return "file://" + os.path.abspath(p)

def main():
    if len(sys.argv) != 3:
        print("Usage: python migrate_warehouse.py <OLD_PARENT_DIR> <NEW_WAREHOUSE_PATH>")
        sys.exit(1)

    old_parent = os.path.abspath(sys.argv[1]).rstrip("/")
    new_wh = os.path.abspath(sys.argv[2]).rstrip("/")

    sqlite_path = os.path.join(old_parent, "iceberg.db")
    old_wh = os.path.join(old_parent, "warehouse")

    if not os.path.exists(sqlite_path):
        print(f"[ERROR] Missing SQLite catalog at: {sqlite_path}")
        sys.exit(1)
    if not os.path.isdir(old_wh):
        print(f"[ERROR] Missing warehouse directory at: {old_wh}")
        sys.exit(1)

    # Build Spark in-code: set Iceberg JARs, enable extensions, and set SparkSessionCatalog to Iceberg.
    spark = (
        SparkSession.builder
        .config("spark.jars.packages", ICEBERG_COORD)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", fs2uri(new_wh))
        .getOrCreate()
    )

    print(f"[INFO] Old parent dir: {old_parent}")
    print(f"[INFO] Old warehouse : {old_wh}")
    print(f"[INFO] New warehouse : {new_wh}")
    print(f"[INFO] SQLite catalog: {sqlite_path}")
    print(f"[INFO] Using Iceberg coord: {ICEBERG_COORD}")
    print("[INFO] Java must be 11+ for Iceberg 1.9.x. If you see classfile 55/52 errors, set JAVA_HOME accordingly.")

    # Read all tables from the PyIceberg SQLite catalog
    con = sqlite3.connect(sqlite_path)
    cur = con.cursor()
    cur.execute("SELECT table_identifier, metadata_location FROM iceberg_tables")
    rows = cur.fetchall()
    con.close()

    if not rows:
        print("[WARN] No tables found in iceberg_tables.")
        return

    # Migrate each table
    for table_identifier, metadata_location in rows:
        table_identifier = table_identifier.strip()
        metadata_location = metadata_location.strip()

        # Split identifier into namespace (may contain dots) and table (last segment)
        if "." in table_identifier:
            ns, tbl = table_identifier.rsplit(".", 1)
        else:
            ns, tbl = "default", table_identifier

        new_table_dir = os.path.join(new_wh, ns, tbl)

        print(f"\n=== Migrating {table_identifier} ===")
        print(f"[STEP] Register from metadata: {metadata_location}")

        try:
            # 1) register the existing table using its current metadata (old paths must be reachable)
            spark.sql(f"""
              CALL spark_catalog.system.register_table(
                '{table_identifier}',
                '{metadata_location}'
              )
            """).show(truncate=False)

            # 2) move table to the new warehouse path
            print(f"[STEP] ALTER TABLE SET LOCATION -> {fs2uri(new_table_dir)}")
            spark.sql(f"""
              ALTER TABLE `{table_identifier}`
              SET LOCATION '{fs2uri(new_table_dir)}'
            """)

            # 3) rewrite manifests so new metadata is written under the NEW location
            print("[STEP] CALL rewrite_manifests")
            spark.sql(f"CALL spark_catalog.system.rewrite_manifests('{table_identifier}')").show(truncate=False)

            # 4) basic verification
            print("[TEST] COUNT(*)")
            spark.sql(f"SELECT COUNT(*) AS cnt FROM `{table_identifier}`").show()
            print("[TEST] SAMPLE ROWS")
            spark.sql(f"SELECT * FROM `{table_identifier}` LIMIT 5").show(truncate=False)

        except Exception as e:
            print(f"[ERROR] Failed migrating {table_identifier}: {e}")

    print("\n[OK] Migration finished. You can now query with plain identifiers, e.g.:")
    print("  SELECT * FROM local.db.my_table LIMIT 5;")

if __name__ == "__main__":
    main()
