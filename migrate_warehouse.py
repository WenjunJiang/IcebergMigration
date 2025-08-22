#!/usr/bin/env python3
"""
Migrate Iceberg tables from a PyIceberg SqlCatalog (SQLite) to a new warehouse,
using Python only (no spark-submit flags). After migration, you can query tables as
`local.db.my_table` with no catalog prefix.

Directory layout assumption for the OLD side:
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

# Iceberg runtime built for Spark 3.5 & Scala 2.12
ICEBERG_COORD = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"

def fs2uri(p: str) -> str:
    """Return a file:// URI for an absolute filesystem path."""
    return "file://" + os.path.abspath(p)

def ensure_file_uri(maybe_path_or_uri: str) -> str:
    """Normalize metadata_location to a file:// URI if it looks like a local path."""
    s = maybe_path_or_uri.strip()
    if s.startswith("file:/"):
        return s
    # treat everything else as a local path
    return fs2uri(s)

def discover_rows(sqlite_path: str):
    """
    Autodetect the catalog table & columns.
    Returns a list of tuples: (table_identifier, metadata_location)
    """
    con = sqlite3.connect(sqlite_path)
    cur = con.cursor()

    # Find tables that contain a metadata_location column
    cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
    candidates = []
    for name, sql in cur.fetchall():
        if sql and "metadata_location" in sql:
            candidates.append(name)

    if not candidates:
        con.close()
        raise RuntimeError("Could not find any table with a 'metadata_location' column in the SQLite catalog.")

    # Prefer common names if present
    preferred_order = ("iceberg_tables", "tables", "pyiceberg_tables")
    table = next((t for t in preferred_order if t in candidates), candidates[0])

    # Inspect columns
    cur.execute(f"PRAGMA table_info({table})")
    cols = [r[1] for r in cur.fetchall()]
    cols_set = set(cols)

    # Build a SELECT that always yields (table_identifier, metadata_location)
    if "table_identifier" in cols_set:
        select_sql = f"SELECT table_identifier, metadata_location FROM {table}"
    elif "identifier" in cols_set:
        select_sql = f"SELECT identifier AS table_identifier, metadata_location FROM {table}"
    elif {"namespace", "table_name"}.issubset(cols_set):
        select_sql = f"SELECT (namespace || '.' || table_name) AS table_identifier, metadata_location FROM {table}"
    elif {"namespace", "name"}.issubset(cols_set):
        select_sql = f"SELECT (namespace || '.' || name) AS table_identifier, metadata_location FROM {table}"
    else:
        con.close()
        raise RuntimeError(
            f"Don’t know how to construct a table identifier from columns {cols}. "
            f"Expected one of: table_identifier / identifier / (namespace + table_name) / (namespace + name)."
        )

    cur.execute(select_sql)
    rows = cur.fetchall()
    con.close()
    return rows

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

    # Build Spark in-code: add Iceberg jars, enable extensions, and make SparkSessionCatalog use Iceberg.
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
    print(f"[INFO] Iceberg coord : {ICEBERG_COORD}")
    print("[INFO] Ensure JAVA_HOME points to Java 11 or 17 (Iceberg 1.9.x requires >= 11).")

    # Read table list from SQLite, with autodetect
    try:
        rows = discover_rows(sqlite_path)
    except Exception as e:
        print(f"[ERROR] Could not read tables from SQLite catalog: {e}")
        sys.exit(1)

    if not rows:
        print("[WARN] No tables found to migrate.")
        return

    # Migrate each table
    for table_identifier, metadata_location in rows:
        table_identifier = table_identifier.strip()
        metadata_location = ensure_file_uri(metadata_location)

        # Split identifier into namespace (may contain dots) and table (last segment)
        if "." in table_identifier:
            ns, tbl = table_identifier.rsplit(".", 1)
        else:
            ns, tbl = "default", table_identifier

        new_table_dir = os.path.join(new_wh, ns, tbl)

        print(f"\n=== Migrating {table_identifier} ===")
        print(f"[STEP] Registering from metadata: {metadata_location}")

        try:
            # 1) Register using the existing metadata (old paths must be reachable)
            #    Iceberg 1.9.x uses positional args: (identifier, metadata_file_or_location)
            spark.sql(f"""
              CALL spark_catalog.system.register_table(
                '{table_identifier}',
                '{metadata_location}'
              )
            """).show(truncate=False)

            # 2) Point table to the NEW warehouse
            new_loc_uri = fs2uri(new_table_dir)
            print(f"[STEP] ALTER TABLE SET LOCATION -> {new_loc_uri}")
            spark.sql(f"""
              ALTER TABLE `{table_identifier}`
              SET LOCATION '{new_loc_uri}'
            """)

            # 3) Rewrite manifests so the new manifest list & manifests are written under the NEW path
            print("[STEP] CALL rewrite_manifests")
            spark.sql(f"CALL spark_catalog.system.rewrite_manifests('{table_identifier}')").show(truncate=False)

            # 4) Verify
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
