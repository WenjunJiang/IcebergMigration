#!/usr/bin/env python3
"""
Migrate Iceberg tables from a PyIceberg SqlCatalog (SQLite) to a new warehouse,
using Python only (no spark-submit flags). After migration, you can query tables as
`namespace.table` (e.g., local.db.my_table) with no catalog prefix.

OLD layout assumption:
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

def ensure_file_uri(maybe_path_or_uri: str) -> str:
    s = maybe_path_or_uri.strip()
    if s.startswith("file:/"):
        return s
    return fs2uri(s)

def discover_rows(sqlite_path: str):
    """
    Autodetect the catalog table/columns and return rows as:
      List[Tuple[str table_identifier, str metadata_location]]
    Supports these schemas:
      - table_identifier + metadata_location
      - identifier + metadata_location
      - namespace + table_name + metadata_location
      - namespace + name + metadata_location
      - table_namespace + table_name + metadata_location   <-- your case
    """
    con = sqlite3.connect(sqlite_path)
    cur = con.cursor()

    cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
    candidates = []
    for name, sql in cur.fetchall():
        if sql and "metadata_location" in sql:
            candidates.append((name, sql))

    if not candidates:
        con.close()
        raise RuntimeError("Could not find any table with a 'metadata_location' column in the SQLite catalog.")

    # Prefer likely names
    ordered = []
    prefs = ("iceberg_tables", "tables", "pyiceberg_tables")
    # sort with preference first if present
    names = [n for n, _ in candidates]
    for p in prefs:
        if p in names:
            ordered.append(p)
    for n, _ in candidates:
        if n not in ordered:
            ordered.append(n)

    # Try each candidate until one yields rows
    last_err = None
    for table in ordered:
        try:
            cur.execute(f"PRAGMA table_info({table})")
            cols = [r[1] for r in cur.fetchall()]
            cols_set = set(cols)

            if "table_identifier" in cols_set:
                select_sql = f"SELECT table_identifier, metadata_location FROM {table}"
            elif "identifier" in cols_set:
                select_sql = f"SELECT identifier AS table_identifier, metadata_location FROM {table}"
            elif {"namespace", "table_name"}.issubset(cols_set):
                select_sql = f"SELECT (namespace || '.' || table_name) AS table_identifier, metadata_location FROM {table}"
            elif {"namespace", "name"}.issubset(cols_set):
                select_sql = f"SELECT (namespace || '.' || name) AS table_identifier, metadata_location FROM {table}"
            elif {"table_namespace", "table_name"}.issubset(cols_set):
                # <-- your schema
                select_sql = f"SELECT (table_namespace || '.' || table_name) AS table_identifier, metadata_location FROM {table}"
            else:
                # Not a match; try next candidate
                last_err = f"Don’t know how to build identifier from columns {cols} in table {table}."
                continue

            cur.execute(select_sql)
            rows = cur.fetchall()
            con.close()
            return rows

        except Exception as e:
            last_err = str(e)
            continue

    con.close()
    raise RuntimeError(last_err or "Failed to read tables from SQLite catalog.")

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

    try:
        rows = discover_rows(sqlite_path)
    except Exception as e:
        print(f"[ERROR] Could not read tables from SQLite catalog: {e}")
        sys.exit(1)

    if not rows:
        print("[WARN] No tables found to migrate.")
        return

    for table_identifier, metadata_location in rows:
        table_identifier = table_identifier.strip()
        metadata_location = ensure_file_uri(metadata_location)

        # Split identifier into namespace (may contain dots) and table (last segment)
        if "." in table_identifier:
            ns, tbl = table_identifier.rsplit(".", 1)
        else:
            ns, tbl = "default", table_identifier

        new_table_dir = os.path.join(new_wh, ns, tbl)
        new_loc_uri = fs2uri(new_table_dir)

        print(f"\n=== Migrating {table_identifier} ===")
        print(f"[STEP] Registering from metadata: {metadata_location}")

        try:
            # 1) Register using existing metadata
            spark.sql(f"""
              CALL spark_catalog.system.register_table(
                '{table_identifier}',
                '{metadata_location}'
              )
            """).show(truncate=False)

            # 2) Point table to the NEW warehouse
            print(f"[STEP] ALTER TABLE SET LOCATION -> {new_loc_uri}")
            spark.sql(f"""
              ALTER TABLE `{table_identifier}`
              SET LOCATION '{new_loc_uri}'
            """)

            # 3) Rewrite manifests so new metadata is written under NEW path
            print("[STEP] CALL rewrite_manifests")
            spark.sql(f"CALL spark_catalog.system.rewrite_manifests('{table_identifier}')").show(truncate=False)

            # 4) Verify
            print("[TEST] COUNT(*)")
            spark.sql(f"SELECT COUNT(*) AS cnt FROM `{table_identifier}`").show()
            print("[TEST] SAMPLE ROWS")
            spark.sql(f"SELECT * FROM `{table_identifier}` LIMIT 5").show(truncate=False)

        except Exception as e:
            print(f"[ERROR] Failed migrating {table_identifier}: {e}")

    print("\n[OK] Migration finished. Query with plain identifiers, e.g.:")
    print("  SELECT * FROM local.db.my_table LIMIT 5;")

if __name__ == "__main__":
    main()
