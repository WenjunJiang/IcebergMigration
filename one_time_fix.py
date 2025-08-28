#!/usr/bin/env python3
"""
One-time fix: update a copied Iceberg SQLite catalog (iceberg.db) so tables point to the NEW warehouse.

It will:
  - Discover tables from the SQLite DB (or use --tables)
  - Find latest metadata JSON under NEW warehouse: <warehouse>/<ns>.db/<table>/metadata/000xx-*.metadata.json
  - DROP TABLE IF EXISTS ice.`ns`.`table`
  - CALL ice.system.register_table(table => 'ns.table', metadata_file => '<NEW .../metadata/000xx-...json>')
  - Sanity check

Usage examples:
  python update_catalog_to_new_warehouse.py \
    --sqlite-db /Users/jwj/data/iceberg/new/iceberg.db \
    --warehouse file:///Users/jwj/data/iceberg/new/warehouse

  # Only a subset of tables:
  python update_catalog_to_new_warehouse.py \
    --sqlite-db /Users/jwj/data/iceberg/new/iceberg.db \
    --warehouse file:///Users/jwj/data/iceberg/new/warehouse \
    --tables local.my_table,analytics.events
"""

import argparse
import os
import re
import sqlite3
from urllib.parse import urlparse

from pyspark.sql import SparkSession

ICEBERG_COORD = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"
SQLITE_COORD  = "org.xerial:sqlite-jdbc:3.50.3.0"


# -------------------- small helpers --------------------
def with_slash(p: str) -> str:
    return p.rstrip("/") + "/"

def uri_to_path(u: str) -> str:
    return urlparse(u).path if u.startswith("file:/") else u

def path_to_uri(p: str) -> str:
    return "file:///" + os.path.abspath(p).lstrip("/")

def latest_metadata_in_dir(meta_dir_uri: str) -> str | None:
    """
    Return file:/// URI of highest 000xx-*.metadata.json in the given metadata dir (URI or raw path).
    """
    meta_dir = uri_to_path(meta_dir_uri)
    if not os.path.isdir(meta_dir):
        return None
    pat = re.compile(r"^(\d+)-.+\.metadata\.json$")
    best = None
    best_n = -1
    for name in os.listdir(meta_dir):
        m = pat.match(name)
        if not m:
            continue
        n = int(m.group(1))
        if n > best_n:
            best_n = n
            best = name
    return path_to_uri(os.path.join(meta_dir, best)) if best else None

def qualify(catalog: str, identifier: str) -> str:
    parts = identifier.split(".")
    return f"{catalog}." + ".".join(f"`{p}`" for p in parts)

def discover_tables_from_sqlite(sqlite_db_path: str) -> list[tuple[str, str]]:
    """
    Returns [(identifier, metadata_location_from_db)], reading common Iceberg JDBC schemas.
    (Read-only: we are not writing to SQLite directly.)
    """
    con = sqlite3.connect(sqlite_db_path)
    cur = con.cursor()
    cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
    candidates = [(n, sql) for n, sql in cur.fetchall() if sql and "metadata_location" in sql]
    if not candidates:
        con.close()
        return []

    preferred = ("iceberg_tables", "tables", "pyiceberg_tables")
    ordered = [p for p, _ in candidates if p in preferred] + [n for n, _ in candidates if n not in preferred]

    for tbl in ordered:
        try:
            cur.execute(f"PRAGMA table_info({tbl})")
            cols = [r[1] for r in cur.fetchall()]
            s = set(cols)
            if "table_identifier" in s:
                q = f"SELECT table_identifier, metadata_location FROM {tbl}"
            elif "identifier" in s:
                q = f"SELECT identifier AS table_identifier, metadata_location FROM {tbl}"
            elif {"table_namespace","table_name"}.issubset(s):
                q = f"SELECT (table_namespace || '.' || table_name) AS table_identifier, metadata_location FROM {tbl}"
            elif {"namespace","table_name"}.issubset(s):
                q = f"SELECT (namespace || '.' || table_name) AS table_identifier, metadata_location FROM {tbl}"
            elif {"namespace","name"}.issubset(s):
                q = f"SELECT (namespace || '.' || name) AS table_identifier, metadata_location FROM {tbl}"
            else:
                continue
            cur.execute(q)
            rows = [(r[0], r[1]) for r in cur.fetchall()]
            con.close()
            return rows
        except Exception:
            continue
    con.close()
    return []


# -------------------- main --------------------
def main():
    ap = argparse.ArgumentParser(description="One-time update of SQLite Iceberg catalog to NEW warehouse.")
    ap.add_argument("--sqlite-db", required=True, help="Path to the *new* SQLite catalog file, e.g., /.../new/iceberg.db")
    ap.add_argument("--warehouse", required=True, help="New warehouse URI, e.g., file:///.../new/warehouse")
    ap.add_argument("--tables", help="Comma-separated list of identifiers (ns.table). If omitted, auto-discovers from SQLite.")
    ap.add_argument("--iceberg", default=ICEBERG_COORD, help="Iceberg Spark runtime coord")
    ap.add_argument("--sqlite-jdbc", default=SQLITE_COORD, help="SQLite JDBC coord")
    args = ap.parse_args()

    sqlite_db = os.path.abspath(args.sqlite_db)
    wh = args.warehouse.rstrip("/")

    if not os.path.exists(sqlite_db):
        raise SystemExit(f"[ERROR] SQLite DB not found: {sqlite_db}")

    # Build the table list
    if args.tables:
        idents = [t.strip() for t in args.tables.split(",") if t.strip()]
        rows = [(i, "") for i in idents]  # we don't need the old metadata_location
    else:
        rows = discover_tables_from_sqlite(sqlite_db)
        idents = [i for i, _ in rows]
        if not idents:
            raise SystemExit("[ERROR] No tables discovered in SQLite. Use --tables to specify manually.")

    print(f"[INFO] Using catalog: jdbc:sqlite:{sqlite_db}")
    print(f"[INFO] New warehouse : {wh}")
    print(f"[INFO] Tables: {', '.join(idents)}")

    # Start Spark on the NEW catalog (this writes pointers into this DB)
    spark = (
        SparkSession.builder
        .appName("Iceberg-JDBC-UpdateCatalogToNewWarehouse")
        .config("spark.jars.packages", f"{args.iceberg},{args.sqlite_jdbc}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "jdbc")
        .config("spark.sql.catalog.ice.uri", f"jdbc:sqlite:{sqlite_db}")
        .config("spark.sql.catalog.ice.driver", "org.sqlite.JDBC")
        .config("spark.sql.catalog.ice.warehouse", wh)  # default for new tables; not used to override existing pointers
        .config("spark.sql.catalog.ice.jdbc.schema-version", "V1")
        .getOrCreate()
    )

    failures = []
    for identifier in idents:
        ns, tbl = identifier.split(".", 1) if "." in identifier else ("default", identifier)

        # Expected table dir under NEW warehouse (typical JDBC layout <wh>/<ns>.db/<tbl>)
        tbl_dir_uri = f"{wh}/{ns}.db/{tbl}"
        meta_dir_uri = f"{tbl_dir_uri}/metadata"
        latest_new_meta = latest_metadata_in_dir(meta_dir_uri)

        print(f"\n=== {identifier} ===")
        if not latest_new_meta:
            print(f"[ERROR] No metadata files found in: {meta_dir_uri}")
            failures.append(identifier)
            continue

        qid = qualify("ice", identifier)
        print(f"[STEP] DROP+REGISTER → {latest_new_meta}")

        # 1) Drop catalog entry only (does NOT delete files)
        try:
            spark.sql(f"DROP TABLE IF EXISTS {qid}")
            print("[INFO] Dropped existing catalog entry (if any).")
        except Exception as e:
            print(f"[WARN] DROP TABLE failed (continuing): {e}")

        # 2) Register to NEW metadata json
        try:
            spark.sql(f"""
              CALL ice.system.register_table(
                table => '{identifier}',
                metadata_file => '{latest_new_meta}'
              )
            """).show(truncate=False)
            print("[INFO] Registered to NEW metadata.")
        except Exception as e:
            print(f"[ERROR] register_table failed: {e}")
            failures.append(identifier)
            continue

        # 3) Sanity check
        try:
            spark.sql(f"SELECT COUNT(*) AS cnt FROM {qid}").show()
            spark.sql(f"SELECT file_path FROM {qid}.files LIMIT 5").show(truncate=False)
        except Exception as e:
            print(f"[WARN] Sanity query failed: {e}")

    if failures:
        print("\n[WARN] Some tables were not updated:")
        for t in failures:
            print(" -", t)
    else:
        print("\n[OK] All tables updated in the new SQLite catalog.")

    spark.stop()


if __name__ == "__main__":
    main()
