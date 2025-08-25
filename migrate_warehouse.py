#!/usr/bin/env python3
"""
Repoint Apache Iceberg tables from an OLD warehouse root to a NEW warehouse root
by rewriting path prefixes in metadata (no dropping/re-creating tables).

Steps per table:
  - Ensure it's visible in spark_catalog (register from metadata if needed)
  - CALL spark_catalog.system.rewrite_table_path for:
      * table dir:    .../<ns(.ns2...)>/<table>
      * metadata dir: .../<ns(.ns2...)>/<table>/metadata
    with variants for file:/ vs file:///

Usage:
  python repoint_warehouse_root.py <OLD_PARENT_DIR> <NEW_WAREHOUSE_PATH> [--rewrite-manifests]

Example:
  python repoint_warehouse_root.py /your/old/parent/ /your/new/warehouse/
"""

import os
import sys
import sqlite3
from urllib.parse import urlparse
from pyspark.sql import SparkSession

ICEBERG_COORD = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"

# ---------- helpers ----------
def fs2uri(p: str) -> str:
    return "file://" + os.path.abspath(p)

def ensure_file_uri(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("file:/"):
        return s
    return fs2uri(s)

def table_dir_from_metadata_uri(meta_uri: str) -> str:
    # meta_uri: file:///.../warehouse/<ns(.ns2...)>/<tbl>/metadata/<file>
    u = urlparse(meta_uri)
    parts = u.path.rstrip("/").split("/")
    parts = parts[:-1]              # drop filename
    if parts and parts[-1] == "metadata":
        parts = parts[:-1]          # drop metadata dir
    if not parts:
        raise ValueError(f"Bad metadata URI: {meta_uri}")
    return f"{u.scheme}://{'/'.join(parts)}"

def discover_rows(sqlite_path: str):
    """Return [(registered_identifier, metadata_location), ...] from a PyIceberg SqlCatalog (SQLite)."""
    con = sqlite3.connect(sqlite_path)
    cur = con.cursor()
    cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
    candidates = [(n, sql) for n, sql in cur.fetchall() if sql and "metadata_location" in sql]
    if not candidates:
        con.close()
        raise RuntimeError("No table in SQLite contains 'metadata_location'.")

    preferred = ("iceberg_tables", "tables", "pyiceberg_tables")
    ordered = [p for p,_ in candidates if p in preferred] + [n for n,_ in candidates if n not in preferred]

    last_err = None
    for tbl in ordered:
        try:
            cur.execute(f"PRAGMA table_info({tbl})")
            cols = [r[1] for r in cur.fetchall()]
            s = set(cols)
            if "table_identifier" in s:
                q = f"SELECT table_identifier, metadata_location FROM {tbl}"
            elif "identifier" in s:
                q = f"SELECT identifier AS table_identifier, metadata_location FROM {tbl}"
            elif {"namespace","table_name"}.issubset(s):
                q = f"SELECT (namespace || '.' || table_name) AS table_identifier, metadata_location FROM {tbl}"
            elif {"namespace","name"}.issubset(s):
                q = f"SELECT (namespace || '.' || name) AS table_identifier, metadata_location FROM {tbl}"
            elif {"table_namespace","table_name"}.issubset(s):
                q = f"SELECT (table_namespace || '.' || table_name) AS table_identifier, metadata_location FROM {tbl}"
            else:
                last_err = f"Unknown schema for {tbl}: {cols}"
                continue
            cur.execute(q)
            rows = cur.fetchall()
            con.close()
            return rows
        except Exception as e:
            last_err = str(e)
            continue

    con.close()
    raise RuntimeError(last_err or "Failed to read tables from SQLite catalog.")

def qualify(catalog: str, identifier: str) -> str:
    # 'local.my_table' -> spark_catalog.`local`.`my_table`
    parts = identifier.split(".")
    return f"{catalog}." + ".".join(f"`{p}`" for p in parts)

def add_variant(pairset: set, src: str, dst: str):
    pairset.add((src, dst))
    if src.startswith("file:///"):
        pairset.add(("file:/" + src[len("file:///"):], dst))

def ensure_registered(spark: SparkSession, identifier: str, metadata_location: str):
    q_id = qualify("spark_catalog", identifier)
    try:
        spark.sql(f"SELECT 1 FROM {q_id} LIMIT 1").collect()
        print(f"[INFO] Visible in spark_catalog: {identifier}")
        return
    except Exception:
        print(f"[INFO] Not visible; registering from metadata: {identifier}")
        spark.sql(f"""
          CALL spark_catalog.system.register_table(
            table => '{identifier}',
            metadata_file => '{metadata_location}'
          )
        """).show(truncate=False)
        spark.sql(f"SELECT 1 FROM {q_id} LIMIT 1").collect()
        print(f"[INFO] Registered: {identifier}")

# ---------- main ----------
def main():
    if len(sys.argv) < 3:
        print("Usage: python repoint_warehouse_root.py <OLD_PARENT_DIR> <NEW_WAREHOUSE_PATH> [--rewrite-manifests]")
        sys.exit(1)

    old_parent = os.path.abspath(sys.argv[1]).rstrip("/")
    new_wh     = os.path.abspath(sys.argv[2]).rstrip("/")
    do_rm      = ("--rewrite-manifests" in sys.argv[3:])

    sqlite_path = os.path.join(old_parent, "iceberg.db")
    old_wh      = os.path.join(old_parent, "warehouse")

    if not os.path.exists(sqlite_path):
        print(f"[ERROR] Missing SQLite catalog at: {sqlite_path}"); sys.exit(1)
    if not os.path.isdir(old_wh):
        print(f"[ERROR] Missing warehouse at: {old_wh}"); sys.exit(1)

    spark = (
        SparkSession.builder
        .config("spark.jars.packages", ICEBERG_COORD)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        # Use NEW warehouse going forward; the rewrite uses explicit prefixes anyway.
        .config("spark.sql.catalog.spark_catalog.warehouse", fs2uri(new_wh))
        .getOrCreate()
    )

    print(f"[INFO] Old parent : {old_parent}")
    print(f"[INFO] Old wh     : {old_wh}")
    print(f"[INFO] New wh     : {new_wh}")
    print(f"[INFO] SQLite     : {sqlite_path}")
    print(f"[INFO] Iceberg    : {ICEBERG_COORD}")

    # IMPORTANT: If you're renaming the directory, do that on disk now (before running):
    #   mv /path/to/old/warehouse  /path/to/new/warehouse

    try:
        rows = discover_rows(sqlite_path)
    except Exception as e:
        print(f"[ERROR] Reading SQLite: {e}"); sys.exit(1)
    if not rows:
        print("[WARN] No tables found."); return

    old_wh_uri = fs2uri(old_wh)
    new_wh_uri = fs2uri(new_wh)

    for identifier, metadata_location in rows:
        identifier = (identifier or "").strip()
        metadata_location = ensure_file_uri(metadata_location)
        q_id = qualify("spark_catalog", identifier)

        try:
            ensure_registered(spark, identifier, metadata_location)
        except Exception as e:
            print(f"[ERROR] ensure_registered({identifier}): {e}")
            continue

        # Compute actual on-disk table dir from metadata location
        try:
            src_table_dir = table_dir_from_metadata_uri(metadata_location)
        except Exception as e:
            print(f"[ERROR] {identifier}: {e}"); continue

        if not src_table_dir.startswith(old_wh_uri):
            print(f"[ERROR] {identifier}: table dir not under old warehouse.")
            print(f"        src_table_dir = {src_table_dir}")
            print(f"        old_wh_uri    = {old_wh_uri}")
            continue

        rel = src_table_dir[len(old_wh_uri):].lstrip("/")
        dst_table_dir = f"{new_wh_uri}/{rel}"
        src_meta_dir  = src_table_dir.rstrip("/") + "/metadata"
        dst_meta_dir  = dst_table_dir.rstrip("/") + "/metadata"

        print(f"\n=== Repointing {identifier} ===")
        print(f"[STEP] table: {src_table_dir}  ->  {dst_table_dir}")
        print(f"[STEP] meta : {src_meta_dir}   ->  {dst_meta_dir}")

        # Build rewrite attempts (table dir + metadata dir, file:/ variants)
        attempts = set()
        add_variant(attempts, src_table_dir, dst_table_dir)
        add_variant(attempts, src_meta_dir,  dst_meta_dir)

        # If your NEW warehouse accidentally wrote paths like .../local/<tbl> (dropped ".db"),
        # also normalize those to .../local.db/<tbl>.
        parts = rel.split("/")
        if len(parts) >= 2 and parts[0].endswith(".db"):
            ns_dir, tbl_dir = parts[0], parts[1]
            bad_ns = ns_dir[:-3]  # strip '.db'
            bad_new_tbl = f"{new_wh_uri}/{bad_ns}/{tbl_dir}"
            bad_new_meta = bad_new_tbl + "/metadata"
            add_variant(attempts, bad_new_tbl,  dst_table_dir)
            add_variant(attempts, bad_new_meta, dst_meta_dir)

        # Execute rewrites
        for s_prefix, t_prefix in attempts:
            print(f"[CALL] rewrite_table_path: {s_prefix} -> {t_prefix}")
            try:
                spark.sql(f"""
                  CALL spark_catalog.system.rewrite_table_path(
                    table => '{identifier}',
                    source_prefix => '{s_prefix}',
                    target_prefix => '{t_prefix}'
                  )
                """).show(truncate=False)
            except Exception as e:
                msg = str(e)
                if "does not start with" in msg or "doesn't start with" in msg:
                    print(f"[INFO] No matches for: {s_prefix} (skipped)")
                else:
                    print(f"[ERROR] rewrite_table_path failed: {e}")

        # Optional manifest compaction (only after paths look correct)
        if do_rm:
            try:
                print("[STEP] rewrite_manifests")
                spark.sql(f"CALL spark_catalog.system.rewrite_manifests(table => '{identifier}')").show(truncate=False)
            except Exception as e:
                print(f"[WARN] rewrite_manifests failed: {e}")
        else:
            print("[INFO] Skipping rewrite_manifests (enable with --rewrite-manifests)")

        # Sanity read
        try:
            print("[TEST] COUNT/SAMPLE")
            spark.sql(f"SELECT COUNT(*) AS cnt FROM {q_id}").show()
            spark.sql(f"SELECT * FROM {q_id} LIMIT 5").show(truncate=False)
        except Exception as e:
            print(f"[WARN] Sanity query failed for {identifier}: {e}")

    print("\n[OK] Repoint finished. Update configs to the NEW warehouse root.")
    print("Spark config:  spark.sql.catalog.spark_catalog.warehouse = file:///.../new/warehouse")
    print("PyIceberg (~/.pyiceberg.yaml) -> catalog warehouse: file:///.../new/warehouse")

if __name__ == "__main__":
    main()
