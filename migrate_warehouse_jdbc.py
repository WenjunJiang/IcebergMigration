#!/usr/bin/env python3
"""
Repoint Apache Iceberg tables from an OLD warehouse root to a NEW warehouse root
by rewriting path prefixes in metadata (no dropping/re-creating tables).

Usage:
  python migrate_warehouse_jdbc.py <OLD_PARENT_DIR> <NEW_WAREHOUSE_PATH> [--rewrite-manifests]
"""

import os
import re
import sys
import sqlite3
from urllib.parse import urlparse
from pyspark.sql import SparkSession
import shutil

ICEBERG_COORD = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"
SQLITE_COORD  = "org.xerial:sqlite-jdbc:3.50.3.0"

# ---------- helpers ----------
def stage_metadata_json(old_tbl_dir_uri: str, new_tbl_dir_uri: str) -> str | None:
    """If NEW <table>/metadata/<latest-json> is missing, copy it from OLD and return NEW json URI."""
    old_meta_dir_uri = old_tbl_dir_uri.rstrip("/") + "/metadata"
    new_meta_dir_uri = new_tbl_dir_uri.rstrip("/") + "/metadata"
    latest_old = latest_metadata_in_dir(old_meta_dir_uri)
    if not latest_old:
        return None
    old_json_path = uri_to_path(latest_old)
    new_meta_dir = uri_to_path(new_meta_dir_uri)
    os.makedirs(new_meta_dir, exist_ok=True)
    new_json_path = os.path.join(new_meta_dir, os.path.basename(old_json_path))
    if not os.path.exists(new_json_path):
        shutil.copy2(old_json_path, new_json_path)
    return path_to_uri(new_json_path)

def fs2uri(p: str) -> str:
    return "file://" + os.path.abspath(p)

def normalize_file_uri(u: str) -> str:
    if u.startswith("file:/") and not u.startswith("file:///"):
        return "file://" + u[len("file:"):]
    if u.startswith("/"):
        return "file://" + u
    return u

def file_uri_variants(u: str):
    u = normalize_file_uri(u)
    return [u, "file:/" + u[len("file:///") :]] if u.startswith("file:///") else [u]

def uri_to_path(u: str) -> str:
    return urlparse(normalize_file_uri(u)).path

def path_to_uri(p: str) -> str:
    return "file://" + os.path.abspath(p)

def table_dir_from_metadata_uri(meta_uri: str) -> str:
    u = urlparse(normalize_file_uri(meta_uri))
    if u.scheme != "file":
        raise ValueError(f"Only file:// metadata supported: {meta_uri}")
    parts = u.path.split("/")
    if parts and parts[-1].endswith(".json"):
        parts = parts[:-1]
    if parts and parts[-1] == "metadata":
        parts = parts[:-1]
    if not parts:
        raise ValueError(f"Bad metadata URI: {meta_uri}")
    return f"{u.scheme}://{'/'.join(parts)}"

def latest_metadata_in_dir(meta_dir_uri: str) -> str | None:
    """Return URI of highest 000xx-*.metadata.json in meta dir, or None."""
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

def discover_rows(sqlite_path: str):
    """Return [(identifier, metadata_location), ...] from SQLite; handle common schemas."""
    con = sqlite3.connect(sqlite_path)
    cur = con.cursor()
    cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
    candidates = [(n, sql) for n, sql in cur.fetchall() if sql and "metadata_location" in sql]
    if not candidates:
        con.close()
        raise RuntimeError("No table in SQLite contains 'metadata_location'.")
    preferred = ("iceberg_tables", "tables", "pyiceberg_tables")
    ordered = [p for p, _ in candidates if p in preferred] + [n for n, _ in candidates if n not in preferred]

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
            elif {"table_namespace","table_name"}.issubset(s):
                q = f"SELECT (table_namespace || '.' || table_name) AS table_identifier, metadata_location FROM {tbl}"
            elif {"namespace","table_name"}.issubset(s):
                q = f"SELECT (namespace || '.' || table_name) AS table_identifier, metadata_location FROM {tbl}"
            elif {"namespace","name"}.issubset(s):
                q = f"SELECT (namespace || '.' || name) AS table_identifier, metadata_location FROM {tbl}"
            else:
                raise RuntimeError(f"Unrecognized catalog schema for table {tbl}; cols={cols}")
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
    parts = identifier.split(".")
    return f"{catalog}." + ".".join(f"`{p}`" for p in parts)

def add_variant(pairset: set, src: str, dst: str):
    for s in file_uri_variants(src):
        pairset.add((s, dst))

def bootstrap_sqlite_catalog(sqlite_path: str, old_wh_uri: str, new_wh_uri: str) -> int:
    """Replace old->new warehouse prefix in metadata_location/previous_metadata_location."""
    con = sqlite3.connect(sqlite_path)
    cur = con.cursor()
    cur.execute("SELECT name, sql FROM sqlite_master WHERE type='table'")
    tbls = [(n, sql) for n, sql in cur.fetchall() if sql]

    changed = 0
    for tbl, _ in tbls:
        cur.execute(f"PRAGMA table_info({tbl})")
        cols = [r[1] for r in cur.fetchall()]
        targets = [c for c in cols if c in ("metadata_location", "previous_metadata_location")]
        if not targets:
            continue
        for col in targets:
            for old_v in file_uri_variants(old_wh_uri):
                for new_v in file_uri_variants(new_wh_uri):
                    cur.execute(
                        f"UPDATE {tbl} SET {col} = REPLACE({col}, ?, ?) WHERE {col} LIKE ?",
                        (old_v.rstrip("/") + "/", new_v.rstrip("/") + "/", old_v.rstrip("/") + "/%")
                    )
                    if cur.rowcount and cur.rowcount > 0:
                        changed += cur.rowcount
    con.commit()
    con.close()
    return changed

def ensure_registered(
    spark: SparkSession,
    identifier: str,
    metadata_location: str,
    old_wh_uri: str,
    new_wh_uri: str,
):
    """
    Make the table queryable in the JDBC catalog by registering it with a
    metadata JSON that actually exists. Looks in BOTH new and old metadata dirs
    and tries the highest-numbered file first. Skips non-existent candidates.
    """
    q_id = qualify("ice", identifier)

    # Already queryable?
    try:
        spark.sql(f"SELECT 1 FROM {q_id} LIMIT 1").collect()
        print(f"[INFO] Visible in ice: {identifier}")
        return
    except Exception:
        pass

    meta_uri = normalize_file_uri(metadata_location)
    # Derive table dir from the (possibly bootstrapped) metadata location
    try:
        tbl_dir = table_dir_from_metadata_uri(meta_uri)
    except Exception:
        tbl_dir = None

    # Compute both NEW and OLD table dirs using relative path mapping
    new_tbl_dir = None
    old_tbl_dir = None
    if tbl_dir and tbl_dir.startswith(new_wh_uri.rstrip("/") + "/"):
        rel = tbl_dir[len(new_wh_uri):].lstrip("/")
        new_tbl_dir = tbl_dir
        old_tbl_dir = f"{old_wh_uri}/{rel}"
    elif tbl_dir and tbl_dir.startswith(old_wh_uri.rstrip("/") + "/"):
        rel = tbl_dir[len(old_wh_uri):].lstrip("/")
        old_tbl_dir = tbl_dir
        new_tbl_dir = f"{new_wh_uri}/{rel}"
    else:
        # Fallback: guess by replacing prefix inside the metadata URI itself
        for ov in file_uri_variants(old_wh_uri):
            if meta_uri.startswith(ov.rstrip("/") + "/"):
                rel = meta_uri[len(ov):].lstrip("/")
                old_tbl_dir = table_dir_from_metadata_uri(ov + "/" + rel)
                new_tbl_dir = table_dir_from_metadata_uri(new_wh_uri + "/" + rel)
                break

    candidates = []

    # If NEW metadata json doesn’t exist, stage a copy from OLD so we can register on NEW.
    if new_tbl_dir and old_tbl_dir:
        staged_new = stage_metadata_json(old_tbl_dir, new_tbl_dir)
        if staged_new:
            candidates.append(staged_new)  # prefer registering on NEW path

    # Latest in NEW metadata dir
    if new_tbl_dir:
        latest_new = latest_metadata_in_dir(new_tbl_dir.rstrip("/") + "/metadata")
        if latest_new:
            candidates.append(latest_new)

    # The relocated metadata (old->new) if it exists
    if meta_uri.startswith(old_wh_uri.rstrip("/") + "/"):
        relocated = new_wh_uri + meta_uri[len(old_wh_uri):]
        candidates.append(relocated)
    elif meta_uri.startswith(new_wh_uri.rstrip("/") + "/"):
        candidates.append(meta_uri)

    # Latest in OLD metadata dir
    if old_tbl_dir:
        latest_old = latest_metadata_in_dir(old_tbl_dir.rstrip("/") + "/metadata")
        if latest_old:
            candidates.append(latest_old)

    # De-dupe and KEEP ORDER
    seen = set()
    ordered = []
    for c in [normalize_file_uri(x) for x in candidates]:
        if c not in seen:
            seen.add(c)
            ordered.append(c)

    print(f"[INFO] Not visible; attempting register(s) for {identifier}")
    any_tried = False
    for m in ordered:
        exists = os.path.exists(uri_to_path(m))
        if not exists:
            print(f"  -> skip (missing): {m}")
            continue
        any_tried = True
        try:
            print(f"  -> register_table with {m}")
            spark.sql(f"""
              CALL ice.system.register_table(
                table => '{identifier}',
                metadata_file => '{m}'
              )
            """).show(truncate=False)
            # Verify it is now queryable
            spark.sql(f"SELECT 1 FROM {q_id} LIMIT 1").collect()
            print(f"[INFO] Registered: {identifier} (metadata_file={m})")
            return
        except Exception as e:
            msg = str(e)
            # If it already exists in the catalog, try to query; if query works, we're done.
            if "already exists" in msg or "Table exists" in msg:
                try:
                    spark.sql(f"SELECT 1 FROM {q_id} LIMIT 1").collect()
                    print(f"[INFO] Table already exists and is queryable: {identifier}")
                    return
                except Exception:
                    print(f"[WARN] Exists but not queryable yet: {e}")
            else:
                print(f"[WARN] register_table attempt failed for {identifier} with {m}: {e}")

    if not any_tried:
        raise RuntimeError(
            f"No usable metadata JSON found for {identifier}.\n"
            f"Checked NEW: {new_tbl_dir}/metadata and OLD: {old_tbl_dir}/metadata"
        )
    raise RuntimeError(f"Could not register/see table {identifier}")

# ---------- main ----------
def main():
    if len(sys.argv) < 3:
        print("Usage: python migrate_warehouse_jdbc.py <OLD_PARENT_DIR> <NEW_WAREHOUSE_PATH> [--rewrite-manifests]")
        sys.exit(1)

    old_parent = os.path.abspath(sys.argv[1]).rstrip("/")
    new_wh_fs  = os.path.abspath(sys.argv[2]).rstrip("/")
    do_rm      = ("--rewrite-manifests" in sys.argv[3:])

    sqlite_path = os.path.join(old_parent, "iceberg.db")
    old_wh_fs   = os.path.join(old_parent, "warehouse")

    print(f"[INFO] Old parent : {old_parent}")
    print(f"[INFO] Old wh     : {old_wh_fs}")
    print(f"[INFO] New wh     : {new_wh_fs}")
    print(f"[INFO] SQLite     : {sqlite_path}")
    print(f"[INFO] Iceberg    : {ICEBERG_COORD}")
    print(f"[INFO] SQLite JDBC: {SQLITE_COORD}")

    if not os.path.exists(sqlite_path):
        print(f"[ERROR] Missing SQLite catalog at: {sqlite_path}"); sys.exit(1)
    if not os.path.isdir(old_wh_fs):
        print(f"[ERROR] Missing OLD warehouse at: {old_wh_fs}"); sys.exit(1)
    if not os.path.isdir(new_wh_fs):
        print(f"[ERROR] Missing NEW warehouse at: {new_wh_fs}"); sys.exit(1)

    old_wh_uri = fs2uri(old_wh_fs)
    new_wh_uri = fs2uri(new_wh_fs)

    # Step 0: Try to rewrite SQLite metadata prefix (ok if it doesn't match anything)
    updated = bootstrap_sqlite_catalog(sqlite_path, old_wh_uri, new_wh_uri)
    print(f"[INFO] Catalog bootstrap updates: {updated} field(s) changed")

    # Step 1: discover tables
    try:
        rows = discover_rows(sqlite_path)
    except Exception as e:
        print(f"[ERROR] Reading SQLite: {e}"); sys.exit(1)
    if not rows:
        print("[INFO] No tables found in catalog 'ice'. Nothing to do.")
        return

    # Step 2: Spark session on JDBC catalog
    spark = (
        SparkSession.builder
        .config("spark.jars.packages", f"{ICEBERG_COORD},{SQLITE_COORD}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.ice", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.ice.type", "jdbc")
        .config("spark.sql.catalog.ice.uri", f"jdbc:sqlite:{sqlite_path}")
        .config("spark.sql.catalog.ice.driver", "org.sqlite.JDBC")
        .config("spark.sql.catalog.ice.warehouse", new_wh_uri)
        .config("spark.sql.catalog.ice.jdbc.schema-version", "V1")
        .getOrCreate()
    )

    # Step 3: per-table registration + rewrite
    for identifier, metadata_location in rows:
        identifier = (identifier or "").strip()
        metadata_location = normalize_file_uri(metadata_location)
        print(f"\n=== Repointing {identifier} ===")

        # Ensure registered/visible (uses robust metadata search)
        try:
            ensure_registered(spark, identifier, metadata_location, old_wh_uri, new_wh_uri)
        except Exception as e:
            print(f"[ERROR] Cannot open/register {identifier}: {e}")
            continue

        # Now compute table dirs from whichever side the catalog currently uses
        try:
            tbl_dir = table_dir_from_metadata_uri(metadata_location)
        except Exception:
            # Derive from identifier (namespace.table)
            ns, tbl = identifier.split(".", 1) if "." in identifier else ("default", identifier)
            # Most local catalogs layout like: <warehouse>/<ns>.db/<table>
            tbl_dir = f"{old_wh_uri}/{ns}.db/{tbl}"

        # Build both source/dest table-dir pairs for rewrite_table_path
        if tbl_dir.startswith(old_wh_uri.rstrip("/") + "/"):
            rel = tbl_dir[len(old_wh_uri):].lstrip("/")
            src_table_dir = tbl_dir
            dst_table_dir = f"{new_wh_uri}/{rel}"
        else:
            # If it already points to NEW, still try a warehouse-level rewrite to catch stragglers
            src_table_dir = f"{old_wh_uri}/" + tbl_dir.split("/")[-2] + "/" + tbl_dir.split("/")[-1]
            dst_table_dir = tbl_dir

        print(f"[STEP] table dir rewrite candidate: {src_table_dir} -> {dst_table_dir}")

        attempts = set()
        add_variant(attempts, old_wh_uri, new_wh_uri)          # warehouse-level
        add_variant(attempts, src_table_dir, dst_table_dir)    # table-level

        for s_prefix, t_prefix in attempts:
            print(f"[CALL] rewrite_table_path: {s_prefix} -> {t_prefix}")
            try:
                spark.sql(f"""
                  CALL ice.system.rewrite_table_path(
                    table => '{identifier}',
                    source_prefix => '{s_prefix}',
                    target_prefix => '{t_prefix}'
                  )
                """).show(truncate=False)
            except Exception as e:
                msg = str(e)
                if ("does not start with" in msg) or ("doesn't start with" in msg) or ("No matching paths" in msg):
                    print(f"[INFO] No matches for: {s_prefix} (skipped)")
                else:
                    print(f"[ERROR] rewrite_table_path failed: {e}")

        if "--rewrite-manifests" in sys.argv[3:]:
            try:
                print("[STEP] rewrite_manifests")
                spark.sql(f"CALL ice.system.rewrite_manifests(table => '{identifier}')").show(truncate=False)
            except Exception as e:
                print(f"[WARN] rewrite_manifests failed: {e}")
        else:
            print("[INFO] Skipping rewrite_manifests (enable with --rewrite-manifests)")

        # Quick sanity
        try:
            q_id = qualify("ice", identifier)
            print("[TEST] COUNT/SAMPLE")
            spark.sql(f"SELECT COUNT(*) AS cnt FROM {q_id}").show()
            spark.sql(f"SELECT * FROM {q_id} LIMIT 5").show(truncate=False)
        except Exception as e:
            print(f"[WARN] Sanity query failed for {identifier}: {e}")

    print("\n[OK] Repoint finished. Update configs to the NEW warehouse root.")
    print("Spark config:  spark.sql.catalog.ice.warehouse = file:///.../new/warehouse")
    print("PyIceberg (~/.pyiceberg.yaml) -> catalog warehouse: file:///.../new/warehouse")

if __name__ == "__main__":
    main()
