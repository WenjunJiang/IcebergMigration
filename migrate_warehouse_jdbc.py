#!/usr/bin/env python3
"""
Repoint Apache Iceberg tables from an OLD warehouse root to a NEW warehouse root
by rewriting path prefixes in metadata, copying files per the copy plan,
and switching the catalog entry via DROP + REGISTER (no direct SQLite updates).

Usage:
  python migrate_warehouse_jdbc.py <OLD_PARENT_DIR> <NEW_WAREHOUSE_PATH> [--rewrite-manifests]
"""

import os
import re
import sys
import csv
import shutil
from urllib.parse import urlparse
from pyspark.sql import SparkSession
import sqlite3  # only used to "discover" tables; not for updates

ICEBERG_COORD = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"
SQLITE_COORD  = "org.xerial:sqlite-jdbc:3.50.3.0"

# ---------- path helpers ----------
def fs2uri(p: str) -> str:
    return "file://" + os.path.abspath(p)

def normalize_file_uri(u: str) -> str:
    # Normalize common 'file:' forms and raw POSIX paths to consistent interpretation
    if u.startswith("file:/") and not u.startswith("file:///"):
        return "file://" + u[len("file:"):]  # normalize to file:///
    if u.startswith("/"):
        return u  # raw path
    return u

def make_file_triple(path_str: str) -> str:
    return "file:///" + path_str.lstrip("/")

def make_file_single(path_str: str) -> str:
    return "file:/" + path_str.lstrip("/")

def file_uri_variants(u: str):
    """
    Return likely representations for the SAME absolute location:
      - file:///Users/... (triple slash)
      - file:/Users/...   (single slash)
      - /Users/...        (raw POSIX path)
    Also accepts raw path input and returns file:// forms.
    """
    u = normalize_file_uri(u)
    out = []
    if u.startswith("file:///"):
        p = urlparse(u).path
        out.extend([u, make_file_single(p), p])
    elif u.startswith("file:/"):
        p = urlparse(u).path
        out.extend([u, make_file_triple(p), p])
    elif u.startswith("/"):
        p = u
        out.extend([p, make_file_triple(p), make_file_single(p)])
    else:
        out.append(u)
    # unique, stable order
    seen, ordered = set(), []
    for x in out:
        if x not in seen:
            seen.add(x); ordered.append(x)
    return ordered

def uri_to_path(u: str) -> str:
    u = normalize_file_uri(u)
    if u.startswith("file:/"):
        return urlparse(u).path
    return u

def path_to_uri(p: str) -> str:
    return make_file_triple(os.path.abspath(p))

def with_slash(p: str) -> str:
    return p.rstrip("/") + "/"

def table_dir_from_metadata_uri(meta_uri: str) -> str:
    u = normalize_file_uri(meta_uri)
    scheme = "file" if u.startswith("file:/") else ""
    path = urlparse(u).path if scheme else u
    parts = path.split("/")
    if parts and parts[-1].endswith(".json"):
        parts = parts[:-1]
    if parts and parts[-1] == "metadata":
        parts = parts[:-1]
    if not parts:
        raise ValueError(f"Bad metadata URI: {meta_uri}")
    # Return in file:/// form by default
    return make_file_triple("/".join(parts))

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

def stage_metadata_json(old_tbl_dir_uri: str, new_tbl_dir_uri: str) -> str | None:
    """If NEW <table>/metadata/<latest-json> is missing, copy it from OLD and return NEW json URI."""
    old_meta_dir_uri = with_slash(old_tbl_dir_uri) + "metadata"
    new_meta_dir_uri = with_slash(new_tbl_dir_uri) + "metadata"
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

# ---------- catalog discovery (read-only) ----------
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

# ---------- copy plan ----------
def copy_plan_csv(file_list_uri: str) -> int:
    """
    Copy files according to the copy plan generated by rewrite_table_path.
    Accepts either a single CSV file path or a directory containing CSV part files.
    Each row is 'sourcepath,targetpath' (no header).
    Returns number of files copied (skips when src == dst or already exists with same size).
    """
    u = normalize_file_uri(file_list_uri)
    if not (u.startswith("file:/") or u.startswith("/")):
        print(f"[WARN] Non-file scheme copy plan not supported: {file_list_uri}")
        return 0

    root = uri_to_path(u)
    plan_files: list[str] = []

    if os.path.isdir(root):
        # Typical Spark output: a folder like .../_rewrite_stage/file-list/ with part files
        for name in sorted(os.listdir(root)):
            if name.startswith("_") or name.startswith("."):
                continue  # skip _SUCCESS, hidden files, etc.
            full = os.path.join(root, name)
            if os.path.isfile(full):
                plan_files.append(full)
        if not plan_files:
            print(f"[WARN] Copy plan directory is empty: {file_list_uri}")
            return 0
    else:
        if not os.path.exists(root):
            print(f"[WARN] Missing copy plan CSV: {file_list_uri}")
            return 0
        plan_files.append(root)

    copied = 0
    seen_pairs = set()  # de-dupe across multiple part files
    for pf in plan_files:
        with open(pf, "r", newline="") as f:
            reader = csv.reader(f)
            for row in reader:
                if not row or len(row) < 2:
                    continue
                src_u = normalize_file_uri(row[0].strip().strip('"'))
                dst_u = normalize_file_uri(row[1].strip().strip('"'))
                src = os.path.abspath(uri_to_path(src_u))
                dst = os.path.abspath(uri_to_path(dst_u))
                if (src, dst) in seen_pairs or src == dst:
                    continue
                seen_pairs.add((src, dst))

                if not os.path.exists(src):
                    print(f"[INFO] Skip missing src: {src_u}")
                    continue
                os.makedirs(os.path.dirname(dst), exist_ok=True)
                # Skip if already identical size (cheap idempotency guard)
                if os.path.exists(dst) and os.path.getsize(dst) == os.path.getsize(src):
                    continue
                shutil.copy2(src, dst)
                copied += 1

    return copied

# ---------- prefix pairing ----------
def align_target_style(src_prefix: str, dst_prefix: str) -> str:
    """
    Convert target prefix to the same URI/path style as the source prefix:
      - if src is 'file:///...', make dst 'file:///...'
      - if src is 'file:/...',   make dst 'file:/...'
      - if src is raw '/...',    make dst '/...'
    """
    dst_path = uri_to_path(dst_prefix) if dst_prefix.startswith("file:") else dst_prefix
    if src_prefix.startswith("file:///"):
        return with_slash(make_file_triple(dst_path))
    if src_prefix.startswith("file:/"):
        return with_slash(make_file_single(dst_path))
    if src_prefix.startswith("/"):
        return with_slash(dst_path)
    return with_slash(dst_prefix)

def add_style_pairs(pairset: set, src_base: str, dst_base: str):
    """For every plausible source style, add a (source, target) pair with aligned target style."""
    for s in file_uri_variants(src_base):
        t = align_target_style(s, dst_base)
        pairset.add((with_slash(s), with_slash(t)))

# ---------- registration helper for pre-rewrite ----------
def ensure_registered_for_rewrite(
    spark: SparkSession,
    identifier: str,
    metadata_location: str,
    old_wh_uri: str,
    new_wh_uri: str,
):
    """
    Ensure the table is queryable, but **only** by registering an OLD metadata.json.
    This avoids flipping the table to NEW before we run rewrite_table_path.
    """
    q_id = qualify("ice", identifier)

    # If already visible, we're done.
    try:
        spark.sql(f"SELECT 1 FROM {q_id} LIMIT 1").collect()
        print(f"[INFO] Visible in ice: {identifier}")
        return
    except Exception:
        pass

    meta_uri = normalize_file_uri(metadata_location)

    # Derive table dirs (OLD/NEW) from the metadata location (or from identifier)
    try:
        tbl_dir = table_dir_from_metadata_uri(meta_uri)
    except Exception:
        tbl_dir = None

    old_tbl_dir = None
    new_tbl_dir = None
    if tbl_dir and tbl_dir.startswith(with_slash(new_wh_uri)):
        # Current metadata under NEW -> compute corresponding OLD table dir
        rel_tbl = urlparse(tbl_dir).path[len(urlparse(new_wh_uri).path):].lstrip("/")
        old_tbl_dir = make_file_triple(with_slash(uri_to_path(old_wh_uri)) + rel_tbl)
        new_tbl_dir = tbl_dir
    elif tbl_dir and tbl_dir.startswith(with_slash(old_wh_uri)):
        old_tbl_dir = tbl_dir
        rel_tbl = urlparse(tbl_dir).path[len(urlparse(old_wh_uri).path):].lstrip("/")
        new_tbl_dir = make_file_triple(with_slash(uri_to_path(new_wh_uri)) + rel_tbl)
    else:
        # Fall back: guess from identifier using typical '<wh>/<ns>.db/<table>' layout
        ns, tbl = identifier.split(".", 1) if "." in identifier else ("default", identifier)
        old_tbl_dir = make_file_triple(f"{uri_to_path(old_wh_uri)}/{ns}.db/{tbl}")
        new_tbl_dir = make_file_triple(f"{uri_to_path(new_wh_uri)}/{ns}.db/{tbl}")

    # Candidate OLD metadata.json (prefer the latest that exists under OLD)
    latest_old = latest_metadata_in_dir(with_slash(old_tbl_dir) + "metadata")
    candidate = latest_old

    # If no 'latest' but metadata_location itself is already an OLD file, use it
    if not candidate and meta_uri.startswith(with_slash(old_wh_uri)):
        candidate = meta_uri

    if candidate and os.path.exists(uri_to_path(candidate)):
        print(f"[INFO] Registering {identifier} with OLD metadata to enable rewrite: {candidate}")
        try:
            spark.sql(f"""
              CALL ice.system.register_table(
                table => '{identifier}',
                metadata_file => '{candidate}'
              )
            """).show(truncate=False)
            spark.sql(f"SELECT 1 FROM {q_id} LIMIT 1").collect()
            print(f"[INFO] Registered (OLD) and queryable: {identifier}")
            return
        except Exception as e:
            msg = str(e)
            if "already exists" in msg or "Table exists" in msg:
                try:
                    spark.sql(f"SELECT 1 FROM {q_id} LIMIT 1").collect()
                    print(f"[INFO] Table already exists and is queryable: {identifier}")
                    return
                except Exception:
                    print(f"[WARN] Exists but not queryable yet: {e}")
            else:
                print(f"[WARN] register_table with OLD metadata failed: {e}")

    # No OLD metadata to register — tell the caller what to do.
    raise RuntimeError(
        "Could not find an OLD metadata.json to register. "
        "Either (a) the table history only has NEW paths now, so run rewrite_table_path "
        "with an 'end_version' equal to the last OLD metadata file, or "
        "(b) manually re-register this table to the last OLD metadata.json, then retry."
    )

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
        .config("spark.sql.catalog.ice.warehouse", old_wh_uri)
        .config("spark.sql.catalog.ice.jdbc.schema-version", "V1")
        .config("spark.sql.caseSensitive", "true")
        .getOrCreate()
    )

    # Step 3: per-table rewrite + copy; then DROP+REGISTER to switch pointer
    for identifier, metadata_location in rows:
        identifier = (identifier or "").strip()
        metadata_location = normalize_file_uri(metadata_location)
        print(f"\n=== Repointing {identifier} ===")

        # Ensure table is queryable (register with OLD metadata only)
        try:
            ensure_registered_for_rewrite(spark, identifier, metadata_location, old_wh_uri, new_wh_uri)
        except Exception as e:
            print(f"[ERROR] Cannot open/register {identifier}: {e}")
            continue

        # Optional: peek at actual file path forms
        try:
            q_id = qualify("ice", identifier)
            print("[DEBUG] Sample file_path forms from metadata table:")
            spark.sql(f"SELECT file_path FROM {q_id}.files LIMIT 5").show(truncate=False)
        except Exception as e:
            print(f"[INFO] Could not read {identifier}.files: {e}")

        # Derive a table dir to compute target locations
        try:
            tbl_dir = table_dir_from_metadata_uri(metadata_location)
        except Exception:
            ns, tbl = identifier.split(".", 1) if "." in identifier else ("default", identifier)
            tbl_dir = make_file_triple(f"{old_wh_fs}/{ns}.db/{tbl}")

        # Build source/dest table-dir pair
        if tbl_dir.startswith(with_slash(old_wh_uri)):
            rel = urlparse(tbl_dir).path[len(urlparse(old_wh_uri).path):].lstrip("/")
            src_table_dir = tbl_dir
            dst_table_dir = make_file_triple(with_slash(uri_to_path(new_wh_uri)) + rel)
        else:
            # If it already points to NEW, compute the OLD candidate for completeness
            parts = urlparse(tbl_dir).path.split("/")
            src_table_dir = make_file_triple(f"{old_wh_fs}/{parts[-2]}/{parts[-1]}")
            dst_table_dir = tbl_dir

        print(f"[STEP] table dir rewrite candidate: {src_table_dir} -> {dst_table_dir}")

        attempts = set()
        # Prefer table-level pairs, then a warehouse-level fallback
        add_style_pairs(attempts, src_table_dir, dst_table_dir)    # table-level (preferred)
        add_style_pairs(attempts, old_wh_uri, new_wh_uri)          # warehouse-level (fallback)

        outputs = []
        for s_prefix, t_prefix in attempts:
            # Stage under TARGET table metadata dir, aligned to source style
            stage_base = with_slash(dst_table_dir) + "metadata/_rewrite_stage"
            stage_loc  = align_target_style(s_prefix, stage_base)
            print(f"[CALL] rewrite_table_path: {s_prefix} -> {t_prefix} (stage: {stage_loc})")
            try:
                df = spark.sql(f"""
                  CALL ice.system.rewrite_table_path(
                    table => '{identifier}',
                    source_prefix => '{s_prefix}',
                    target_prefix => '{t_prefix}',
                    staging_location => '{stage_loc}'
                  )
                """)
                row = df.collect()[0].asDict()
                latest_version = row.get("latest_version")
                file_list_location = row.get("file_list_location")
                print(f"  -> latest_version={latest_version}, file_list_location={file_list_location}")
                if latest_version and file_list_location:
                    outputs.append((latest_version, file_list_location, t_prefix))
            except Exception as e:
                msg = str(e)
                if ("does not start with" in msg) or ("doesn't start with" in msg) or ("No matching paths" in msg):
                    print(f"[INFO] No matches for: {s_prefix} (skipped)")
                else:
                    print(f"[ERROR] rewrite_table_path failed: {e}")

        if not outputs:
            print(f"[INFO] No rewrite outputs for {identifier}; nothing to copy/register.")
            continue

        # Copy plan(s) BEFORE switching the catalog pointer
        total_copied = 0
        seen_plans = set()
        for latest_version, file_list_location, _ in outputs:
            if file_list_location in seen_plans:
                continue
            seen_plans.add(file_list_location)
            copied = copy_plan_csv(file_list_location)
            total_copied += copied
        print(f"[INFO] Copied {total_copied} file(s) per copy plan(s)")

        # Switch the catalog entry by DROP + REGISTER to the NEW staged metadata file
        latest_version = outputs[-1][0]  # use the last successful one
        target_metadata = with_slash(dst_table_dir) + f"metadata/{latest_version}"
        target_metadata = normalize_file_uri(target_metadata)
        q_id = qualify("ice", identifier)

        print(f"[STEP] Switch catalog entry by DROP+REGISTER")
        print(f"       metadata_file => {target_metadata}")

        # 1) Drop catalog entry only (no PURGE)
        try:
            spark.sql(f"DROP TABLE IF EXISTS {q_id}")
            print(f"[INFO] Dropped catalog entry for {identifier}")
        except Exception as e:
            print(f"[WARN] DROP TABLE failed for {identifier}: {e} (will try register anyway)")

        # 2) Re-register at NEW metadata
        try:
            spark.sql(f"""
              CALL ice.system.register_table(
                table => '{identifier}',
                metadata_file => '{target_metadata}'
              )
            """).show(truncate=False)
            print(f"[INFO] Re-registered {identifier} to {target_metadata}")
        except Exception as e:
            print(f"[ERROR] register_table failed for {identifier}: {e}")
            continue

        # 3) Optional maintenance after pointer switch
        if do_rm:
            try:
                print(f"[STEP] rewrite_manifests on {identifier}")
                spark.sql(f"CALL ice.system.rewrite_manifests(table => '{identifier}')").show(truncate=False)
            except Exception as e:
                print(f"[WARN] rewrite_manifests failed for {identifier}: {e}")
        else:
            print(f"[INFO] Skipping rewrite_manifests for {identifier} (enable with --rewrite-manifests)")

        # 4) Sanity on NEW pointers
        try:
            print(f"[TEST] COUNT/SAMPLE for {identifier}")
            spark.sql(f"SELECT COUNT(*) AS cnt FROM {q_id}").show()
            spark.sql(f"SELECT * FROM {q_id} LIMIT 5").show(truncate=False)
            print(f"[TEST] file_path samples for {identifier}")
            spark.sql(f"SELECT file_path FROM {q_id}.files LIMIT 5").show(truncate=False)
        except Exception as e:
            print(f"[WARN] Post-update sanity failed for {identifier}: {e}")

    print("\n[OK] Repoint finished. Update configs to the NEW warehouse root.")
    print("Spark config:  spark.sql.catalog.ice.warehouse = file:///.../new/warehouse")
    print("PyIceberg (~/.pyiceberg.yaml) -> catalog warehouse: file:///.../new/warehouse")

if __name__ == "__main__":
    main()
