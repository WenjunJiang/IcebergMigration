#!/usr/bin/env python3
"""
Migrate an Iceberg JDBC (SQLite) catalog with NO SYMLINKS after you've already moved
both iceberg.db and warehouse/ into a NEW directory, and the *old* parent path does not exist.

Strategy (offline rewrite of metadata & avro):
  1) For each table, read the *new* warehouse's latest metadata JSON.
  2) Infer OLD warehouse prefix from metadata['location'] (it still embeds the old URI).
  3) Write a *new* metadata JSON where:
       - 'location' uses the NEW warehouse
       - each snapshot's 'manifest-list' path uses the NEW warehouse
       - any old-prefixed paths in JSON are rewritten to the NEW prefix
  4) For each manifest list (.avro) referenced, rewrite records' 'manifest_path' to NEW prefix.
  5) For each manifest (.avro) referenced, rewrite each data file path to NEW prefix
     (handles v2 'data_file.file_path' and older 'path').
  6) Register the table against the new metadata JSON in the JDBC catalog.
  7) Optionally CALL rewrite_manifests.

Usage:
  python migrate_after_move_no_symlink.py <NEW_PARENT_DIR> [--rewrite-manifests]
"""

import os
import re
import sys
import glob
import json
import uuid
import sqlite3
from typing import Dict, Any, List, Tuple, Set
from urllib.parse import urlparse

# Avro: required. Install with: pip install fastavro
try:
    from fastavro import reader as avro_reader, writer as avro_writer, parse_schema
except Exception as e:
    print("[ERROR] fastavro is required. Install with: pip install fastavro")
    raise

from pyspark.sql import SparkSession

ICEBERG_COORD = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"
SQLITE_COORD  = "org.xerial:sqlite-jdbc:3.50.3.0"


# -------------------- URI / path helpers --------------------

def fs2uri(p: str) -> str:
    """'/abs/path' -> 'file:///abs/path' (triple slash)"""
    return "file://" + os.path.abspath(p)

def ensure_file_uri(s: str) -> str:
    if s.startswith("file:/"):
        return s
    if s.startswith("/"):
        return "file://" + s
    return s

def uri_to_local(uri: str) -> str:
    u = urlparse(uri)
    if u.scheme != "file":
        raise ValueError(f"Only file:// URIs are supported: {uri}")
    return u.path

def split_warehouse_ns_table_from_table_dir(table_dir_uri: str) -> Tuple[str, str, str]:
    """
    Input: table_dir like file:///.../warehouse/<ns_dir>/<table>
    Output: (warehouse_uri, ns_dir, table_dirname)
    """
    p = urlparse(table_dir_uri).path
    ix = p.rfind("/warehouse/")
    if ix < 0:
        raise ValueError(f"Cannot find '/warehouse/' in: {table_dir_uri}")
    wh_root = p[: ix + len("/warehouse/") - 1]   # includes '/warehouse'
    tail = p[ix + len("/warehouse/"):]
    comps = tail.split("/")
    if len(comps) < 2:
        raise ValueError(f"Cannot parse namespace/table from: {table_dir_uri}")
    ns_dir, tbl_dir = comps[0], comps[1]
    return ("file://" + wh_root, ns_dir, tbl_dir)

def table_dir_from_metadata_uri(meta_uri: str) -> str:
    """file:///.../<ns>/<table>/metadata/<file.json> -> file:///.../<ns>/<table>"""
    u = urlparse(meta_uri)
    parts = u.path.split("/")
    if parts and parts[-1].endswith(".json"):
        parts = parts[:-1]
    if parts and parts[-1] == "metadata":
        parts = parts[:-1]
    if not parts:
        raise ValueError(f"Bad metadata URI: {meta_uri}")
    return f"{u.scheme}://{'/'.join(parts)}"

def newest_metadata_in_dir(meta_dir: str) -> str | None:
    vh = os.path.join(meta_dir, "version-hint.text")
    if os.path.isfile(vh):
        name = open(vh, "r", encoding="utf-8").read().strip()
        cand = os.path.join(meta_dir, name)
        if os.path.isfile(cand):
            return cand
    files = glob.glob(os.path.join(meta_dir, "*.metadata.json"))
    if not files:
        return None

    def ver_num(path):
        base = os.path.basename(path)
        m = re.match(r"(\d+)-.*\.metadata\.json$", base)
        return int(m.group(1)) if m else -1

    files.sort(key=ver_num)
    return files[-1]

def all_strings_in_json(d: Any) -> List[str]:
    """Collect every string value in a (nested) JSON-like structure."""
    out = []
    if isinstance(d, dict):
        for v in d.values():
            out.extend(all_strings_in_json(v))
    elif isinstance(d, list):
        for v in d:
            out.extend(all_strings_in_json(v))
    elif isinstance(d, str):
        out.append(d)
    return out

def replace_prefix(s: str, old_prefixes: List[str], new_prefix: str) -> str:
    """Replace any of the old prefixes (file:/ or file:///) wherever they appear."""
    out = s
    for op in old_prefixes:
        out = out.replace(op, new_prefix)
    return out


# -------------------- SQLite discovery --------------------

def discover_rows(sqlite_path: str) -> List[Tuple[str, str]]:
    """
    Return [(identifier, metadata_location), ...] from a SQLite Iceberg catalog.
    Tries multiple known schemas.
    """
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
            elif {"table_namespace", "table_name"}.issubset(s):
                q = f"SELECT (table_namespace || '.' || table_name) AS table_identifier, metadata_location FROM {tbl}"
            elif {"namespace", "table_name"}.issubset(s):
                q = f"SELECT (namespace || '.' || table_name) AS table_identifier, metadata_location FROM {tbl}"
            elif {"namespace", "name"}.issubset(s):
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


# -------------------- Avro rewrite helpers --------------------

def rewrite_manifest_list_avro(path_local: str, old_prefixes: List[str], new_prefix: str) -> None:
    """Rewrite 'manifest_path' inside a manifest-list Avro file, in-place (via temp + replace)."""
    tmp = path_local + ".tmpwrite"
    with open(path_local, "rb") as fin:
        r = avro_reader(fin)
        schema = r.writer_schema
        records = []
        for rec in r:
            if "manifest_path" in rec and isinstance(rec["manifest_path"], str):
                rec["manifest_path"] = replace_prefix(rec["manifest_path"], old_prefixes, new_prefix)
            records.append(rec)
    with open(tmp, "wb") as fout:
        avro_writer(fout, parse_schema(schema), records)
    os.replace(tmp, path_local)

def rewrite_manifest_avro(path_local: str, old_prefixes: List[str], new_prefix: str) -> None:
    """
    Rewrite data-file paths inside a manifest Avro file, in-place:
      - v2: rec['data_file']['file_path']
      - v1: rec['path'] or rec['data_file']['path']
    """
    tmp = path_local + ".tmpwrite"
    with open(path_local, "rb") as fin:
        r = avro_reader(fin)
        schema = r.writer_schema
        records = []
        for rec in r:
            # V2: nested data_file.file_path
            if "data_file" in rec and isinstance(rec["data_file"], dict):
                df = rec["data_file"]
                if "file_path" in df and isinstance(df["file_path"], str):
                    df["file_path"] = replace_prefix(df["file_path"], old_prefixes, new_prefix)
                if "path" in df and isinstance(df["path"], str):  # older writer variants
                    df["path"] = replace_prefix(df["path"], old_prefixes, new_prefix)
            # V1: top-level 'path'
            if "path" in rec and isinstance(rec["path"], str):
                rec["path"] = replace_prefix(rec["path"], old_prefixes, new_prefix)
            records.append(rec)
    with open(tmp, "wb") as fout:
        avro_writer(fout, parse_schema(schema), records)
    os.replace(tmp, path_local)


# -------------------- Metadata JSON rewrite --------------------

def load_json(path_local: str) -> Dict[str, Any]:
    with open(path_local, "r", encoding="utf-8") as f:
        return json.load(f)

def dump_json(obj: Dict[str, Any], path_local: str) -> None:
    with open(path_local, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, sort_keys=False)

def infer_old_wh_from_location(location_uri: str) -> Tuple[str, str, str]:
    """
    From 'location' (file:///.../warehouse/<ns>/<table>), infer:
      - old_wh_uri (file:///.../warehouse)
      - ns_dir
      - tbl_dir
    """
    table_dir_uri = ensure_file_uri(location_uri)
    old_wh_uri, ns_dir, tbl_dir = split_warehouse_ns_table_from_table_dir(table_dir_uri)
    return old_wh_uri, ns_dir, tbl_dir

def next_metadata_filename(meta_dir: str) -> str:
    """Create a new metadata filename (Iceberg-style but not strictly required)."""
    # Try to increment version if pattern present
    latest = newest_metadata_in_dir(meta_dir)
    if latest:
        base = os.path.basename(latest)
        m = re.match(r"(\d+)-.*\.metadata\.json$", base)
        if m:
            next_ver = int(m.group(1)) + 1
            name = f"{next_ver:05d}-{uuid.uuid4().hex}.metadata.json"
            return os.path.join(meta_dir, name)
    # Fallback generic
    name = f"migrated-{uuid.uuid4().hex}.metadata.json"
    return os.path.join(meta_dir, name)

def rewrite_metadata_json_in_place(new_wh_uri: str, meta_path_local: str) -> Tuple[str, List[str], str, str]:
    """
    Load the *current* metadata JSON (in the NEW warehouse), infer old_wh, ns, tbl.
    Rewrite JSON content to NEW prefix and write a *new* metadata JSON file in the same dir.
    Returns:
      (new_meta_path_local, manifest_list_uris_after_rewrite, old_wh_uri, new_table_dir_uri)
    """
    meta = load_json(meta_path_local)

    if "location" not in meta or not isinstance(meta["location"], str):
        raise RuntimeError(f"Metadata missing 'location': {meta_path_local}")

    old_wh_uri, ns_dir, tbl_dir = infer_old_wh_from_location(meta["location"])
    new_table_dir_uri = f"{new_wh_uri}/{ns_dir}/{tbl_dir}"

    old_prefixes = [old_wh_uri, old_wh_uri.replace("file:///", "file:/")]
    # Update 'location'
    meta["location"] = replace_prefix(meta["location"], old_prefixes, new_wh_uri)

    # Update snapshot paths (manifest-list + possible 'manifests' array for older metadata)
    manifest_list_uris: List[str] = []
    if "snapshots" in meta and isinstance(meta["snapshots"], list):
        for snap in meta["snapshots"]:
            if isinstance(snap, dict):
                if "manifest-list" in snap and isinstance(snap["manifest-list"], str):
                    snap["manifest-list"] = replace_prefix(snap["manifest-list"], old_prefixes, new_wh_uri)
                    manifest_list_uris.append(snap["manifest-list"])
                if "manifests" in snap and isinstance(snap["manifests"], list):
                    new_m_list = []
                    for mpath in snap["manifests"]:
                        if isinstance(mpath, str):
                            new_m_list.append(replace_prefix(mpath, old_prefixes, new_wh_uri))
                    snap["manifests"] = new_m_list

    # Also rewrite any other old-prefixed strings in JSON defensively
    def recurse(x):
        if isinstance(x, dict):
            return {k: recurse(v) for k, v in x.items()}
        if isinstance(x, list):
            return [recurse(v) for v in x]
        if isinstance(x, str):
            return replace_prefix(x, old_prefixes, new_wh_uri)
        return x
    meta = recurse(meta)

    # Write a new metadata JSON
    meta_dir = os.path.dirname(meta_path_local)
    new_meta_path_local = next_metadata_filename(meta_dir)
    dump_json(meta, new_meta_path_local)

    # Update version-hint
    with open(os.path.join(meta_dir, "version-hint.text"), "w", encoding="utf-8") as f:
        f.write(os.path.basename(new_meta_path_local))

    return new_meta_path_local, manifest_list_uris, old_wh_uri, new_table_dir_uri


# -------------------- Spark helpers --------------------

def qualify(catalog: str, identifier: str) -> str:
    parts = identifier.split(".")
    return f"{catalog}." + ".".join(f"`{p}`" for p in parts)

def ensure_registered_from_new_meta(spark: SparkSession, identifier: str, new_meta_uri: str) -> None:
    q_id = qualify("ice", identifier)
    try:
        spark.sql(f"SELECT 1 FROM {q_id} LIMIT 1").collect()
        print(f"[INFO] Visible in ice: {identifier}")
        return
    except Exception:
        pass
    print(f"[INFO] Registering with NEW metadata: {identifier}")
    spark.sql(f"""
      CALL ice.system.register_table(
        table => '{identifier}',
        metadata_file => '{new_meta_uri}'
      )
    """).show(truncate=False)
    spark.sql(f"SELECT 1 FROM {q_id} LIMIT 1").collect()
    print(f"[INFO] Registered: {identifier}")


# -------------------- main --------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python migrate_after_move_no_symlink.py <NEW_PARENT_DIR> [--rewrite-manifests]")
        sys.exit(1)

    new_parent = os.path.abspath(sys.argv[1]).rstrip("/")
    do_rm      = ("--rewrite-manifests" in sys.argv[2:])

    sqlite_path = os.path.join(new_parent, "iceberg.db")
    new_wh      = os.path.join(new_parent, "warehouse")

    if not os.path.exists(sqlite_path):
        print(f"[ERROR] Missing SQLite catalog at: {sqlite_path}")
        sys.exit(1)
    if not os.path.isdir(new_wh):
        print(f"[ERROR] Missing warehouse at: {new_wh}")
        sys.exit(1)

    print(f"[INFO] NEW parent : {new_parent}")
    print(f"[INFO] NEW wh     : {new_wh}")
    print(f"[INFO] SQLite     : {sqlite_path}")
    print(f"[INFO] Iceberg    : {ICEBERG_COORD}")
    print(f"[INFO] SQLite JDBC: {SQLITE_COORD}")

    # Discover identifiers from SQLite (we ignore stored metadata_location strings)
    try:
        rows = discover_rows(sqlite_path)
    except Exception as e:
        print(f"[ERROR] Reading SQLite: {e}")
        sys.exit(1)
    if not rows:
        print("[WARN] No tables found.")
        sys.exit(0)

    new_wh_uri = fs2uri(new_wh)

    # For each table: fix JSON + avro in the NEW metadata dir
    fixed_tables: List[Tuple[str, str]] = []  # (identifier, new_meta_uri)

    for identifier, _old_meta_loc in rows:
        identifier = (identifier or "").strip()

        # Resolve table dir from new warehouse (we need ns_dir/tbl_dir; infer from the JSON itself)
        # Find the current *new* metadata JSON file by scanning the table's metadata dir.
        # First, locate the namespace folder(s). We do a small scan under new warehouse.
        # (You can replace this with a direct mapping if your ns is known.)
        cand_meta_dirs = []
        # We expect layout: <new_wh>/<ns_dir>/<table>/metadata
        # Try typical two-level depth (<ns_dir>/<table>/metadata)
        for ns_dir in os.listdir(new_wh):
            ns_path = os.path.join(new_wh, ns_dir)
            if not os.path.isdir(ns_path):
                continue
            tbl_path = os.path.join(ns_path, identifier.split(".")[-1])
            meta_dir = os.path.join(tbl_path, "metadata")
            if os.path.isdir(meta_dir):
                cand_meta_dirs.append(meta_dir)

        # If multiple candidates (rare), keep those that actually contain metadata files
        cand_meta_dirs = [d for d in cand_meta_dirs if glob.glob(os.path.join(d, "*.metadata.json"))]
        if not cand_meta_dirs:
            print(f"[WARN] {identifier}: no metadata dir found under {new_wh}")
            continue
        meta_dir_local = cand_meta_dirs[0]
        meta_path_local = newest_metadata_in_dir(meta_dir_local)
        if not meta_path_local:
            print(f"[WARN] {identifier}: empty metadata dir: {meta_dir_local}")
            continue

        print(f"\n=== Processing {identifier} ===")
        print(f"[INFO] Latest metadata JSON: {meta_path_local}")

        # 1) Rewrite metadata JSON (also returns manifest-list URIs to fix, old_wh_uri, new_table_dir_uri)
        try:
            new_meta_local, manifest_list_uris, old_wh_uri, new_table_dir_uri = rewrite_metadata_json_in_place(
                new_wh_uri=new_wh_uri,
                meta_path_local=meta_path_local
            )
        except Exception as e:
            print(f"[ERROR] {identifier}: metadata rewrite failed: {e}")
            continue

        print(f"[INFO] Wrote new metadata JSON: {new_meta_local}")
        print(f"[INFO] old_wh_uri -> new_wh_uri : {old_wh_uri} -> {new_wh_uri}")
        print(f"[INFO] table location           : {new_table_dir_uri}")

        # 2) Rewrite manifest-list avro files to point to NEW manifest paths
        old_prefixes = [old_wh_uri, old_wh_uri.replace("file:///", "file:/")]
        new_prefix   = new_wh_uri

        # Ensure we have the final (NEW) manifest list URIs (they were re-written in JSON)
        for mlist_uri in manifest_list_uris:
            mlist_local = uri_to_local(mlist_uri)
            if not os.path.isfile(mlist_local):
                # If a manifest-list entry was missing in the new dir (unlikely), skip safely.
                print(f"[WARN] Missing manifest-list file (skipped): {mlist_local}")
                continue
            print(f"[STEP] Rewriting manifest-list: {mlist_local}")
            try:
                rewrite_manifest_list_avro(mlist_local, old_prefixes, new_prefix)
            except Exception as e:
                print(f"[ERROR] Failed to rewrite manifest-list {mlist_local}: {e}")
                continue

            # 3) Collect manifest paths from the just-updated manifest-list and rewrite each manifest
            #    We must read the file *again* (after rewrite) to get the NEW manifest paths.
            manifests_new: List[str] = []
            with open(mlist_local, "rb") as fin:
                r = avro_reader(fin)
                for rec in r:
                    if "manifest_path" in rec and isinstance(rec["manifest_path"], str):
                        manifests_new.append(rec["manifest_path"])

            for m_uri in manifests_new:
                m_local = uri_to_local(m_uri)
                if not os.path.isfile(m_local):
                    print(f"[WARN] Missing manifest file (skipped): {m_local}")
                    continue
                print(f"[STEP] Rewriting manifest: {m_local}")
                try:
                    rewrite_manifest_avro(m_local, old_prefixes, new_prefix)
                except Exception as e:
                    print(f"[ERROR] Failed to rewrite manifest {m_local}: {e}")

        # Stash final new metadata URI for registration
        fixed_tables.append((identifier, fs2uri(new_meta_local)))

    if not fixed_tables:
        print("\n[WARN] Nothing to register; no tables were successfully rewritten.")
        sys.exit(0)

    # 4) Register all tables against the *new* metadata JSON and (optionally) compact manifests
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

    for identifier, new_meta_uri in fixed_tables:
        print(f"\n=== Registering {identifier} ===")
        try:
            ensure_registered_from_new_meta(spark, identifier, new_meta_uri)
        except Exception as e:
            print(f"[ERROR] Registration failed for {identifier}: {e}")
            continue

        if do_rm:
            try:
                print("[STEP] rewrite_manifests")
                spark.sql(f"CALL ice.system.rewrite_manifests(table => '{identifier}')").show(truncate=False)
            except Exception as e:
                print(f"[WARN] rewrite_manifests failed for {identifier}: {e}")

        # Quick sanity
        try:
            qid = qualify("ice", identifier)
            print("[TEST] COUNT")
            spark.sql(f"SELECT COUNT(*) AS cnt FROM {qid}").show()
        except Exception as e:
            print(f"[WARN] Sanity COUNT failed for {identifier}: {e}")

    print("\n[OK] Migration complete with NO symlinks.")
    print("Your catalog now points at the NEW warehouse and all manifests/data paths were rewritten.")

if __name__ == "__main__":
    main()
