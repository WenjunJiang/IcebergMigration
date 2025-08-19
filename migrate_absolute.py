import os
import sqlite3
import json
from pathlib import Path

OLD_WAREHOUSE_URI = "file:///Users/jwj/data/iceberg/old/warehouse"
NEW_WAREHOUSE_URI = "file:///Users/jwj/data/iceberg/new/warehouse"

SQLITE_PATH = "/Users/jwj/data/iceberg/new/iceberg.db"
WAREHOUSE_DIR = Path("/Users/jwj/data/iceberg/new/warehouse")

# Some tools may have written file:/... instead of file:///...
OLD_URI_ALT = OLD_WAREHOUSE_URI.replace("file:///", "file:/")
NEW_URI_ALT = NEW_WAREHOUSE_URI.replace("file:///", "file:/")

def _replace_prefix(s: str) -> str:
    """Replace both file:/// and file:/ style prefixes."""
    return s.replace(OLD_WAREHOUSE_URI, NEW_WAREHOUSE_URI).replace(OLD_URI_ALT, NEW_URI_ALT)

def update_sqlite_catalog(sqlite_path: str):
    con = sqlite3.connect(sqlite_path)
    try:
        cur = con.cursor()
        # Find all tables
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        for t in tables:
            # See if table has metadata_location / previous_metadata_location
            cur.execute(f"PRAGMA table_info('{t}')")
            cols = {row[1] for row in cur.fetchall()}
            for col in ("metadata_location", "previous_metadata_location"):
                if col in cols:
                    # Update rows where the old prefix appears
                    cur.execute(
                        f"UPDATE '{t}' "
                        f"SET {col} = REPLACE(REPLACE({col}, ?, ?), ?, ?) "
                        f"WHERE {col} LIKE ? OR {col} LIKE ?",
                        (
                            OLD_WAREHOUSE_URI, NEW_WAREHOUSE_URI,
                            OLD_URI_ALT, NEW_URI_ALT,
                            f"{OLD_WAREHOUSE_URI}%",
                            f"{OLD_URI_ALT}%",
                        ),
                    )
                    print(f"[sqlite] Updated {cur.rowcount} row(s) in {t}.{col}")
        con.commit()
    finally:
        con.close()

def rewrite_text_file_inplace(p: Path, replacer=_replace_prefix):
    txt = p.read_text(encoding="utf-8")
    new_txt = replacer(txt)
    if new_txt != txt:
        backup = p.with_suffix(p.suffix + ".bak")
        if not backup.exists():
            backup.write_text(txt, encoding="utf-8")
        p.write_text(new_txt, encoding="utf-8")
        print(f"[json] Rewrote {p}")

def gather_metadata_json_files(warehouse_dir: Path):
    # Iceberg table metadata lives at <warehouse>/<ns>/<table>/metadata/*.json
    for meta_json in warehouse_dir.glob("**/metadata/*.json"):
        # Avoid picking up manifest lists named .avro, only JSON
        if meta_json.is_file() and meta_json.suffix.lower() == ".json":
            yield meta_json

def try_rewrite_avro_files_from_metadata(meta_json_path: Path):
    """Optional: Rewrite manifest-list and manifest Avro files if fastavro is available."""
    try:
        from fastavro import reader, writer, parse_schema
    except Exception:
        # fastavro not available; skip quietly
        return

    # Collect manifest-list paths from the metadata JSON (across all snapshots)
    try:
        meta = json.loads(meta_json_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[warn] Could not parse JSON {meta_json_path}: {e}")
        return

    # Iceberg metadata uses keys with hyphens, e.g., "manifest-list"
    snapshots = meta.get("snapshots", []) or []
    manifest_lists = []
    for snap in snapshots:
        ml = snap.get("manifest-list") or snap.get("manifest_list")
        if isinstance(ml, str):
            manifest_lists.append(Path(_replace_prefix(ml)).expanduser())

    # Process each manifest-list: rewrite its manifest_path fields and then process each manifest
    for ml_path in manifest_lists:
        if not ml_path.exists():
            continue

        # Read all records
        with ml_path.open("rb") as f:
            try:
                r = reader(f)
                schema = r.writer_schema
                records = list(r)
            except Exception as e:
                print(f"[warn] Could not read manifest-list {ml_path}: {e}")
                continue

        # Update manifest_path and collect the manifest paths
        manifest_paths = []
        changed = False
        for rec in records:
            if "manifest_path" in rec and isinstance(rec["manifest_path"], str):
                new_path = _replace_prefix(rec["manifest_path"])
                if new_path != rec["manifest_path"]:
                    rec["manifest_path"] = new_path
                    changed = True
                manifest_paths.append(Path(rec["manifest_path"]).expanduser())

        if changed:
            # Write back in-place with backup
            backup = ml_path.with_suffix(ml_path.suffix + ".bak")
            if not backup.exists():
                backup.write_bytes(ml_path.read_bytes())
            with ml_path.open("wb") as out:
                writer(out, parse_schema(schema), records)
            print(f"[avro] Rewrote manifest-list {ml_path}")

        # Now rewrite each manifest file's data_file.file_path
        for man_path in manifest_paths:
            if not man_path.exists():
                continue
            with man_path.open("rb") as f:
                try:
                    r = reader(f)
                    schema = r.writer_schema
                    records = list(r)
                except Exception as e:
                    print(f"[warn] Could not read manifest {man_path}: {e}")
                    continue

            changed_m = False
            for rec in records:
                # Iceberg v2 often uses "data_file"; older may use "file"
                df = rec.get("data_file") or rec.get("file")
                if isinstance(df, dict) and isinstance(df.get("file_path"), str):
                    new_fp = _replace_prefix(df["file_path"])
                    if new_fp != df["file_path"]:
                        df["file_path"] = new_fp
                        changed_m = True

            if changed_m:
                backup = man_path.with_suffix(man_path.suffix + ".bak")
                if not backup.exists():
                    backup.write_bytes(man_path.read_bytes())
                with man_path.open("wb") as out:
                    writer(out, parse_schema(schema), records)
                print(f"[avro] Rewrote manifest {man_path}")

def migrate():
    # 1) Update SQLite catalog rows
    update_sqlite_catalog(SQLITE_PATH)

    # 2) Rewrite table metadata JSON files under the new warehouse
    for meta_json in gather_metadata_json_files(WAREHOUSE_DIR):
        # First do a blanket prefix replacement on the JSON text
        rewrite_text_file_inplace(meta_json)

        # Then (optionally) rewrite referenced Avro files (manifest-list & manifests)
        try_rewrite_avro_files_from_metadata(meta_json)

if __name__ == "__main__":
    # BACKUP: Highly recommended to back up the whole /new folder before running.
    migrate()
    print("\nDone. Now you can load the catalog with the new paths:\n")
    print("from pyiceberg.catalog import load_catalog")
    print(f'catalog = load_catalog("default", type="sql", '
          f'uri="sqlite:////Users/jwj/data/iceberg/new/iceberg.db", '
          f'warehouse="{NEW_WAREHOUSE_URI}")')
    print('table = catalog.load_table("local.my_table")')
