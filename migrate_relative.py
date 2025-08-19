"""
migrate_to_relative.py

Make a PyIceberg[sqlite] catalog portable by rewriting all absolute paths to
relative ones. After this, you can move the whole folder anywhere and load
via sqlite URI and a relative warehouse path.

Usage:
  1) BACK UP your folder first.
  2) Edit BASE to your current folder (where iceberg.db and warehouse/ live).
  3) Run: python migrate_to_relative.py
  4) Load with:
        from pyiceberg.catalog import load_catalog
        catalog = load_catalog(
            "default",
            type="sql",
            uri="sqlite:///./iceberg.db",        # relative sqlite
            warehouse="file:./warehouse",        # or just "./warehouse"
        )
        tbl = catalog.load_table("local.my_table")
"""

from __future__ import annotations
import json
import os
import sqlite3
from pathlib import Path
from urllib.parse import urlparse, unquote

# -------- EDIT THIS to the folder where your moved files are now ----------
BASE = Path("/Users/jwj/data/iceberg/new").resolve()
# --------------------------------------------------------------------------

SQLITE_PATH = BASE / "iceberg.db"
WAREHOUSE_DIR = BASE / "warehouse"

def _to_posix_rel(p: Path, start: Path) -> str:
    """Return a POSIX-style relative path string from start -> p."""
    return Path(os.path.relpath(p, start)).as_posix()

def _parse_any_path(s: str) -> Path:
    """Accept file: URIs or plain paths and return a Path (absolute or relative)."""
    u = urlparse(s)
    if u.scheme and u.scheme != "file":
        # non-local schemes are out-of-scope for this local migration
        raise ValueError(f"Unsupported scheme in path: {s}")
    if u.scheme == "file":
        # urlparse(file:///...) gives absolute path in u.path
        return Path(unquote(u.path))
    # plain path
    return Path(s)

def _as_plain_relative_str(target: Path, start: Path) -> str:
    """Relative path string (no scheme), from 'start' to 'target'."""
    # Normalize first (resolve() only if absolute; keep relative as-is)
    if target.is_absolute():
        return _to_posix_rel(target, start)
    return target.as_posix()

# --------------------- Step 1. Rewrite SQLite -----------------------------

def rewrite_sqlite_to_relative(sqlite_path: Path, base_dir: Path) -> None:
    """Make metadata_location columns relative to the sqlite file location."""
    sqlite_dir = sqlite_path.parent
    con = sqlite3.connect(str(sqlite_path))
    try:
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]

        for t in tables:
            cur.execute(f"PRAGMA table_info('{t}')")
            cols = {row[1] for row in cur.fetchall()}
            for col in ("metadata_location", "previous_metadata_location"):
                if col not in cols:
                    continue
                # Fetch & transform each row; SQLite lacks a robust URI->path replace for relative
                cur.execute(f"SELECT rowid, {col} FROM '{t}'")
                rows = cur.fetchall()
                changed = 0
                for rowid, loc in rows:
                    try:
                        p = _parse_any_path(loc)
                    except Exception:
                        # skip non-local
                        continue
                    # If the stored path is inside BASE, we convert to relative-from-sqlite
                    try:
                        abs_p = (p if p.is_absolute() else (base_dir / p)).resolve()
                    except Exception:
                        continue
                    if str(abs_p).startswith(str(base_dir)):
                        rel = _as_plain_relative_str(abs_p, sqlite_dir)
                        if rel != loc:
                            cur.execute(f"UPDATE '{t}' SET {col}=? WHERE rowid=?", (rel, rowid))
                            changed += 1
                if changed:
                    print(f"[sqlite] {t}.{col}: rewrote {changed} row(s) to relative")
        con.commit()
    finally:
        con.close()

# -------- Step 2. Rewrite table metadata JSON to relative -----------------

def _rewrite_metadata_json(meta_json: Path, base_dir: Path) -> None:
    """
    Make fields relative:
      - 'location' -> relative path from BASE to the table directory
      - snapshot 'manifest-list' -> 'metadata/<file>'
    """
    raw = meta_json.read_text(encoding="utf-8")
    meta = json.loads(raw)

    table_dir = meta_json.parent.parent  # .../<table>/metadata/<vN.json> -> table dir

    changed = False

    # 2a) location -> relative from BASE so it survives a move
    loc = meta.get("location")
    if isinstance(loc, str):
        try:
            loc_path = _parse_any_path(loc)
        except Exception:
            loc_path = None
        # Prefer the actual table_dir we know
        new_loc = _as_plain_relative_str(table_dir, base_dir)
        if not new_loc.startswith("./") and not new_loc.startswith("../"):
            new_loc = "./" + new_loc
        if loc != new_loc:
            meta["location"] = new_loc
            changed = True

    # 2b) snapshots[].manifest-list -> metadata/<file>
    snaps = meta.get("snapshots") or []
    for s in snaps:
        ml = s.get("manifest-list") or s.get("manifest_list")
        if isinstance(ml, str):
            try:
                ml_path = _parse_any_path(ml)
            except Exception:
                ml_path = None
            # If we can derive a filename, rewrite to metadata/<filename>
            if ml_path is not None:
                fname = (ml_path.name if ml_path.name else Path(ml).name)
                if fname:  # defensive
                    new_ml = f"metadata/{fname}"
                    if ml != new_ml:
                        # Respect original key style
                        if "manifest-list" in s:
                            s["manifest-list"] = new_ml
                        else:
                            s["manifest_list"] = new_ml
                        changed = True

    if changed:
        backup = meta_json.with_suffix(meta_json.suffix + ".bak")
        if not backup.exists():
            backup.write_text(raw, encoding="utf-8")
        meta_json.write_text(json.dumps(meta, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
        print(f"[json] Rewrote {meta_json}")

# Optional: also rewrite Avro (manifest lists & manifests) to relative
def try_rewrite_avro_to_relative(meta_json: Path, base_dir: Path) -> None:
    try:
        from fastavro import reader, writer, parse_schema
    except Exception:
        return  # silently skip if fastavro not present

    table_dir = meta_json.parent.parent

    def _rel_to_table(p: Path) -> str:
        # Return path relative to table_dir
        return _as_plain_relative_str(p, table_dir)

    # Read metadata JSON for snapshot list
    meta = json.loads(meta_json.read_text(encoding="utf-8"))
    snaps = meta.get("snapshots") or []
    manifest_lists = []
    for s in snaps:
        ml = s.get("manifest-list") or s.get("manifest_list")
        if isinstance(ml, str):
            manifest_lists.append(table_dir / ml)  # after JSON rewrite, it's metadata/<file>

    for ml_path in manifest_lists:
        if not ml_path.exists():
            continue
        with ml_path.open("rb") as f:
            r = reader(f)
            schema = r.writer_schema
            recs = list(r)

        changed_ml = False
        manifest_paths = []
        for rec in recs:
            if "manifest_path" in rec and isinstance(rec["manifest_path"], str):
                abs_mp = _parse_any_path(rec["manifest_path"])
                # If absolute, make relative to table dir
                if abs_mp.is_absolute():
                    new_mp = _rel_to_table(abs_mp)
                else:
                    # keep as-is but ensure 'metadata/<file>' form
                    new_mp = ("metadata/" + abs_mp.name) if abs_mp.parent not in (Path("metadata"),) else abs_mp.as_posix()
                if rec["manifest_path"] != new_mp:
                    rec["manifest_path"] = new_mp
                    changed_ml = True
                manifest_paths.append((ml_path.parent / Path(new_mp)).resolve())

        if changed_ml:
            backup = ml_path.with_suffix(ml_path.suffix + ".bak")
            if not backup.exists():
                backup.write_bytes(ml_path.read_bytes())
            with ml_path.open("wb") as out:
                writer(out, parse_schema(schema), recs)
            print(f"[avro] Rewrote manifest-list {ml_path}")

        # Rewrite each manifest's data_file.file_path -> relative to table_dir
        for man_path in manifest_paths:
            if not man_path.exists():
                continue
            with man_path.open("rb") as f:
                r = reader(f)
                schema = r.writer_schema
                m_recs = list(r)

            changed_m = False
            for rec in m_recs:
                df = rec.get("data_file") or rec.get("file")
                if isinstance(df, dict) and isinstance(df.get("file_path"), str):
                    p = _parse_any_path(df["file_path"])
                    new_fp = _rel_to_table(p if p.is_absolute() else (table_dir / p))
                    if df["file_path"] != new_fp:
                        df["file_path"] = new_fp
                        changed_m = True

            if changed_m:
                backup = man_path.with_suffix(man_path.suffix + ".bak")
                if not backup.exists():
                    backup.write_bytes(man_path.read_bytes())
                with man_path.open("wb") as out:
                    writer(out, parse_schema(schema), m_recs)
                print(f"[avro] Rewrote manifest {man_path}")

def migrate_to_relative():
    # 1) SQLite: absolute -> relative
    rewrite_sqlite_to_relative(SQLITE_PATH, BASE)

    # 2) JSON & Avro: absolute -> relative
    for meta_json in WAREHOUSE_DIR.glob("**/metadata/*.json"):
        if meta_json.is_file():
            _rewrite_metadata_json(meta_json, BASE)
            try_rewrite_avro_to_relative(meta_json, BASE)

# -------------------- Optional helper: rehydrate ---------------------------

def rehydrate_absolute_here():
    """
    If an engine balks at relative paths, quickly expand the relative paths in
    the SQLite catalog to absolute paths for the *current* machine, without
    touching JSON/Avro. Safe to run repeatedly.
    """
    sqlite_dir = SQLITE_PATH.parent

    con = sqlite3.connect(str(SQLITE_PATH))
    try:
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [r[0] for r in cur.fetchall()]
        for t in tables:
            cur.execute(f"PRAGMA table_info('{t}')")
            cols = {row[1] for row in cur.fetchall()}
            for col in ("metadata_location", "previous_metadata_location"):
                if col not in cols:
                    continue
                cur.execute(f"SELECT rowid, {col} FROM '{t}'")
                changed = 0
                for rowid, loc in cur.fetchall():
                    try:
                        p = _parse_any_path(loc)
                    except Exception:
                        continue
                    abs_p = (p if p.is_absolute() else (sqlite_dir / p)).resolve()
                    uri = "file://" + abs_p.as_posix()
                    if loc != uri:
                        cur.execute(f"UPDATE '{t}' SET {col}=? WHERE rowid=?", (uri, rowid))
                        changed += 1
                if changed:
                    print(f"[rehydrate] {t}.{col}: expanded {changed} row(s) to absolute")
        con.commit()
    finally:
        con.close()

if __name__ == "__main__":
    assert SQLITE_PATH.exists(), f"Missing {SQLITE_PATH}"
    assert WAREHOUSE_DIR.exists(), f"Missing {WAREHOUSE_DIR}"
    migrate_to_relative()
    print("\n✔ Done. Next time you load, use relative URIs/paths, e.g.:")
    print('  uri="sqlite:///./iceberg.db", warehouse="file:./warehouse"   (or warehouse="./warehouse")')
