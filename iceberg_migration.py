# iceberg_migrate_all_in_one.py (v3: no backups, --new_base required)
from __future__ import annotations
from pathlib import Path
from typing import Dict, Tuple, Optional, List
from urllib.parse import urlparse, unquote
import argparse, sqlite3, json, re, sys

# ---------- Helpers ----------
def _from_uri_or_path(s: str) -> Path:
    u = urlparse(s)
    if u.scheme and u.scheme != "file":
        return Path(s)   # leave non-local schemes alone
    if u.scheme == "file":
        return Path(unquote(u.path))
    return Path(s)

def _to_file_uri(p: Path) -> str:
    return "file://" + p.as_posix()

def _suffix_after_segment(p: Path, seg: str) -> Optional[Path]:
    parts = p.parts
    try:
        i = len(parts) - 1 - parts[::-1].index(seg)
    except ValueError:
        return None
    return Path(*parts[i+1:]) if i + 1 < len(parts) else None

def _map_to_new_warehouse(p: Path, table_dir: Path, warehouse_dir: Path) -> Path:
    """
    Map an *old* absolute path to the new warehouse:
      1) If it contains '/warehouse/', graft suffix onto warehouse_dir.
      2) If basename looks like a manifest-list (snap-*.avro), put under table_dir/metadata.
      3) If it's a data file and contains '/data/', map that suffix under warehouse_dir/data.
      4) Otherwise, fall back to table_dir/<basename>.
    """
    if p.is_absolute():
        suf = _suffix_after_segment(p, "warehouse")
        if suf:
            return (warehouse_dir / suf).resolve()
        if "/data/" in p.as_posix():
            suf2 = Path("data") / p.as_posix().split("/data/", 1)[1]
            return (warehouse_dir / suf2).resolve()
        if p.name.startswith("snap-") and p.suffix == ".avro":
            return (table_dir / "metadata" / p.name).resolve()
    return (table_dir / p.name).resolve()

def _pick_current_metadata(meta_dir: Path) -> Optional[Path]:
    hint = meta_dir / "version-hint.text"
    if hint.exists():
        try:
            v = int(hint.read_text().strip())
            pat = re.compile(rf"0*{v}-.*\.metadata\.json$")
            m = [p for p in meta_dir.glob("*.metadata.json") if pat.match(p.name)]
            if m:
                return m[0]
        except Exception:
            pass
    metas = list(meta_dir.glob("*.metadata.json"))
    return max(metas, key=lambda p: p.stat().st_mtime) if metas else None

def _discover_tables(warehouse: Path) -> Dict[Tuple[str, str], Dict[str, Path]]:
    """
    Scan <warehouse>/**/metadata/*.metadata.json and return:
      {(namespace, table): {'table_dir': Path, 'meta_json': Path}}
    Namespace can be multi-level (joined by dots).
    """
    found: Dict[Tuple[str, str], Dict[str, Path]] = {}
    for meta_json in warehouse.rglob("*.metadata.json"):
        if meta_json.parent.name != "metadata":
            continue
        rel = meta_json.relative_to(warehouse)
        parts = rel.parts
        try:
            i = parts.index("metadata")
        except ValueError:
            continue
        if i < 1:
            continue
        table = parts[i - 1]
        ns_parts = parts[: i - 1]
        namespace = ".".join(ns_parts) if ns_parts else "local"
        meta_cur = _pick_current_metadata(meta_json.parent)
        if not meta_cur:
            continue
        found[(namespace, table)] = {"table_dir": meta_json.parent.parent, "meta_json": meta_cur}
    return found

# ---------- Step 1: JSON -> absolute (with remap) ----------
def rewrite_metadata_json_absolute(meta_json: Path, warehouse_dir: Path) -> List[Path]:
    """
    - location -> file://<abs table_dir>
    - snapshots[*].manifest-list -> file://<abs path under new warehouse> (remapped)
    Returns list of absolute manifest-list Paths.
    """
    raw = meta_json.read_text(encoding="utf-8")
    meta = json.loads(raw)
    td = meta_json.parent.parent.resolve()
    changed = False
    ml_abs_paths: List[Path] = []

    new_loc = _to_file_uri(td)
    if meta.get("location") != new_loc:
        meta["location"] = new_loc
        changed = True

    snaps = meta.get("snapshots") or []
    for s in snaps:
        key = "manifest-list" if "manifest-list" in s else ("manifest_list" if "manifest_list" in s else None)
        if not key:
            continue
        ml = s.get(key)
        if isinstance(ml, str):
            old = _from_uri_or_path(ml)
            ml_abs = _map_to_new_warehouse(old, td, warehouse_dir)
            new_ml = _to_file_uri(ml_abs)
            if s[key] != new_ml:
                s[key] = new_ml
                changed = True
            ml_abs_paths.append(ml_abs)

    if changed:
        # No backup to save space
        meta_json.write_text(json.dumps(meta, indent=2) + "\n", encoding="utf-8")
        print(f"[json] absolute: {meta_json}")
    return list(set(ml_abs_paths))

# ---------- Step 2: Avro -> absolute ----------
def rewrite_avro_absolute(ml_paths: List[Path], table_dir: Path, warehouse_dir: Path):
    try:
        from fastavro import reader, writer, parse_schema
    except Exception:
        print("[skip] fastavro not installed; skipping Avro rewrites.")
        return

    for ml_path in ml_paths:
        if not ml_path.exists():
            continue

        with ml_path.open("rb") as f:
            r = reader(f); schema = r.writer_schema; recs = list(r)

        manifests_abs: List[Path] = []
        changed_ml = False
        for rec in recs:
            mp = rec.get("manifest_path")
            if not isinstance(mp, str):
                continue
            old = _from_uri_or_path(mp)
            new_abs = _map_to_new_warehouse(old, table_dir, warehouse_dir)
            new_uri = _to_file_uri(new_abs)
            if new_uri != mp:
                rec["manifest_path"] = new_uri
                changed_ml = True
            manifests_abs.append(new_abs)

        if changed_ml:
            # No backup to save space
            with ml_path.open("wb") as out:
                writer(out, parse_schema(schema), recs)
            print(f"[ml ] {ml_path} -> absolute manifest_path")

        for man_path in manifests_abs:
            if not man_path.exists():
                candidate = (table_dir / "metadata" / man_path.name)
                if candidate.exists():
                    man_path = candidate
                else:
                    continue

            with man_path.open("rb") as f:
                r = reader(f); schema = r.writer_schema; m_recs = list(r)

            changed_m = False
            for rec in m_recs:
                df = rec.get("data_file") or rec.get("file")
                if not (isinstance(df, dict) and isinstance(df.get("file_path"), str)):
                    continue
                fp = df["file_path"]
                old = _from_uri_or_path(fp)
                new_abs = _map_to_new_warehouse(old, table_dir, warehouse_dir)
                new_uri = _to_file_uri(new_abs)
                if new_uri != fp:
                    df["file_path"] = new_uri
                    changed_m = True

            if changed_m:
                # No backup to save space
                with man_path.open("wb") as out:
                    writer(out, parse_schema(schema), m_recs)
                print(f"[m  ] {man_path} -> absolute data_file.file_path")

# ---------- Step 3: SQLite pointers (opportunistic fix) ----------
def update_sqlite_catalog(sqlite_path: Path, warehouse_dir: Path):
    con = sqlite3.connect(str(sqlite_path))
    try:
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        for t, in cur.fetchall():
            cur.execute(f"PRAGMA table_info('{t}')")
            cols = {row[1] for row in cur.fetchall()}
            for col in ("metadata_location", "previous_metadata_location"):
                if col not in cols:
                    continue
                cur.execute(f"SELECT rowid, {col} FROM '{t}'")
                updates = 0
                for rowid, loc in cur.fetchall():
                    if not isinstance(loc, str):
                        continue
                    oldp = _from_uri_or_path(loc)
                    if not oldp.is_absolute():
                        continue
                    suf = _suffix_after_segment(oldp, "warehouse")
                    if not suf:
                        continue
                    new_abs = (warehouse_dir / suf).resolve()
                    new_uri = _to_file_uri(new_abs)
                    if new_uri != loc:
                        cur.execute(f"UPDATE '{t}' SET {col}=? WHERE rowid=?", (new_uri, rowid))
                        updates += 1
                if updates:
                    print(f"[sqlite] {t}.{col}: updated {updates} row(s)")
        con.commit()
    finally:
        con.close()

# ---------- Step 4: Register (or re-register) ----------
def ensure_namespace_chain(catalog, namespace: str):
    existing = set(tuple(ns) for ns in catalog.list_namespaces())
    parts = namespace.split(".")
    for i in range(1, len(parts) + 1):
        sub = tuple(parts[:i])
        if sub not in existing:
            catalog.create_namespace(sub)
            existing.add(sub)
            print(f"[ns ] created namespace: {'.'.join(sub)}")

def register_tables_sqlite(tables: Dict[Tuple[str,str], Dict[str,Path]], sqlite_uri: str, warehouse_uri: str):
    from pyiceberg.catalog import load_catalog
    catalog = load_catalog("default", type="sql", uri=sqlite_uri, warehouse=warehouse_uri)
    print(f"[cat] Connected to catalog: {sqlite_uri}")
    for (ns, tbl), info in sorted(tables.items()):
        ensure_namespace_chain(catalog, ns)
        ident = f"{ns}.{tbl}"
        meta_uri = _to_file_uri(info["meta_json"].resolve())
        try:
            catalog.register_table(ident, meta_uri)
            print(f"[reg] {ident} -> {meta_uri}")
        except Exception:
            # If already exists, just load to confirm
            _ = catalog.load_table(ident)
            print(f"[reg] {ident} already present; loaded OK")

# ---------- Step 5: Verify ----------
def verify_scan(tables: Dict[Tuple[str,str], Dict[str,Path]], sqlite_uri: str, warehouse_uri: str, limit: int = 5):
    try:
        import pyarrow  # noqa
    except Exception:
        print("[note] pyarrow not installed; skipping to_pandas() verification.")
        return
    from pyiceberg.catalog import load_catalog
    catalog = load_catalog("default", type="sql", uri=sqlite_uri, warehouse=warehouse_uri)

    for (ns, tbl) in sorted(tables.keys()):
        ident = f"{ns}.{tbl}"
        try:
            t = catalog.load_table(ident)
            print(f"[load] {ident} -> OK")
            try:
                nm = t.name()
            except TypeError:
                nm = t.name
            print(f"      name: {nm}")
            try:
                df = t.scan(limit=limit).to_pandas()
                print(f"      scan -> {len(df)} row(s); head:\n{df.head()}\n")
            except Exception as e:
                print(f"[warn] scan() failed for {ident}: {e}")
        except Exception as e:
            print(f"[warn] load_table failed for {ident}: {e}")

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser(description="Migrate a PyIceberg[sqlite] catalog + warehouse to a new base path.")
    ap.add_argument("--new_base", required=True, help="Directory containing 'iceberg.db' and 'warehouse/'. (Required)")
    ap.add_argument("--verify_limit", type=int, default=5, help="Rows to read in verification scan (default: 5).")
    args = ap.parse_args()

    new_base = Path(args.new_base).resolve()
    sqlite_path = new_base / "iceberg.db"
    warehouse_dir = new_base / "warehouse"
    sqlite_uri = f"sqlite:///{sqlite_path.as_posix()}"
    warehouse_uri = f"file://{warehouse_dir.as_posix()}"

    if not sqlite_path.exists():
        print(f"ERROR: Missing SQLite file: {sqlite_path}"); sys.exit(2)
    if not warehouse_dir.exists():
        print(f"ERROR: Missing warehouse dir: {warehouse_dir}"); sys.exit(2)

    tables = _discover_tables(warehouse_dir)
    if not tables:
        print(f"[disc] Found 0 tables under {warehouse_dir}")
        print("       Expecting layout like: <warehouse>/<ns>/<table>/metadata/vN.metadata.json")
        sys.exit(1)
    print(f"[disc] Found {len(tables)} table(s): " + ", ".join([f"{ns}.{t}" for (ns,t) in tables.keys()]))

    # 1) JSON rewrite (location + manifest-list remap)
    ml_index: Dict[Tuple[str,str], List[Path]] = {}
    for key, info in tables.items():
        ml_paths = rewrite_metadata_json_absolute(info["meta_json"], warehouse_dir)
        ml_index[key] = ml_paths

    # 2) Avro rewrite (manifest-lists + manifests)
    for key, info in tables.items():
        td = info["table_dir"]
        rewrite_avro_absolute(ml_index.get(key, []), td, warehouse_dir)

    # 3) SQLite pointers (opportunistic)
    update_sqlite_catalog(sqlite_path, warehouse_dir)

    # 4) Register tables
    register_tables_sqlite(tables, sqlite_uri, warehouse_uri)

    # 5) Verify
    verify_scan(tables, sqlite_uri, warehouse_uri, limit=args.verify_limit)

    print("\n✔ Migration complete.")
    print("Load example:")
    print(f'  from pyiceberg.catalog import load_catalog')
    print(f'  catalog = load_catalog("default", type="sql", uri="{sqlite_uri}", warehouse="{warehouse_uri}")')
    print(f'  tbl = catalog.load_table("<namespace>.<table>")')
    print(f'  df = tbl.scan(limit=10).to_pandas()')

if __name__ == "__main__":
    main()
