"""
Merge multiple Parquet shard folders into one dataset (DataCite)
----------------------------------------------------------------
- Unifies schemas across shards (casts 'score' to float if needed)
- Recursively discovers *.parquet under provided parent dirs
- Writes a single merged Parquet (and optional PKL snapshot)

Typical usage (via scripts/merge_runs.py):
    python scripts/merge_runs.py --base-dir data/runs/datacite_preprints \
        --subdir-name datacite_preprints_batches --out data/merged --name datacite_preprints_merged
"""
from __future__ import annotations

import os, re, glob, time, logging
from datetime import datetime
from typing import List, Tuple, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logging.getLogger().setLevel(logging.ERROR)

# ========= HELPERS =========
def _norm(p: str) -> str:
    return os.path.normpath(os.path.expanduser(p))

def _ensure_dir(path: str) -> str:
    path = _norm(path)
    os.makedirs(path, exist_ok=True)
    return path

DATE_RANGE_REGEXES = [
    re.compile(r'(?P<start>\d{4}-\d{2}-\d{2}).{0,5}(?P<end>\d{4}-\d{2}-\d{2})'),
    re.compile(r'from[_\-]?(?P<start>\d{4}-\d{2}-\d{2}).{0,5}until[_\-]?(?P<end>\d{4}-\d{2}-\d{2})'),
]

def extract_date_range_from_name(name: str) -> Tuple[Optional[str], Optional[str]]:
    base = os.path.basename(name)
    for rx in DATE_RANGE_REGEXES:
        m = rx.search(base)
        if m:
            return m.group('start'), m.group('end')
    for rx in DATE_RANGE_REGEXES:
        m = rx.search(name)
        if m:
            return m.group('start'), m.group('end')
    return None, None

def discover_parquet_files(parents: List[str], name_contains: str = "", one_level_deep: bool = True) -> List[dict]:
    records = []
    for parent in parents:
        parent = _norm(parent)
        if not os.path.exists(parent):
            print(f"[warn] Missing parent dir: {parent}")
            continue

        folders = [parent]
        if one_level_deep:
            folders.extend(
                os.path.join(parent, d)
                for d in os.listdir(parent)
                if os.path.isdir(os.path.join(parent, d))
            )

        seen = set()
        for folder in folders:
            pattern = os.path.join(folder, "**", "*.parquet")
            for fpath in glob.iglob(pattern, recursive=True):
                fpath = _norm(fpath)
                if fpath in seen:
                    continue
                seen.add(fpath)

                fname = os.path.basename(fpath)
                if name_contains and name_contains not in fname:
                    continue

                records.append({
                    "folder": folder,
                    "filepath": fpath,
                    "filename": fname,
                })
    return records

def _collect_unified_schema(paths: List[str]) -> pa.schema:
    schemas = []
    for p in paths:
        try:
            pf = pq.ParquetFile(p)
            schema = pf.schema_arrow
            # Align score→float64 if present
            if "score" in schema.names and not schema.field("score").type.equals(pa.float64()):
                fields = []
                for field in schema:
                    if field.name == "score":
                        fields.append(pa.field("score", pa.float64()))
                    else:
                        fields.append(field)
                schema = pa.schema(fields)
            schemas.append(schema)
        except Exception as e:
            print(f"[warn] Could not read schema for {p}: {e}")
    return pa.unify_schemas(schemas) if schemas else pa.schema([])

def _align_table_to_schema(tbl: pa.Table, schema: pa.schema) -> pa.Table:
    cols = []
    for f in schema:
        if f.name in tbl.schema.names:
            col = tbl[f.name]
            if not col.type.equals(f.type):
                try:
                    col = pa.compute.cast(col, f.type)
                except Exception:
                    pass
            cols.append(col)
        else:
            cols.append(pa.nulls(length=tbl.num_rows, type=f.type).rename(f.name))
    return pa.table(cols, schema=schema)

# ========= MAIN =========
def merge_parquets(
    input_parent_dirs: List[str],
    output_dir: str,
    merged_basename: str = "datacite_preprints_merged",
    name_contains: str = "",
    one_level_deep: bool = True,
    write_pkl_too: bool = False,
):
    output_dir = _ensure_dir(output_dir)

    # 1) Discover shards
    recs = discover_parquet_files(input_parent_dirs, name_contains=name_contains, one_level_deep=one_level_deep)
    if not recs:
        raise RuntimeError("No .parquet files found under the provided parent directories.")
    all_paths = [r["filepath"] for r in recs]

    # 2) Unified schema
    unified_schema = _collect_unified_schema(all_paths)
    if len(unified_schema) == 0:
        raise RuntimeError("Could not determine a unified schema from the shards.")

    # 3) Prepare outputs
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_parquet = _norm(os.path.join(output_dir, f"{merged_basename}_{ts}.parquet"))
    out_parquet_tmp = out_parquet + ".tmp"
    out_pkl = _norm(os.path.join(output_dir, f"{merged_basename}_{ts}.pkl"))

    total_rows_written = 0
    parquet_ok = True
    writer = None

    os.makedirs(os.path.dirname(out_parquet_tmp), exist_ok=True)

    try:
        try:
            writer = pq.ParquetWriter(out_parquet_tmp, schema=unified_schema, use_dictionary=True, compression="snappy")
        except Exception as e:
            parquet_ok = False
            print(f"[warn] Could not open ParquetWriter, will skip Parquet output: {e}")

        for p in all_paths:
            try:
                table = pq.read_table(p)
                table = _align_table_to_schema(table, unified_schema)
                if parquet_ok and writer is not None:
                    writer.write_table(table)
                total_rows_written += table.num_rows
            except Exception as e:
                print(f"[warn] Skipping unreadable parquet during merge: {p} ({e})")
    finally:
        if writer is not None:
            writer.close()

    # finalize parquet
    if parquet_ok and os.path.exists(out_parquet_tmp):
        try:
            os.replace(out_parquet_tmp, out_parquet)
        except Exception as e:
            print(f"[warn] Could not finalize Parquet file: {e}")
            parquet_ok = False
            try:
                os.remove(out_parquet_tmp)
            except Exception:
                pass

    # optional PKL
    if write_pkl_too and parquet_ok:
        try:
            merged_df = pd.read_parquet(out_parquet)
            merged_df.to_pickle(out_pkl)
            del merged_df
        except Exception as e:
            print(f"[warn] Could not write PKL: {e}")

    print("✅ Done.")
    if parquet_ok:
        print(f"Merged Parquet: {out_parquet}")
    else:
        print("Merged Parquet: (skipped)")
    if write_pkl_too:
        print(f"Merged PKL:     {out_pkl}")

    return {
        "parquet": out_parquet if parquet_ok else None,
        "pkl": out_pkl if (write_pkl_too and parquet_ok) else None,
        "output_dir": output_dir,
        "rows_written": int(total_rows_written),
        "inputs": all_paths,
    }
