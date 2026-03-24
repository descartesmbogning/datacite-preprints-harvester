from __future__ import annotations
import argparse, json, time
from pathlib import Path
from typing import Optional, Iterable

import pandas as pd
import requests

# Optional but recommended for large Parquet streaming
try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.dataset as ds
    _HAVE_ARROW = True
except Exception:
    _HAVE_ARROW = False

DATACITE_BASE = "https://api.datacite.org"
DEFAULT_MAILTO = "your.email@example.com"

def _ua(mailto: str) -> str:
    return f"DataCite-ClientsEnricher/1.2 (mailto:{mailto})"

# ---------------------------- HTTP ----------------------------
def _request_json(
    url: str,
    params: Optional[dict] = None,
    headers: Optional[dict] = None,
    retries: int = 6,
    base_sleep: float = 0.5,
    timeout: int = 60,
    verbose: bool = False,
) -> dict:
    headers = {"Accept": "application/vnd.api+json", **(headers or {})}
    last = None
    for i in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                if verbose:
                    print(f"[retry {i+1}] {r.status_code} from {url}", flush=True)
                time.sleep(base_sleep * (2 ** i))
                continue
            try:
                print("DataCite error:", r.json(), flush=True)
            except Exception:
                print("DataCite error text:", r.text[:800], flush=True)
            r.raise_for_status()
        except requests.RequestException as e:
            last = e
            if verbose:
                print(f"[retry {i+1}] network error: {e}", flush=True)
            time.sleep(base_sleep * (2 ** i))
    if last:
        raise last
    return {}

def _flatten_client(record: dict) -> dict:
    if not record:
        return {}
    out = {}
    out["id"] = record.get("id")
    out["type"] = record.get("type")
    attr = record.get("attributes") or {}
    rel  = record.get("relationships") or {}

    out["name"]         = attr.get("name")
    out["displayName"]  = attr.get("displayName") or attr.get("name")
    out["symbol"]       = attr.get("symbol")
    out["clientType"]   = attr.get("clientType")
    out["isActive"]     = attr.get("isActive")
    out["year"]         = attr.get("year")
    out["description"]  = attr.get("description")
    out["systemEmail"]  = attr.get("systemEmail")
    out["contactEmail"] = attr.get("contactEmail")
    out["url"]          = attr.get("url")
    out["domains"]      = ", ".join(attr.get("domains") or []) if isinstance(attr.get("domains"), list) else attr.get("domains")
    out["createdAt"]    = attr.get("createdAt")
    out["updatedAt"]    = attr.get("updatedAt")

    prov = (rel.get("provider") or {}).get("data") or {}
    mem  = (rel.get("member") or {}).get("data") or {}
    out["provider_id"]   = prov.get("id")
    out["provider_type"] = prov.get("type")
    out["member_id"]     = mem.get("id")
    out["member_type"]   = mem.get("type")

    out["raw_json"]      = json.dumps(record, ensure_ascii=False)
    return out

def _fetch_client_by_id(client_id: str, mailto: str, verbose: bool = False):
    # Try direct
    url = f"{DATACITE_BASE}/clients/{client_id}"
    if verbose:
        print(f"  → GET {url}", flush=True)
    try:
        js = _request_json(url, headers={"User-Agent": _ua(mailto)}, verbose=verbose)
        data = js.get("data")
        if isinstance(data, dict):
            return data
    except Exception as e:
        print(f"[WARN] direct fetch failed for {client_id}: {e}", flush=True)

    # Fallback search
    qurl = f"{DATACITE_BASE}/clients"
    if verbose:
        print(f"  → GET {qurl}?query={client_id}", flush=True)
    try:
        js = _request_json(qurl, params={"query": client_id},
                           headers={"User-Agent": _ua(mailto)}, verbose=verbose)
        arr = js.get("data") or []
        for item in arr:
            if item.get("id") == client_id:
                return item
        if arr:
            return arr[0]
    except Exception as e:
        print(f"[WARN] fallback search failed for {client_id}: {e}", flush=True)
    return None

# ------------------------ merged picking -----------------------
def _pick_latest_merged(merged_dir: Path, prefix: str, ext: str) -> Path:
    """Pick the newest file by mtime when multiple merged files exist."""
    candidates = sorted(merged_dir.glob(prefix + "*" + ext), key=lambda p: p.stat().st_mtime, reverse=True)
    if not candidates:
        raise SystemExit(f"No merged files like {prefix}*{ext} found in {merged_dir}")
    return candidates[0]

def _iter_client_ids_from_file(p: Path) -> Iterable[str]:
    """Stream client_id values efficiently without loading the whole file."""
    if p.suffix.lower() == ".parquet":
        # Parquet: read only one column (fast)
        df = pd.read_parquet(p, columns=["client_id"])
        for v in df["client_id"].dropna().astype(str):
            s = v.strip()
            if s:
                yield s
    else:
        # CSV: chunked
        for chunk in pd.read_csv(p, usecols=["client_id"], chunksize=1_000_000, dtype=str):
            for v in chunk["client_id"].dropna():
                s = v.strip()
                if s:
                    yield s

# ------------------------ labeling helpers ---------------------
def _year_from_iso(s: Optional[str]):
    if pd.isna(s) or s is None:
        return pd.NA
    s = str(s)
    return s[:4] if len(s) >= 4 and s[:4].isdigit() else pd.NA

def _label_csv_streaming(in_path: Path, out_path: Path, map_func):
    """Chunked CSV → CSV with server_name + helper year columns."""
    header_written = False
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")
    if tmp.exists():
        tmp.unlink()
    for i, chunk in enumerate(pd.read_csv(in_path, chunksize=1_000_000, dtype=str)):
        if "server_name" not in chunk.columns:
            chunk["server_name"] = chunk["client_id"].map(map_func)
        if "created" in chunk.columns:
            chunk["created_year"] = chunk["created"].map(_year_from_iso)
        else:
            chunk["created_year"] = pd.NA
        if "registered" in chunk.columns:
            chunk["registered_year"] = chunk["registered"].map(_year_from_iso)
        else:
            chunk["registered_year"] = pd.NA

        # keep order if present, otherwise write everything
        keep = [
            "doi","url","publisher","prefix","client_id","provider_id",
            "resource_type","resource_type_general","title","created","registered",
            "updated","published","published_year","server_name","created_year","registered_year"
        ]
        for c in keep:
            if c not in chunk.columns:
                chunk[c] = pd.NA
        slim = chunk[keep]

        mode = "w" if not header_written else "a"
        slim.to_csv(tmp, index=False, header=not header_written, mode=mode, encoding="utf-8")
        header_written = True
        if (i+1) % 5 == 0:
            print(f"  wrote {i+1:,} CSV chunks…", flush=True)
    tmp.replace(out_path)

def _label_parquet_streaming(in_path: Path, out_path: Path, map_func, batch_notice_every: int = 5):
    """True streaming Parquet → Parquet with server_name + helper year columns (atomic)."""
    if not _HAVE_ARROW:
        raise SystemExit("pyarrow not available; cannot stream Parquet. Install pyarrow or use CSV.")

    def add_columns(tbl: 'pa.Table') -> 'pa.Table':
        pdf = tbl.to_pandas(types_mapper=pd.ArrowDtype)
        if "server_name" not in pdf.columns:
            pdf["server_name"] = pdf["client_id"].map(map_func)
        pdf["created_year"]   = pdf["created"].map(_year_from_iso)     if "created"   in pdf.columns else pd.NA
        pdf["registered_year"] = pdf["registered"].map(_year_from_iso) if "registered" in pdf.columns else pd.NA

        # preserve a useful column set (present or NA-added)
        keep = [
            "doi","url","publisher","prefix","client_id","provider_id",
            "resource_type","resource_type_general","title","created","registered",
            "updated","published","published_year","server_name","created_year","registered_year"
        ]
        for c in keep:
            if c not in pdf.columns:
                pdf[c] = pd.NA
        return pa.Table.from_pandas(pdf[keep], preserve_index=False)

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(out_path.suffix + ".part")
    if tmp.exists():
        tmp.unlink()

    dataset = ds.dataset(str(in_path), format="parquet")
    # Only load columns we need to derive output
    needed = {
        "doi","url","publisher","prefix","client_id","provider_id",
        "resource_type","resource_type_general","title","created","registered",
        "updated","published","published_year"
    }
    needed = list(needed)
    scanner = ds.Scanner.from_dataset(dataset, columns=needed, use_threads=True)

    writer = None
    try:
        for i, rb in enumerate(scanner.to_batches()):
            tbl = pa.Table.from_batches([rb])
            tbl_out = add_columns(tbl)
            if writer is None:
                writer = pq.ParquetWriter(tmp, tbl_out.schema)
            writer.write_table(tbl_out)
            if (i+1) % batch_notice_every == 0:
                print(f"  wrote {i+1} parquet batches…", flush=True)
    finally:
        if writer is not None:
            writer.close()
    tmp.replace(out_path)

# ----------------------------- main op -----------------------------
def run_clients_enricher(
    merged: Path,
    clients_catalog: Path = Path("data/output/clients_catalog.csv"),
    out_clients_xlsx: Path = Path("data/output/clients_metadata.xlsx"),
    out_merged: Path | None = None,
    mailto: str = DEFAULT_MAILTO,
    verbose: bool = False,
    max_clients: Optional[int] = None,
    latest_prefix: str = "datacite_preprints_merged_",
    latest_ext: str = ".parquet",
) -> dict:
    start_ts = time.time()

    # 0) Resolve latest merged if merged points to a directory
    if merged.is_dir():
        print(f"[info] --merged is a directory; picking latest merged file in {merged}", flush=True)
        merged = _pick_latest_merged(merged, prefix=latest_prefix, ext=latest_ext)

    print(f"[info] Reading client_id from: {merged}", flush=True)

    # 1) Build unique client_id set quickly (scan-only one column)
    unique_ids: set[str] = set()
    n_rows_scanned = 0
    for cid in _iter_client_ids_from_file(merged):
        n_rows_scanned += 1
        unique_ids.add(cid)
        if n_rows_scanned % 500_000 == 0:
            print(f"  scanned rows ≈ {n_rows_scanned:,}, unique client_id = {len(unique_ids):,}", flush=True)
        if max_clients and len(unique_ids) >= max_clients:
            break

    client_ids = sorted(unique_ids)
    print(f"[info] Unique client_id count: {len(client_ids):,}", flush=True)

    # 2) Load existing catalog, fetch only new client_ids
    def _load_catalog(path: Path) -> pd.DataFrame:
        if path.exists():
            try:
                return pd.read_csv(path, dtype=str).fillna("")
            except Exception as e:
                print(f"[WARN] Could not read existing catalog: {e}", flush=True)
        return pd.DataFrame(columns=[
            "id","type","name","displayName","symbol","clientType","isActive","year","description",
            "systemEmail","contactEmail","url","domains","createdAt","updatedAt",
            "provider_id","provider_type","member_id","member_type","raw_json"
        ])

    def _save_catalog(df: pd.DataFrame, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        path_tmp = path.with_suffix(".part")
        df.to_csv(path_tmp, index=False, encoding="utf-8")
        path_tmp.replace(path)

    catalog_df = _load_catalog(clients_catalog)
    known = set(catalog_df["id"].tolist()) if not catalog_df.empty else set()

    to_fetch = [cid for cid in client_ids if cid not in known]
    print(f"[info] New client_ids to fetch: {len(to_fetch):,}", flush=True)

    new_rows = []
    for i, cid in enumerate(to_fetch, 1):
        if verbose or (i % 25 == 0):
            print(f"[{i}/{len(to_fetch)}] fetching {cid}", flush=True)
        rec = _fetch_client_by_id(cid, mailto, verbose=verbose)
        if rec:
            new_rows.append(_flatten_client(rec))
        else:
            new_rows.append({
                "id": cid, "type": "", "name": "", "displayName": "", "symbol": "",
                "clientType": "", "isActive": "", "year": "", "description": "",
                "systemEmail": "", "contactEmail": "", "url": "", "domains": "",
                "createdAt": "", "updatedAt": "", "provider_id": "", "provider_type": "",
                "member_id": "", "member_type": "", "raw_json": ""
            })
        if i % 10 == 0:
            time.sleep(0.2)  # polite pool

    if new_rows:
        add = pd.DataFrame(new_rows)
        catalog_df = pd.concat([catalog_df, add], ignore_index=True)
        catalog_df.drop_duplicates(subset=["id"], keep="first", inplace=True)

    # 3) Persist catalog & Excel (atomic)
    print(f"[info] Writing catalog CSV → {clients_catalog}", flush=True)
    _save_catalog(catalog_df, clients_catalog)

    out_clients_xlsx.parent.mkdir(parents=True, exist_ok=True)
    print(f"[info] Writing Excel → {out_clients_xlsx}", flush=True)
    with pd.ExcelWriter(out_clients_xlsx, engine="openpyxl") as xw:
        catalog_df.sort_values("id").to_excel(xw, index=False, sheet_name="clients")
        if "provider_id" in catalog_df.columns:
            prov = (
                catalog_df.assign(provider_id=catalog_df["provider_id"].replace("", pd.NA))
                          .dropna(subset=["provider_id"])
                          .groupby("provider_id").size().reset_index(name="n_clients")
                          .sort_values("n_clients", ascending=False)
            )
            prov.to_excel(xw, index=False, sheet_name="providers_summary")

    # 4) Build mapping dicts and label merged
    print("[info] Labeling merged file with server_name…", flush=True)
    id_to_name    = dict(zip(catalog_df["id"], catalog_df["name"]))
    id_to_display = dict(zip(catalog_df["id"], catalog_df.get("displayName", catalog_df["name"])))

    def _server_for_val(cid):
        if pd.isna(cid):
            return ""
        cid = str(cid).strip()
        return (id_to_name.get(cid) or id_to_display.get(cid) or "").strip()

    if out_merged is None:
        # default: same folder as merged with *_labeled
        out_merged = merged.with_name(merged.stem + "_labeled" + merged.suffix)

    out_merged.parent.mkdir(parents=True, exist_ok=True)

    if merged.suffix.lower() == ".parquet":
        _label_parquet_streaming(merged, out_merged, _server_for_val)
    else:
        _label_csv_streaming(merged, out_merged, _server_for_val)

    took = time.time() - start_ts
    return {
        "unique_client_ids": len(client_ids),
        "new_clients_fetched": len(new_rows),
        "scanned_rows_estimate": n_rows_scanned,
        "clients_catalog_csv": str(clients_catalog),
        "clients_excel": str(out_clients_xlsx),
        "merged_labeled": str(out_merged),
        "seconds": round(took, 1),
    }

# ----------------------------- CLI -----------------------------
def _parse_args():
    ap = argparse.ArgumentParser(description="Enrich DataCite merged file with client metadata and server_name mapping.")
    ap.add_argument("--merged", required=True,
                    help="Path to merged .csv/.parquet OR a directory (then newest file is chosen).")
    ap.add_argument("--clients-catalog", default="data/output/clients_catalog.csv",
                    help="Persistent CSV catalog (grows with new client_ids).")
    ap.add_argument("--out-clients-xlsx", default="data/output/clients_metadata.xlsx",
                    help="Excel workbook with client metadata and provider summary.")
    ap.add_argument("--out-merged", default=None,
                    help="Output labeled merged path (.csv or .parquet). Default: next to merged file.")
    ap.add_argument("--mailto", default=DEFAULT_MAILTO,
                    help="Email for DataCite polite pool UA.")
    ap.add_argument("--verbose", action="store_true",
                    help="Print per-request details.")
    ap.add_argument("--max-clients", type=int, default=None,
                    help="Only fetch first N new client_ids (smoke test).")
    ap.add_argument("--latest-prefix", default="datacite_preprints_merged_",
                    help="When --merged is a directory, filename prefix to search (e.g., datacite_texts_merged_).")
    ap.add_argument("--latest-ext", default=".parquet",
                    help="When --merged is a directory, filename extension to search (e.g., .parquet).")
    return ap.parse_args()

def main():
    args = _parse_args()
    merged_path = Path(args.merged)
    summary = run_clients_enricher(
        merged=merged_path,
        clients_catalog=Path(args.clients_catalog),
        out_clients_xlsx=Path(args.out_clients_xlsx),
        out_merged=Path(args.out_merged) if args.out_merged else None,
        mailto=args.mailto,
        verbose=args.verbose,
        max_clients=args.max_clients,
        latest_prefix=args.latest_prefix,
        latest_ext=args.latest_ext,
    )
    print(json.dumps(summary, indent=2), flush=True)

__all__ = ["run_clients_enricher", "main"]
