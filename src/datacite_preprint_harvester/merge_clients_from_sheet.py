# src/datacite_preprint_harvester/merge_clients_from_sheet.py
from __future__ import annotations
import argparse, io
from pathlib import Path
from typing import Optional, List

import pandas as pd
import requests


def _csv_export_url(sheet_url: Optional[str], sheet_id: Optional[str], gid: Optional[str]) -> str:
    """Build a Google Sheets CSV export URL from a view URL or explicit ids."""
    if sheet_url:
        if "/export" in sheet_url and "format=csv" in sheet_url:
            return sheet_url
        try:
            sid = sheet_url.split("/d/")[1].split("/", 1)[0]
            g = sheet_url.split("gid=")[-1].split("&")[0] if "gid=" in sheet_url else (gid or "0")
            return f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={g}"
        except Exception:
            return sheet_url  # fall back (maybe it's already a direct CSV link)

    if not sheet_id:
        raise SystemExit("You must provide --sheet-url or --sheet-id.")
    if not gid:
        gid = "0"
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"


def _read_sheet_csv(sheet_url: str, sheet_local: Optional[Path] = None, timeout: int = 60) -> pd.DataFrame:
    """Read Google Sheet as CSV (or a local CSV if provided)."""
    if sheet_local:
        return pd.read_csv(sheet_local, dtype=str).fillna("")
    resp = requests.get(sheet_url, timeout=timeout)
    resp.raise_for_status()
    return pd.read_csv(io.StringIO(resp.text), dtype=str).fillna("")


def merge_clients_with_sheet(
    clients_catalog: Path,
    sheet_url: Optional[str] = None,
    sheet_id: Optional[str] = None,
    sheet_gid: Optional[str] = None,
    sheet_local_csv: Optional[Path] = None,
    on_left: str = "id",
    on_right: str = "source_id",
    select_cols: Optional[List[str]] = None,  # columns to bring from sheet
    how: str = "left",
    # NEW: write targets can be provided independently
    out_csv: Optional[Path] = Path("data/output/clients_catalog_enriched.csv"),
    out_xlsx: Optional[Path] = None,
    also_parquet: Optional[Path] = None,
) -> dict:
    """
    Merge local clients catalog (CSV) with a Google Sheet, matching on_left↔on_right.
    You can request BOTH CSV and XLSX outputs (and optional Parquet).
    Returns a dict with written paths.
    """
    # Load catalog
    df = pd.read_csv(clients_catalog, dtype=str).fillna("")
    if on_left not in df.columns:
        raise SystemExit(f"Column '{on_left}' not found in {clients_catalog}")

    # Get sheet data
    sheet_csv_url = _csv_export_url(sheet_url, sheet_id, sheet_gid)
    sheet_df = _read_sheet_csv(sheet_csv_url, sheet_local_csv)

    if on_right not in sheet_df.columns:
        raise SystemExit(f"Column '{on_right}' not found in the sheet (available: {list(sheet_df.columns)[:12]}...)")

    # Keep only requested columns (plus join key)
    if select_cols:
        missing = [c for c in select_cols if c not in sheet_df.columns]
        if missing:
            raise SystemExit(f"Missing columns in sheet: {missing}")
        sheet_df = sheet_df[[on_right] + select_cols]
    else:
        # bring everything; ensure on_right is present
        if on_right not in sheet_df.columns:
            sheet_df[on_right] = ""
        # otherwise keep all as-is

    # De-duplicate on the joining key
    sheet_df = sheet_df.drop_duplicates(subset=[on_right], keep="first")

    # Merge
    merged = df.merge(sheet_df, how=how, left_on=on_left, right_on=on_right)

    # Save CSV
    written_csv = None
    if out_csv:
        out_csv.parent.mkdir(parents=True, exist_ok=True)
        merged.to_csv(out_csv, index=False, encoding="utf-8")
        written_csv = str(out_csv)

    # Save XLSX
    written_xlsx = None
    if out_xlsx:
        out_xlsx.parent.mkdir(parents=True, exist_ok=True)
        with pd.ExcelWriter(out_xlsx, engine="openpyxl") as xw:
            merged.to_excel(xw, index=False, sheet_name="catalog_enriched")
        written_xlsx = str(out_xlsx)

    # Save Parquet
    written_parquet = None
    if also_parquet:
        also_parquet.parent.mkdir(parents=True, exist_ok=True)
        merged.to_parquet(also_parquet, index=False)
        written_parquet = str(also_parquet)

    return {
        "out_csv": written_csv,
        "out_xlsx": written_xlsx,
        "out_parquet": written_parquet,
        "rows": int(merged.shape[0]),
        "cols": int(merged.shape[1]),
    }


def _parse_args():
    ap = argparse.ArgumentParser(
        description="Merge clients_catalog with a Google Sheet (id↔source_id) and select columns to add. Saves CSV and/or XLSX."
    )
    ap.add_argument("--clients-catalog", default="data/output/clients_catalog.csv", help="Path to clients_catalog.csv")
    # Provide either --sheet-url OR (--sheet-id AND --gid). Or pass --sheet-local to read a local CSV.
    ap.add_argument("--sheet-url", default=None, help="Full Google Sheet URL (view or CSV-export).")
    ap.add_argument("--sheet-id", default=None, help="Sheet ID (if not using --sheet-url).")
    ap.add_argument("--gid", default=None, help="Sheet GID/tab (if not using --sheet-url); default 0.")
    ap.add_argument("--sheet-local", default=None, help="Path to a local CSV exported from the sheet (offline mode).")
    ap.add_argument("--on-left", default="id", help="Join key in clients catalog (default: id).")
    ap.add_argument("--on-right", default="source_id", help="Join key in sheet (default: source_id).")
    ap.add_argument("--select-cols", default=None,
                    help="Comma-separated list of sheet columns to include, e.g. 'server_name,platform,notes'. If omitted, include all.")
    ap.add_argument("--how", default="left", choices=["left","right","inner","outer"], help="Pandas merge how (default: left).")
    # NEW: explicit outputs
    ap.add_argument("--out-csv", default="data/output/clients_catalog_enriched.csv", help="CSV output path (optional).")
    ap.add_argument("--out-xlsx", default=None, help="XLSX output path (optional).")
    ap.add_argument("--also-parquet", default=None, help="Optional path to also save a Parquet version.")
    return ap.parse_args()


def main():
    args = _parse_args()
    select_cols = [c.strip() for c in args.select_cols.split(",")] if args.select_cols else None
    sheet_local = Path(args.sheet_local) if args.sheet_local else None
    summary = merge_clients_with_sheet(
        clients_catalog=Path(args.clients_catalog),
        sheet_url=args.sheet_url,
        sheet_id=args.sheet_id,
        sheet_gid=args.gid,
        sheet_local_csv=sheet_local,
        on_left=args.on_left,
        on_right=args.on_right,
        select_cols=select_cols,
        how=args.how,
        out_csv=Path(args.out_csv) if args.out_csv else None,
        out_xlsx=Path(args.out_xlsx) if args.out_xlsx else None,
        also_parquet=Path(args.also_parquet) if args.also_parquet else None,
    )
    print(summary)
