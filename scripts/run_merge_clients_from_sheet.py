#!/usr/bin/env python3
"""
Convenience runner for merging clients_catalog.csv with the Google Sheet
(classification of preprint servers).
Run with:
    python scripts/run_merge_clients_from_sheet.py
"""

import sys
from pathlib import Path

# ---- src shim ----
ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
# -------------------

from datacite_preprint_harvester.merge_clients_from_sheet import merge_clients_with_sheet


def main():
    # Inputs
    CLIENTS_CATALOG = Path("data/output/clients_catalog.csv")
    SHEET_URL = (
        "https://docs.google.com/spreadsheets/d/"
        "10_7FdcpZjntqFsEHIii7bAM72uF__of_iUohSD5w8w4"
        "/edit?gid=881157301"
    )
    ON_LEFT = "id"
    ON_RIGHT = "source_id"

    # Choose the sheet columns you want to bring over
    SELECT_COLS = [
        "How do they define themselves (as a preprint server or)",
        "Server Main Page Link",
        "Dimensions Query",
        "OpenAlex query",
    ]

    # Outputs (ask for BOTH)
    OUT_CSV = Path("data/output/clients_catalog_enriched.csv")
    OUT_XLSX = Path("data/output/clients_catalog_enriched.xlsx")
    OUT_PARQUET = Path("data/output/clients_catalog_enriched.parquet")

    print("[info] Merging clients catalog with Google Sheet…", flush=True)
    summary = merge_clients_with_sheet(
        clients_catalog=CLIENTS_CATALOG,
        sheet_url=SHEET_URL,
        on_left=ON_LEFT,
        on_right=ON_RIGHT,
        select_cols=SELECT_COLS,
        how="left",
        out_csv=OUT_CSV,
        out_xlsx=OUT_XLSX,
        # also_parquet=OUT_PARQUET,
    )
    print("[done] Wrote:")
    for k, v in summary.items():
        if k.startswith("out_") and v:
            print(f"  - {v}")


if __name__ == "__main__":
    main()
