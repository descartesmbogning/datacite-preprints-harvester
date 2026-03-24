#!/usr/bin/env python3
"""
Runner script for DataCite Client Metadata Enricher.
Usage:
    python scripts/run_clients_enricher.py
"""

import sys
from pathlib import Path
import argparse

# ---- src/ import shim ----
ROOT = Path(__file__).resolve().parent.parent
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
# --------------------------

from datacite_preprint_harvester.datacite_clients_enricher import run_clients_enricher

# ================================
# Defaults (you can override via CLI)
# ================================
DEFAULT_MERGED_PATH = r"data/merged"   # directory OK → auto-pick latest by mtime
DEFAULT_CLIENTS_CATALOG = "data/output/clients_catalog.csv"
DEFAULT_OUT_EXCEL       = "data/output/clients_metadata.xlsx"
DEFAULT_OUT_MERGED      = "data/output/datacite_preprints_labeled.parquet"  # forces label into output folder
DEFAULT_MAILTO          = "your.email@example.com"
DEFAULT_LATEST_PREFIX   = "datacite_preprints_merged_"   # change to 'datacite_texts_merged_' for texts
DEFAULT_LATEST_EXT      = ".parquet"
# ================================

def main():
    p = argparse.ArgumentParser(description="Run DataCite Clients Enricher (wrapper).")
    p.add_argument("--merged", default=DEFAULT_MERGED_PATH,
                   help="Path to merged CSV/Parquet OR a folder (auto-pick latest by mtime).")
    p.add_argument("--clients-catalog", default=DEFAULT_CLIENTS_CATALOG,
                   help="Persistent clients catalog CSV (grows over time).")
    p.add_argument("--out-clients-xlsx", default=DEFAULT_OUT_EXCEL,
                   help="Excel workbook (.xlsx) with full client metadata + provider summary.")
    p.add_argument("--out-merged", default=DEFAULT_OUT_MERGED,
                   help="Where to save the labeled merged (.csv or .parquet).")
    p.add_argument("--mailto", default=DEFAULT_MAILTO, help="Email for DataCite UA polite pool.")
    p.add_argument("--verbose", action="store_true", help="Chatty HTTP logging.")
    p.add_argument("--max-clients", type=int, default=None, help="Fetch only first N new clients (smoke test).")
    p.add_argument("--latest-prefix", default=DEFAULT_LATEST_PREFIX,
                   help="If --merged is a directory, filename prefix to select the latest (e.g., datacite_texts_merged_).")
    p.add_argument("--latest-ext", default=DEFAULT_LATEST_EXT,
                   help="If --merged is a directory, filename extension to select the latest (e.g., .parquet).")

    args = p.parse_args()

    summary = run_clients_enricher(
        merged=Path(args.merged),
        clients_catalog=Path(args.clients_catalog),
        out_clients_xlsx=Path(args.out_clients_xlsx),
        out_merged=Path(args.out_merged),
        mailto=args.mailto,
        verbose=args.verbose,
        max_clients=args.max_clients,
        latest_prefix=args.latest_prefix,
        latest_ext=args.latest_ext,
    )
    print(summary)

if __name__ == "__main__":
    main()
