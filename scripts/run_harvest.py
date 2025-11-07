#!/usr/bin/env python3

# scripts/run_harvest.py
import os, sys, argparse
from datetime import date

# ---- src/ import shim (so you don't need pip install -e . yet)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
# -------------------------------------------------------------

from datacite_preprint_harvester.harvest_datacite import harvest_datacite_preprints_dataframe

# Edit these as needed (NO trailing commas!)
resource_type_id = "preprint"     #_ALLOWED_TYPES = {"preprint","text","Dataset","Software","Image","Collection","Event","Model","Sound","PhysicalObject","Workflow","Other"}
# type_label_value=None
date_start="1800-01-01" #"2020-01-01",
date_end=date.today().isoformat()

if __name__ == "__main__":
    # Example: choose canonical "Preprint"
    df = harvest_datacite_preprints_dataframe(
        mailto="dmbogning15@gmail.com",
        date_field="registered",
        date_start=date_start,
        date_end=date_end,
        client_ids=None,                      # or ["arxiv.content", "cern.zenodo", ...]
        #
        resource_type_id=f"{resource_type_id}",          # canonical controlled list
        #
        # type_mode="canonical",                # use "label" to switch to types.resourceType
        # type_mode="label",                  # <- uncomment to use label search instead
        # type_label_value=f"{type_label_value}",        # default falls back to resource_type_id
        #
        rows_per_call=1000,
        include_affiliation=True,
        resume=True,
        #
        checkpoint_dir=f"data/runs/{resource_type_id}/{date_start}_{date_end}/checkpoints_{resource_type_id}s",            # where to store cursor checkpoints
        incremental_save_dir=f"data/runs/{resource_type_id}/{date_start}_{date_end}/datacite_{resource_type_id}s_batches",
        incremental_every=100_000,
        save_parquet_path=None,
        save_csv_path=None,
        save_pkl_path=None,
        save_ndjson_path=None,
    )
    print("Final shape:", df.shape)
