# scripts/merge_runs.py
import os, sys

# ---- src/ import shim
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
SRC = os.path.join(ROOT, "src")
if SRC not in sys.path:
    sys.path.insert(0, SRC)
# ---------------------------------------------------

from datacite_preprint_harvester.merge_parquets import merge_parquets

def find_datacite_batches(base_dir: str, subdir_name: str = "datacite_preprints_batches"):
    hits = []
    for root, dirs, files in os.walk(base_dir):
        if os.path.basename(root) == subdir_name:
            hits.append(root)
    return hits

if __name__ == "__main__":
    BASE_RUNS_DIR = os.path.join("data", "runs", "preprint")  # e.g. data/runs/Preprint/2025-10-01_2025-11-07/...
    INPUT_PARENT_DIRS = find_datacite_batches(BASE_RUNS_DIR)

    if not INPUT_PARENT_DIRS:
        print(f"⚠️ No '{BASE_RUNS_DIR}/**/datacite_preprints_batches' folders found.")
        sys.exit(1)

    OUTPUT_DIR = os.path.join("data", "merged")
    BASENAME = "datacite_preprints_merged"

    # Call and safely handle any return signature
    ret = merge_parquets(
        input_parent_dirs=INPUT_PARENT_DIRS,
        output_dir=OUTPUT_DIR,
        merged_basename=BASENAME,
        write_pkl_too=False
    )

    outputs = None
    total_rows_written = None
    report_df = None

    if isinstance(ret, tuple):
        if len(ret) == 2:
            # Could be (outputs, total_rows_written) OR (outputs, report_df)
            outputs, second = ret
            if isinstance(second, int):
                total_rows_written = second
            else:
                report_df = second
        elif len(ret) >= 1:
            outputs = ret[0]
            # optionally ignore extras
    else:
        outputs = ret

    print("✅ Merge completed")
    if outputs is not None:
        print(outputs)
    if total_rows_written is not None:
        print(f"Rows written (sum over shards): {total_rows_written:,}")
    if report_df is not None:
        try:
            print(f"Report rows: {getattr(report_df, 'shape', ('?', '?'))[0]}")
        except Exception:
            pass
