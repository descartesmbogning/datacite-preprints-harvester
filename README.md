# DataCite Preprints Harvester
A robust, resumable, reproducible pipeline for harvesting preprint metadata from DataCite at scale.

## Overview
DataCite Preprints Harvester is a fully reproducible Python package designed to collect, store, and merge large-scale preprint metadata from the DataCite REST API.

It provides:
- Cursor-based streaming with automatic resume  
- Incremental chunk saving (Parquet)  
- DOI-level wide-format records  
- Checkpointing for long runs  
- Unified schema merging across independent batches  
- Simple CLI scripts  
- Clean repository structure compatible with research workflows

## Repository Structure
```
datacite-preprints-harvester/
├─ README.md
├─ requirements.txt
├─ .gitignore
├─ LICENSE
├─ CITATION.cff
├─ data/
│  ├─ runs/
│  └─ merged/
├─ src/
│  └─ datacite_preprint_harvester/
│     ├─ __init__.py
│     ├─ harvest_datacite.py
│     └─ merge_parquets.py
├─ scripts/
│  ├─ run_harvest.py
│  └─ merge_runs.py
└─ docs/
   ├─ REPORT.md
   ├─ DATA_DICTIONARY.csv
   ├─ QA_CHECKLIST.md
   └─ RUN_SHEET.txt
```

## Installation
```bash
git clone https://github.com/<your-username>/datacite-preprints-harvester.git
cd datacite-preprints-harvester
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Usage
### Run the Harvester
```bash
python scripts/run_harvest.py
```

### Merge Runs
```bash
python scripts/merge_runs.py
```

## Outputs
- Incremental Parquet shards per run  
- Final unified merged Parquet dataset in `data/merged/`

## License
MIT License.

## Citation
Please cite using the CITATION.cff file.

## Contact
Maintainer: Maxime Descartes Mbogning Fonkou  
Email: dmbogning15@gmail.com  
Affiliation: ScholCommLab, Simon Fraser University
