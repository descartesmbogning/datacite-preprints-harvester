# ✅ QA_CHECKLIST.md — DataCite Preprints Harvester

**Version:** 1.0  
**Last updated:** 2025-11-07  
**Maintainer:** Maxime Descartes Mbogning Fonkou  
**Supervisor :** Juan Alperin,
**Affiliation :** Scholcommlab, SFU university,

---

# ✅ 1. PURPOSE OF THIS DOCUMENT

This Quality Assurance (QA) Checklist ensures that every harvesting run and merged dataset produced by the *DataCite Preprints Harvester* is:

- **Reproducible**
- **Complete**
- **Consistent**
- **Traceable**
- **Scientifically valid**
- **Ready for downstream analysis (SciSci, dashboards, publication workflows)**

This document is designed to be used **before**, **during**, and **after** a harvesting job.

---

# ✅ 2. ENVIRONMENT VERIFICATION

| Check | Status |
|------|--------|
| Python version ≥ 3.10 | ☐ |
| Required packages installed from `requirements.txt` | ☐ |
| Virtual environment activated | ☐ |
| Internet connection stable | ☐ |
| Storage space available (>50 GB recommended for full harvests) | ☐ |
| Permissions to write to `data/` | ☐ |

---

# ✅ 3. PRE-RUN CHECKS

### ✅ 3.1 Parameters validation

| Parameter | Expected | Value Used | Status |
|----------|----------|------------|--------|
| `resource_type_id` | "Preprint" or None |  | ☐ |
| `type_mode` | canonical / label / both |  | ☐ |
| `date_start` | ISO format YYYY-MM-DD |  | ☐ |
| `date_end` | ISO format or today() |  | ☐ |
| `client_ids` | list or None |  | ☐ |
| `include_affiliation` | True/False |  | ☐ |
| `incremental_every` | >20,000 |  | ☐ |

### ✅ 3.2 Folder structure exists

```
data/
 └─ runs/
     └─ preprint/<date_range>/
         ├─ checkpoints_preprints/
         └─ datacite_preprints_batches/
```

| Folder | Status |
|--------|--------|
| `data/runs/` | ☐ |
| Correct `<resource_type>` chosen | ☐ |
| Checkpoint folder present | ☐ |
| Batch folder created | ☐ |

---

# ✅ 4. DURING-RUN CHECKS

### ✅ 4.1 Logs monitoring

| Expected behavior | Status |
|------------------|--------|
| Cursor values increase | ☐ |
| Chunks saved periodically | ☐ |
| No repeated chunk filenames | ☐ |
| API returns HTTP 200 steadily | ☐ |
| Backoff retries visible when 429 occurs | ☐ |
| No infinite loops | ☐ |

### ✅ 4.2 Incremental chunks

| Check | Status |
|-------|--------|
| Chunks appear every N rows | ☐ |
| No chunk exceeds expected row size | ☐ |
| Filenames correctly increment | ☐ |

---

# ✅ 5. POST-RUN CHECKS — HARVEST

### ✅ 5.1 Integrity of chunk files

| Check | Status |
|-------|--------|
| All Parquet files readable | ☐ |
| Parquet schema consistent across shards | ☐ |
| No empty Parquet files | ☐ |
| No corrupted parquet warnings | ☐ |

### ✅ 5.2 Duplicate detection

Run:

```python
df.duplicated("doi").sum()
```

| Expectation | Status |
|-------------|--------|
| Duplicates minimal (<1%) | ☐ |
| Duplicates correctly resolved after dedupe | ☐ |

---

# ✅ 6. MERGE CHECKS

After running:

```
python scripts/merge_runs.py
```

### ✅ 6.1 Output files present

| File | Status |
|------|--------|
| `data/merged/*.parquet` | ☐ |
| Optional: `*.pkl` | ☐ |

### ✅ 6.2 Schema harmonization

```python
df.columns
```

Must match expected schema from `_one_row_wide`.

| Check | Status |
|-------|--------|
| No unexpected columns | ☐ |
| All JSON fields preserved | ☐ |
| Dates normalized | ☐ |

---

# ✅ 7. DATA COMPLETENESS CHECKS

### ✅ 7.1 Required fields

| Column | Acceptable Missing? | Status |
|--------|---------------------|--------|
| `doi` | ❌ Must exist | ☐ |
| `prefix` | ✅ optional | ☐ |
| `publisher` | ✅ optional | ☐ |
| `registered` | ❌ required | ☐ |
| `resource_type` | ✅ expected missing for label-mode | ☐ |

---

# ✅ 8. QUALITY METRICS

### ✅ Metadata Completeness (example)

| Metric | Formula | Target | Status |
|--------|---------|--------|--------|
| Creators coverage | % non-null `creators_json` | >85% | ☐ |
| Subjects coverage | % non-null `subjects_json` | >30% | ☐ |
| Funding coverage | % non-null `funding_refs_json` | >10% | ☐ |
| Related Identifiers coverage | non-null `related_ids_json` | >40% | ☐ |

---

# ✅ 9. SCIENTIFIC VALIDATION

| Validation task | Purpose | Status |
|-----------------|----------|--------|
| Verify known yearly growth patterns | sanity check | ☐ |
| Compare counts vs OpenAlex for same servers | validation | ☐ |
| Compare provider distribution to expectations | provenance check | ☐ |
| Spot anomalies (e.g., huge surges) | metadata-quality audit | ☐ |

---

# ✅ 10. ARCHIVING & REPRODUCIBILITY

### ✅ Files to archive

- `REPORT.md`
- `QA_CHECKLIST.md` (this document)
- `RUN_SHEET.txt`
- `DATA_DICTIONARY.csv`
- `merged parquet`
- Git commit hash
- Full run logs
- Environment (requirements.txt)

### ✅ Reproducibility requirements

| Requirement | Status |
|------------|--------|
| Code version controlled | ☐ |
| Parameters recorded | ☐ |
| Checkpoints stored | ☐ |
| Date of API run logged | ☐ |
| Script versions noted | ☐ |

---

# ✅ 11. SIGN-OFF

| Role | Name | Date | Signature |
|------|------|-------|-----------|
| Run Operator |  |  |  |
| Data Reviewer |  |  |  |
| Scientific Lead |  |  |  |

---

✅ *End of QA Checklist.*  
This template is suitable for publication workflows, reproducible research pipelines, and internal lab documentation.
