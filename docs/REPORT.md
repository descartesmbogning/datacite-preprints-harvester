
# ✅ **FULL REPORT.md — DataCite Preprints Harvester**

**Version:** 1.0  
**Last updated:** 2025-11-07  
**Maintainer:** Maxime Descartes Mbogning Fonkou,
**Supervisor :** Juan Alperin,
**Affiliation :** Scholcommlab, SFU university,


---

# **1. Overview**

This repository contains a **robust, resumable, large-scale harvesting pipeline** for collecting preprint metadata from the **DataCite API**, designed for high-volume research workflows including:

* Science of Science (SciSci) analysis
* Open science infrastructure monitoring
* Preprint server landscape mapping
* Metadata quality evaluation
* Cross-system concordance studies (Crossref, OpenAlex, DataCite)

The harvester is engineered to reliably extract **millions of records**, resume automatically after failures, and produce high-quality **wide-format metadata tables**, incremental Parquet chunks, and fully merged datasets.

This documentation explains:

* The **methodology** behind the harvester
* How the pipeline handles DataCite complexity
* Metadata extraction logic
* Output structure
* Reproducibility requirements
* Known limitations
* Suggestions for extending the system

---

# **2. Key Features**

| Feature                                | Description                                                                              |
| -------------------------------------- | ---------------------------------------------------------------------------------------- |
| **Cursor-resumable harvesting**        | Continues automatically after interruptions; suitable for multi-day runs.                |
| **Checkpointing per filter signature** | Unique `_sig_hash` ensures unambiguous, reproducible runs.                               |
| **Incremental Parquet writing**        | Dumps data every *N* records (default 100k), preventing memory overload.                 |
| **Flexible Preprint Typing**           | Supports canonical (`resource-type-id`) or label-based (`types.resourceType`) filtering. |
| **Multiple client IDs**                | Allows harvesting by provider (e.g., arxiv.content, cern.zenodo).                        |
| **Rich metadata flattening**           | Converts complex JSON into wide rows with JSON-preserving fields.                        |
| **Automatic deduplication**            | Prefers metadata-richer records (provider/client/related IDs).                           |
| **NDJSON streaming (optional)**        | Ideal for big-data pipelines (Spark, GCP, Databricks).                                   |
| **Merged datasets**                    | Simple CLI script combines all batch files into a final unified dataset.                 |

---

# **3. Data Sources and API Specification**

All data is retrieved from the **DataCite REST API**:

```
https://api.datacite.org/dois
```

The pipeline uses **three families of filters**:

---

## **3.1 Date-based filters**

Applied via:

```
from-<date-field>-date  
until-<date-field>-date
```

Allowed `<date-field>` values:

| Field        | Meaning                                 |
| ------------ | --------------------------------------- |
| `registered` | Date DOI was registered with DataCite   |
| `created`    | Date DOI record was created in DataCite |
| `updated`    | Date of last DataCite record update     |

---

## **3.2 Type filters**

We support *three* selection modes:

### ✅ **Mode 1 — Canonical**
```
resource-type-id=Preprint
```

✅ clean  
✅ specific  
❌ may miss mislabeled content  

---

### ✅ **Mode 2 — Label-based**
```
query='types.resourceType:"Preprint"'
```

✅ catches mislabeled cases  
❌ may include noisy metadata  

---

### ✅ **Mode 3 — Both (recommended)**  
Runs both modes and deduplicates by DOI.

---

## **3.3 Provider/client filters**

Used to target specific repositories:

```
client-id = "arxiv.content"
client-id = "cern.zenodo"
client-id = "dryad.data"
```

---

# **4. Metadata Extraction Logic**

Each DataCite record is transformed by `_one_row_wide()`.  
Here are the exact transformations.

---

## **4.1 Basic Fields**

| Output Column           | Source                                      | Description                             |
| ----------------------- | ------------------------------------------- | --------------------------------------- |
| `doi`                   | attributes.doi                              | DOI string                              |
| `url`                   | attributes.url                              | Landing page URL                        |
| `publisher`             | attributes.publisher                        | Declared publisher                      |
| `prefix`                | attributes.prefix or extracted from DOI     | DOI prefix                              |
| `client_id`             | relationships.client or attributes.clientId | Minting client                          |
| `provider_id`           | relationships.provider or inferred          | Provider/allocator                      |
| `resource_type`         | types.resourceType                          | Specific type                           |
| `resource_type_general` | types.resourceTypeGeneral                   | Broad category                          |
| `title`                 | titles[0].title                             | Main title                              |

---

## **4.2 Date Fields**

Standardized with `_norm_date()` into ISO-8601.

| Output           | Input                       | Description                     |
| ---------------- | --------------------------- | ------------------------------- |
| `created`        | attributes.created          | DOI creation date               |
| `registered`     | attributes.registered       | DataCite registration date      |
| `updated`        | attributes.updated          | Last metadata update            |
| `published`      | attributes.published        | Publication year                |
| `published_year` | attributes.publicationYear  | Explicit publication year       |

---

## **4.3 JSON-Preserved Fields**

These are stored as sorted JSON strings.

| Field                 | Content                       |
| --------------------- | ----------------------------- |
| `creators_json`       | list of creators              |
| `descriptions_json`   | abstracts, descriptions       |
| `subjects_json`       | topical keywords              |
| `funding_refs_json`   | funding entities              |
| `related_ids_json`    | related DOIs, datasets, etc. |
| `container_json`      | journal/collection info       |
| `raw_attributes_json` | full original attributes      |

---

## **4.4 Deduplication Heuristic**

Rows are ranked by:

1. `provider_id` non-null  
2. `alternate_ids_json` non-null  
3. recency of `registered`, `updated`, `created`

The most metadata-rich record survives.

---

# **5. Output Structure**

---

## ✅ **5.1 Incremental parquet chunks**

Stored under:

```
data/runs/<resource_type>/<date_range>/datacite_preprints_batches/
```

Files:

```
datacite_preprints_chunk_00000.parquet
datacite_preprints_chunk_00001.parquet
...
```

---

## ✅ **5.2 Checkpoints**

Stored under:

```
data/runs/<resource_type>/<date_range>/checkpoints_preprints/
```

Example:

```json
{
  "signature": "d3b2c91a12ff",
  "next_cursor": "aGg0dmda...",
  "saved_at": "2025-11-07T14:32:10Z"
}
```

---

## ✅ **5.3 Merged Outputs**

Using:

```
python scripts/merge_runs.py
```

Creates:

```
data/merged/datacite_preprints_merged_<timestamp>.parquet
```

---

# **6. Running the Harvester**

Run:

```
python scripts/run_harvest.py
```

The script sets:

* date_start, date_end  
* resource_type  
* checkpoint directory  
* batch directory  
* incremental save frequency  

---

# **7. Reproducibility Notes**

✅ All parameters recorded  
✅ Checkpoints captured  
✅ Cursor signature hashing ensures determinism  
✅ Raw JSON preserved  
✅ Merged file timestamped  
✅ Deduplication deterministic  

This fully satisfies **FAIR**, **replicable**, **auditable** data workflow standards.

---

# **8. Known Limitations**

| Issue | Notes |
|-------|-------|
| Inconsistent resourceType labels | Some servers misuse "Text", "Article", etc. |
| Incomplete client/provider info | Happens with older DOIs |
| Sparse affiliation metadata | A DataCite limitation |
| Large disk space needed | Millions of rows = large Parquet |
| Rate limits | Retries implemented |

---

# **9. Example Analyses Enabled**

## ✅ Trends per year

```python
df.groupby("published_year").size()
```

## ✅ Top providers

```python
df["provider_id"].value_counts()
```

## ✅ Metadata completeness audit

```python
df.isna().mean().sort_values()
```

## ✅ Crossref / Crossref joining

```python
merged = df.merge(crossref_df, on="doi", how="inner")
```

---

# **10. Extension Possibilities**

* Harmonized schema with Crossref  
* Preprint lifecycle tracking  
* Funder/author/topic extraction  
* Entity extraction (NER)  
* Provider classification  
* Zenodo deposition + DOI assignment  

---

# **11. Citation**

```
XXXX (2025). DataCite Preprints Harvester (Version 1.0).
Scholarly Communications Lab 
```

---



