# datacite_preprints_harvester_resume.py
# Cursor pagination • robust retries • per-page checkpoint • incremental chunks
# --------------------------------------------------------------
# pip install requests pandas pyarrow tqdm python-dateutil

import os, time, json, gzip, hashlib
import requests, pandas as pd
from datetime import date, datetime, timezone
from typing import Iterable, Dict, Any, List, Optional

DATACITE_DOIS = "https://api.datacite.org/dois"
DEFAULT_MAILTO = "your.email@example.com"     # <<< SET THIS
UA = f"DataCite-PreprintHarvester/1.2 (mailto:{DEFAULT_MAILTO})"

try:
    from tqdm import tqdm
    _HAVE_TQDM = True
except Exception:
    _HAVE_TQDM = False


# -------------------- helpers --------------------
def _set_mailto(mailto: str) -> None:
    global UA
    UA = f"DataCite-PreprintHarvester/1.2 (mailto:{mailto})"

def _json(obj: Any) -> Optional[str]:
    if obj is None: return None
    try: return json.dumps(obj, ensure_ascii=False, sort_keys=True)
    except Exception: return None

def _first(lst: Optional[List[Dict]], key: Optional[str] = None):
    if not lst: return None
    v = lst[0]
    return v.get(key) if key and isinstance(v, dict) else v

def _norm_date(iso: Optional[str]) -> Optional[str]:
    if not iso: return None
    try: return datetime.fromisoformat(iso.replace("Z", "+00:00")).date().isoformat()
    except Exception: return str(iso)[:10] if isinstance(iso, str) else None

def _parse_next_cursor(next_url: str) -> Optional[str]:
    try:
        from urllib.parse import urlparse, parse_qs
        qs = parse_qs(urlparse(next_url).query)
        return qs.get("page[cursor]", [None])[0]
    except Exception:
        return None

def _sig_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """Create a stable signature (only filter-relevant keys)."""
    keep = [
        "resource-type-id", "query",  # include query so canonical/label modes checkpoint separately
        "from-registered-date", "until-registered-date",
        "from-created-date", "until-created-date",
        "from-updated-date", "until-updated-date",
        "client-id"
    ]
    return {k: d[k] for k in keep if k in d and d[k] is not None}

def _sig_hash(d: Dict[str, Any]) -> str:
    payload = json.dumps(d, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(payload.encode("utf-8")).hexdigest()[:12]

def _as_str_or_none(v):
    if isinstance(v, tuple) and len(v) == 1:
        v = v[0]
    return None if v is None else str(v)

# -------------------- checkpointing --------------------
def _checkpoint_path(checkpoint_dir: str, signature_hash: str, client_id: Optional[str]) -> str:
    """Generate a consistent checkpoint filename based on signature and client_id."""
    fname = f"checkpoint_{signature_hash}"
    if client_id:
        # Sanitize client_id for filename
        sanitized_cid = client_id.replace(".", "_").replace("/", "_").replace(":", "_")
        fname += f"_{sanitized_cid}"
    return os.path.join(checkpoint_dir, f"{fname}.json")

def _load_checkpoint(path: str) -> Optional[Dict[str, Any]]:
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading checkpoint {path}: {e}")
        return None

def _save_checkpoint(path: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Error saving checkpoint {path}: {e}")


# -------------------- network core --------------------
def _request_get(url: str, params: Optional[Dict[str, Any]] = None,
                 headers: Optional[Dict[str, str]] = None,
                 timeout: int = 60, retries: int = 6, base_sleep: float = 0.5) -> Dict[str, Any]:
    if headers is None: headers = {}
    headers = {"User-Agent": UA, "Accept": "application/vnd.api+json", **headers}
    last_exc = None
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(base_sleep * (2 ** attempt)); continue
            try: print("DataCite error payload:", r.json())
            except Exception: print("DataCite error text:", r.text[:800])
            r.raise_for_status()
        except requests.RequestException as e:
            last_exc = e; time.sleep(base_sleep * (2 ** attempt))
    if last_exc: raise last_exc


# -------------------- param builder --------------------
def _build_params(
    resource_type_id: Optional[str] = "Preprint",  # now Optional
    date_field: str = "registered",                # created | updated | registered
    date_start: Optional[str] = None,              # e.g., "2010-01-01"
    date_end: Optional[str] = None,                # e.g., "2010-01-31"
    client_id: Optional[str] = None,
    rows: int = 1000,
    cursor: Optional[str] = "1",
    include_affiliation: bool = False,
    disable_facets: bool = True,
    query: Optional[str] = None,
) -> Dict[str, Any]:
    assert 1 <= rows <= 1000, "page[size] must be 1..1000"
    if date_end is None: date_end = date.today().isoformat()

    params: Dict[str, Any] = {
        "page[size]": rows,
        "page[cursor]": cursor or "1",
    }
    if resource_type_id:                          # only add if provided
        params["resource-type-id"] = resource_type_id

    ds = _as_str_or_none(date_start)
    de = _as_str_or_none(date_end)
    if ds:
        params[f"from-{date_field}-date"] = ds
    if de:
        params[f"until-{date_field}-date"] = de

    if client_id:
        params["client-id"] = client_id
    if include_affiliation:
        params["affiliation"] = "true"
    if disable_facets:
        params["disable-facets"] = "true"
    if query:                                     # e.g., types.resourceType:"preprint"
        params["query"] = query
    return params

def probe_total(params: dict) -> int:
    test = dict(params)
    test["page[size]"] = 0
    js = _request_get(DATACITE_DOIS, params=test)
    return int((js.get("meta") or {}).get("total") or 0)


# -------------------- record → wide row --------------------
def _one_row_wide(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not item or "attributes" not in item:
        return None

    a = item["attributes"]
    rel = item.get("relationships") or {}

    # --- simple helpers ---
    def _fallback_prefix_from_doi(doi: Optional[str]) -> Optional[str]:
        if not doi or "/" not in doi:
            return None
        return doi.split("/", 1)[0]

    from html import unescape  # for &amp; etc.

    # --- basic fields from attributes ---
    doi       = a.get("doi")
    url       = a.get("url")
    publisher = a.get("publisher")
    language  = a.get("language")
    version   = a.get("version")
    schema_version = a.get("schemaVersion")
    state     = a.get("state")

    # prefix can be missing occasionally; derive from DOI as fallback
    prefix    = a.get("prefix") or _fallback_prefix_from_doi(doi)
    suffix    = a.get("suffix")

    # client/provider: prefer relationships, fall back to attributes if present
    client_id = (rel.get("client") or {}).get("data", {}).get("id") or a.get("clientId")
    provider_id = (rel.get("provider") or {}).get("data", {}).get("id") or a.get("providerId")
    if not provider_id and client_id and "." in client_id:
        provider_id = client_id.split(".", 1)[0]

    types          = a.get("types") or {}
    types_json     = _json(types)
    type_resource  = types.get("resourceType")
    type_general   = types.get("resourceTypeGeneral")

    titles       = a.get("titles") or []
    title_raw    = _first(titles, "title")
    title        = unescape(title_raw) if isinstance(title_raw, str) else title_raw
    titles_json  = _json(titles)

    created    = _norm_date(a.get("created"))
    registered = _norm_date(a.get("registered"))
    updated    = _norm_date(a.get("updated"))

    # published can be a bare year; keep both as-is and the int year if present
    published       = a.get("published")
    published_year  = a.get("publicationYear")

    creators            = a.get("creators")
    contributors        = a.get("contributors")
    creators_json       = _json(creators)
    contributors_json   = _json(contributors)

    subjects_json       = _json(a.get("subjects"))
    descriptions_json   = _json(a.get("descriptions"))
    identifiers_json    = _json(a.get("identifiers"))
    alt_ids_json        = _json(a.get("alternateIdentifiers"))
    related_ids_json    = _json(a.get("relatedIdentifiers"))
    container_json      = _json(a.get("container"))
    funding_refs_json   = _json(a.get("fundingReferences"))
    rights_list_json    = _json(a.get("rightsList"))
    sizes_json          = _json(a.get("sizes"))
    formats_json        = _json(a.get("formats"))
    geo_locations_json  = _json(a.get("geoLocations"))
    references_json     = _json(a.get("references"))
    citations_json      = _json(a.get("citations"))
    url_alternate_json  = _json(a.get("urlAlternate"))
    raw_attributes_json = _json(a)
    raw_relationships_json = _json(rel)

    return {
        "doi": doi, "url": url, "publisher": publisher, "language": language,
        "version": version, "schema_version": schema_version, "state": state,
        "prefix": prefix, #"suffix": suffix,
        "client_id": client_id, "provider_id": provider_id,
        "resource_type": type_resource, "resource_type_general": type_general,
        "types_json": types_json,
        "title": title, "titles_json": titles_json,
        "created": created, "registered": registered, "updated": updated,
        "published": published, "published_year": published_year,
        "creators_json": creators_json, "contributors_json": contributors_json,
        "subjects_json": subjects_json, "descriptions_json": descriptions_json,
        "identifiers_json": identifiers_json, "alternate_ids_json": alt_ids_json,
        "related_ids_json": related_ids_json, "container_json": container_json,
        "funding_refs_json": funding_refs_json, "rights_list_json": rights_list_json,
        "sizes_json": sizes_json, "formats_json": formats_json,
        "geo_locations_json": geo_locations_json, "references_json": references_json,
        "citations_json": citations_json, "url_alternate_json": url_alternate_json,
        "raw_attributes_json": raw_attributes_json,
        "raw_relationships_json": raw_relationships_json,
    }


# -------------------- streaming with resume --------------------
def stream_datacite_with_resume(
    base_params: Dict[str, Any],
    checkpoint_dir: str,
    client_id_for_ckpt: Optional[str],
    show_progress: bool = True,
) -> Iterable[Dict[str, Any]]:
    """
    Stream /dois items, saving a checkpoint after every page so we can resume.
    """
    sig = _sig_dict(base_params)
    sig_h = _sig_hash(sig)
    ckpt_path = _checkpoint_path(checkpoint_dir, sig_h, client_id_for_ckpt)

    # Try to resume
    ckpt = _load_checkpoint(ckpt_path)
    if ckpt and ckpt.get("signature") == sig and ckpt.get("next_cursor"):
        base_params["page[cursor]"] = ckpt["next_cursor"]
        print(f"[RESUME] Using checkpoint {ckpt_path} (next cursor: {ckpt['next_cursor']})")
    else:
        # fresh start
        base_params.setdefault("page[cursor]", "1")

    # First call
    js = _request_get(DATACITE_DOIS, params=base_params)
    meta = js.get("meta") or {}; total = meta.get("total")
    items = js.get("data") or []

    pbar = None
    if show_progress and _HAVE_TQDM and isinstance(total, int) and total > 0:
        pbar = tqdm(total=total, desc="Fetching DataCite preprints", unit="rec")

    # Yield first page
    for it in items:
        if pbar: pbar.update(1)
        yield it

    # Save checkpoint (next cursor of this page)
    links = js.get("links") or {}; nxt = links.get("next")
    next_cursor = _parse_next_cursor(nxt) if nxt else None
    _save_checkpoint(
        ckpt_path,
        {"signature": sig, "next_cursor": next_cursor, "saved_at": datetime.now(timezone.utc).isoformat()}
    )

    # Continue over next pages
    while next_cursor:
        base_params["page[cursor]"] = next_cursor
        js = _request_get(DATACITE_DOIS, params=base_params)
        items = js.get("data") or []
        if not items: break
        for it in items:
            if pbar: pbar.update(1)
            yield it

        links = js.get("links") or {}; nxt = links.get("next")
        next_cursor = _parse_next_cursor(nxt) if nxt else None
        _save_checkpoint(
            ckpt_path,
            {"signature": sig, "next_cursor": next_cursor, "saved_at": datetime.now(timezone.utc).isoformat()}
        )

    if pbar: pbar.close()
    print(f"[DONE] Checkpoint saved at {ckpt_path} (next_cursor={next_cursor})")


# -------------------- orchestrator --------------------
def harvest_datacite_preprints_dataframe(
    mailto: str = DEFAULT_MAILTO,
    date_field: str = "registered",          # "created" | "updated" | "registered"
    date_start: str = "2000-01-01",
    date_end: Optional[str] = None,
    client_ids: Optional[List[str]] = None,  # e.g., ["arxiv.content","cern.zenodo"]
    resource_type_id: str = "Preprint",      # canonical type (resourceTypeGeneral)
    rows_per_call: int = 1000,
    include_affiliation: bool = False,
    start_cursor: Optional[str] = None,      # ignored when resume finds a cursor
    # NEW: selector mode
    type_mode: str = "canonical",            # "canonical" | "label" | "both"
    type_label_value: Optional[str] = None,  # used when type_mode in {"label","both"}
    # Resume & outputs
    resume: bool = True,
    checkpoint_dir: str = "./checkpoints",
    incremental_save_dir: Optional[str] = "./datacite_preprints_batches",
    incremental_every: int = 10_000,         # write a parquet chunk every N rows
    save_parquet_path: Optional[str] = None,
    save_csv_path: Optional[str] = None,
    save_pkl_path: Optional[str] = None,
    save_ndjson_path: Optional[str] = None,
) -> pd.DataFrame:

    _set_mailto(mailto)
    if date_end is None: date_end = date.today().isoformat()

    all_rows: List[Dict[str, Any]] = []
    ndj_fh = None
    if save_ndjson_path:
        ndj_fh = gzip.open(save_ndjson_path, "at", encoding="utf-8") if save_ndjson_path.endswith(".gz") \
                 else open(save_ndjson_path, "a", encoding="utf-8")

    try:
        client_list = client_ids if client_ids else [None]
        chunk_idx_global = 0

        # Build passes depending on type_mode
        passes: List[Dict[str, Any]] = []
        if type_mode == "canonical":
            passes.append({"rtid": resource_type_id, "query": None})
        elif type_mode == "label":
            label = type_label_value or resource_type_id
            passes.append({"rtid": None, "query": f'types.resourceType:"{label}"'})
        elif type_mode == "both":
            label = type_label_value or resource_type_id
            passes.append({"rtid": resource_type_id, "query": None})
            passes.append({"rtid": None, "query": f'types.resourceType:"{label}"'})
        else:
            raise ValueError('type_mode must be "canonical", "label", or "both"')

        for cid in client_list:
            for sel in passes:
                params = _build_params(
                    resource_type_id=sel["rtid"],
                    date_field=date_field,
                    date_start=date_start,
                    date_end=date_end,
                    client_id=cid,
                    rows=rows_per_call,
                    cursor=start_cursor or "1",
                    include_affiliation=include_affiliation,
                    disable_facets=True,
                    query=sel["query"],
                )
                print("Filter total =", probe_total(params))
                print(f"Querying with params: {params}")

                streamer = stream_datacite_with_resume(
                    base_params=params,
                    checkpoint_dir=checkpoint_dir,
                    client_id_for_ckpt=cid,
                    show_progress=True,
                )

                buffer_rows: List[Dict[str, Any]] = []
                for item in streamer:
                    row = _one_row_wide(item)
                    if row:
                        buffer_rows.append(row)

                    # Periodic flush to chunk parquet & NDJSON to avoid memory growth
                    if len(buffer_rows) >= incremental_every:
                        df_inc = pd.DataFrame(buffer_rows)
                        if incremental_save_dir:
                            os.makedirs(incremental_save_dir, exist_ok=True)
                            fname = os.path.join(
                                incremental_save_dir,
                                f"datacite_preprints_chunk_{chunk_idx_global:05d}.parquet"
                            )
                            df_inc.to_parquet(fname, index=False)
                            print(f"Saved incremental chunk: {fname}")
                            chunk_idx_global += 1
                        if ndj_fh:
                            for r in buffer_rows:
                                ndj_fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                        buffer_rows.clear()

                # Flush any remainder from this pass
                if buffer_rows:
                    df_inc = pd.DataFrame(buffer_rows)
                    if incremental_save_dir:
                        os.makedirs(incremental_save_dir, exist_ok=True)
                        fname = os.path.join(
                            incremental_save_dir,
                            f"datacite_preprints_chunk_{chunk_idx_global:05d}.parquet"
                        )
                        df_inc.to_parquet(fname, index=False)
                        print(f"Saved incremental chunk: {fname}")
                        chunk_idx_global += 1
                    if ndj_fh:
                        for r in buffer_rows:
                            ndj_fh.write(json.dumps(r, ensure_ascii=False) + "\n")
                    all_rows.extend(buffer_rows)

        # Build final DataFrame from in-memory remainder (optional; main data is in chunks)
        df_final = pd.DataFrame(all_rows)

        # Optional: consolidate all parquet chunks now
        if incremental_save_dir:
            import glob
            paths = sorted(glob.glob(os.path.join(incremental_save_dir, "datacite_preprints_chunk_*.parquet")))
            if paths:
                dfs = [pd.read_parquet(p) for p in paths]
                if not df_final.empty: dfs.append(df_final)
                df_final = pd.concat(dfs, ignore_index=True)

        # Deduplicate by DOI (prefer rows with richer metadata)
        if not df_final.empty:
            # Put rows with provider_id / alternate_ids first so drop_duplicates keeps richer ones
            df_final["_rich"] = (
                df_final["provider_id"].notna().astype(int)
                + df_final["alternate_ids_json"].notna().astype(int)
            )
            df_final.sort_values(["_rich", "registered", "updated", "created"],
                                 ascending=[False, True, True, True],
                                 inplace=True, na_position="last")
            df_final.drop(columns=["_rich"], inplace=True, errors="ignore")
            df_final.drop_duplicates(subset=["doi"], keep="first", inplace=True)

        # # Final saves
        # timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        # out_dir = f'./datacite_preprints_wide_{date_start}_{date_end}_{timestamp}'
        # os.makedirs(out_dir, exist_ok=True)

        # if save_parquet_path and not df_final.empty:
        #     parquet_filename = f'{out_dir}/{os.path.splitext(os.path.basename(save_parquet_path))[0]}_{timestamp}{os.path.splitext(save_parquet_path)[1]}'
        #     df_final.to_parquet(parquet_filename, index=False)
        #     print(f"Saved Parquet: {parquet_filename}")

        # if save_csv_path:
        #     csv_filename = f'{out_dir}/{os.path.splitext(os.path.basename(save_csv_path))[0]}_{timestamp}{os.path.splitext(save_csv_path)[1]}'
        #     df_final.to_csv(csv_filename, index=False)
        #     print(f"Saved CSV: {csv_filename}")

        # if save_pkl_path:
        #     pkl_filename = f'{out_dir}/{os.path.splitext(os.path.basename(save_pkl_path))[0]}_{timestamp}{os.path.splitext(save_pkl_path)[1]}'
        #     df_final.to_pickle(pkl_filename)
        #     print(f"Saved PKL: {pkl_filename}")

        # return df_final

    finally:
        if ndj_fh:
            ndj_fh.close()
            print(f"NDJSON stream written to: {save_ndjson_path}")


