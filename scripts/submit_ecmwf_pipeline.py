#!/usr/bin/env python3
"""Submit Frozon ECMWF Open Data worker jobs directly from the GH Actions runner.

ECMWF Open Data differs from the OPERA / S1 pipelines: no CMR, no Earthdata
Login. The free real-time IFS feed publishes per-run GRIB files at deterministic
anonymous-HTTP URLs on data.ecmwf.int, so "discovery" is an HTTP existence check
per date — no granule search, no granule-count threshold filtering.

We ingest the **step-0 (T+0 analysis) field of the 00 UTC oper HRES run** as the
daily snapshot. That single step-0 file carries every parameter, so discovery is
product-independent: one HEAD per date tells us whether all products
(airtemp / wind_ns / wind_ew / wind_arrows) are available.

Per run:

  1. Walk back day-by-day, HEAD-checking the oper step-0 GRIB URL, collecting
     the INGEST_LAST_N_COMPLETE_DAYS most recent dates that exist. (The Open
     Data feed only retains roughly the last ~4 days.)
  2. For each requested product: retention sweep (keep RETAIN_DAYS most recent
     date folders per collection), S3 pre-check (the worker does NOT dedupe
     against S3), then submit one `frozon-iss-ingest-ecmwf:v1` worker per
     (product, missing date).

Required env:
    MAAP_TOKEN — MAAP API token for headless auth.
"""

import os
import re
import sys
from datetime import datetime, timedelta, timezone

from maap.maap import MAAP

# Per-product default collection IDs (mirror PRODUCTS in src/ingest_ecmwf.py).
PRODUCT_COLLECTIONS = {
    "airtemp": "frozon-ecmwf-airtemp-daily",
    "wind_ns": "frozon-ecmwf-wind-ns-daily",
    "wind_ew": "frozon-ecmwf-wind-ew-daily",
    # Derived 10u+10v decimated point GeoJSON for the MMGIS arrow layer.
    "wind_arrows": "frozon-ecmwf-wind-arrows-daily",
}

DEFAULTS = {
    "MAAP_HOST":                  "api.maap-project.org",
    "WORKER_ALGO_ID":             "frozon-iss-ingest-ecmwf",
    "WORKER_ALGO_VERSION":        "v1",
    "QUEUE":                      "maap-dps-worker-8gb",
    # Which products to ingest this run (comma-separated subset).
    "PRODUCTS":                   "airtemp,wind_ns,wind_ew,wind_arrows",
    # Open Data run + step that defines the daily snapshot.
    "RUN_TIME":                   "0",     # 00z
    "STEP":                       "0",     # T+0 analysis
    "RESOL":                      "0p25",
    "OPENDATA_BASE":              "https://data.ecmwf.int/forecasts",
    # Output.
    "S3_BUCKET":                  "maap-ops-workspace",
    "S3_PREFIX":                  "jdrodrig/frozon/cogs/",
    # Discovery window. Open Data retains ~4 days, so keep the window tight.
    "LOOKBACK_DAYS":              "7",
    "INGEST_LAST_N_COMPLETE_DAYS": "4",
    # Worker tuning.
    "COMPRESS":                   "DEFLATE",
    "BLOCKSIZE":                  "512",
    "MAX_MEMORY":                 "512",
    "OVERWRITE":                  "false",
    "RETAIN_DAYS":                "7",
}


def env(key: str) -> str:
    return os.environ.get(key, DEFAULTS.get(key, ""))


# --------------------------------------------------------------------------
# ECMWF Open Data existence check (discovery)
# --------------------------------------------------------------------------

def opendata_url(date_key: str) -> str:
    """Deterministic URL of the oper step-N GRIB file for a date.

    e.g. .../forecasts/20260620/00z/ifs/0p25/oper/20260620000000-0h-oper-fc.grib2
    The step-0 file carries every parameter, so its existence covers all three
    products."""
    run = int(env("RUN_TIME"))
    step = int(env("STEP"))
    resol = env("RESOL")
    base = env("OPENDATA_BASE").rstrip("/")
    stamp = f"{date_key}{run:02d}0000"
    fname = f"{stamp}-{step}h-oper-fc.grib2"
    return f"{base}/{date_key}/{run:02d}z/ifs/{resol}/oper/{fname}"


def file_exists(url: str) -> bool:
    """True if the file is published. Prefer a cheap HEAD; fall back to a
    1-byte ranged GET for servers that don't answer HEAD cleanly."""
    import requests
    try:
        r = requests.head(url, timeout=30, allow_redirects=True)
        if r.status_code == 200:
            return True
        if r.status_code == 404:
            return False
        r = requests.get(url, timeout=30, stream=True,
                         headers={"Range": "bytes=0-0"})
        return r.status_code in (200, 206)
    except Exception as e:  # noqa: BLE001 — treat network hiccup as "unknown -> absent"
        print(f"    HEAD/GET error for {url}: {e}")
        return False


def discover_dates() -> list[str]:
    """Walk back day-by-day, returning the most recent existing dates
    (newest-first), up to INGEST_LAST_N_COMPLETE_DAYS.

    Product-independent: the oper step-0 file holds every parameter."""
    lookback = int(env("LOOKBACK_DAYS"))
    want = int(env("INGEST_LAST_N_COMPLETE_DAYS"))
    today = datetime.now(timezone.utc).date()
    print(f"walking back from {today.isoformat()} (≤{lookback}d), "
          f"collecting {want} existing date(s).")

    found: list[str] = []
    for offset in range(0, lookback + 1):
        if len(found) >= want:
            break
        day = (today - timedelta(days=offset)).strftime("%Y%m%d")
        if file_exists(opendata_url(day)):
            found.append(day)
            print(f"    {day}: present ({len(found)}/{want})")
    return found


# --------------------------------------------------------------------------
# Workspace credentials + retention + S3 pre-check (per collection)
# --------------------------------------------------------------------------

def _maap_s3_client(maap: MAAP):
    import boto3
    response = maap.aws.workspace_bucket_credentials()
    creds = response.get("credentials") if isinstance(response, dict) else None
    if not creds or "aws_access_key_id" not in creds:
        raise RuntimeError(f"Unexpected creds shape: {response!r}")
    return boto3.client(
        "s3",
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
        aws_session_token=creds["aws_session_token"],
    )


def _collection_prefix(collection_id: str) -> str:
    parts = [p for p in (env("S3_PREFIX").strip("/"), collection_id) if p]
    return "/".join(parts) + "/"


def prune_old_cogs(s3, collection_id: str) -> None:
    retain_days = int(env("RETAIN_DAYS"))
    if retain_days <= 0:
        return
    from datetime import date as _date
    bucket = env("S3_BUCKET")
    prefix = _collection_prefix(collection_id)
    date_pattern = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")
    objects_by_date: dict[_date, list[str]] = {}
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            m = date_pattern.search(obj["Key"])
            if m:
                d = _date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                objects_by_date.setdefault(d, []).append(obj["Key"])
    if not objects_by_date:
        print(f"[{collection_id}] retention: no date folders under s3://{bucket}/{prefix}.")
        return
    sorted_desc = sorted(objects_by_date.keys(), reverse=True)
    keep = set(sorted_desc[:retain_days])
    to_delete = [k for d in sorted_desc if d not in keep for k in objects_by_date[d]]
    if not to_delete:
        print(f"[{collection_id}] retention: {len(sorted_desc)} date(s), all within "
              f"retain_days={retain_days}; nothing to delete.")
        return
    drop = [d for d in sorted_desc if d not in keep]
    print(f"[{collection_id}] retention: deleting {len(to_delete)} object(s) from {drop}")
    for i in range(0, len(to_delete), 1000):
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in to_delete[i:i + 1000]]},
        )


def cog_exists_in_s3(s3, collection_id: str, date_key: str,
                     min_count: int = 1) -> bool:
    """True if the dated folder already holds at least min_count objects.
    wind_arrows uses min_count=2 (decimated + _full GeoJSON), so dates
    ingested before the full-res output existed get resubmitted once."""
    yyyy, mm, dd = date_key[:4], date_key[4:6], date_key[6:8]
    bucket = env("S3_BUCKET")
    key_prefix = _collection_prefix(collection_id) + f"{yyyy}/{mm}/{dd}/"
    r = s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix, MaxKeys=min_count)
    return r.get("KeyCount", 0) >= min_count


# --------------------------------------------------------------------------
# Submit
# --------------------------------------------------------------------------

def submit_worker(maap: MAAP, product: str, collection_id: str, date_key: str):
    params = {
        "identifier":     f"Frozon-ECMWF-{product}_{date_key}",
        "algo_id":        env("WORKER_ALGO_ID"),
        "version":        env("WORKER_ALGO_VERSION"),
        "queue":          env("QUEUE"),
        "product":        product,
        "date":           date_key,
        "time":           env("RUN_TIME"),
        "step":           env("STEP"),
        "resol":          env("RESOL"),
        "collection_id":  collection_id,
        "s3_bucket":      env("S3_BUCKET"),
        "s3_prefix":      env("S3_PREFIX"),
        "compress":       env("COMPRESS"),
        "blocksize":      env("BLOCKSIZE"),
        "max_memory":     env("MAX_MEMORY"),
        "overwrite":      env("OVERWRITE"),
    }
    return maap.submitJob(**{k: v for k, v in params.items() if v})


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main() -> int:
    token = os.environ.get("MAAP_TOKEN")
    if token:
        os.environ["MAAP_PGT"] = token
    maap = MAAP(maap_host=env("MAAP_HOST"))

    products = [p.strip() for p in env("PRODUCTS").split(",") if p.strip()]
    unknown = [p for p in products if p not in PRODUCT_COLLECTIONS]
    if unknown:
        print(f"Unknown product(s) {unknown}; valid: {sorted(PRODUCT_COLLECTIONS)}")
        return 1

    s3 = _maap_s3_client(maap)
    last_n = int(env("INGEST_LAST_N_COMPLETE_DAYS"))

    # Discovery is product-independent (one step-0 file holds every parameter).
    dates = discover_dates()
    candidates = dates[:last_n]
    if not candidates:
        print("no dates discovered on the Open Data feed. Nothing to do.")
        return 0
    print(f"candidate dates: {candidates}")

    total_submitted, total_failed = [], []
    for product in products:
        collection_id = PRODUCT_COLLECTIONS[product]
        print(f"\n=== Product '{product}' -> collection '{collection_id}' ===")

        prune_old_cogs(s3, collection_id)

        expected_files = 2 if product == "wind_arrows" else 1
        to_submit = [d for d in candidates
                     if not cog_exists_in_s3(s3, collection_id, d,
                                             min_count=expected_files)]
        skipped = [d for d in candidates if d not in to_submit]
        if skipped:
            print(f"[{product}] skipping {len(skipped)} with existing COG: {skipped}")
        if not to_submit:
            print(f"[{product}] all candidates already ingested.")
            continue
        print(f"[{product}] submitting {len(to_submit)} worker(s): {to_submit}")

        for d in to_submit:
            try:
                job = submit_worker(maap, product, collection_id, d)
            except Exception as e:  # noqa: BLE001
                print(f"  ✗ {product} {d}: submitJob raised: {e}")
                total_failed.append((product, d))
                continue
            if not getattr(job, "id", None):
                print(f"  ✗ {product} {d}: submitJob returned no id: {job}")
                total_failed.append((product, d))
                continue
            print(f"  ✓ {product} {d}: {job.id}  status={getattr(job, 'status', '?')}")
            total_submitted.append(job.id)

    print(f"\nSummary: {len(total_submitted)} submitted, {len(total_failed)} failed.")
    return 0 if not total_failed else 2


if __name__ == "__main__":
    sys.exit(main())
