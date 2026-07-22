#!/usr/bin/env python3
"""Submit Frozon CMEMS ocean-current worker jobs from the GH Actions runner.

CMEMS (Copernicus Marine) has no deterministic per-file URL to HEAD-probe like
OSI SAF / ECMWF, and the daily-mean analysis+forecast product reliably carries
the most recent days (plus forecast days). So "discovery" here is simply the
most recent INGEST_LAST_N_COMPLETE_DAYS calendar days — the worker does the
actual credentialed subset and returns exit code 6 if a given date isn't in the
store yet. The runner stays credential-free (it never touches Copernicus); only
the DPS worker logs in via the MAAP secret.

Per run, for each requested product (ocean_u / ocean_v):
  1. Retention sweep: keep RETAIN_DAYS most recent date folders per collection.
  2. S3 pre-check (the worker does NOT dedupe against S3) — submit one
     `frozon-iss-ingest-cmems:v1` worker per (product, missing date).

Required env:
    MAAP_TOKEN — MAAP API token for headless auth.
"""

import os
import re
import sys
from datetime import datetime, timedelta, timezone

from maap.maap import MAAP

# Per-product default collection IDs (mirror PRODUCTS in src/ingest_cmems.py).
PRODUCT_COLLECTIONS = {
    "ocean_u": "frozon-cmems-ocean-u-daily",
    "ocean_v": "frozon-cmems-ocean-v-daily",
}

DEFAULTS = {
    "MAAP_HOST":                  "api.maap-project.org",
    "WORKER_ALGO_ID":             "frozon-iss-ingest-cmems",
    "WORKER_ALGO_VERSION":        "v8",
    "QUEUE":                      "maap-dps-worker-8gb",
    # Which products to ingest this run (comma-separated subset).
    "PRODUCTS":                   "ocean_u,ocean_v",
    # CMEMS source + access knobs (forwarded to the worker).
    "DATASET_ID":                 "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m",
    "CMEMS_SECRET_NAME":          "copernicus-marine-frozon",
    "BBOX":                       "-180,20,180,90",
    # Output.
    "S3_BUCKET":                  "maap-ops-workspace",
    "S3_PREFIX":                  "jdrodrig/frozon/cogs/",
    # Discovery window — most recent N calendar days (worker skips any not yet
    # in the store via exit code 6).
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
# Discovery (calendar window — no per-file existence probe for CMEMS)
# --------------------------------------------------------------------------

def discover_dates() -> list[str]:
    """The most recent INGEST_LAST_N_COMPLETE_DAYS dates ending today
    (newest-first). Product-independent."""
    last_n = int(env("INGEST_LAST_N_COMPLETE_DAYS"))
    today = datetime.now(timezone.utc).date()
    dates = [(today - timedelta(days=off)).strftime("%Y%m%d") for off in range(last_n)]
    print(f"candidate window: {last_n} most recent day(s) ending "
          f"{today.isoformat()} -> {dates}")
    return dates


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


def cog_exists_in_s3(s3, collection_id: str, date_key: str) -> bool:
    yyyy, mm, dd = date_key[:4], date_key[4:6], date_key[6:8]
    bucket = env("S3_BUCKET")
    key_prefix = _collection_prefix(collection_id) + f"{yyyy}/{mm}/{dd}/"
    r = s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix, MaxKeys=1)
    return r.get("KeyCount", 0) > 0


# --------------------------------------------------------------------------
# Submit
# --------------------------------------------------------------------------

def submit_worker(maap: MAAP, product: str, collection_id: str, date_key: str):
    params = {
        "identifier":        f"Frozon-CMEMS-{product}_{date_key}",
        "algo_id":           env("WORKER_ALGO_ID"),
        "version":           env("WORKER_ALGO_VERSION"),
        "queue":             env("QUEUE"),
        "product":           product,
        "date":              date_key,
        "dataset_id":        env("DATASET_ID"),
        "cmems_secret_name": env("CMEMS_SECRET_NAME"),
        "bbox":              env("BBOX"),
        "collection_id":     collection_id,
        "s3_bucket":         env("S3_BUCKET"),
        "s3_prefix":         env("S3_PREFIX"),
        "compress":          env("COMPRESS"),
        "blocksize":         env("BLOCKSIZE"),
        "max_memory":        env("MAX_MEMORY"),
        "overwrite":         env("OVERWRITE"),
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
    candidates = discover_dates()

    total_submitted, total_failed = [], []
    for product in products:
        collection_id = PRODUCT_COLLECTIONS[product]
        print(f"\n=== Product '{product}' -> collection '{collection_id}' ===")

        prune_old_cogs(s3, collection_id)

        to_submit = [d for d in candidates if not cog_exists_in_s3(s3, collection_id, d)]
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
