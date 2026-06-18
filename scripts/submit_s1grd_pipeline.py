#!/usr/bin/env python3
"""Submit Frozon S1 GRD HH+HV worker jobs directly from the GH Actions runner.

Same shape as scripts/submit_cog_pipeline.py:

  1. CMR-walk back day-by-day across BOTH SENTINEL-1A_DP_GRD_MEDIUM and
     SENTINEL-1C_DP_GRD_MEDIUM, counting matching `*_1SDH_*` granules
     per date until we have MOSAIC_LAST_N_COMPLETE_DAYS + 1 dates with
     data.
  2. Drop the newest date (potentially still landing) plus any date
     below MIN_GRANULE_FRACTION of the max count.
  3. Pre-check S3 to skip dates whose COG already exists.
  4. Submit one `frozon-iss-ingest-s1grd:v1` worker per missing date.

The worker re-queries CMR itself for its single date (CMR-self-query
mode) — keeps the submitJob payload tiny regardless of granule count.

Retention sweep at the top of the run keeps RETAIN_DAYS most recent
date folders.

Required env:
    MAAP_TOKEN — MAAP API token for headless auth.
"""

import os
import re
import sys
from datetime import datetime, timedelta, timezone

from maap.maap import MAAP

DEFAULTS = {
    "MAAP_HOST":                    "api.maap-project.org",
    "WORKER_ALGO_ID":               "frozon-iss-ingest-s1grd",
    "WORKER_ALGO_VERSION":          "v2",
    "QUEUE":                        "maap-dps-worker-32vcpu-64gb",
    # CMR query — both active S1 satellites.
    "CMR_SHORT_NAMES":              "SENTINEL-1A_DP_GRD_MEDIUM,SENTINEL-1C_DP_GRD_MEDIUM",
    "CMR_BBOX":                     "-180,60,180,90",
    "EDL_SECRET_NAME":              "earthdata-token-frozon",
    # Output. With multiple calibrations, the worker resolves the per-cal
    # collection_id from COLLECTION_ID_TEMPLATE by substituting
    # {calibration}. The legacy single-collection COLLECTION_ID is only
    # used for retention + S3 pre-check (pinned to the σ⁰ collection
    # since that's the canonical "do we already have this date" check).
    "CALIBRATIONS":                 "sigma0,beta0",
    "COLLECTION_ID_TEMPLATE":       "frozon-s1-ew-hh-{calibration}-daily",
    "COLLECTION_ID":                "frozon-s1-ew-hh-sigma0-daily",
    "S3_BUCKET":                    "maap-ops-workspace",
    "S3_PREFIX":                    "jdrodrig/frozon/cogs/",
    # Filter — keep only HH+HV dual-pol granules (mode code 1SDH).
    "GRANULE_FILTER":               "*_1SDH_*",
    # Polarization the worker mosaics (HH or HV).
    "POLARIZATION":                 "HH",
    # Discovery window.
    "LOOKBACK_DAYS":                "30",
    "MOSAIC_LAST_N_COMPLETE_DAYS":  "7",
    "MIN_GRANULE_FRACTION":         "0.5",
    "DISCOVERY_BUFFER":             "2",
    # Worker tuning.
    "COMPRESS":                     "DEFLATE",
    "BLOCKSIZE":                    "512",
    "MAX_MEMORY":                   "4096",
    "RESAMPLING":                   "nearest",
    "OVERVIEW_RESAMPLING":          "average",
    "OVERWRITE":                    "false",
    "RETAIN_DAYS":                  "7",
}


def env(key: str) -> str:
    return os.environ.get(key, DEFAULTS.get(key, ""))


# --------------------------------------------------------------------------
# Workspace credentials (for S3 pre-check + retention)
# --------------------------------------------------------------------------

def _login_edl(maap: MAAP) -> None:
    import earthaccess
    secret_name = env("EDL_SECRET_NAME")
    secret = maap.secrets.get_secret(secret_name)
    body = secret if isinstance(secret, str) else secret.get("value", "")
    lines = [ln for ln in body.splitlines() if ln.strip()]
    if body.strip().startswith("token=") or (len(lines) == 1 and "=" not in lines[0]):
        os.environ["EARTHDATA_TOKEN"] = body.split("=", 1)[-1].strip()
    elif len(lines) >= 2:
        os.environ["EARTHDATA_USERNAME"] = lines[0].strip()
        os.environ["EARTHDATA_PASSWORD"] = lines[1].strip()
    else:
        raise RuntimeError(f"EDL secret {secret_name!r} is empty or malformed.")
    auth = earthaccess.login(strategy="environment")
    if not auth or not auth.authenticated:
        raise RuntimeError(f"EDL auth failed via MAAP secret {secret_name!r}")


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


# --------------------------------------------------------------------------
# Discovery
# --------------------------------------------------------------------------

def discover_acquisition_dates(maap: MAAP) -> list[tuple[str, int]]:
    """Walk back day-by-day across both S1A and S1C collections, summing
    matching granule counts per date. Returns [(YYYYMMDD, count), ...]
    newest-first."""
    import earthaccess
    _login_edl(maap)

    short_names = [s.strip() for s in env("CMR_SHORT_NAMES").split(",") if s.strip()]
    bbox = env("CMR_BBOX")
    bbox_tuple = tuple(float(c) for c in bbox.split(",")) if bbox else None
    filter_pat = env("GRANULE_FILTER")
    lookback = int(env("LOOKBACK_DAYS"))
    target = int(env("MOSAIC_LAST_N_COMPLETE_DAYS")) + 1 + int(env("DISCOVERY_BUFFER"))

    today = datetime.now(timezone.utc).date()
    print(f"Walking back from {today.isoformat()} up to {lookback} day(s); "
          f"collecting {target} dates with {filter_pat!r} hits across "
          f"{short_names}.")

    from fnmatch import fnmatch
    found: list[tuple[str, int]] = []
    for offset in range(1, lookback + 1):
        if len(found) >= target:
            break
        day = today - timedelta(days=offset)
        next_day = day + timedelta(days=1)
        count_this_day = 0
        for sn in short_names:
            try:
                results = earthaccess.search_data(
                    short_name=sn,
                    temporal=(day.isoformat(), next_day.isoformat()),
                    bounding_box=bbox_tuple,
                )
            except Exception as e:
                print(f"  {day.isoformat()} [{sn}]: search failed ({e})")
                continue
            # Filter to matching granule names. We don't need URL details
            # at the runner — the worker re-queries with the same filter.
            for g in results:
                try:
                    granule_ur = g["umm"]["GranuleUR"]
                except Exception:
                    continue
                if fnmatch(granule_ur, filter_pat):
                    count_this_day += 1
        if count_this_day > 0:
            found.append((day.strftime("%Y%m%d"), count_this_day))
            print(f"  {day.isoformat()}: {count_this_day} matching granule(s) "
                  f"({len(found)}/{target})")
        else:
            print(f"  {day.isoformat()}: 0 matching granules")

    return found


# --------------------------------------------------------------------------
# Retention + S3 pre-check
# --------------------------------------------------------------------------

def prune_old_cogs(maap: MAAP) -> None:
    retain_days = int(env("RETAIN_DAYS"))
    if retain_days <= 0:
        return
    from datetime import date as _date
    bucket = env("S3_BUCKET")
    prefix_parts = [p for p in (env("S3_PREFIX").strip("/"), env("COLLECTION_ID")) if p]
    prefix = "/".join(prefix_parts) + "/"
    s3 = _maap_s3_client(maap)
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
        print(f"Retention: no date folders under s3://{bucket}/{prefix}.")
        return
    sorted_desc = sorted(objects_by_date.keys(), reverse=True)
    keep = set(sorted_desc[:retain_days])
    drop = [d for d in sorted_desc if d not in keep]
    to_delete = [k for d in drop for k in objects_by_date[d]]
    if not to_delete:
        print(f"Retention: {len(sorted_desc)} date(s) present, all within "
              f"retain_days={retain_days}; nothing to delete.")
        return
    print(f"Retention: deleting {len(to_delete)} object(s) from {drop}")
    for i in range(0, len(to_delete), 1000):
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in to_delete[i:i + 1000]]},
        )


def cog_exists_in_s3(s3, date_key: str) -> bool:
    yyyy, mm, dd = date_key[:4], date_key[4:6], date_key[6:8]
    bucket = env("S3_BUCKET")
    prefix_parts = [p for p in (env("S3_PREFIX").strip("/"), env("COLLECTION_ID")) if p]
    key_prefix = "/".join(prefix_parts) + f"/{yyyy}/{mm}/{dd}/"
    r = s3.list_objects_v2(Bucket=bucket, Prefix=key_prefix, MaxKeys=1)
    return r.get("KeyCount", 0) > 0


# --------------------------------------------------------------------------
# Submit
# --------------------------------------------------------------------------

def submit_worker(maap: MAAP, date_key: str):
    date_dt = datetime.strptime(date_key, "%Y%m%d")
    this_day = date_dt.strftime("%Y-%m-%d")
    next_day = (date_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    params = {
        "identifier":                   f"Frozon-S1GRD-Daily_{date_key}",
        "algo_id":                      env("WORKER_ALGO_ID"),
        "version":                      env("WORKER_ALGO_VERSION"),
        "queue":                        env("QUEUE"),
        "cmr_short_names":              env("CMR_SHORT_NAMES"),
        "cmr_temporal_start":           this_day,
        "cmr_temporal_end":             next_day,
        "cmr_bbox":                     env("CMR_BBOX"),
        "filter":                       env("GRANULE_FILTER"),
        "polarization":                 env("POLARIZATION"),
        "mosaic_date":                  date_key,
        "earthdata_token_secret_name":  env("EDL_SECRET_NAME"),
        "calibrations":                 env("CALIBRATIONS"),
        "collection_id_template":       env("COLLECTION_ID_TEMPLATE"),
        "s3_bucket":                    env("S3_BUCKET"),
        "s3_prefix":                    env("S3_PREFIX"),
        "compress":                     env("COMPRESS"),
        "blocksize":                    env("BLOCKSIZE"),
        "max_memory":                   env("MAX_MEMORY"),
        "resampling":                   env("RESAMPLING"),
        "overview_resampling":          env("OVERVIEW_RESAMPLING"),
        "overwrite":                    env("OVERWRITE"),
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

    prune_old_cogs(maap)

    available = discover_acquisition_dates(maap)
    if len(available) < 2:
        print(f"Only {len(available)} acquisition date(s) discovered; need ≥2. Exit.")
        return 1

    newest, newest_count = available[0]
    print(f"Dropped newest date {newest} ({newest_count} granule(s) — "
          f"assumed potentially incomplete).")

    rest = available[1:]
    threshold_factor = float(env("MIN_GRANULE_FRACTION"))
    if threshold_factor > 0 and rest:
        max_count = max(c for _, c in rest)
        floor = max_count * threshold_factor
        below = [(d, c) for d, c in rest if c < floor]
        rest = [(d, c) for d, c in rest if c >= floor]
        if below:
            print(f"Dropped {len(below)} below-threshold date(s) "
                  f"(< {floor:.0f} = {threshold_factor:.0%} of max {max_count}):")
            for d, c in below:
                print(f"  {d}: {c} granule(s)")

    last_n = int(env("MOSAIC_LAST_N_COMPLETE_DAYS"))
    candidates = [d for d, _ in rest[:last_n]]
    if not candidates:
        print("No candidate dates after filtering. Nothing to do.")
        return 0

    s3 = _maap_s3_client(maap)
    to_submit, skipped = [], []
    for d in candidates:
        (skipped if cog_exists_in_s3(s3, d) else to_submit).append(d)
    if skipped:
        print(f"Skipping {len(skipped)} date(s) with existing COG: {skipped}")
    if not to_submit:
        print("All candidates already have COGs. Nothing to submit.")
        return 0
    print(f"Submitting {len(to_submit)} worker(s) for: {to_submit}")

    submitted, failed = [], []
    for d in to_submit:
        try:
            job = submit_worker(maap, d)
        except Exception as e:
            print(f"  ✗ {d}: submitJob raised: {e}")
            failed.append(d)
            continue
        if not getattr(job, "id", None):
            print(f"  ✗ {d}: submitJob returned no id: {job}")
            failed.append(d)
            continue
        print(f"  ✓ {d}: {job.id}  status={getattr(job, 'status', '?')}")
        submitted.append(job.id)

    print(f"\nSummary: {len(submitted)} submitted, {len(failed)} failed, "
          f"{len(skipped)} already-existing skipped (out of {len(candidates)} candidate date(s)).")
    return 0 if not failed else 2


if __name__ == "__main__":
    sys.exit(main())
