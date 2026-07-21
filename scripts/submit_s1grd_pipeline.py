#!/usr/bin/env python3
"""Submit Frozon S1 GRD HH+HV worker jobs directly from the GH Actions runner.

Same shape as scripts/submit_cog_pipeline.py:

  1. Walk back day-by-day, counting matching `*_1SDH_*` granules per
     date on BOTH catalogs: CMR/ASF (SENTINEL-1A_DP_GRD_MEDIUM +
     SENTINEL-1C_DP_GRD_MEDIUM) and the Copernicus Data Space (CDSE)
     origin catalog, until we have MOSAIC_LAST_N_COMPLETE_DAYS + 1
     dates with data.
  2. Per date, prefer ASF when it has caught up to CDSE (its per-date
     count reaches CDSE's — counts match exactly once ASF backfills);
     otherwise submit the worker in CDSE mode. ASF mirrors CDSE with a
     lag observed to reach ~10 days, so without the CDSE path the most
     recent week of mosaics simply doesn't exist.
  3. Drop the newest date (potentially still landing) plus any date
     below MIN_GRANULES / MIN_GRANULE_FRACTION.
  4. Pre-check S3 to skip dates whose COG already exists.
  5. Submit one `frozon-iss-ingest-s1grd` worker per missing date,
     passing input_source=asf|cdse (+ the CDSE secret name).

The worker re-queries the chosen catalog itself for its single date
(self-query mode) — keeps the submitJob payload tiny regardless of
granule count.

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

# cdse.py is written to load standalone (stdlib + requests only) so the
# runner doesn't drag in the rest of src/'s import chain — see its
# module docstring. Reusing it here guarantees the runner's per-date
# counts and the worker's download list apply identical filters.
sys.path.insert(0, os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "src", "input_sources"))
import cdse

DEFAULTS = {
    "MAAP_HOST":                    "api.maap-project.org",
    "WORKER_ALGO_ID":               "frozon-iss-ingest-s1grd",
    "WORKER_ALGO_VERSION":          "v6",
    "QUEUE":                        "maap-dps-worker-32vcpu-64gb",
    # CMR query — both active S1 satellites.
    "CMR_SHORT_NAMES":              "SENTINEL-1A_DP_GRD_MEDIUM,SENTINEL-1C_DP_GRD_MEDIUM",
    "CMR_BBOX":                     "-180,60,180,90",
    "EDL_SECRET_NAME":              "earthdata-token-frozon",
    # CDSE fallback for dates ASF hasn't mirrored yet. MAAP secret holds
    # either `username\npassword` or `client_id=...\nclient_secret=...`
    # for a dataspace.copernicus.eu account. Empty string disables the
    # CDSE path (dates only ASF is missing get skipped, as before).
    "CDSE_SECRET_NAME":             "cdse-creds-frozon",
    # Output. Production is σ⁰ only — set CALIBRATIONS="sigma0,beta0"
    # (env override) and COLLECTION_ID_TEMPLATE="frozon-s1-ew-hh-{calibration}-daily"
    # if/when a consumer asks for β⁰ too. The dual-cal code path is in
    # the worker and ready to activate without code changes.
    "CALIBRATIONS":                 "sigma0",
    "COLLECTION_ID_TEMPLATE":       "",
    "COLLECTION_ID":                "frozon-s1-ew-hh-daily",
    "S3_BUCKET":                    "maap-ops-workspace",
    "S3_PREFIX":                    "jdrodrig/frozon/cogs/",
    # Filter — keep only HH+HV dual-pol granules (mode code 1SDH).
    "GRANULE_FILTER":               "*_1SDH_*",
    # Polarization the worker mosaics (HH or HV).
    "POLARIZATION":                 "HH",
    # Discovery window.
    "LOOKBACK_DAYS":                "30",
    "MOSAIC_LAST_N_COMPLETE_DAYS":  "7",
    # Absolute floor: drop dates with fewer than MIN_GRANULES granules (failed
    # downlink / partial CMR index). Preferred over the fraction filter because
    # S1 Arctic coverage is legitimately ~5x variable by orbit — a
    # fraction-of-max threshold wrongly drops real low-coverage days for good.
    "MIN_GRANULES":                 "20",
    # Legacy fraction-of-max filter; default 0 (off). Set >0 to also drop dates
    # below that fraction of the window's max count.
    "MIN_GRANULE_FRACTION":         "0",
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

def discover_acquisition_dates(maap: MAAP) -> list[tuple[str, int, int]]:
    """Walk back day-by-day, counting matching granules per date on both
    catalogs: CMR (summed across the S1A/S1C collections) and CDSE.
    Returns [(YYYYMMDD, cmr_count, cdse_count), ...] newest-first. A
    date counts as "found" when either catalog has data."""
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
          f"{short_names} (CMR + CDSE).")

    from fnmatch import fnmatch
    found: list[tuple[str, int, int]] = []
    for offset in range(1, lookback + 1):
        if len(found) >= target:
            break
        day = today - timedelta(days=offset)
        next_day = day + timedelta(days=1)
        cmr_count = 0
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
                    cmr_count += 1
        # A CDSE outage must not take down the ASF path — count as 0 and
        # let the per-date preference fall back to ASF.
        try:
            cdse_count = len(cdse.search_products(
                short_names, day.isoformat(), next_day.isoformat(),
                bbox=bbox_tuple, filter_pattern=filter_pat,
            ))
        except Exception as e:
            print(f"  {day.isoformat()} [CDSE]: search failed ({e})")
            cdse_count = 0
        if cmr_count > 0 or cdse_count > 0:
            found.append((day.strftime("%Y%m%d"), cmr_count, cdse_count))
            print(f"  {day.isoformat()}: cmr={cmr_count} cdse={cdse_count} "
                  f"({len(found)}/{target})")
        else:
            print(f"  {day.isoformat()}: 0 matching granules on either catalog")

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

def submit_worker(maap: MAAP, date_key: str, source: str = "asf"):
    date_dt = datetime.strptime(date_key, "%Y%m%d")
    this_day = date_dt.strftime("%Y-%m-%d")
    next_day = (date_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    params = {
        "identifier":                   f"Frozon-S1GRD-Daily_{date_key}",
        "algo_id":                      env("WORKER_ALGO_ID"),
        "version":                      env("WORKER_ALGO_VERSION"),
        "queue":                        env("QUEUE"),
        "input_source":                 source,
        "cdse_secret_name":             env("CDSE_SECRET_NAME") if source == "cdse" else "",
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
        "collection_id":                env("COLLECTION_ID"),
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

    newest, newest_cmr, newest_cdse = available[0]
    print(f"Dropped newest date {newest} (cmr={newest_cmr} cdse={newest_cdse} "
          f"granule(s) — assumed potentially incomplete).")

    # Per-date source decision. ASF is preferred: once it backfills a
    # date, its count matches CDSE's exactly, so "caught up" is simply
    # cmr_count >= cdse_count. Any shortfall means ASF is still
    # mirroring that date and CDSE is the complete catalog.
    cdse_secret = env("CDSE_SECRET_NAME")
    rest = []  # (date, count-for-chosen-source, source)
    for d, cmr_count, cdse_count in available[1:]:
        source = "asf" if cmr_count >= cdse_count else "cdse"
        if source == "cdse" and not cdse_secret:
            print(f"  {d}: ASF behind (cmr={cmr_count} < cdse={cdse_count}) but "
                  f"CDSE_SECRET_NAME is unset — falling back to ASF's partial view.")
            source = "asf"
        count = cmr_count if source == "asf" else cdse_count
        rest.append((d, count, source))
        print(f"  {d}: cmr={cmr_count} cdse={cdse_count} → source={source}")

    # Absolute floor first: only near-empty days (failed/partial acquisitions)
    # are dropped; genuinely-sparse-but-real Arctic days survive.
    min_granules = int(env("MIN_GRANULES"))
    if min_granules > 0 and rest:
        below = [t for t in rest if t[1] < min_granules]
        rest = [t for t in rest if t[1] >= min_granules]
        if below:
            print(f"Dropped {len(below)} date(s) below MIN_GRANULES={min_granules}:")
            for d, c, s in below:
                print(f"  {d}: {c} granule(s) [{s}]")

    # Optional fraction-of-max filter (off by default — see MIN_GRANULES).
    threshold_factor = float(env("MIN_GRANULE_FRACTION"))
    if threshold_factor > 0 and rest:
        max_count = max(c for _, c, _ in rest)
        floor = max_count * threshold_factor
        below = [t for t in rest if t[1] < floor]
        rest = [t for t in rest if t[1] >= floor]
        if below:
            print(f"Dropped {len(below)} below-fraction date(s) "
                  f"(< {floor:.0f} = {threshold_factor:.0%} of max {max_count}):")
            for d, c, s in below:
                print(f"  {d}: {c} granule(s) [{s}]")

    last_n = int(env("MOSAIC_LAST_N_COMPLETE_DAYS"))
    candidates = [(d, s) for d, _, s in rest[:last_n]]
    if not candidates:
        print("No candidate dates after filtering. Nothing to do.")
        return 0

    s3 = _maap_s3_client(maap)
    to_submit, skipped = [], []
    for d, s in candidates:
        (skipped if cog_exists_in_s3(s3, d) else to_submit).append((d, s))
    if skipped:
        print(f"Skipping {len(skipped)} date(s) with existing COG: "
              f"{[d for d, _ in skipped]}")
    if not to_submit:
        print("All candidates already have COGs. Nothing to submit.")
        return 0
    print(f"Submitting {len(to_submit)} worker(s) for: "
          f"{[f'{d}[{s}]' for d, s in to_submit]}")

    submitted, failed = [], []
    for d, s in to_submit:
        try:
            job = submit_worker(maap, d, source=s)
        except Exception as e:
            print(f"  ✗ {d}: submitJob raised: {e}")
            failed.append(d)
            continue
        if not getattr(job, "id", None):
            print(f"  ✗ {d}: submitJob returned no id: {job}")
            failed.append(d)
            continue
        print(f"  ✓ {d} [{s}]: {job.id}  status={getattr(job, 'status', '?')}")
        submitted.append(job.id)

    print(f"\nSummary: {len(submitted)} submitted, {len(failed)} failed, "
          f"{len(skipped)} already-existing skipped (out of {len(candidates)} candidate date(s)).")
    return 0 if not failed else 2


if __name__ == "__main__":
    sys.exit(main())
