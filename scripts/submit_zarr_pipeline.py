#!/usr/bin/env python3
"""Sync the Frozon Zarr time series with the current COG state in S3.

Runs in the GH Actions cron after the COG cron has had time to land
fresh outputs. Bypasses the Zarr orchestrator algorithm (`frozon-iss-zarr-pipeline`)
the same way the COG side did — discovery and submitJob happen here,
the only DPS-side piece is the worker (`frozon-iss-ingest-zarr:v3`).

Flow:

  1. List COG objects under the configured prefix; their `/YYYY/MM/DD/`
     date folders define the **desired** time dimension.
  2. Read the existing Zarr's `time` coordinate from S3 (if present).
  3. Compute the diff:
       to_add  = desired_dates − existing_dates  (download these COGs)
       to_drop = existing_dates − desired_dates  (worker drops slices)
  4. If nothing changed → exit cleanly; no DPS job submitted.
  5. Otherwise submit one worker job with:
       --input-s3-urls <JSON list of S3 URIs for to_add COGs>
       --desired-dates <CSV of the full target date set>
     and let the worker rewrite the Zarr to match.

Required env:
    MAAP_TOKEN — MAAP API token for headless auth.
"""

import json
import os
import re
import sys
from datetime import datetime, timezone

from maap.maap import MAAP

DEFAULTS = {
    "MAAP_HOST":                 "api.maap-project.org",
    "WORKER_ALGO_ID":            "frozon-iss-ingest-zarr",
    "WORKER_ALGO_VERSION":       "v3",
    # Same queue the COG worker runs on — known warm/scaled, and the
    # extra headroom is welcome on first-build runs (7 × 25 GiB COGs
    # staged to local disk + Zarr write).
    "QUEUE":                     "maap-dps-worker-32vcpu-64gb",
    # Source COGs we sync FROM — now the Sentinel-1 EW HH+HV product
    # (previously: frozon-rtc-s1-vh-daily, OPERA VH IW). Old OPERA
    # collection is being retired; see PIPELINE_TEMPLATE.md gotchas.
    "COG_S3_BUCKET":             "maap-ops-workspace",
    "COG_S3_PREFIX":             "jdrodrig/frozon/cogs/",
    "COG_COLLECTION_ID":         "frozon-s1-ew-hh-daily",
    # Zarr we sync TO.
    "ZARR_S3_BUCKET":            "maap-ops-workspace",
    "ZARR_S3_PREFIX":            "jdrodrig/frozon/zarrs/",
    "ZARR_COLLECTION_ID":        "frozon-s1-ew-hh-zarr",
    # Worker tuning.
    "CHUNK_SIZE":                "1024",
    # The COG filename contains the acquisition date as `_YYYYMMDD_daily_COG`,
    # so the worker can derive each new slice's timestamp from filename
    # alone. (build_zarr_sparse_streaming uses this regex's `start_date`
    # group for the time coordinate.)
    "TIME_REGEX":                r"_(?P<start_date>\d{8})_daily_COG",
}


def env(key: str) -> str:
    return os.environ.get(key, DEFAULTS.get(key, ""))


def _maap_s3_client(maap: MAAP):
    """Boto3 S3 client authorized via maap-py's workspace credentials."""
    import boto3

    response = maap.aws.workspace_bucket_credentials()
    creds = response.get("credentials") if isinstance(response, dict) else None
    if not creds or "aws_access_key_id" not in creds:
        raise RuntimeError(
            f"Unexpected shape from workspace_bucket_credentials(): {response!r}"
        )
    return boto3.client(
        "s3",
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
        aws_session_token=creds["aws_session_token"],
    )


def _maap_s3fs(maap: MAAP):
    """An s3fs filesystem using maap-py workspace credentials so we can
    open the existing Zarr from S3 with xarray/zarr."""
    import s3fs

    response = maap.aws.workspace_bucket_credentials()
    creds = response["credentials"]
    return s3fs.S3FileSystem(
        key=creds["aws_access_key_id"],
        secret=creds["aws_secret_access_key"],
        token=creds["aws_session_token"],
    )


def list_cog_dates(s3) -> dict[str, str]:
    """Return `{YYYYMMDD: s3_uri}` for every daily-COG object under
    the configured prefix. Picks one URI per date (the COG itself, not
    any companion files). Date is parsed from the `/YYYY/MM/DD/` slot
    in the key — the same path the COG runner writes."""
    bucket = env("COG_S3_BUCKET")
    prefix_parts = [
        p for p in (env("COG_S3_PREFIX").strip("/"), env("COG_COLLECTION_ID")) if p
    ]
    prefix = "/".join(prefix_parts) + "/"

    date_re = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")
    out: dict[str, str] = {}

    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.lower().endswith((".tif", ".tiff")):
                continue
            m = date_re.search(key)
            if not m:
                continue
            ymd = f"{m.group(1)}{m.group(2)}{m.group(3)}"
            uri = f"s3://{bucket}/{key}"
            # If a date has multiple TIFFs (shouldn't happen), prefer the
            # one named "...daily_COG..." so we ingest the real product.
            existing = out.get(ymd)
            if existing is None or "daily_COG" in key:
                out[ymd] = uri
    return out


def existing_zarr_dates(maap: MAAP) -> set[str]:
    """Read the existing Zarr's `time` coordinate from S3 and return the
    set of YYYYMMDD strings. Empty set if no Zarr exists yet."""
    import xarray as xr

    coll = env("ZARR_COLLECTION_ID")
    prefix_parts = [
        p for p in (env("ZARR_S3_PREFIX").strip("/"), coll) if p
    ]
    zarr_path = f"{env('ZARR_S3_BUCKET')}/{'/'.join(prefix_parts)}/{coll}.zarr"

    fs = _maap_s3fs(maap)
    try:
        if not fs.exists(zarr_path):
            print(f"No existing Zarr at s3://{zarr_path}.")
            return set()
    except Exception as e:
        print(f"Could not check Zarr existence at s3://{zarr_path}: {e}")
        return set()

    try:
        ds = xr.open_zarr(fs.get_mapper(zarr_path), consolidated=True, decode_times=True)
    except Exception:
        # Older stores may not be consolidated.
        ds = xr.open_zarr(fs.get_mapper(zarr_path), consolidated=False, decode_times=True)

    try:
        import pandas as pd
        times = pd.to_datetime(ds.time.values)
        return set(times.strftime("%Y%m%d"))
    finally:
        ds.close()


def submit_sync_job(maap: MAAP, desired_dates: list[str],
                    to_add_uris: list[str]):
    """Submit one Zarr worker in sync mode."""
    desired_csv = ",".join(sorted(desired_dates))
    identifier = f"Frozon-Zarr-Sync_{datetime.now(timezone.utc).strftime('%Y%m%d')}"

    params = {
        "identifier":          identifier,
        "algo_id":             env("WORKER_ALGO_ID"),
        "version":             env("WORKER_ALGO_VERSION"),
        "queue":               env("QUEUE"),
        "collection_id":       env("ZARR_COLLECTION_ID"),
        "s3_bucket":           env("ZARR_S3_BUCKET"),
        "s3_prefix":           env("ZARR_S3_PREFIX"),
        "input_s3_urls":       json.dumps(to_add_uris),
        "desired_dates":       desired_csv,
        "chunk_size":          env("CHUNK_SIZE"),
        "time_regex":          env("TIME_REGEX"),
        "allow_bounds_expansion": "true",
        "retain_days":         "0",
    }
    return maap.submitJob(**{k: v for k, v in params.items() if v})


def main() -> int:
    token = os.environ.get("MAAP_TOKEN")
    if token:
        os.environ["MAAP_PGT"] = token
    maap = MAAP(maap_host=env("MAAP_HOST"))

    s3 = _maap_s3_client(maap)
    cog_by_date = list_cog_dates(s3)
    desired_dates = set(cog_by_date.keys())
    print(f"COG state: {len(desired_dates)} date(s) — "
          f"{sorted(desired_dates) if len(desired_dates) <= 30 else 'too many to list'}")

    existing_dates = existing_zarr_dates(maap)
    print(f"Existing Zarr: {len(existing_dates)} date(s) — "
          f"{sorted(existing_dates) if len(existing_dates) <= 30 else 'too many to list'}")

    to_add = sorted(desired_dates - existing_dates)
    to_drop = sorted(existing_dates - desired_dates)
    print(f"Diff: +{len(to_add)} new, -{len(to_drop)} drop")
    if to_add:
        print(f"  to_add:  {to_add}")
    if to_drop:
        print(f"  to_drop: {to_drop}")

    if not to_add and not to_drop:
        print("Zarr already in sync with COG state. Nothing to do.")
        return 0

    if not desired_dates:
        print("No COGs present — refusing to submit a sync that would drop "
              "every slice. Exit without action.")
        return 0

    to_add_uris = [cog_by_date[d] for d in to_add]
    print(f"Submitting Zarr worker in sync mode "
          f"(desired={len(desired_dates)} dates, "
          f"input_s3_urls={len(to_add_uris)} URI(s))")

    try:
        job = submit_sync_job(maap, sorted(desired_dates), to_add_uris)
    except Exception as e:
        print(f"submitJob raised: {e}", file=sys.stderr)
        return 2

    if not getattr(job, "id", None):
        print(f"submitJob returned no id: {job}", file=sys.stderr)
        return 2

    print(f"  ✓ {job.id}  status={getattr(job, 'status', '?')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
