#!/usr/bin/env python3
"""Submit Frozon COG worker jobs directly from the GH Actions runner.

Bypasses the orchestrator-as-DPS-job path entirely (MAAP's image
distribution kept serving stale images for the orchestrator container
no matter how many fresh builds we pushed). All discovery and job
submission now happens here, in the runner:

  1. CMR search across `LOOKBACK_DAYS` for the configured collection/bbox.
  2. Extract unique acquisition dates from granule filenames via TIME_REGEX.
  3. Drop the newest date (still landing — partial), keep the next
     `MOSAIC_LAST_N_COMPLETE_DAYS`.
  4. Submit one worker DPS job per kept date (CMR-self-query mode — the
     worker re-runs CMR for its own date and downloads).

Workers continue to dedupe via `overwrite=false` — dates whose COG
already exists in S3 are no-ops on the worker side, so the steady-state
cost is one new mosaic per cron run.

Required env:
    MAAP_TOKEN — MAAP API token for headless auth.

All other knobs are env-overridable; defaults below.
"""

import os
import re
import sys
from datetime import datetime, timedelta, timezone

from maap.maap import MAAP

DEFAULTS = {
    "MAAP_HOST":                    "api.maap-project.org",
    # Worker algo registration we submit each per-date job to.
    "WORKER_ALGO_ID":               "frozon-iss-ingest-cog",
    "WORKER_ALGO_VERSION":          "v2",
    "QUEUE":                        "maap-dps-worker-32vcpu-64gb",
    # CMR + EDL.
    "CMR_SHORT_NAME":               "OPERA_L2_RTC-S1_V1",
    "CMR_BBOX":                     "-180,60,180,90",
    "CMR_PREFER_HTTPS":             "true",
    "EDL_SECRET_NAME":              "earthdata-token-frozon",
    # Output.
    "COLLECTION_ID":                "frozon-rtc-s1-vh-daily",
    "S3_BUCKET":                    "maap-ops-workspace",
    # Inside the user's MAAP workspace namespace so the runner can use
    # maap-py's workspace_bucket_credentials() (which only authorize
    # the `jdrodrig/...` prefix) for both retention and worker output.
    "S3_PREFIX":                    "jdrodrig/frozon/cogs/",
    # Filename / discovery.
    "FILTER":                       "*VH*.tif",
    "TIME_REGEX":                   r"_(?P<start_date>\d{8}T\d{6})Z_",
    # CMR temporal window for discovery (generous; we pick from what's there).
    "LOOKBACK_DAYS":                "30",
    # Of the dates returned by CMR, drop the newest (potentially still
    # landing) and submit workers for the next N most recent dates.
    "MOSAIC_LAST_N_COMPLETE_DAYS":  "7",
    # Any candidate date whose granule count is below this fraction of the
    # max granule count we observed during discovery gets dropped as
    # likely still-landing. Set 0 to disable the filter. Per recent
    # observation, full-Arctic daily counts ~5000-7700; lagging dates
    # (e.g. 2026-06-10 at 1980) are obvious outliers at 0.5.
    "MIN_GRANULE_FRACTION":         "0.5",
    # Extra dates to query past the keep window so the threshold filter
    # has slack — if `buffer` dates are dropped as partial, we still
    # have enough complete dates to fill the keep window.
    "DISCOVERY_BUFFER":             "2",
    # Worker tuning — forwarded to each per-date job.
    "COMPRESS":                     "DEFLATE",
    "BLOCKSIZE":                    "512",
    "MAX_MEMORY":                   "4096",
    "RESAMPLING":                   "nearest",
    "OVERVIEW_RESAMPLING":          "average",
    "OVERWRITE":                    "false",
    # Retention — keep the N most recent acquisition-date folders in S3,
    # delete the rest. Runs at the top of each cron so we free space
    # before submitting new mosaics. Set 0 to disable.
    "RETAIN_DAYS":                  "7",
}


def env(key: str) -> str:
    return os.environ.get(key, DEFAULTS.get(key, ""))


def _login_edl(maap: MAAP) -> None:
    """Resolve an EDL credential via MAAP secret and log into earthaccess.

    Accepts either a single-line `token=...` secret or a two-line
    `username\\npassword` secret — same format the worker uses.
    """
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
        raise RuntimeError(
            f"EDL secret {secret_name!r} is empty or malformed."
        )

    auth = earthaccess.login(strategy="environment")
    if not auth or not auth.authenticated:
        raise RuntimeError(f"EDL auth failed via MAAP secret {secret_name!r}")


def discover_acquisition_dates(maap: MAAP) -> list[tuple[str, int]]:
    """Walk back day-by-day from today, querying CMR for the granule count
    per date. Returns `[(YYYYMMDD, count), ...]` newest-first.

    We collect `last_n + 1 (drop-newest) + buffer (threshold filter slack)`
    dates with data so the threshold filter applied in main() can drop a
    few partial-day candidates without leaving the keep window short.

    Per-day `.hits()` queries are O(KB) each — keeps us well under CMR's
    connection limits (the earlier bulk get_all() tripped RemoteDisconnected).
    """
    import earthaccess

    _login_edl(maap)

    short_name = env("CMR_SHORT_NAME")
    bbox = env("CMR_BBOX")
    bbox_tuple = (
        tuple(float(c) for c in bbox.split(",")) if bbox else None
    )
    lookback = int(env("LOOKBACK_DAYS"))
    target_count = (
        int(env("MOSAIC_LAST_N_COMPLETE_DAYS"))
        + 1   # newest dropped as potentially incomplete
        + int(env("DISCOVERY_BUFFER"))  # slack for threshold filter
    )

    today = datetime.now(timezone.utc).date()
    print(f"Walking back from {today.isoformat()} up to {lookback} day(s), "
          f"collecting {target_count} dates with data.")

    dates_with_counts: list[tuple[str, int]] = []
    for offset in range(1, lookback + 1):
        if len(dates_with_counts) >= target_count:
            break
        day = today - timedelta(days=offset)
        next_day = day + timedelta(days=1)
        q = earthaccess.DataGranules().short_name(short_name).temporal(
            day.isoformat(), next_day.isoformat()
        )
        if bbox_tuple:
            q = q.bounding_box(*bbox_tuple)
        try:
            count = q.hits()
        except Exception as e:
            print(f"  {day.isoformat()}: CMR hits() failed ({e}) — skip")
            continue
        if count > 0:
            dates_with_counts.append((day.strftime("%Y%m%d"), count))
            print(f"  {day.isoformat()}: {count} granule(s) ✓ "
                  f"({len(dates_with_counts)}/{target_count})")
        else:
            print(f"  {day.isoformat()}: 0 granules")

    return dates_with_counts


def _maap_s3_client(maap: MAAP):
    """Return a boto3 S3 client authorized via maap-py's workspace
    bucket credentials. The runner already has MAAP_PGT, and MAAP
    vends short-lived credentials valid for the user's workspace
    prefix (e.g. s3://maap-ops-workspace/jdrodrig/*).
    """
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


def prune_old_cogs(maap: MAAP) -> None:
    """List the configured collection prefix in S3, group objects by their
    `/YYYY/MM/DD/` date folder, keep the `RETAIN_DAYS` most recent, and
    delete the rest. Count-based — sparse data never gets pruned below
    `RETAIN_DAYS` even when those days are spread far apart.

    S3 credentials come from maap-py's workspace credentials API so the
    runner only needs MAAP_PGT — no separate AWS secrets to manage.
    """
    retain_days = int(env("RETAIN_DAYS"))
    if retain_days <= 0:
        print(f"Retention disabled (RETAIN_DAYS={retain_days}).")
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
            key = obj["Key"]
            m = date_pattern.search(key)
            if m:
                d = _date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                objects_by_date.setdefault(d, []).append(key)

    if not objects_by_date:
        print(f"Retention: no date folders under s3://{bucket}/{prefix}.")
        return

    sorted_desc = sorted(objects_by_date.keys(), reverse=True)
    dates_to_keep = set(sorted_desc[:retain_days])
    dates_to_drop = [d for d in sorted_desc if d not in dates_to_keep]
    to_delete = [k for d in dates_to_drop for k in objects_by_date[d]]

    print(f"Retention: {len(sorted_desc)} date folder(s) present. "
          f"Keeping {len(dates_to_keep)}: {sorted(d.isoformat() for d in dates_to_keep)}")

    if not to_delete:
        print("Retention: nothing past the keep window — no deletes.")
        return

    print(f"Retention: deleting {len(to_delete)} object(s) "
          f"from {[d.isoformat() for d in dates_to_drop]}")
    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i:i + 1000]
        s3.delete_objects(
            Bucket=bucket,
            Delete={"Objects": [{"Key": k} for k in batch]},
        )
    print(f"Retention: deleted {len(to_delete)} object(s).")


def submit_worker_for_date(maap: MAAP, date_key: str):
    """Submit one DPS worker job in CMR-self-query mode for a single date."""
    date_dt = datetime.strptime(date_key, "%Y%m%d")
    this_day = date_dt.strftime("%Y-%m-%d")
    next_day = (date_dt + timedelta(days=1)).strftime("%Y-%m-%d")

    params = {
        "identifier":                   f"Frozon-COG-Daily_{date_key}",
        "algo_id":                      env("WORKER_ALGO_ID"),
        "version":                      env("WORKER_ALGO_VERSION"),
        "queue":                        env("QUEUE"),
        "collection_id":                env("COLLECTION_ID"),
        "s3_bucket":                    env("S3_BUCKET"),
        "s3_prefix":                    env("S3_PREFIX"),
        "compress":                     env("COMPRESS"),
        "blocksize":                    env("BLOCKSIZE"),
        "max_memory":                   env("MAX_MEMORY"),
        "resampling":                   env("RESAMPLING"),
        "overview_resampling":          env("OVERVIEW_RESAMPLING"),
        "overwrite":                    env("OVERWRITE"),
        "mosaic_date":                  date_key,
        "cmr_short_name":               env("CMR_SHORT_NAME"),
        "cmr_temporal_start":           this_day,
        "cmr_temporal_end":             next_day,
        "cmr_bbox":                     env("CMR_BBOX"),
        "cmr_prefer_https":             env("CMR_PREFER_HTTPS"),
        "filter":                       env("FILTER"),
        "earthdata_token_secret_name":  env("EDL_SECRET_NAME"),
    }
    params = {k: v for k, v in params.items() if v}
    return maap.submitJob(**params)


def main() -> int:
    token = os.environ.get("MAAP_TOKEN")
    if token:
        os.environ["MAAP_PGT"] = token
    maap = MAAP(maap_host=env("MAAP_HOST"))

    prune_old_cogs(maap)

    available = discover_acquisition_dates(maap)
    if len(available) < 2:
        print(f"Only {len(available)} acquisition date(s) discovered — "
              f"need ≥2 (one to drop, one to mosaic). Exiting.")
        return 1

    newest_date, newest_count = available[0]
    print(f"Dropped newest date {newest_date} "
          f"({newest_count} granule(s) — assumed potentially incomplete).")

    rest = available[1:]

    # Granule-count threshold filter — any date with significantly fewer
    # granules than the max in this window is probably still landing.
    threshold_factor = float(env("MIN_GRANULE_FRACTION"))
    if threshold_factor > 0 and rest:
        max_count = max(c for _, c in rest)
        floor = max_count * threshold_factor
        below = [(d, c) for d, c in rest if c < floor]
        rest = [(d, c) for d, c in rest if c >= floor]
        if below:
            print(f"Dropped {len(below)} below-threshold date(s) "
                  f"(< {floor:.0f} granules = {threshold_factor:.0%} of "
                  f"max {max_count}):")
            for d, c in below:
                print(f"  {d}: {c} granule(s)")

    last_n = int(env("MOSAIC_LAST_N_COMPLETE_DAYS"))
    candidates = [d for d, _ in rest[:last_n]]

    if not candidates:
        print("No candidate dates survived filtering. Nothing to submit.")
        return 0
    print(f"Submitting {len(candidates)} worker(s) for: {candidates}")

    submitted: list[str] = []
    failed: list[str] = []
    for date_key in candidates:
        try:
            job = submit_worker_for_date(maap, date_key)
        except Exception as e:
            print(f"  ✗ {date_key}: submitJob raised: {e}")
            failed.append(date_key)
            continue
        if not getattr(job, "id", None):
            print(f"  ✗ {date_key}: submitJob returned no id: {job}")
            failed.append(date_key)
            continue
        print(f"  ✓ {date_key}: {job.id}  status={getattr(job, 'status', '?')}")
        submitted.append(job.id)

    print()
    print(f"Summary: {len(submitted)} submitted, {len(failed)} failed "
          f"out of {len(candidates)} candidate date(s).")
    print("Workers run independently; each handles overwrite=false dedup so "
          "dates whose COG already exists in S3 will no-op.")
    return 0 if not failed else 2


if __name__ == "__main__":
    sys.exit(main())
