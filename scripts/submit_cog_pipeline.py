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
    "S3_PREFIX":                    "frozon/cogs/",
    # Filename / discovery.
    "FILTER":                       "*VH*.tif",
    "TIME_REGEX":                   r"_(?P<start_date>\d{8}T\d{6})Z_",
    # CMR temporal window for discovery (generous; we pick from what's there).
    "LOOKBACK_DAYS":                "30",
    # Of the dates returned by CMR, drop the newest (potentially still
    # landing) and submit workers for the next N most recent dates.
    "MOSAIC_LAST_N_COMPLETE_DAYS":  "7",
    # Worker tuning — forwarded to each per-date job.
    "COMPRESS":                     "DEFLATE",
    "BLOCKSIZE":                    "512",
    "MAX_MEMORY":                   "4096",
    "RESAMPLING":                   "nearest",
    "OVERVIEW_RESAMPLING":          "average",
    "OVERWRITE":                    "false",
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


def discover_acquisition_dates(maap: MAAP) -> list[str]:
    """CMR-search the configured collection/bbox for the lookback window and
    return the sorted-descending list of unique acquisition dates (YYYYMMDD)
    extracted from granule filenames matching FILTER."""
    import earthaccess

    _login_edl(maap)

    today = datetime.now(timezone.utc).date()
    start = today - timedelta(days=int(env("LOOKBACK_DAYS")))
    # CMR temporal_end is exclusive at start-of-day; push past today so the
    # newest still-partial day is visible (we drop it below).
    end = today + timedelta(days=1)

    kwargs = {
        "short_name": env("CMR_SHORT_NAME"),
        "temporal": (start.isoformat(), end.isoformat()),
    }
    bbox = env("CMR_BBOX")
    if bbox:
        kwargs["bounding_box"] = tuple(float(c) for c in bbox.split(","))

    print(f"CMR search: {kwargs}")
    results = earthaccess.search_data(**kwargs)
    print(f"  → {len(results)} granule(s)")

    # FILTER is a shell-style glob (e.g. "*VH*.tif"); convert to a regex.
    filt_re = re.compile(
        env("FILTER").replace(".", r"\.").replace("*", ".*")
    )
    name_re = re.compile(env("TIME_REGEX"))

    dates: set[str] = set()
    for g in results:
        try:
            urls = g.data_links(access="external") or []
        except Exception:
            urls = []
        for url in urls:
            name = os.path.basename(url.split("?", 1)[0])
            if not name.lower().endswith((".tif", ".tiff")):
                continue
            if not filt_re.search(name):
                continue
            m = name_re.search(name)
            if not m:
                continue
            try:
                ts = m.group("start_date")
            except (IndexError, KeyError):
                ts = m.group(1)
            dates.add(ts[:8])

    return sorted(dates, reverse=True)


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

    available = discover_acquisition_dates(maap)
    if len(available) < 2:
        print(f"Only {len(available)} acquisition date(s) discovered — "
              f"need ≥2 (one to drop, one to mosaic). Exiting.")
        return 1

    last_n = int(env("MOSAIC_LAST_N_COMPLETE_DAYS"))
    newest = available[0]
    candidates = available[1:1 + last_n]
    print(f"Dropped newest date {newest} (potentially incomplete).")
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
