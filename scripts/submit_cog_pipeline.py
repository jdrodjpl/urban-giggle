#!/usr/bin/env python3
"""Submit a Frozon COG ingest pipeline job to MAAP DPS.

Designed to be called from a GitHub Actions cron workflow. Computes a
temporal window ending today and submits against the registered
frozon-iss-cog-pipeline algorithm. After workers complete, the
orchestrator's --retain-days flag cleans up COGs older than the
newest-anchored window from the S3 output prefix.

Required environment variables:
    MAAP_TOKEN      — MAAP API token for headless auth.

All pipeline parameters are configurable via env vars with sensible
defaults (see DEFAULTS dict below).
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from maap.maap import MAAP

DEFAULTS = {
    "MAAP_HOST":             "api.maap-project.org",
    "ALGO_ID":               "frozon-iss-cog-pipeline",
    "ALGO_VERSION":          "main",
    # Full-Arctic VH daily mosaics need the high-vCPU/RAM worker.
    "QUEUE":                 "maap-dps-worker-32vcpu-64gb",
    "INPUT_SOURCE_TYPE":     "cmr",
    "CMR_SHORT_NAME":        "OPERA_L2_RTC-S1_V1",
    "CMR_BBOX":              "-180,60,180,90",
    "CMR_PREFER_HTTPS":      "true",
    "EDL_SECRET_NAME":       "earthdata-token-frozon",
    "COLLECTION_ID":         "frozon-rtc-s1-vh-daily",
    "S3_BUCKET":             "maap-ops-workspace",
    "S3_PREFIX":             "frozon/cogs/",
    "RETAIN_DAYS":           "7",
    "FILTER":                "*VH*.tif",
    "TIME_REGEX":            r"_(?P<start_date>\d{8}T\d{6})Z_",
    # TIME_REGEX is now also used by the orchestrator to group inputs
    # into per-date daily mosaics (one COG per acquisition date).
    "LIMIT":                 "",
    # Target the acquisition date (today UTC) - TARGET_OFFSET_DAYS. Default 2
    # because RTC-S1 granules can take 12+ hours after acquisition to land in
    # CMR, and we want the target day fully populated before we mosaic it.
    "TARGET_OFFSET_DAYS":    "2",
    # Plus extra days of CMR search before the target — handles backfill if
    # previous runs failed. orchestrator groups by acquisition date and
    # skips dates whose COG already exists in S3 (overwrite=false), so
    # healthy days incur zero extra mosaic work.
    "BACKFILL_DAYS":         "3",
}


def env(key: str) -> str:
    return os.environ.get(key, DEFAULTS.get(key, ""))


def main() -> int:
    token = os.environ.get("MAAP_TOKEN")
    if token:
        os.environ["MAAP_PGT"] = token
    maap = MAAP(maap_host=env("MAAP_HOST"))

    today = datetime.now(timezone.utc).date()
    target_offset = int(env("TARGET_OFFSET_DAYS"))
    backfill = int(env("BACKFILL_DAYS"))
    # target = the newest acquisition date we expect to be fully landed in CMR.
    # search range covers `backfill` extra prior days for self-healing.
    target_date = today - timedelta(days=target_offset)
    start = target_date - timedelta(days=backfill)
    # CMR treats date-only temporal_end as an exclusive boundary at start-of-day,
    # so we shift end one day past target so the target day itself is included.
    end = target_date + timedelta(days=1)

    job_params = {
        "identifier":                      f"frozon-cog-daily-{target_date.isoformat()}",
        "algo_id":                         env("ALGO_ID"),
        "version":                         env("ALGO_VERSION"),
        "queue":                           env("QUEUE"),
        "input_source_type":               env("INPUT_SOURCE_TYPE"),
        "cmr_short_name":                  env("CMR_SHORT_NAME"),
        "cmr_temporal_start":              start.isoformat(),
        "cmr_temporal_end":                end.isoformat(),
        "cmr_bbox":                        env("CMR_BBOX"),
        "cmr_prefer_https":                env("CMR_PREFER_HTTPS"),
        "earthdata_token_secret_name":     env("EDL_SECRET_NAME"),
        "collection_id":                   env("COLLECTION_ID"),
        "s3_bucket":                       env("S3_BUCKET"),
        "s3_prefix":                       env("S3_PREFIX"),
        "retain_days":                     env("RETAIN_DAYS"),
        "filter":                          env("FILTER"),
        "time_regex":                      env("TIME_REGEX"),
        # Lock the orchestrator to mosaicking ONLY through target_date.
        # Even if CMR returns some target+1 granules at the temporal-end
        # boundary, the orchestrator drops that bucket so we don't
        # produce a partial-day COG for today-1.
        "max_acquisition_date":            target_date.isoformat(),
    }
    limit = env("LIMIT")
    if limit:
        job_params["limit"] = limit

    job_params = {k: v for k, v in job_params.items() if v}

    print(f"Submitting COG pipeline: target={target_date}, search window {start} → {end}")
    for k, v in sorted(job_params.items()):
        print(f"  {k}: {v}")

    job = maap.submitJob(**job_params)

    if not job.id:
        print(f"ERROR: submitJob failed: {job}", file=sys.stderr)
        return 2

    print(f"Job submitted: {job.id}  status: {job.status}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
