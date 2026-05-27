#!/usr/bin/env python3
"""Submit a Frozon Zarr ingest pipeline job to MAAP DPS.

Designed to be called from a GitHub Actions cron workflow (or any
headless environment). Computes a 7-day temporal window ending today
and submits against the registered frozon-iss-zarr-pipeline algorithm.

Required environment variables:
    MAAP_HOST       — MAAP API host (default: api.maap-project.org)
    MAAP_TOKEN      — MAAP API token for headless auth (from your MAAP
                      profile page). Set as a GitHub Actions secret.

All pipeline parameters are configurable via env vars with sensible
defaults (see DEFAULTS dict below). Override any of them by setting
the corresponding env var in the workflow YAML.
"""

import os
import sys
from datetime import datetime, timedelta, timezone

from maap.maap import MAAP

DEFAULTS = {
    "MAAP_HOST":             "api.maap-project.org",
    "ALGO_ID":               "frozon-iss-zarr-pipeline",
    "ALGO_VERSION":          "main",
    "QUEUE":                 "maap-dps-worker-16gb",
    "INPUT_SOURCE_TYPE":     "cmr",
    "CMR_SHORT_NAME":        "OPERA_L2_RTC-S1_V1",
    "CMR_BBOX":              "-180,60,180,90",
    "CMR_PREFER_HTTPS":      "true",
    "EDL_SECRET_NAME":       "earthdata-token-frozon",
    "COLLECTION_ID":         "frozon-s1-zarr",
    "S3_BUCKET":             "maap-ops-workspace",
    "S3_PREFIX":             "jdrodrig/frozon-test/zarrs/",
    "RETAIN_DAYS":           "7",
    "FILTER":                "*VH*.tif",
    "TIME_REGEX":            r"_(?P<start_date>\d{8}T\d{6})Z_",
    "LIMIT":                 "",
    "TEMPORAL_WINDOW_DAYS":  "14",
}


def env(key: str) -> str:
    return os.environ.get(key, DEFAULTS.get(key, ""))


def main() -> int:
    maap_host = env("MAAP_HOST")
    token = os.environ.get("MAAP_TOKEN")
    if token:
        # Running outside the ADE (e.g. GitHub Actions) — set MAAP_PGT
        # so maap-py picks up the auth token during init.
        os.environ["MAAP_PGT"] = token
    # Inside the ADE, no token needed — session auth is implicit.
    maap = MAAP(maap_host=maap_host)

    end = datetime.now(timezone.utc).date()
    window = int(env("TEMPORAL_WINDOW_DAYS"))
    start = end - timedelta(days=window)

    job_params = {
        "identifier":                      f"frozon-zarr-daily-{end.isoformat()}",
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
    }
    limit = env("LIMIT")
    if limit:
        job_params["limit"] = limit

    # Drop empty values so MAAP doesn't serialize them as "".
    job_params = {k: v for k, v in job_params.items() if v}

    print(f"Submitting Zarr pipeline: {start} → {end}")
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
