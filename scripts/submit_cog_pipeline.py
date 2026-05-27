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
    "QUEUE":                 "maap-dps-worker-8gb",
    "INPUT_SOURCE_TYPE":     "cmr",
    "CMR_SHORT_NAME":        "OPERA_L2_RTC-S1_V1",
    "CMR_BBOX":              "-180,60,180,90",
    "CMR_PREFER_HTTPS":      "true",
    "EDL_SECRET_NAME":       "earthdata-token-frozon",
    "COLLECTION_ID":         "frozon-s1-cog",
    "S3_BUCKET":             "maap-ops-workspace",
    "S3_PREFIX":             "jdrodrig/frozon-test/cogs/",
    "RETAIN_DAYS":           "7",
    "FILTER":                "*VH*.tif",
    "TIME_REGEX":            r"_(?P<start_date>\d{8}T\d{6})Z_",
    "LIMIT":                 "",
    "TEMPORAL_WINDOW_DAYS":  "14",
}


def env(key: str) -> str:
    return os.environ.get(key, DEFAULTS.get(key, ""))


def main() -> int:
    token = os.environ.get("MAAP_TOKEN")
    if not token:
        print("ERROR: MAAP_TOKEN env var is required.", file=sys.stderr)
        return 1

    # maap-py reads auth from env vars, not constructor args.
    os.environ["MAAP_PGT"] = token
    maap = MAAP(maap_host=env("MAAP_HOST"))

    end = datetime.now(timezone.utc).date()
    window = int(env("TEMPORAL_WINDOW_DAYS"))
    start = end - timedelta(days=window)

    job_params = {
        "identifier":                      f"frozon-cog-daily-{end.isoformat()}",
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
    }
    limit = env("LIMIT")
    if limit:
        job_params["limit"] = limit

    job_params = {k: v for k, v in job_params.items() if v}

    print(f"Submitting COG pipeline: {start} → {end}")
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
