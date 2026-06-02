"""
Orchestrator: Frozon ISS COG pipeline.

Submits MAAP DPS jobs that wrap ingest_cog.py for one or more TIFF inputs,
waits for them, then drives STAC cataloging into MMGIS.

Modeled on czdt-iss-ingest-job/src/pipeline_generic.py.
"""

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import date, timedelta
from typing import Dict, List, Optional

import backoff
from maap.dps.dps_job import DPSJob

from catalog_orchestration import catalog_products
from common_utils import AWSUtils, ConfigUtils, LoggingUtils, MaapUtils
from input_sources import InputRef, ensure_edl_login, make_source

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ALGO_ID = "frozon-iss-ingest-cog"
ALGO_VERSION = "main"


def parse_arguments() -> argparse.Namespace:
    args = ConfigUtils.get_cog_argument_parser().parse_args()
    logger.debug(f"Parsed arguments: {vars(args)}")
    return args


@backoff.on_exception(backoff.expo, Exception, max_value=64, max_time=172800)
async def wait_for_completion(job: DPSJob) -> DPSJob:
    await asyncio.to_thread(job.retrieve_status)
    if job.status.lower() in ["deleted", "accepted", "running"]:
        logger.debug(f"Current status is {job.status}. Backing off.")
        raise RuntimeError
    return job


def group_refs_by_date(input_refs: List[InputRef],
                       time_regex: Optional[str]) -> Dict[str, List[InputRef]]:
    """Group input refs by acquisition date (YYYYMMDD).

    Uses the same `time_regex` the Zarr pipeline uses to extract a
    datetime from each filename, then keys on the date portion. Refs
    whose filename doesn't match fall into a single 'unknown' bucket
    that gets logged loudly and skipped.
    """
    if not time_regex:
        # No grouping requested — treat every input as its own date bucket
        # (preserves the legacy per-granule behavior).
        return {ref.name: [ref] for ref in input_refs}

    pattern = re.compile(time_regex)
    groups: Dict[str, List[InputRef]] = defaultdict(list)
    skipped: List[str] = []
    for ref in input_refs:
        m = pattern.search(ref.name)
        if not m:
            skipped.append(ref.name)
            continue
        ts = m.group('start_date') if 'start_date' in (m.groupdict() or {}) else m.group(1)
        date_key = ts[:8]   # YYYYMMDD
        groups[date_key].append(ref)
    if skipped:
        logger.warning(f"group_refs_by_date: {len(skipped)} ref(s) didn't match "
                       f"time_regex; skipped: {skipped[:3]}{'...' if len(skipped) > 3 else ''}")
    return dict(groups)


async def submit_daily_mosaic_job(args: argparse.Namespace, maap,
                                  date_key: str, refs: List[InputRef]) -> DPSJob:
    """Submit one ingest_cog DPS job that mosaics all granules from `date_key`
    into a single daily COG."""
    msg = (f"Submitting daily-mosaic COG job for {date_key} "
           f"({len(refs)} granule(s))")
    logger.info(msg)
    LoggingUtils.cmss_logger(msg, args.cmss_logger_host)

    job_params = {
        "identifier": f"Frozon-COG-Daily_{date_key}",
        "algo_id": ALGO_ID,
        "version": ALGO_VERSION,
        "queue": args.job_queue,
        "collection_id": args.collection_id,
        "s3_bucket": args.s3_bucket,
        "s3_prefix": args.s3_prefix,
        "compress": args.compress,
        "blocksize": str(args.blocksize),
        "max_memory": str(args.max_memory),
        "resampling": args.resampling,
        "overview_resampling": args.overview_resampling,
        "overwrite": "true" if args.overwrite else "false",
        "mosaic_date": date_key,
    }
    if args.role_arn:
        job_params["role_arn"] = args.role_arn
    if args.scp_host:
        job_params["scp_host"] = args.scp_host
        job_params["scp_port"] = str(args.scp_port)
        if args.scp_user:
            job_params["scp_user"] = args.scp_user
        if args.scp_remote_dir:
            job_params["scp_remote_dir"] = args.scp_remote_dir
        if args.scp_key_secret_name:
            job_params["scp_key_secret_name"] = args.scp_key_secret_name

    # All refs in a daily bucket share the same auth_kind in practice
    # (they all came from the same CMR query). Use the first ref to decide.
    auth_kind = refs[0].auth_kind
    urls = [r.url for r in refs]
    if auth_kind == "s3":
        # Pass S3 URLs as a JSON list; worker handles either one or many.
        job_params["input_s3_urls"] = json.dumps(urls)
    elif auth_kind == "https_edl":
        job_params["input_https_urls"] = json.dumps(urls)
        if args.earthdata_token_secret_name:
            job_params["earthdata_token_secret_name"] = args.earthdata_token_secret_name
    else:
        raise RuntimeError(f"Unknown auth_kind {auth_kind!r}")

    job_params = {k: v for k, v in job_params.items() if v is not None}

    logger.debug(f"Daily-mosaic job parameters: {job_params}")
    job = maap.submitJob(**job_params)

    if not job.id:
        err = MaapUtils.job_error_message(job)
        raise RuntimeError(f"Failed to submit daily-mosaic job: {err}")

    logger.info(f"Daily-mosaic job submitted ({date_key}): {job.id}")
    await wait_for_completion(job)
    logger.info(f"Daily-mosaic job completed ({date_key}): {job.id}")
    return job




def cleanup_old_cogs(s3_bucket: str, s3_prefix: str, collection_id: str,
                     retain_days: int, role_arn: Optional[str] = None) -> int:
    """Keep only the `retain_days` most recent calendar days of COGs in S3;
    delete everything else.

    COGs live at `<prefix>/<collection>/YYYY/MM/DD/<file>.tif`. We collect
    all distinct date-folders, sort newest-first, keep the top N, and
    delete objects from the rest. This is "last N days of available data",
    not a sliding time window — so sparse data never gets pruned below N
    days even if those days are spread far apart.

    Returns the number of objects deleted."""
    if retain_days <= 0:
        return 0

    safe_coll = collection_id.replace('/', '_')
    parts = []
    if s3_prefix:
        parts.append(s3_prefix.strip('/'))
    parts.append(safe_coll)
    prefix = '/'.join(parts) + '/'

    s3 = AWSUtils.get_s3_client(role_arn=role_arn, bucket_name=s3_bucket)
    paginator = s3.get_paginator('list_objects_v2')

    date_pattern = re.compile(r'/(\d{4})/(\d{2})/(\d{2})/')
    objects_by_date: dict[date, list[str]] = {}

    for page in paginator.paginate(Bucket=s3_bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            m = date_pattern.search(key)
            if m:
                d = date(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                objects_by_date.setdefault(d, []).append(key)

    if not objects_by_date:
        logger.info(f"cleanup_old_cogs: no date-folders found under s3://{s3_bucket}/{prefix}")
        return 0

    sorted_dates = sorted(objects_by_date.keys(), reverse=True)
    dates_to_keep = set(sorted_dates[:retain_days])

    to_delete: list[str] = []
    for d, keys in objects_by_date.items():
        if d not in dates_to_keep:
            to_delete.extend(keys)

    if not to_delete:
        logger.info(
            f"cleanup_old_cogs: only {len(sorted_dates)} date(s) present, "
            f"all within retain_days={retain_days}; nothing to delete"
        )
        return 0

    dates_dropped = sorted(d for d in objects_by_date if d not in dates_to_keep)
    logger.info(
        f"cleanup_old_cogs: keeping {len(dates_to_keep)} newest date(s) "
        f"({sorted(dates_to_keep)}), deleting {len(to_delete)} object(s) "
        f"from {dates_dropped}"
    )

    for i in range(0, len(to_delete), 1000):
        batch = to_delete[i:i + 1000]
        s3.delete_objects(
            Bucket=s3_bucket,
            Delete={'Objects': [{'Key': k} for k in batch]},
        )

    return len(to_delete)


async def main() -> None:
    args = parse_arguments()
    try:
        ConfigUtils.validate_arguments(args)
        input_type = ConfigUtils.detect_input_type(args)
        logger.info(f"Detected input type: {input_type}")

        maap_host = os.environ.get('MAAP_API_HOST', args.maap_host)
        maap = MaapUtils.get_maap_instance(maap_host)

        if input_type == "s3_tiff":
            input_refs = [InputRef(url=args.input_s3,
                                   name=os.path.basename(args.input_s3),
                                   auth_kind="s3")]
        elif input_type in ("s3_prefix", "cmr"):
            # CMR queries need an EDL session up front; S3 listing doesn't.
            if input_type == "cmr":
                ensure_edl_login(args.earthdata_token_secret_name, maap_instance=maap)
            source = make_source(args)
            logger.info(f"Resolving inputs from {source.description}")
            input_refs = source.list_inputs()
            # Apply the user's --filter glob to whatever the source returns.
            # CMRTiffSource doesn't filter internally (it returns every .tif
            # from each granule, which for OPERA RTC means VH, VV, AND mask),
            # so this is where we drop the polarizations the user doesn't want.
            if args.filter_pattern:
                from fnmatch import fnmatch
                before = len(input_refs)
                input_refs = [r for r in input_refs if fnmatch(r.name, args.filter_pattern)]
                logger.info(
                    f"Filtered {before} input(s) → {len(input_refs)} matching "
                    f"{args.filter_pattern!r}"
                )
            if not input_refs:
                raise RuntimeError(f"No TIFFs found via {source.description}")
        else:
            raise ValueError(
                "local_tiff input is only supported when running ingest_cog.py "
                "directly, not the orchestrator"
            )

        date_groups = group_refs_by_date(input_refs, args.time_regex)
        logger.info(
            f"Grouped {len(input_refs)} input(s) into {len(date_groups)} "
            f"daily mosaic(s): {sorted(date_groups.keys())}"
        )
        if not date_groups:
            raise RuntimeError("No inputs could be grouped by date")

        logger.info(f"Submitting {len(date_groups)} daily-mosaic job(s) "
                    f"to queue {args.job_queue}")

        cog_jobs = await asyncio.gather(*[
            submit_daily_mosaic_job(args, maap, date_key, refs)
            for date_key, refs in sorted(date_groups.items())
        ])

        catalog_products(args, maap, cog_jobs, primary_asset_keys=("asset",))

        retain = getattr(args, 'retain_days', 0) or 0
        if retain > 0:
            deleted = cleanup_old_cogs(
                args.s3_bucket, args.s3_prefix, args.collection_id,
                retain, args.role_arn,
            )
            if deleted:
                logger.info(f"Cleaned up {deleted} old COG object(s)")

        logger.info("Frozon COG pipeline completed successfully.")

    except ValueError as e:
        logger.error(f"TERMINATED: invalid argument: {e}")
        sys.exit(6)
    except RuntimeError as e:
        logger.error(f"TERMINATED: runtime error: {e}")
        sys.exit(7)
    except Exception as e:
        logger.error(f"TERMINATED: unexpected error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
