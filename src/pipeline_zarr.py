"""
Orchestrator: Frozon ISS Zarr pipeline.

Submits a single MAAP DPS worker that wraps ingest_zarr.py for an entire
batch of TIFF inputs (resolved from an S3 prefix). The worker handles
build/append/rebuild logic against the existing Zarr; this orchestrator
just dispatches, awaits, and drives STAC cataloging + the per-item
post-STAC webhook via the shared catalog_orchestration module.

Single-job-per-Zarr is intentional — concurrent merges into the same
store would race. For batches large enough to need parallelism, switch
to a fan-out + merge pattern; not needed yet.
"""

import argparse
import asyncio
import json
import logging
import os
import sys
from typing import Optional

import backoff
from maap.dps.dps_job import DPSJob

from catalog_orchestration import catalog_products
from common_utils import ConfigUtils, LoggingUtils, MaapUtils
from input_sources import ensure_edl_login, make_source

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

ALGO_ID = "frozon-iss-ingest-zarr"
ALGO_VERSION = "main"


def parse_arguments() -> argparse.Namespace:
    args = ConfigUtils.get_zarr_argument_parser().parse_args()
    logger.debug(f"Parsed arguments: {vars(args)}")
    return args


@backoff.on_exception(backoff.expo, Exception, max_value=64, max_time=172800)
async def wait_for_completion(job: DPSJob) -> DPSJob:
    await asyncio.to_thread(job.retrieve_status)
    if job.status.lower() in ["deleted", "accepted", "running"]:
        logger.debug(f"Current status is {job.status}. Backing off.")
        raise RuntimeError
    return job


async def submit_zarr_job(args: argparse.Namespace, maap,
                          input_https_urls: Optional[list] = None) -> DPSJob:
    """Submit one DPS worker that consumes the whole input batch.

    `input_https_urls` is set when --input-source-type=cmr; the worker
    downloads each URL via EDL auth. Otherwise the worker reads from
    `input_s3_prefix`.
    """
    suffix = args.collection_id[-10:]
    src_desc = args.input_s3_prefix or args.input_tiff_dir or (
        f"<{len(input_https_urls)} HTTPS URLs>" if input_https_urls else "<unset>"
    )
    msg = (
        f"Submitting Zarr worker for {src_desc} → "
        f"s3://{args.s3_bucket}/{args.s3_prefix.strip('/')}/{args.collection_id}/{args.collection_id}.zarr/"
    )
    logger.info(msg)
    LoggingUtils.cmss_logger(msg, args.cmss_logger_host)

    job_params = {
        "identifier": f"Frozon-Zarr-Pipeline_{suffix}",
        "algo_id": ALGO_ID,
        "version": ALGO_VERSION,
        "queue": args.job_queue,
        "collection_id": args.collection_id,
        "s3_bucket": args.s3_bucket,
        "s3_prefix": args.s3_prefix,
        "time_regex": args.time_regex or "",
        "chunk_size": str(args.chunk_size),
        "filter": args.filter_pattern or "",
        "exclude": args.exclude_pattern or "",
        "limit": str(args.limit) if args.limit else "",
        "allow_bounds_expansion": "true" if args.allow_bounds_expansion else "false",
        "retain_days": str(args.retain_days),
    }
    if args.role_arn:
        job_params["role_arn"] = args.role_arn
    if input_https_urls is not None:
        job_params["input_https_urls"] = json.dumps(input_https_urls)
        if args.earthdata_token_secret_name:
            job_params["earthdata_token_secret_name"] = args.earthdata_token_secret_name
    else:
        job_params["input_s3_prefix"] = args.input_s3_prefix or ""

    # Drop None values defensively (MAAP serializes them as "None").
    job_params = {k: v for k, v in job_params.items() if v is not None}

    logger.debug(f"Worker job parameters: {job_params}")
    job = maap.submitJob(**job_params)

    if not job.id:
        err = MaapUtils.job_error_message(job)
        raise RuntimeError(f"Failed to submit Zarr worker job: {err}")

    logger.info(f"Zarr worker job submitted: {job.id}")
    await wait_for_completion(job)
    logger.info(f"Zarr worker job completed: {job.id}")
    return job


async def main() -> None:
    args = parse_arguments()
    try:
        ConfigUtils.validate_arguments(args)
        source_type = (args.input_source_type or 's3').lower()

        maap_host = os.environ.get('MAAP_API_HOST', args.maap_host)
        maap = MaapUtils.get_maap_instance(maap_host)

        input_https_urls = None
        if source_type == 'cmr':
            ensure_edl_login(args.earthdata_token_secret_name, maap_instance=maap)
            source = make_source(args)
            logger.info(f"Resolving inputs from {source.description}")
            refs = source.list_inputs()
            if not refs:
                raise RuntimeError(f"No TIFFs found via {source.description}")
            # Zarr worker is batch — pass the full URL list in one shot.
            input_https_urls = [r.url for r in refs]
            logger.info(f"Resolved {len(input_https_urls)} CMR granule(s) for Zarr ingest")
        else:
            if not args.input_s3_prefix:
                raise ValueError(
                    "pipeline_zarr requires --input-s3-prefix when --input-source-type=s3; "
                    "--input-tiff-dir is for direct worker invocation only"
                )

        worker_job = await submit_zarr_job(args, maap, input_https_urls=input_https_urls)

        catalog_products(args, maap, [worker_job], primary_asset_keys=("data",))
        logger.info("Frozon Zarr pipeline completed successfully.")

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
