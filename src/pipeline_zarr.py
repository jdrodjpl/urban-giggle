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
import logging
import os
import sys
from typing import Optional

import backoff
from maap.dps.dps_job import DPSJob

from catalog_orchestration import catalog_products
from common_utils import ConfigUtils, LoggingUtils, MaapUtils

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


async def submit_zarr_job(args: argparse.Namespace, maap) -> DPSJob:
    """Submit one DPS worker that consumes the whole input batch."""
    suffix = args.collection_id[-10:]
    msg = (
        f"Submitting Zarr worker for {args.input_s3_prefix or args.input_tiff_dir} "
        f"→ s3://{args.s3_bucket}/{args.s3_prefix.strip('/')}/{args.collection_id}/{args.collection_id}.zarr/"
    )
    logger.info(msg)
    LoggingUtils.cmss_logger(msg, args.cmss_logger_host)

    job_params = {
        "identifier": f"Frozon-Zarr-Pipeline_{suffix}",
        "algo_id": ALGO_ID,
        "version": ALGO_VERSION,
        "queue": args.job_queue,
        "input_s3_prefix": args.input_s3_prefix or "",
        "collection_id": args.collection_id,
        "s3_bucket": args.s3_bucket,
        "s3_prefix": args.s3_prefix,
        "role_arn": args.role_arn,
        "time_regex": args.time_regex or "",
        "chunk_size": str(args.chunk_size),
        "filter": args.filter_pattern or "",
        "exclude": args.exclude_pattern or "",
        "limit": str(args.limit) if args.limit else "",
        "allow_bounds_expansion": "true" if args.allow_bounds_expansion else "false",
    }
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
        if not args.input_s3_prefix:
            raise ValueError(
                "pipeline_zarr requires --input-s3-prefix; "
                "--input-tiff-dir is for direct worker invocation only"
            )

        maap_host = os.environ.get('MAAP_API_HOST', args.maap_host)
        maap = MaapUtils.get_maap_instance(maap_host)

        worker_job = await submit_zarr_job(args, maap)

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
