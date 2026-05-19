"""
Orchestrator: Frozon ISS COG pipeline.

Submits MAAP DPS jobs that wrap ingest_cog.py for one or more TIFF inputs,
waits for them, then drives STAC cataloging into MMGIS.

Modeled on czdt-iss-ingest-job/src/pipeline_generic.py.
"""

import argparse
import asyncio
import logging
import os
import sys
from typing import List, Optional

import backoff
from maap.dps.dps_job import DPSJob

from catalog_orchestration import catalog_products
from common_utils import AWSUtils, ConfigUtils, LoggingUtils, MaapUtils

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


def list_s3_tiffs(s3_prefix: str, role_arn: Optional[str],
                  filter_pattern: Optional[str], limit: Optional[int]) -> List[str]:
    """Enumerate TIFFs under an S3 prefix, optionally filtered."""
    bucket, prefix = AWSUtils.parse_s3_path(s3_prefix.rstrip('/'))
    s3 = AWSUtils.get_s3_client(role_arn=role_arn, bucket_name=bucket)
    paginator = s3.get_paginator('list_objects_v2')

    urls = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.lower().endswith(('.tif', '.tiff')):
                continue
            if filter_pattern:
                from fnmatch import fnmatch
                if not fnmatch(os.path.basename(key), filter_pattern):
                    continue
            urls.append(f"s3://{bucket}/{key}")

    urls.sort()
    if limit:
        urls = urls[:limit]
    logger.info(f"Discovered {len(urls)} TIFF input(s) under {s3_prefix}")
    return urls


async def submit_cog_job(args: argparse.Namespace, maap, input_s3_url: str) -> DPSJob:
    """Submit a single ingest_cog DPS job for one TIFF."""
    base = os.path.splitext(os.path.basename(input_s3_url))[0]
    identifier_suffix = base[-10:] if len(base) >= 10 else base

    msg = f"Submitting COG conversion job for {input_s3_url}"
    logger.info(msg)
    LoggingUtils.cmss_logger(msg, args.cmss_logger_host)

    job_params = {
        "identifier": f"Frozon-COG-Pipeline_{identifier_suffix}",
        "algo_id": ALGO_ID,
        "version": ALGO_VERSION,
        "queue": args.job_queue,
        "input_s3": input_s3_url,
        "collection_id": args.collection_id,
        "s3_bucket": args.s3_bucket,
        "s3_prefix": args.s3_prefix,
        "role_arn": args.role_arn,
        "compress": args.compress,
        "blocksize": str(args.blocksize),
        "max_memory": str(args.max_memory),
        "resampling": args.resampling,
        "overview_resampling": args.overview_resampling,
        "overwrite": "true" if args.overwrite else "false",
    }
    logger.debug(f"COG job parameters: {job_params}")
    job = maap.submitJob(**job_params)

    if not job.id:
        err = MaapUtils.job_error_message(job)
        raise RuntimeError(f"Failed to submit COG job: {err}")

    logger.info(f"COG job submitted: {job.id}")
    await wait_for_completion(job)
    logger.info(f"COG job completed: {job.id}")
    return job




async def main() -> None:
    args = parse_arguments()
    try:
        ConfigUtils.validate_arguments(args)
        input_type = ConfigUtils.detect_input_type(args)
        logger.info(f"Detected input type: {input_type}")

        maap_host = os.environ.get('MAAP_API_HOST', args.maap_host)
        maap = MaapUtils.get_maap_instance(maap_host)

        if input_type == "s3_tiff":
            inputs = [args.input_s3]
        elif input_type == "s3_prefix":
            inputs = list_s3_tiffs(
                args.input_s3_prefix, args.role_arn,
                args.filter_pattern, args.limit,
            )
            if not inputs:
                raise RuntimeError(f"No TIFFs found under {args.input_s3_prefix}")
        else:
            raise ValueError(
                "local_tiff input is only supported when running ingest_cog.py "
                "directly, not the orchestrator"
            )

        logger.info(f"Submitting {len(inputs)} COG job(s) to queue {args.job_queue}")

        cog_jobs = await asyncio.gather(*[
            submit_cog_job(args, maap, url) for url in inputs
        ])

        catalog_products(args, maap, cog_jobs, primary_asset_keys=("asset",))
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
