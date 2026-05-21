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
import backoff
from maap.dps.dps_job import DPSJob

from catalog_orchestration import catalog_products
from common_utils import ConfigUtils, LoggingUtils, MaapUtils
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


async def submit_cog_job(args: argparse.Namespace, maap, input_ref: InputRef) -> DPSJob:
    """Submit a single ingest_cog DPS job for one input."""
    base = os.path.splitext(input_ref.name)[0]
    identifier_suffix = base[-10:] if len(base) >= 10 else base

    msg = f"Submitting COG conversion job for {input_ref.url}"
    logger.info(msg)
    LoggingUtils.cmss_logger(msg, args.cmss_logger_host)

    job_params = {
        "identifier": f"Frozon-COG-Pipeline_{identifier_suffix}",
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
    if input_ref.auth_kind == "s3":
        job_params["input_s3"] = input_ref.url
    elif input_ref.auth_kind == "https_edl":
        job_params["input_https"] = input_ref.url
        if args.earthdata_token_secret_name:
            job_params["earthdata_token_secret_name"] = args.earthdata_token_secret_name
    else:
        raise RuntimeError(f"Unknown auth_kind {input_ref.auth_kind!r} for {input_ref.url}")

    # MAAP serializes None as the string "None" in _job.json which breaks the
    # worker's argparse. Drop None values defensively in case anything else
    # leaks through.
    job_params = {k: v for k, v in job_params.items() if v is not None}

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
            if not input_refs:
                raise RuntimeError(f"No TIFFs found via {source.description}")
        else:
            raise ValueError(
                "local_tiff input is only supported when running ingest_cog.py "
                "directly, not the orchestrator"
            )

        logger.info(f"Submitting {len(input_refs)} COG job(s) to queue {args.job_queue}")

        cog_jobs = await asyncio.gather(*[
            submit_cog_job(args, maap, ref) for ref in input_refs
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
