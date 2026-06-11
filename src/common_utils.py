"""
Common utility functions for the Frozon ISS Ingest Job pipeline.

Provides AWS S3, MAAP, logging, and argument-parsing helpers shared across
pipeline scripts. Modeled on czdt-iss-ingest-job/src/common_utils.py and
trimmed to what the COG pipeline currently needs.
"""

import argparse
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests
import backoff
from botocore.exceptions import ClientError, NoCredentialsError
from maap.maap import MAAP


class AWSUtils:
    """AWS S3 helpers."""

    @staticmethod
    def get_bucket_region(bucket_name: str, s3_client=None) -> Optional[str]:
        if not s3_client:
            s3_client = boto3.client('s3')
        try:
            response = s3_client.head_bucket(Bucket=bucket_name)
            region = response['ResponseMetadata']['HTTPHeaders'].get('x-amz-bucket-region')
            if region:
                logging.debug(f"Detected bucket {bucket_name} in region: {region}")
                return region
        except ClientError as e:
            logging.warning(f"Failed to detect region for bucket {bucket_name}: {e}")
        return None

    @staticmethod
    def get_s3_client(role_arn: str = None, aws_region: str = None, bucket_name: str = None):
        if not aws_region and bucket_name:
            aws_region = AWSUtils.get_bucket_region(bucket_name)
            if aws_region:
                logging.info(f"Auto-detected region {aws_region} for bucket {bucket_name}")

        if role_arn:
            sts_client = boto3.client('sts')
            assumed_role = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=f"frozon-iss-session-{os.getpid()}"
            )
            credentials = assumed_role['Credentials']
            return boto3.client(
                's3',
                region_name=aws_region,
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken']
            )
        return boto3.client('s3', region_name=aws_region)

    @staticmethod
    def parse_s3_path(s3_path: str) -> Tuple[str, str]:
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Invalid S3 path format: {s3_path}")
        path_without_prefix = s3_path[5:]
        if '/' not in path_without_prefix:
            return path_without_prefix, ""
        bucket, key = path_without_prefix.split('/', 1)
        return bucket, key

    @staticmethod
    def upload_to_s3(file_path: str, bucket: str, key: str, s3_client=None, role_arn: str = None) -> str:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        if not s3_client:
            s3_client = AWSUtils.get_s3_client(role_arn=role_arn, bucket_name=bucket)
        logging.info(f"Uploading {file_path} to s3://{bucket}/{key}")
        s3_client.upload_file(file_path, bucket, key)
        s3_url = f"s3://{bucket}/{key}"
        logging.info(f"Successfully uploaded to {s3_url}")
        return s3_url

    @staticmethod
    def download_s3_file(bucket: str, key: str, local_path: str, s3_client=None, role_arn: str = None) -> str:
        if not s3_client:
            s3_client = AWSUtils.get_s3_client(role_arn=role_arn, bucket_name=bucket)
        os.makedirs(os.path.dirname(local_path) or '.', exist_ok=True)
        logging.info(f"Downloading s3://{bucket}/{key} to {local_path}")
        s3_client.download_file(bucket, key, local_path)
        return local_path

    @staticmethod
    def file_exists_in_s3(bucket: str, key: str, s3_client=None) -> bool:
        if not s3_client:
            s3_client = boto3.client('s3')
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            raise

    @staticmethod
    def convert_s3_http_to_s3_uri(http_s3_link: str) -> Optional[str]:
        path_style = re.match(r"https?://s3\.amazonaws\.com/([^/]+)/(.*)", http_s3_link)
        if path_style:
            return f"s3://{path_style.group(1)}/{path_style.group(2)}"
        virtual = re.match(r"https?://([^.]+)\.s3\.amazonaws\.com/(.*)", http_s3_link)
        if virtual:
            return f"s3://{virtual.group(1)}/{virtual.group(2)}"
        return None


class MaapUtils:
    """MAAP DPS helpers."""

    @staticmethod
    @backoff.on_exception(backoff.expo, RuntimeError, max_value=64, max_time=172800)
    def get_maap_instance(maap_host_url: str = "api.maap-project.org") -> MAAP:
        try:
            logging.info(f"Initializing MAAP client for host: {maap_host_url}")
            client = MAAP(maap_host=maap_host_url)
            logging.info("MAAP client initialized successfully.")
            return client
        except Exception as e:
            logging.error(f"Failed to initialize MAAP instance for '{maap_host_url}': {e}", exc_info=True)
            raise RuntimeError(f"Could not initialize MAAP instance: {e}")

    @staticmethod
    def job_error_message(job) -> str:
        if isinstance(job.error_details, str):
            try:
                return json.loads(job.error_details)["message"]
            except (json.JSONDecodeError, KeyError):
                return job.error_details
        return job.response_code or "Unknown error"

    @staticmethod
    def get_job_id() -> str:
        if os.path.exists("_job.json"):
            with open("_job.json", 'r') as fr:
                job_info = json.load(fr).get("job_info", {})
                return job_info.get("job_payload", {}).get("payload_task_id", "")
        return ""

    @staticmethod
    def get_dps_output(jobs: List, file_ext: str, prefixes_only: bool = False) -> List[str]:
        s3 = boto3.resource('s3')
        output = set()
        job_outputs = [
            next((path for path in j.retrieve_result() if path.startswith("s3")), None)
            for j in jobs
        ]
        for job_output in job_outputs:
            if job_output is None:
                continue
            bucket_name, path = AWSUtils.parse_s3_path(job_output)
            dps_bucket = s3.Bucket(bucket_name)
            for obj in dps_bucket.objects.filter(Prefix=path):
                if prefixes_only:
                    folder_prefix = os.path.dirname(obj.key)
                    if folder_prefix.endswith(file_ext):
                        output.add(f"s3://{bucket_name}/{folder_prefix}")
                else:
                    if obj.key.endswith(file_ext):
                        output.add(f"s3://{bucket_name}/{obj.key}")
        return list(output)


class LoggingUtils:
    """CMSS log + product-availability notifications."""

    @staticmethod
    def cmss_logger(message: str, host: str, token: str = None) -> None:
        if not host:
            return
        try:
            url = f"{host}/log"
            body = {"level": "info", "msg_body": str(message)}
            response = requests.post(url, json=body, timeout=10)
            if response.status_code != 200:
                logging.warning(f"CMSS logging failed with status {response.status_code}")
        except Exception as e:
            logging.warning(f"Failed to send log to CMSS: {e}")

    @staticmethod
    def cmss_product_available(product_info: Dict[str, Any], host: str, token: str = None) -> None:
        if not host:
            return
        try:
            headers = {'Content-Type': 'application/json'}
            if token:
                headers['Authorization'] = f'Bearer {token}'
            response = requests.post(f"{host}/product", json=product_info, headers=headers, timeout=10)
            if response.status_code != 200:
                logging.warning(f"CMSS product notification failed with status {response.status_code}")
        except Exception as e:
            logging.warning(f"Failed to notify CMSS of product availability: {e}")

    @staticmethod
    def post_stac_webhook(
        webhook_url: Optional[str],
        collection_id: str,
        item_id: str,
        asset_uri: str,
        token: Optional[str] = None,
        extra: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Stub: poke an external webservice after a STAC item is upserted,
        prompting it to download the COG from S3.

        No-op when `webhook_url` is empty/None — that's the default. Failures
        are logged and swallowed so a flaky webhook doesn't fail the
        pipeline. Returns True on 2xx.

        TODO(integration): swap the JSON shape to whatever the receiving
        service actually expects (HMAC signing, retry policy, async fan-out).
        """
        if not webhook_url:
            return False

        payload: Dict[str, Any] = {
            "event": "stac_item_upserted",
            "collection_id": collection_id,
            "item_id": item_id,
            "asset_uri": asset_uri,
        }
        if extra:
            payload.update(extra)

        try:
            headers = {"Content-Type": "application/json"}
            if token:
                headers["Authorization"] = f"Bearer {token}"
            response = requests.post(webhook_url, json=payload, headers=headers, timeout=10)
            if 200 <= response.status_code < 300:
                logging.info(
                    f"Webhook posted: {item_id} → {webhook_url} "
                    f"(status {response.status_code})"
                )
                return True
            logging.warning(
                f"Webhook to {webhook_url} returned non-2xx: "
                f"{response.status_code} {response.text[:200]}"
            )
            return False
        except Exception as e:
            logging.warning(f"Webhook to {webhook_url} failed: {e}")
            return False


class ConfigUtils:
    """Argument parsing for the COG pipeline."""

    @staticmethod
    def get_cog_argument_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Frozon ISS COG pipeline — converts TIFF inputs to Cloud Optimized GeoTIFFs."
        )

        input_group = parser.add_mutually_exclusive_group(required=False)
        input_group.add_argument("--input-s3",
                                 help="S3 URL of a single TIFF input")
        input_group.add_argument("--input-s3-prefix",
                                 help="S3 prefix containing TIFF inputs (each gets its own DPS job)")
        input_group.add_argument("--input-tiff",
                                 help="Local TIFF file path (used in localized/test mode)")

        parser.add_argument("--input-source-type", default="s3",
                            choices=["s3", "cmr"],
                            help="How to resolve inputs: 's3' lists --input-s3-prefix; "
                                 "'cmr' searches NASA CMR via --cmr-* args.")
        parser.add_argument("--cmr-short-name", default=None,
                            help="CMR collection short_name (required when --input-source-type=cmr)")
        parser.add_argument("--cmr-version", default=None,
                            help="CMR collection version. Optional.")
        parser.add_argument("--cmr-temporal-start", default=None,
                            help="ISO date or datetime, inclusive lower bound (UTC).")
        parser.add_argument("--cmr-temporal-end", default=None,
                            help="ISO date or datetime, inclusive upper bound (UTC).")
        parser.add_argument("--cmr-bbox", default=None,
                            help="Bounding box as 'west,south,east,north' (degrees).")
        parser.add_argument("--cmr-granule-ids", default=None,
                            help="Comma-separated list of specific granule UR identifiers, "
                                 "overrides temporal/bbox.")
        parser.add_argument("--cmr-prefer-https",
                            action=argparse.BooleanOptionalAction, default=True,
                            help="When True, request HTTPS granule URLs (require EDL token); "
                                 "when False, prefer in-region s3:// links. Default: HTTPS.")
        parser.add_argument("--earthdata-token-secret-name", default=None,
                            help="MAAP secret name holding an EDL bearer token (or "
                                 "username\\npassword). Required when fetching HTTPS+EDL granules.")

        parser.add_argument("--collection-id", required=True,
                            help="STAC collection ID for the products")
        parser.add_argument("--s3-bucket", required=True,
                            help="Target S3 bucket for COG outputs")
        parser.add_argument("--s3-prefix", default="",
                            help="Optional S3 prefix within the output bucket")
        parser.add_argument("--role-arn",
                            help="AWS IAM Role ARN to assume for S3 read/write. "
                                 "Omit to use the worker's default credential chain.")
        parser.add_argument("--cmss-logger-host",
                            help="Host for logging pipeline messages")
        parser.add_argument("--mmgis-host",
                            help="Host for cataloging STAC items")
        parser.add_argument("--titiler-token-secret-name",
                            help="MAAP secret name for MMGIS host token")
        parser.add_argument("--job-queue", required=True,
                            help="MAAP DPS queue name for the COG ingest jobs")
        parser.add_argument("--maap-host", default="api.maap-project.org",
                            help="MAAP API host")

        parser.add_argument("--compress", default="DEFLATE",
                            choices=["DEFLATE", "LZW", "JPEG", "WEBP", "NONE"],
                            help="COG compression method (default DEFLATE)")
        parser.add_argument("--blocksize", type=int, default=512, choices=[256, 512, 1024],
                            help="COG tile block size (default 512)")
        parser.add_argument("--max-memory", type=int, default=512,
                            help="GDAL_CACHEMAX in MB for the conversion subprocess (default 512)")
        parser.add_argument("--resampling", default="nearest",
                            choices=["nearest", "bilinear", "cubic", "average"],
                            help="Resampling method for base data")
        parser.add_argument("--overview-resampling", default="average",
                            choices=["nearest", "average", "cubic", "mode"],
                            help="Resampling method for overviews")
        parser.add_argument("--overwrite", action="store_true",
                            help="Overwrite existing COG outputs in S3")
        parser.add_argument("--upsert", action=argparse.BooleanOptionalAction, default=True,
                            help="Upsert items into existing STAC collections instead of "
                                 "failing on conflict. Default: on. Use --no-upsert to opt out.")
        parser.add_argument("--post-stac-webhook-url", default=None,
                            help="Optional URL to POST after each STAC item upsert. "
                                 "The receiver is expected to fetch the COG from S3. "
                                 "Empty = no webhook (default).")
        parser.add_argument("--post-stac-webhook-token-secret-name", default=None,
                            help="Optional MAAP secret name holding a bearer token "
                                 "for the post-STAC webhook.")
        parser.add_argument("--filter", dest="filter_pattern",
                            help="Glob pattern for input filtering when using --input-s3-prefix")
        parser.add_argument("--limit", type=int,
                            help="Max number of files to process when using --input-s3-prefix")
        parser.add_argument("--local-download-path", default="output",
                            help="Local working directory for downloads/outputs")

        parser.add_argument("--time-regex", default=None,
                            help="Regex with named group 'start_date' (or first "
                                 "capture) used to extract YYYYMMDD from input "
                                 "filenames so the orchestrator can group inputs "
                                 "into per-date mosaics. e.g. "
                                 r"_(?P<start_date>\d{8}T\d{6})Z_")

        parser.add_argument("--max-acquisition-date", default=None,
                            help="If set, the orchestrator drops any date bucket "
                                 "whose key is strictly after this YYYY-MM-DD "
                                 "(or YYYYMMDD) date before submitting workers. "
                                 "Used by the cron to prevent partial-day mosaics "
                                 "of granules acquired after the safe-landing target.")

        parser.add_argument("--mosaic-last-n-complete-days", type=int, default=0,
                            help="If > 0, after grouping by date the orchestrator "
                                 "drops the single newest date (assumed potentially "
                                 "incomplete) and keeps only the next N most recent "
                                 "dates. Lets the cron self-discover the safe-landing "
                                 "boundary instead of betting on a fixed offset.")

        parser.add_argument("--retain-days", type=int, default=0,
                            help="If > 0, keep only the N most recent calendar "
                                 "days of COG outputs in S3; delete the rest. "
                                 "Counts distinct date-folders, not a time window.")

        parser.add_argument("--scp-host", default=None,
                            help="Optional: SCP delivery hostname/IP. When set, "
                                 "the worker also pushes each COG via SCP after "
                                 "S3 upload.")
        parser.add_argument("--scp-port", type=int, default=22)
        parser.add_argument("--scp-user", default=None)
        parser.add_argument("--scp-remote-dir", default=None,
                            help="Remote directory; the COG is written as "
                                 "<remote-dir>/<filename> (path flattened).")
        parser.add_argument("--scp-key-secret-name", default=None,
                            help="MAAP secret name holding the SSH private key (PEM).")

        return parser

    @staticmethod
    def get_zarr_argument_parser() -> argparse.ArgumentParser:
        parser = argparse.ArgumentParser(
            description="Frozon ISS Zarr pipeline — builds/upserts a sparse Zarr "
                        "time series from TIFF inputs."
        )

        input_group = parser.add_mutually_exclusive_group(required=False)
        input_group.add_argument("--input-s3-prefix",
                                 help="S3 prefix containing TIFF inputs to merge")
        input_group.add_argument("--input-tiff-dir",
                                 help="Local directory of TIFFs (worker/test mode only)")
        input_group.add_argument("--input-https-urls", default=None,
                                 help="JSON list of HTTPS+EDL URLs to download "
                                      "(populated by the orchestrator from CMR search).")

        parser.add_argument("--input-source-type", default="s3",
                            choices=["s3", "cmr"],
                            help="How to resolve inputs: 's3' lists --input-s3-prefix; "
                                 "'cmr' searches NASA CMR via --cmr-* args.")
        parser.add_argument("--cmr-short-name", default=None,
                            help="CMR collection short_name (required when --input-source-type=cmr)")
        parser.add_argument("--cmr-version", default=None,
                            help="CMR collection version. Optional.")
        parser.add_argument("--cmr-temporal-start", default=None,
                            help="ISO date or datetime, inclusive lower bound (UTC).")
        parser.add_argument("--cmr-temporal-end", default=None,
                            help="ISO date or datetime, inclusive upper bound (UTC).")
        parser.add_argument("--cmr-bbox", default=None,
                            help="Bounding box as 'west,south,east,north' (degrees).")
        parser.add_argument("--cmr-granule-ids", default=None,
                            help="Comma-separated CMR granule UR identifiers.")
        parser.add_argument("--cmr-prefer-https",
                            action=argparse.BooleanOptionalAction, default=True,
                            help="Prefer HTTPS granule URLs (require EDL token).")
        parser.add_argument("--earthdata-token-secret-name", default=None,
                            help="MAAP secret name holding EDL bearer token (or "
                                 "username\\npassword). Required for CMR/EDL.")

        parser.add_argument("--collection-id", required=True,
                            help="STAC collection ID; also names the output Zarr "
                                 "(<collection-id>.zarr)")
        parser.add_argument("--s3-bucket", required=True,
                            help="Target S3 bucket for the Zarr output")
        parser.add_argument("--s3-prefix", default="",
                            help="Optional S3 prefix within the output bucket")
        parser.add_argument("--role-arn",
                            help="AWS IAM Role ARN to assume for S3 read/write. "
                                 "Omit to use the worker's default credential chain.")
        parser.add_argument("--cmss-logger-host",
                            help="Host for logging pipeline messages")
        parser.add_argument("--mmgis-host",
                            help="Host for cataloging STAC items")
        parser.add_argument("--titiler-token-secret-name",
                            help="MAAP secret name for MMGIS host token")
        parser.add_argument("--job-queue", required=True,
                            help="MAAP DPS queue name for the worker job")
        parser.add_argument("--maap-host", default="api.maap-project.org",
                            help="MAAP API host")

        parser.add_argument("--time-regex", default=None,
                            help="Regex with 'start_date' named group (or first capture) "
                                 "to extract datetime from TIFF filenames. Falls back to "
                                 "the streaming script's built-in patterns.")
        parser.add_argument("--chunk-size", type=int, default=1024,
                            help="Zarr spatial chunk size (default 1024)")
        parser.add_argument("--filter", dest="filter_pattern",
                            help='Glob filter applied to input filenames (e.g. "*ssha*.tif")')
        parser.add_argument("--exclude", dest="exclude_pattern",
                            help='Glob exclusion applied to input filenames')
        parser.add_argument("--limit", type=int,
                            help="Cap number of new TIFFs (testing)")
        parser.add_argument("--allow-bounds-expansion",
                            action=argparse.BooleanOptionalAction, default=True,
                            help="When new inputs extend beyond the existing Zarr's "
                                 "grid, dump existing slices and rebuild on the union "
                                 "grid. Default on. --no-allow-bounds-expansion makes "
                                 "out-of-bounds inputs a fatal error.")
        parser.add_argument("--local-download-path", default="output",
                            help="Local working directory")

        parser.add_argument("--upsert", action=argparse.BooleanOptionalAction, default=True,
                            help="Upsert STAC items instead of failing on conflict. "
                                 "Default on.")
        parser.add_argument("--post-stac-webhook-url", default=None,
                            help="Optional URL POSTed after STAC item upsert.")
        parser.add_argument("--post-stac-webhook-token-secret-name", default=None,
                            help="Optional MAAP secret name for the webhook bearer token.")

        parser.add_argument("--retain-days", type=int, default=0,
                            help="If > 0, keep only the N most recent calendar "
                                 "days of Zarr time slices; drop the rest. "
                                 "Counts distinct dates in the store, not a "
                                 "time window. 0 disables pruning.")

        return parser

    @staticmethod
    def validate_arguments(args: argparse.Namespace) -> None:
        s3 = getattr(args, 'input_s3', None)
        prefix = getattr(args, 'input_s3_prefix', None)
        if s3 and not s3.startswith('s3://'):
            raise ValueError(f"Invalid S3 URL: {s3}")
        if prefix and not prefix.startswith('s3://'):
            raise ValueError(f"Invalid S3 prefix: {prefix}")
        if s3 and not s3.lower().endswith(('.tif', '.tiff')):
            raise ValueError(f"Input must be a .tif/.tiff file: {s3}")

    @staticmethod
    def detect_input_type(args: argparse.Namespace) -> str:
        if (getattr(args, 'input_source_type', None) or '').lower() == 'cmr':
            return "cmr"
        if getattr(args, 'input_s3', None):
            return "s3_tiff"
        if getattr(args, 'input_s3_prefix', None):
            return "s3_prefix"
        if getattr(args, 'input_tiff', None):
            return "local_tiff"
        if getattr(args, 'input_tiff_dir', None):
            return "local_tiff_dir"
        raise ValueError("No valid input provided")
