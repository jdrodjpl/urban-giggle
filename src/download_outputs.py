#!/usr/bin/env python3
"""
Daily downloader for Frozon ISS pipeline outputs (COGs + Zarr).

Runs on a built-in 24-hour schedule. Tracks previously-downloaded objects
in a local JSON manifest so only new or modified files are fetched.

Usage:
    python src/download_outputs.py \
        --s3-bucket my-bucket \
        --collection-id frozon-test-s1 \
        --local-dir ./downloads \
        --run-hour 2

    # One-shot (no scheduling):
    python src/download_outputs.py \
        --s3-bucket my-bucket \
        --collection-id frozon-test-s1 \
        --local-dir ./downloads \
        --once
"""

import argparse
import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


MANIFEST_FILENAME = ".download_manifest.json"


def get_s3_client(role_arn: Optional[str] = None,
                  bucket_name: Optional[str] = None):
    region = None
    if bucket_name:
        try:
            resp = boto3.client("s3").head_bucket(Bucket=bucket_name)
            region = resp["ResponseMetadata"]["HTTPHeaders"].get(
                "x-amz-bucket-region"
            )
        except ClientError:
            pass

    if role_arn:
        creds = boto3.client("sts").assume_role(
            RoleArn=role_arn,
            RoleSessionName=f"frozon-download-{os.getpid()}",
        )["Credentials"]
        return boto3.client(
            "s3",
            region_name=region,
            aws_access_key_id=creds["AccessKeyId"],
            aws_secret_access_key=creds["SecretAccessKey"],
            aws_session_token=creds["SessionToken"],
        )
    return boto3.client("s3", region_name=region)


def list_objects(s3, bucket: str, prefix: str) -> List[Dict]:
    """Paginated listing. Returns dicts with Key, Size, LastModified, ETag."""
    paginator = s3.get_paginator("list_objects_v2")
    results = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith("/"):
                continue
            results.append({
                "Key": obj["Key"],
                "Size": obj["Size"],
                "LastModified": obj["LastModified"].isoformat(),
                "ETag": obj["ETag"],
            })
    return results


def load_manifest(manifest_path: Path) -> Dict[str, str]:
    """Returns {s3_key: etag} for previously downloaded objects."""
    if manifest_path.exists():
        return json.loads(manifest_path.read_text())
    return {}


def save_manifest(manifest_path: Path, manifest: Dict[str, str]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def build_cog_prefix(s3_prefix: str, collection_id: str) -> str:
    parts = [p for p in [s3_prefix.strip("/")] if p]
    parts.append(collection_id)
    return "/".join(parts) + "/"


def build_zarr_prefix(s3_prefix: str, collection_id: str) -> str:
    parts = [p for p in [s3_prefix.strip("/")] if p]
    parts.extend([collection_id, f"{collection_id}.zarr"])
    return "/".join(parts) + "/"


def sync_prefix(s3, bucket: str, prefix: str, local_dir: Path,
                manifest: Dict[str, str], label: str) -> int:
    """Download new/changed objects under `prefix`. Returns count of files fetched."""
    objects = list_objects(s3, bucket, prefix)
    fetched = 0
    for obj in objects:
        key = obj["Key"]
        etag = obj["ETag"]
        if manifest.get(key) == etag:
            continue
        rel = key[len(prefix):]
        if not rel:
            continue
        local_path = local_dir / rel
        local_path.parent.mkdir(parents=True, exist_ok=True)
        log.info(f"[{label}] Downloading s3://{bucket}/{key}")
        s3.download_file(bucket, key, str(local_path))
        manifest[key] = etag
        fetched += 1
    return fetched


def run_sync(args: argparse.Namespace) -> None:
    """Execute one sync cycle for both COG and Zarr outputs."""
    local_dir = Path(args.local_dir)
    manifest_path = local_dir / MANIFEST_FILENAME
    manifest = load_manifest(manifest_path)

    s3 = get_s3_client(role_arn=args.role_arn, bucket_name=args.s3_bucket)

    total = 0
    for cid in args.collection_id:
        cog_prefix = build_cog_prefix(args.s3_prefix, cid)
        cog_dir = local_dir / "cog" / cid
        count = sync_prefix(s3, args.s3_bucket, cog_prefix, cog_dir,
                            manifest, f"COG/{cid}")
        total += count
        log.info(f"COG/{cid}: {count} new file(s)")

        zarr_prefix = build_zarr_prefix(args.s3_prefix, cid)
        zarr_dir = local_dir / "zarr" / cid / f"{cid}.zarr"
        count = sync_prefix(s3, args.s3_bucket, zarr_prefix, zarr_dir,
                            manifest, f"Zarr/{cid}")
        total += count
        log.info(f"Zarr/{cid}: {count} new file(s)")

    save_manifest(manifest_path, manifest)
    log.info(f"Sync complete — {total} file(s) downloaded")


def seconds_until_hour(target_hour: int) -> float:
    """Seconds from now until the next occurrence of `target_hour` (UTC)."""
    now = datetime.now(timezone.utc)
    target = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target = target.replace(day=target.day + 1)
    return (target - now).total_seconds()


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily incremental downloader for Frozon pipeline outputs."
    )
    parser.add_argument("--s3-bucket", required=True,
                        help="S3 bucket where pipeline outputs live")
    parser.add_argument("--s3-prefix", default="",
                        help="Optional S3 prefix within the bucket")
    parser.add_argument("--collection-id", required=True, nargs="+",
                        help="One or more collection IDs to download")
    parser.add_argument("--role-arn", default=None,
                        help="AWS IAM Role ARN to assume (omit for default creds)")
    parser.add_argument("--local-dir", default="./downloads",
                        help="Local root directory for downloaded files (default: ./downloads)")
    parser.add_argument("--run-hour", type=int, default=2,
                        help="UTC hour to run the daily sync (0-23, default: 2)")
    parser.add_argument("--once", action="store_true",
                        help="Run a single sync and exit (no scheduling)")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()

    if args.once:
        run_sync(args)
        return

    shutdown = False

    def handle_signal(signum, _frame):
        nonlocal shutdown
        log.info(f"Received signal {signum}, shutting down after current cycle…")
        shutdown = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    log.info(f"Scheduler started — daily sync at {args.run_hour:02d}:00 UTC")

    # Run immediately on startup, then schedule subsequent runs.
    run_sync(args)

    while not shutdown:
        wait = seconds_until_hour(args.run_hour)
        log.info(f"Next sync in {wait / 3600:.1f} hours")

        slept = 0.0
        while slept < wait and not shutdown:
            chunk = min(wait - slept, 60.0)
            time.sleep(chunk)
            slept += chunk

        if not shutdown:
            run_sync(args)


if __name__ == "__main__":
    main()
