#!/usr/bin/env python3
"""
Daily downloader for Frozon ISS pipeline outputs (COGs + Zarr).

Uses maap-py to broker AWS credentials for S3 access, so this script can
run outside the MAAP worker environment (local machine, external server).

Driven by a JSON config file that defines multiple sync sources, each with
its own bucket, prefix, collection ID, and output types.

When mmgis_host and mmgis_token_secret_name are set in the config, newly
synced files are cataloged into MMGIS as STAC items automatically.

Usage:
    # One-shot sync:
    python src/download_outputs.py --config sync-config.json --once

    # Daily scheduled (default 02:00 UTC):
    python src/download_outputs.py --config sync-config.json

    # Override schedule from CLI:
    python src/download_outputs.py --config sync-config.json --run-hour 6
"""

import argparse
import json
import logging
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Set

import boto3
import numpy as np
import pystac
import requests
import rio_stac
import zarr
from maap.maap import MAAP

import create_stac_items

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger(__name__)


MANIFEST_FILENAME = ".download_manifest.json"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

def load_config(config_path: str) -> dict:
    with open(config_path) as f:
        cfg = json.load(f)
    for i, src in enumerate(cfg.get("sources", [])):
        label = src.get("name", f"source {i}")
        if not src.get("s3_bucket"):
            raise ValueError(f"{label}: missing s3_bucket")
        if not src.get("collection_id"):
            raise ValueError(f"{label}: missing collection_id")
        if not src.get("sync"):
            raise ValueError(f"{label}: missing sync (must be list of 'cog' and/or 'zarr')")
        for kind in src["sync"]:
            if kind not in ("cog", "zarr"):
                raise ValueError(f"{label}: invalid sync type '{kind}'")
        if src.get("stac_update"):
            if not cfg.get("mmgis_host"):
                raise ValueError(f"{label}: stac_update requires mmgis_host in config")
            if not cfg.get("mmgis_token_secret_name"):
                raise ValueError(f"{label}: stac_update requires mmgis_token_secret_name in config")
    return cfg


# ---------------------------------------------------------------------------
# AWS / S3
# ---------------------------------------------------------------------------

def get_s3_client_from_maap(maap: MAAP) -> "boto3.client":
    log.info("Requesting AWS credentials from MAAP…")
    creds = maap.aws.requester_pays_credentials()
    return boto3.client(
        "s3",
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
        aws_session_token=creds["aws_session_token"],
    )


def list_objects(s3, bucket: str, prefix: str) -> List[Dict]:
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


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------

def load_manifest(manifest_path: Path) -> Dict[str, str]:
    if manifest_path.exists():
        return json.loads(manifest_path.read_text())
    return {}


def save_manifest(manifest_path: Path, manifest: Dict[str, str]) -> None:
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))


# ---------------------------------------------------------------------------
# S3 prefix builders
# ---------------------------------------------------------------------------

def build_cog_prefix(s3_prefix: str, collection_id: str) -> str:
    parts = [p for p in [s3_prefix.strip("/")] if p]
    parts.append(collection_id)
    return "/".join(parts) + "/"


def build_zarr_prefix(s3_prefix: str, collection_id: str) -> str:
    parts = [p for p in [s3_prefix.strip("/")] if p]
    parts.extend([collection_id, f"{collection_id}.zarr"])
    return "/".join(parts) + "/"


# ---------------------------------------------------------------------------
# Sync
# ---------------------------------------------------------------------------

class SyncResult:
    __slots__ = ("fetched", "pruned", "new_keys", "pruned_keys")

    def __init__(self):
        self.fetched: int = 0
        self.pruned: int = 0
        self.new_keys: Set[str] = set()
        self.pruned_keys: Set[str] = set()


def sync_prefix(s3, bucket: str, prefix: str, local_dir: Path,
                manifest: Dict[str, str], label: str) -> SyncResult:
    """Sync local_dir to match S3 prefix exactly (rsync --delete semantics)."""
    result = SyncResult()
    objects = list_objects(s3, bucket, prefix)
    remote_keys = {obj["Key"] for obj in objects}

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
        result.new_keys.add(key)
        result.fetched += 1

    if local_dir.exists():
        for local_path in sorted(local_dir.rglob("*")):
            if not local_path.is_file():
                continue
            rel = local_path.relative_to(local_dir).as_posix()
            key = prefix + rel
            if key not in remote_keys:
                log.info(f"[{label}] Pruning {local_path} (gone from S3)")
                local_path.unlink()
                manifest.pop(key, None)
                result.pruned_keys.add(key)
                result.pruned += 1

        for dirpath in sorted(local_dir.rglob("*"), reverse=True):
            if dirpath.is_dir() and not any(dirpath.iterdir()):
                dirpath.rmdir()

    stale = [k for k in manifest if k.startswith(prefix) and k not in remote_keys]
    for k in stale:
        del manifest[k]

    return result


# ---------------------------------------------------------------------------
# STAC catalog helpers
# ---------------------------------------------------------------------------

def build_cog_stac_items(new_keys: Set[str], bucket: str,
                         cog_prefix: str, cog_dir: Path,
                         collection_id: str) -> List[pystac.Item]:
    """Build STAC items for newly downloaded COGs using rio_stac."""
    items = []
    for key in sorted(new_keys):
        if not key.lower().endswith((".tif", ".tiff")):
            continue
        rel = key[len(cog_prefix):]
        local_path = cog_dir / rel
        if not local_path.exists():
            continue
        try:
            item = rio_stac.create_stac_item(
                source=str(local_path),
                id=local_path.stem,
                collection=collection_id,
                asset_name="asset",
                asset_media_type=pystac.MediaType.COG,
                with_proj=True,
                with_raster=True,
            )
            item.assets["asset"].href = f"s3://{bucket}/{key}"
            items.append(item)
        except Exception as e:
            log.warning(f"Failed to build STAC item for {local_path.name}: {e}")
    return items


def build_zarr_stac_item(zarr_dir: Path, bucket: str,
                         zarr_prefix: str,
                         collection_id: str) -> Optional[pystac.Item]:
    """Build a single STAC item for a Zarr store from its attributes."""
    try:
        try:
            store = zarr.open_consolidated(str(zarr_dir), mode="r")
        except (KeyError, ValueError):
            store = zarr.open(str(zarr_dir), mode="r")

        attrs = dict(store.attrs)
        required = ("bounds_west", "bounds_south", "bounds_east", "bounds_north")
        if not all(k in attrs for k in required):
            log.warning(f"Zarr store at {zarr_dir} missing bounds attrs, skipping STAC")
            return None

        west = float(attrs["bounds_west"])
        south = float(attrs["bounds_south"])
        east = float(attrs["bounds_east"])
        north = float(attrs["bounds_north"])
        bbox = [west, south, east, north]
        geom = {
            "type": "Polygon",
            "coordinates": [[
                [west, south], [east, south], [east, north],
                [west, north], [west, south],
            ]],
        }

        times = store["time"][:]
        dt_values = []
        for t in times:
            if isinstance(t, np.datetime64):
                dt_values.append(
                    datetime.fromtimestamp(
                        t.astype("datetime64[s]").astype(int), tz=timezone.utc
                    )
                )
            else:
                dt_values.append(
                    datetime.fromtimestamp(int(t) / 1e9, tz=timezone.utc)
                )

        min_dt = min(dt_values) if dt_values else datetime.now(timezone.utc)
        max_dt = max(dt_values) if dt_values else datetime.now(timezone.utc)

        zarr_s3_url = f"s3://{bucket}/{zarr_prefix.rstrip('/')}/"
        item_id = f"{collection_id}-timeseries"

        item = pystac.Item(
            id=item_id,
            geometry=geom,
            bbox=bbox,
            datetime=None,
            properties={
                "start_datetime": min_dt.isoformat(),
                "end_datetime": max_dt.isoformat(),
                "proj:wkt2": attrs.get("crs"),
            },
        )
        item.add_asset("data", pystac.Asset(
            href=zarr_s3_url,
            media_type="application/vnd+zarr",
            roles=["data"],
            title=f"{collection_id} sparse Zarr time series",
        ))
        return item

    except Exception as e:
        log.warning(f"Failed to build Zarr STAC item for {zarr_dir}: {e}")
        return None


def upsert_stac(mmgis_host: str, mmgis_token: str,
                collection_id: str, items: List[pystac.Item]) -> None:
    """Build a pystac Collection from items and upsert into MMGIS."""
    if not items:
        return

    all_bboxes = [i.bbox for i in items if i.bbox]
    if all_bboxes:
        spatial_bbox = [
            min(b[0] for b in all_bboxes), min(b[1] for b in all_bboxes),
            max(b[2] for b in all_bboxes), max(b[3] for b in all_bboxes),
        ]
    else:
        spatial_bbox = [-180.0, -90.0, 180.0, 90.0]

    dts = []
    for item in items:
        if item.datetime:
            dts.append(item.datetime)
        else:
            props = item.properties
            if props.get("start_datetime"):
                dts.append(datetime.fromisoformat(props["start_datetime"]))
            if props.get("end_datetime"):
                dts.append(datetime.fromisoformat(props["end_datetime"]))
    min_dt = min(dts) if dts else None
    max_dt = max(dts) if dts else None

    collection = pystac.Collection(
        id=collection_id,
        description=f"Frozon ISS products for {collection_id}",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([spatial_bbox]),
            temporal=pystac.TemporalExtent([[min_dt, max_dt]]),
        ),
        license="proprietary",
    )
    for item in items:
        collection.add_item(item)

    create_stac_items.upsert_collection(
        mmgis_url=mmgis_host,
        mmgis_token=mmgis_token,
        collection_id=collection_id,
        collection=collection,
        collection_items=items,
        upsert_items=True,
    )
    log.info(f"Upserted {len(items)} STAC item(s) into {collection_id}")


def s3_key_to_item_id(key: str) -> Optional[str]:
    """Derive the STAC item ID from a pruned S3 key.

    COG items use the filename stem as their ID (matches rio_stac /
    ingest_cog.py build_stac_item). Non-TIFF keys return None."""
    if not key.lower().endswith((".tif", ".tiff")):
        return None
    return Path(key).stem


def delete_stac_items(mmgis_host: str, mmgis_token: str,
                      collection_id: str, item_ids: List[str]) -> None:
    """Delete STAC items from MMGIS by ID."""
    headers = {
        "Authorization": f"Bearer {mmgis_token}",
        "Content-Type": "application/json",
    }
    for item_id in item_ids:
        url = f"{mmgis_host}/stac/collections/{collection_id}/items/{item_id}"
        try:
            resp = requests.delete(url, headers=headers)
            if 200 <= resp.status_code < 300:
                log.info(f"Deleted STAC item {item_id} from {collection_id}")
            elif resp.status_code == 404:
                log.debug(f"STAC item {item_id} already absent from {collection_id}")
            else:
                log.warning(
                    f"Failed to delete STAC item {item_id}: "
                    f"{resp.status_code} {resp.text[:200]}"
                )
        except requests.RequestException as e:
            log.warning(f"Error deleting STAC item {item_id}: {e}")


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_sync(config: dict, maap: MAAP) -> None:
    local_dir = Path(config["local_dir"])
    manifest_path = local_dir / MANIFEST_FILENAME
    manifest = load_manifest(manifest_path)

    s3 = get_s3_client_from_maap(maap)

    mmgis_host = config.get("mmgis_host", "")
    mmgis_token_secret = config.get("mmgis_token_secret_name", "")
    any_stac = any(s.get("stac_update") for s in config["sources"])
    mmgis_token = None
    if mmgis_host and mmgis_token_secret and any_stac:
        mmgis_token = maap.secrets.get_secret(mmgis_token_secret)

    total_fetched = 0
    total_pruned = 0

    for source in config["sources"]:
        name = source.get("name", source["collection_id"])
        bucket = source["s3_bucket"]
        prefix = source.get("s3_prefix", "")
        cid = source["collection_id"]
        sync_types = source["sync"]
        do_stac = source.get("stac_update", False) and mmgis_token

        if "cog" in sync_types:
            cog_prefix = build_cog_prefix(prefix, cid)
            cog_dir = local_dir / name / "cog"
            sr = sync_prefix(s3, bucket, cog_prefix, cog_dir,
                             manifest, f"{name}/cog")
            total_fetched += sr.fetched
            total_pruned += sr.pruned
            log.info(f"{name}/cog: {sr.fetched} new, {sr.pruned} pruned")

            if sr.new_keys and do_stac:
                items = build_cog_stac_items(sr.new_keys, bucket,
                                             cog_prefix, cog_dir, cid)
                upsert_stac(mmgis_host, mmgis_token, cid, items)

            if sr.pruned_keys and do_stac:
                ids_to_delete = [
                    iid for k in sr.pruned_keys
                    if (iid := s3_key_to_item_id(k)) is not None
                ]
                if ids_to_delete:
                    delete_stac_items(mmgis_host, mmgis_token, cid,
                                      ids_to_delete)

        if "zarr" in sync_types:
            zarr_prefix = build_zarr_prefix(prefix, cid)
            zarr_dir = local_dir / name / "zarr" / f"{cid}.zarr"
            sr = sync_prefix(s3, bucket, zarr_prefix, zarr_dir,
                             manifest, f"{name}/zarr")
            total_fetched += sr.fetched
            total_pruned += sr.pruned
            log.info(f"{name}/zarr: {sr.fetched} new, {sr.pruned} pruned")

            if sr.new_keys and do_stac:
                item = build_zarr_stac_item(zarr_dir, bucket, zarr_prefix, cid)
                if item:
                    upsert_stac(mmgis_host, mmgis_token, cid, [item])

    save_manifest(manifest_path, manifest)
    log.info(f"Sync complete — {total_fetched} downloaded, {total_pruned} pruned")


def seconds_until_hour(target_hour: int) -> float:
    now = datetime.now(timezone.utc)
    target = now.replace(hour=target_hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target = target.replace(day=target.day + 1)
    return (target - now).total_seconds()


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Daily incremental downloader for Frozon pipeline outputs."
    )
    parser.add_argument("--config", required=True,
                        help="Path to JSON config file")
    parser.add_argument("--run-hour", type=int, default=None,
                        help="Override run_hour_utc from config (0-23)")
    parser.add_argument("--once", action="store_true",
                        help="Run a single sync and exit (no scheduling)")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    run_hour = args.run_hour if args.run_hour is not None else config.get("run_hour_utc", 2)
    maap = MAAP(maap_host=config.get("maap_host", "api.maap-project.org"))

    if args.once:
        run_sync(config, maap)
        return

    shutdown = False

    def handle_signal(signum, _frame):
        nonlocal shutdown
        log.info(f"Received signal {signum}, shutting down after current cycle…")
        shutdown = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    log.info(f"Scheduler started — daily sync at {run_hour:02d}:00 UTC")
    log.info(f"Syncing {len(config['sources'])} source(s)")

    run_sync(config, maap)

    while not shutdown:
        wait = seconds_until_hour(run_hour)
        log.info(f"Next sync in {wait / 3600:.1f} hours")

        slept = 0.0
        while slept < wait and not shutdown:
            chunk = min(wait - slept, 60.0)
            time.sleep(chunk)
            slept += chunk

        if not shutdown:
            run_sync(config, maap)


if __name__ == "__main__":
    main()
