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
import os
import re
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

try:
    import antimeridian
except ImportError:
    antimeridian = None


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
            if not os.environ.get("MMGIS_TOKEN"):
                raise ValueError(f"{label}: stac_update requires MMGIS_TOKEN in the environment")
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
    __slots__ = ("fetched", "pruned", "new_keys", "pruned_keys", "all_keys")

    def __init__(self):
        self.fetched: int = 0
        self.pruned: int = 0
        self.new_keys: Set[str] = set()
        self.pruned_keys: Set[str] = set()
        self.all_keys: Set[str] = set()


def sync_prefix(s3, bucket: str, prefix: str, local_dir: Path,
                manifest: Dict[str, str], label: str) -> SyncResult:
    """Sync local_dir to match S3 prefix exactly (rsync --delete semantics)."""
    result = SyncResult()
    objects = list_objects(s3, bucket, prefix)
    remote_keys = {obj["Key"] for obj in objects}
    result.all_keys = remote_keys

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

ARCTIC_BBOX = [-180.0, 60.0, 180.0, 90.0]


def _has_time_components(time_format: str) -> bool:
    """True if the format includes hours/minutes/seconds tokens."""
    return any(tok in time_format for tok in ("%H", "%I", "%M", "%S", "%T", "%f", "%p"))


def _extract_datetime_from_filename(filename: str,
                                    time_regex: Optional[str],
                                    time_format: str) -> Optional[datetime]:
    """Pull an acquisition datetime out of a filename.

    Mirrors the dateline scripts' approach: try the Sentinel-1 pattern
    first, then a user-supplied `time_regex` (using a `start_date` named
    group or the first capture). Returns None if nothing matches.

    Date-only formats (no %H/%M/%S in `time_format`) snap to noon UTC
    instead of midnight so the value displays as the right day in
    western-hemisphere timezones (UTC midnight rendered in PDT shows as
    the previous evening)."""
    sentinel = re.search(
        r"S1[AB]_\w+_\w+_\w+_(\d{8}T\d{6})_(\d{8}T\d{6})", filename
    )
    if sentinel:
        try:
            return datetime.strptime(sentinel.group(1), "%Y%m%dT%H%M%S").replace(tzinfo=timezone.utc)
        except ValueError:
            pass
    if time_regex:
        m = re.search(time_regex, filename)
        if m:
            try:
                date_str = m.groupdict().get("start_date")
                if date_str is None and m.groups():
                    date_str = m.group(1)
                if date_str:
                    dt = datetime.strptime(date_str, time_format)
                    if not _has_time_components(time_format):
                        dt = dt.replace(hour=12)
                    return dt.replace(tzinfo=timezone.utc)
            except ValueError:
                pass
    return None


def _remap_path(host_path: Path, path_remove: Optional[str],
                path_replace_with: Optional[str]) -> str:
    """Apply the dateline-script style path remap: strip `path_remove` from
    the host path and substitute `path_replace_with`. Used so STAC asset
    hrefs match what the titiler container sees inside its bind mount.

    Symlinks are NOT resolved when a remap is configured — the user's
    `path_remove` is matched against the path as written (so e.g. the
    `/home/mmgis/frozon-efs` symlink survives the substitution).

    With no path_remove configured, returns the file:// URI of the
    resolved host path."""
    if path_remove:
        s = str(host_path)
        replacement = path_replace_with or ""
        if s.startswith(path_remove):
            return replacement + s[len(path_remove):]
        return s.replace(path_remove, replacement, 1)
    return host_path.resolve().as_uri()


def _apply_antimeridian_fix(item: pystac.Item) -> None:
    """In-place fix for geometries crossing ±180°. Recomputes bbox when
    the antimeridian split turns the polygon into a MultiPolygon."""
    if antimeridian is None:
        log.warning("antimeridian package not installed — skipping fix")
        return
    if item.geometry is None:
        return
    fixed = antimeridian.fix_geojson(item.geometry)
    item.geometry = fixed
    if fixed.get("type") == "MultiPolygon":
        coords = [c for poly in fixed["coordinates"] for ring in poly for c in ring]
        lons = [c[0] for c in coords]
        lats = [c[1] for c in coords]
        item.bbox = [min(lons), min(lats), max(lons), max(lats)]


def build_cog_stac_items(new_keys: Set[str], bucket: str,
                         cog_prefix: str, cog_dir: Path,
                         collection_id: str,
                         path_remove: Optional[str] = None,
                         path_replace_with: Optional[str] = None,
                         fix_antimeridian: bool = True,
                         arctic_bbox: bool = False,
                         time_regex: Optional[str] = None,
                         time_format: str = "%Y%m%dT%H%M%S") -> List[pystac.Item]:
    """Build STAC items for newly downloaded COGs using rio_stac.

    Path remapping (`path_remove` / `path_replace_with`) rewrites the host
    file path to whatever titiler sees inside its container bind mount.
    `time_regex` extracts an acquisition datetime from the filename so the
    STAC item's `datetime` matches the data instead of "now"."""
    items = []
    for key in sorted(new_keys):
        if not key.lower().endswith((".tif", ".tiff")):
            continue
        rel = key[len(cog_prefix):]
        local_path = cog_dir / rel
        if not local_path.exists():
            continue
        try:
            asset_href = _remap_path(local_path, path_remove, path_replace_with)
            input_dt = _extract_datetime_from_filename(
                local_path.name, time_regex, time_format
            )
            if input_dt is None:
                log.warning(
                    f"{local_path.name}: no datetime extracted from filename "
                    "— rio_stac will fall back to file metadata or 'now'"
                )
            item = rio_stac.create_stac_item(
                source=str(local_path),
                id=local_path.stem,
                collection=collection_id,
                input_datetime=input_dt,
                asset_name="asset",
                asset_href=asset_href,
                asset_media_type=pystac.MediaType.COG,
                with_proj=True,
                with_raster=True,
                with_eo=True,
            )
            if fix_antimeridian:
                _apply_antimeridian_fix(item)
            if arctic_bbox:
                item.bbox = list(ARCTIC_BBOX)
            items.append(item)
        except Exception as e:
            log.warning(f"Failed to build STAC item for {local_path.name}: {e}")
    return items


def build_zarr_stac_item(zarr_dir: Path, bucket: str,
                         zarr_prefix: str,
                         collection_id: str,
                         path_remove: Optional[str] = None,
                         path_replace_with: Optional[str] = None,
                         fix_antimeridian: bool = True,
                         arctic_bbox: bool = False) -> Optional[pystac.Item]:
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

        zarr_href = _remap_path(zarr_dir, path_remove, path_replace_with)
        if not zarr_href.endswith("/"):
            zarr_href += "/"
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
            href=zarr_href,
            media_type="application/vnd+zarr",
            roles=["data"],
            title=f"{collection_id} sparse Zarr time series",
        ))
        if fix_antimeridian:
            _apply_antimeridian_fix(item)
        if arctic_bbox:
            item.bbox = list(ARCTIC_BBOX)
        return item

    except Exception as e:
        log.warning(f"Failed to build Zarr STAC item for {zarr_dir}: {e}")
        return None


def _stac_headers(token: Optional[str]) -> Dict[str, str]:
    h = {"Content-Type": "application/json", "Accept": "application/json"}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def upsert_stac(mmgis_host: str, mmgis_token: Optional[str],
                collection_id: str, items: List[pystac.Item]) -> None:
    """Upsert a STAC collection + items into stac-fastapi.

    Talks directly to the STAC API root (e.g. http://localhost:32775), using
    the transaction-extension endpoints:
      GET    /collections/{id}             — existence check
      POST   /collections                  — create
      PUT    /collections/{id}             — update extent
      POST   /collections/{id}/bulk_items  — bulk upsert items
    """
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

    headers = _stac_headers(mmgis_token)
    host = mmgis_host.rstrip("/")

    coll_url = f"{host}/collections/{collection_id}"
    get_resp = requests.get(coll_url, headers=headers)
    if get_resp.status_code == 200:
        # Merge temporal extents with the existing collection before PUT.
        existing = get_resp.json()
        existing_intervals = (
            existing.get("extent", {}).get("temporal", {}).get("interval", [])
        )
        all_dts = []
        for d in dts:
            all_dts.append(d if d.tzinfo else d.replace(tzinfo=timezone.utc))
        for interval in existing_intervals:
            for ts in interval:
                if ts:
                    parsed = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    all_dts.append(
                        parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
                    )
        merged_min = min(all_dts) if all_dts else None
        merged_max = max(all_dts) if all_dts else None
        collection.extent.temporal = pystac.TemporalExtent([[merged_min, merged_max]])
        put = requests.put(coll_url, json=collection.to_dict(), headers=headers)
        put.raise_for_status()
        log.info(f"Updated STAC collection {collection_id}")
    elif get_resp.status_code == 404:
        post = requests.post(f"{host}/collections", json=collection.to_dict(),
                             headers=headers)
        post.raise_for_status()
        log.info(f"Created STAC collection {collection_id}")
    else:
        get_resp.raise_for_status()

    bulk_url = f"{host}/collections/{collection_id}/bulk_items"
    payload = {
        "items": {item.id: item.to_dict() for item in items},
        "method": "upsert",
    }
    bulk = requests.post(bulk_url, json=payload, headers=headers)
    bulk.raise_for_status()
    log.info(f"Upserted {len(items)} STAC item(s) into {collection_id}")


def s3_key_to_item_id(key: str) -> Optional[str]:
    """Derive the STAC item ID from a pruned S3 key.

    COG items use the filename stem as their ID (matches rio_stac /
    cog_helpers.build_stac_item). Non-TIFF keys return None."""
    if not key.lower().endswith((".tif", ".tiff")):
        return None
    return Path(key).stem


def delete_stac_items(mmgis_host: str, mmgis_token: Optional[str],
                      collection_id: str, item_ids: List[str]) -> None:
    """Delete STAC items by ID via the transaction extension."""
    headers = _stac_headers(mmgis_token)
    host = mmgis_host.rstrip("/")
    for item_id in item_ids:
        url = f"{host}/collections/{collection_id}/items/{item_id}"
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

def run_sync(config: dict, maap: MAAP, catalog_existing: bool = False) -> None:
    local_dir = Path(config["local_dir"])
    manifest_path = local_dir / MANIFEST_FILENAME
    manifest = load_manifest(manifest_path)

    s3 = get_s3_client_from_maap(maap)

    mmgis_host = config.get("mmgis_host", "")
    any_stac = any(s.get("stac_update") for s in config["sources"])
    mmgis_token = os.environ.get("MMGIS_TOKEN") if any_stac else None
    if any_stac and not mmgis_token:
        log.warning("stac_update is enabled but MMGIS_TOKEN is not set — STAC steps will be skipped")

    total_fetched = 0
    total_pruned = 0

    for source in config["sources"]:
        name = source.get("name", source["collection_id"])
        bucket = source["s3_bucket"]
        prefix = source.get("s3_prefix", "")
        cid = source["collection_id"]
        sync_types = source["sync"]
        do_stac = source.get("stac_update", False) and mmgis_token
        path_remove = source.get("path_remove")
        path_replace_with = source.get("path_replace_with")
        fix_antimeridian = source.get("fix_antimeridian", True)
        arctic_bbox = source.get("arctic_bbox", False)
        time_regex = source.get("time_regex")
        time_format = source.get("time_format", "%Y%m%dT%H%M%S")

        if "cog" in sync_types:
            cog_prefix = build_cog_prefix(prefix, cid)
            cog_dir = local_dir / name
            sr = sync_prefix(s3, bucket, cog_prefix, cog_dir,
                             manifest, f"{name}/cog")
            total_fetched += sr.fetched
            total_pruned += sr.pruned
            log.info(f"{name}/cog: {sr.fetched} new, {sr.pruned} pruned")

            if do_stac:
                # catalog_existing → upsert every COG that exists on disk,
                # not just the freshly-downloaded ones.
                catalog_keys = sr.all_keys if catalog_existing else sr.new_keys
                if catalog_keys:
                    items = build_cog_stac_items(
                        catalog_keys, bucket, cog_prefix, cog_dir, cid,
                        path_remove=path_remove,
                        path_replace_with=path_replace_with,
                        fix_antimeridian=fix_antimeridian,
                        arctic_bbox=arctic_bbox,
                        time_regex=time_regex,
                        time_format=time_format,
                    )
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
            zarr_dir = local_dir / name / f"{cid}.zarr"
            sr = sync_prefix(s3, bucket, zarr_prefix, zarr_dir,
                             manifest, f"{name}/zarr")
            total_fetched += sr.fetched
            total_pruned += sr.pruned
            log.info(f"{name}/zarr: {sr.fetched} new, {sr.pruned} pruned")

            should_catalog_zarr = (
                do_stac and (sr.new_keys or (catalog_existing and sr.all_keys))
            )
            if should_catalog_zarr:
                item = build_zarr_stac_item(
                    zarr_dir, bucket, zarr_prefix, cid,
                    path_remove=path_remove,
                    path_replace_with=path_replace_with,
                    fix_antimeridian=fix_antimeridian,
                    arctic_bbox=arctic_bbox,
                )
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
    parser.add_argument("--catalog-existing", action="store_true",
                        help="Backfill STAC: upsert items for every file already on disk "
                             "(not only the freshly-downloaded ones). Useful after a manual "
                             "download or when first enabling stac_update.")
    return parser.parse_args(argv)


def main() -> None:
    args = parse_args()
    config = load_config(args.config)

    run_hour = args.run_hour if args.run_hour is not None else config.get("run_hour_utc", 2)
    maap = MAAP(maap_host=config.get("maap_host", "api.maap-project.org"))

    if args.once:
        run_sync(config, maap, catalog_existing=args.catalog_existing)
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

    run_sync(config, maap, catalog_existing=args.catalog_existing)

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
