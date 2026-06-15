#!/usr/bin/env python3
"""
Worker stage: build/upsert a sparse Zarr time series from TIFF inputs.

Runs inside a single MAAP DPS job container. Three modes:

  1. **No existing Zarr at the destination** → fresh build via
     `build_zarr_sparse_streaming.build_zarr_streaming`.
  2. **Existing Zarr, new bounds fit inside** → append along the time
     dim using xarray's append-mode write.
  3. **Existing Zarr, new bounds expand** → dump every existing time
     slice to a temp TIFF (mtime-stamped with the slice datetime, so
     the streaming script's mtime fallback recovers it), feed
     existing+new TIFFs to `build_zarr_streaming`, full rebuild on the
     union grid. This is the expected common case for sparse swaths.

After the local Zarr is written, upload to
`s3://<bucket>/<s3-prefix>/<collection-id>/<collection-id>.zarr/` and
emit a STAC catalog with one stable item per collection
(`<collection-id>-timeseries`) so reruns idempotently update the same
item via the orchestrator's upsert step.
"""

import argparse
import gc
import json
import logging
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pystac
import rasterio
import xarray as xr
import zarr

from common_utils import AWSUtils
import build_zarr_sparse_streaming as bzss
import zarr_io

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s',
)
logger = logging.getLogger(__name__)


def stage_inputs_from_s3(s3_prefix: str, local_dir: Path,
                         role_arn: Optional[str],
                         filter_pattern: Optional[str],
                         exclude_pattern: Optional[str],
                         limit: Optional[int]) -> List[Path]:
    """Download all TIFFs under an S3 prefix to a local directory."""
    bucket, prefix = AWSUtils.parse_s3_path(s3_prefix.rstrip('/'))
    s3 = AWSUtils.get_s3_client(role_arn=role_arn, bucket_name=bucket)
    paginator = s3.get_paginator('list_objects_v2')

    keys: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.lower().endswith(('.tif', '.tiff')):
                continue
            name = os.path.basename(key)
            if filter_pattern and not fnmatch(name, filter_pattern):
                continue
            if exclude_pattern and fnmatch(name, exclude_pattern):
                continue
            keys.append(key)

    keys.sort()
    if limit:
        keys = keys[:limit]

    local_dir.mkdir(parents=True, exist_ok=True)
    local_paths: List[Path] = []
    for key in keys:
        local_path = local_dir / Path(key).name
        s3.download_file(bucket, key, str(local_path))
        local_paths.append(local_path)

    logger.info(f"Staged {len(local_paths)} TIFF(s) from {s3_prefix}")
    return local_paths


def stage_inputs_from_s3_urls(urls: List[str], local_dir: Path,
                              role_arn: Optional[str]) -> List[Path]:
    """Download a list of explicit `s3://bucket/key` URIs to `local_dir`.

    Used by sync mode — the runner passes the exact COG paths we want to
    add as new time slices, rather than an S3 prefix to crawl. Lets the
    runner do all the filtering (e.g. "only the dates missing from the
    existing Zarr") without the worker having to walk a whole bucket.
    """
    local_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []
    seen_buckets: dict[str, object] = {}
    for url in urls:
        bucket, key = AWSUtils.parse_s3_path(url)
        s3 = seen_buckets.get(bucket)
        if s3 is None:
            s3 = AWSUtils.get_s3_client(role_arn=role_arn, bucket_name=bucket)
            seen_buckets[bucket] = s3
        local_path = local_dir / Path(key).name
        s3.download_file(bucket, key, str(local_path))
        paths.append(local_path)
    logger.info(f"Staged {len(paths)} TIFF(s) from explicit S3 URLs")
    return paths


def stage_inputs_from_https(urls: List[str], local_dir: Path,
                             earthdata_token_secret_name: Optional[str]) -> List[Path]:
    """Download a list of Earthdata-protected HTTPS URLs to `local_dir`.
    Uses the shared CMR/EDL helper. Returns local Path objects in input order."""
    from input_sources.cmr_tiff import download_https_edl

    local_dir.mkdir(parents=True, exist_ok=True)
    paths: List[Path] = []
    for url in urls:
        local_path = download_https_edl(url, local_dir, earthdata_token_secret_name)
        paths.append(local_path)
    logger.info(f"Staged {len(paths)} TIFF(s) from HTTPS+EDL inputs")
    return paths


def prune_to_window(local_zarr: Path, retain_days: int) -> bool:
    """Keep slices from the `retain_days` most recent calendar days of
    data; drop everything else.

    "Last N days of available data" means we count distinct calendar
    dates in the store, sort newest-first, keep the top N dates, and
    drop all slices that fall on older dates. This is a retention-period
    count, NOT a sliding time window — sparse data never gets pruned
    below N days even if those days are spread far apart.

    Returns True iff any slices were dropped."""
    if retain_days <= 0:
        return False

    ds = xr.open_zarr(str(local_zarr), consolidated=False, decode_times=True)
    times = ds.coords['time'].values
    total = len(times)
    if total == 0:
        ds.close()
        return False

    # Extract distinct calendar dates and keep the most recent N.
    dates = np.array([t.astype('datetime64[D]') for t in times])
    unique_dates = sorted(set(dates), reverse=True)
    dates_to_keep = set(unique_dates[:retain_days])

    keep_mask = np.array([d in dates_to_keep for d in dates])
    kept = int(keep_mask.sum())

    if kept == total:
        logger.info(
            f"prune_to_window: only {len(unique_dates)} distinct date(s), "
            f"all within retain_days={retain_days}; no-op"
        )
        ds.close()
        return False

    pruned = ds.isel(time=np.where(keep_mask)[0])
    ds.close()

    tmp_path = local_zarr.parent / (local_zarr.name + ".pruning")
    if tmp_path.exists():
        shutil.rmtree(tmp_path)
    pruned.to_zarr(str(tmp_path), mode='w', consolidated=True)

    shutil.rmtree(local_zarr)
    os.rename(str(tmp_path), str(local_zarr))

    dates_dropped = sorted(d for d in unique_dates if d not in dates_to_keep)
    logger.info(
        f"prune_to_window: kept {len(dates_to_keep)} newest date(s), "
        f"dropped {total - kept} slice(s) from {dates_dropped}"
    )
    return True


def compute_new_bounds(tiff_files: List[Path]) -> Optional[dict]:
    """Compute union bounds of new TIFFs without writing anything."""
    if not tiff_files:
        return None
    bounds, _resolution, _crs, _nodata = bzss.get_global_bounds_and_resolution(tiff_files)
    return bounds


def union_bounds(a: dict, b: dict) -> dict:
    """Outer extent of two bounds dicts."""
    west = min(a['west'], b['west'])
    south = min(a['south'], b['south'])
    east = max(a['east'], b['east'])
    north = max(a['north'], b['north'])
    return {
        'west': west, 'south': south, 'east': east, 'north': north,
        'width': east - west, 'height': north - south,
    }


def append_in_place(local_zarr: Path, new_tiffs: List[Path],
                   time_regex: Optional[str]) -> None:
    """Append new TIFFs as time slices to an existing Zarr that already
    contains them spatially. Uses xarray's append-along-time mode.

    Each TIFF becomes one slice on the existing grid. Inputs whose
    spatial extent doesn't exactly match the existing grid are clipped
    using the same translate-to-grid logic the streaming script uses;
    partial-overlap clipping is correct because we've already verified
    bounds containment before calling this."""
    existing = xr.open_zarr(str(local_zarr), consolidated=False, decode_times=True)
    grid_x = existing.coords['x'].values
    grid_y = existing.coords['y'].values
    resolution = float(existing.attrs['resolution'])
    grid_west = float(existing.attrs['bounds_west'])
    grid_north = float(existing.attrs['bounds_north'])
    existing.close()

    grid_height = len(grid_y)
    grid_width = len(grid_x)

    new_slices: List[np.ndarray] = []
    new_times: List[datetime] = []

    for tiff in new_tiffs:
        dt = bzss.extract_datetime_from_filename(tiff.name, time_regex=time_regex)
        if dt is None:
            dt = datetime.fromtimestamp(tiff.stat().st_mtime, tz=timezone.utc)

        slice_arr = np.full((grid_height, grid_width), np.nan, dtype=np.float32)
        with rasterio.open(str(tiff)) as src:
            file_bounds = src.bounds
            file_height, file_width = src.height, src.width
            data = src.read(1).astype(np.float32)
            file_grid_y_start = int(np.round((grid_north - file_bounds.top) / resolution))
            file_grid_x_start = int(np.round((file_bounds.left - grid_west) / resolution))

            grid_y0 = max(0, file_grid_y_start)
            grid_x0 = max(0, file_grid_x_start)
            grid_y1 = min(grid_height, file_grid_y_start + file_height)
            grid_x1 = min(grid_width, file_grid_x_start + file_width)
            src_y0 = max(0, -file_grid_y_start)
            src_x0 = max(0, -file_grid_x_start)
            src_y1 = src_y0 + (grid_y1 - grid_y0)
            src_x1 = src_x0 + (grid_x1 - grid_x0)

            if grid_y0 < grid_y1 and grid_x0 < grid_x1:
                slice_arr[grid_y0:grid_y1, grid_x0:grid_x1] = (
                    data[src_y0:src_y1, src_x0:src_x1]
                )

        new_slices.append(slice_arr)
        new_times.append(dt)
        gc.collect()

    if not new_slices:
        logger.warning("append_in_place: no new slices to append")
        return

    new_data = np.stack(new_slices, axis=0)
    new_time = np.array(new_times, dtype='datetime64[ns]')

    new_ds = xr.Dataset(
        {'data': (('time', 'y', 'x'), new_data)},
        coords={'time': new_time, 'y': grid_y, 'x': grid_x},
    )
    new_ds.to_zarr(str(local_zarr), mode='a', append_dim='time', consolidated=True)
    logger.info(f"Appended {len(new_slices)} slice(s) in place to {local_zarr}")


def sync_zarr(args: argparse.Namespace,
              new_tiffs: List[Path],
              work_dir: Path,
              final_zarr: Path,
              zarr_s3_url: str) -> Tuple[str, datetime, datetime, dict]:
    """Sync mode: rewrite the Zarr so its time dimension contains exactly
    the dates in `args.desired_dates`.

    Pulls down the existing Zarr (if any), dumps only the slices whose
    date is still desired (via `dump_zarr_slices_to_tiffs(keep_dates=...)`),
    combines them with `new_tiffs` (the runner is expected to have
    pre-filtered the input set to "dates not already in the Zarr"), and
    runs `build_zarr_streaming` on the union.

    The runner is responsible for passing both halves correctly:
      - `--desired-dates`: the final set of YYYYMMDD dates.
      - input source (`--input-s3-urls` etc.): only the dates the existing
        Zarr doesn't already have. The runner has already done that diff.
    """
    desired_dates = {d.strip() for d in args.desired_dates.split(",") if d.strip()}
    if not desired_dates:
        raise RuntimeError("--desired-dates parsed to an empty set")

    has_existing = zarr_io.zarr_store_exists_in_s3(zarr_s3_url, args.role_arn)
    kept_tiffs: List[Path] = []
    if has_existing:
        existing_local = work_dir / "existing.zarr"
        zarr_io.download_zarr_store(zarr_s3_url, existing_local, args.role_arn)
        slice_dir = work_dir / "kept_slices"
        kept_tiffs = zarr_io.dump_zarr_slices_to_tiffs(
            existing_local, slice_dir, keep_dates=desired_dates,
        )
        logger.info(
            f"Sync: kept {len(kept_tiffs)} slice(s) from existing Zarr "
            f"that match desired_dates."
        )
    else:
        logger.info(f"Sync: no existing Zarr at {zarr_s3_url}; building fresh.")

    combined = kept_tiffs + new_tiffs
    if not combined:
        raise RuntimeError(
            "Sync: nothing to write — existing Zarr (if any) has no slices "
            "matching desired_dates, and no new tiffs were supplied."
        )

    ok = bzss.build_zarr_streaming(
        tiff_files=combined,
        output_path=final_zarr,
        chunk_size=args.chunk_size,
        time_regex=args.time_regex,
    )
    if not ok:
        raise RuntimeError("build_zarr_streaming failed during sync")

    bounds, min_dt, max_dt = read_zarr_summary(final_zarr)
    return "sync", min_dt, max_dt, bounds


def build_or_upsert(args: argparse.Namespace,
                   new_tiffs: List[Path],
                   work_dir: Path,
                   final_zarr: Path,
                   zarr_s3_url: str) -> Tuple[str, datetime, datetime, dict]:
    """Decide build/append/rebuild path and write the resulting Zarr to
    `final_zarr` locally. Returns (mode, min_dt, max_dt, bounds) for the
    STAC item builder."""
    has_existing = zarr_io.zarr_store_exists_in_s3(zarr_s3_url, args.role_arn)
    new_bounds = compute_new_bounds(new_tiffs)
    if new_bounds is None:
        raise RuntimeError("Could not compute bounds from new inputs")

    if not has_existing:
        logger.info(f"No existing Zarr at {zarr_s3_url} — running fresh build")
        ok = bzss.build_zarr_streaming(
            tiff_files=new_tiffs,
            output_path=final_zarr,
            chunk_size=args.chunk_size,
            time_regex=args.time_regex,
        )
        if not ok:
            raise RuntimeError("build_zarr_streaming failed")
        mode = "fresh"
    else:
        existing_local = work_dir / "existing.zarr"
        zarr_io.download_zarr_store(zarr_s3_url, existing_local, args.role_arn)
        existing_bounds = zarr_io.read_existing_zarr_bounds(existing_local)
        if existing_bounds is None:
            raise RuntimeError(
                f"Existing store at {zarr_s3_url} is missing the bounds "
                "attributes written by build_zarr_sparse_streaming — "
                "cannot determine layout safely"
            )

        if zarr_io.bounds_contain(existing_bounds, new_bounds):
            logger.info("New inputs fit existing grid — appending in place")
            append_in_place(existing_local, new_tiffs, args.time_regex)
            os.rename(existing_local, final_zarr)
            mode = "append"
        else:
            if not args.allow_bounds_expansion:
                raise RuntimeError(
                    f"New inputs extend beyond existing Zarr bounds and "
                    f"--no-allow-bounds-expansion was set. "
                    f"existing={existing_bounds} new={new_bounds}"
                )
            logger.info("Bounds expansion needed — dumping existing slices and rebuilding")
            slice_dir = work_dir / "existing_slices"
            existing_tiffs = zarr_io.dump_zarr_slices_to_tiffs(existing_local, slice_dir)
            combined = existing_tiffs + new_tiffs
            ok = bzss.build_zarr_streaming(
                tiff_files=combined,
                output_path=final_zarr,
                chunk_size=args.chunk_size,
                time_regex=args.time_regex,
            )
            if not ok:
                raise RuntimeError("build_zarr_streaming failed during rebuild")
            mode = "rebuild"

    bounds, min_dt, max_dt = read_zarr_summary(final_zarr)
    return mode, min_dt, max_dt, bounds


def read_zarr_summary(local_zarr: Path) -> Tuple[dict, datetime, datetime]:
    """Pull bounds + time range from the just-written store for the STAC item."""
    try:
        store = zarr.open_consolidated(str(local_zarr), mode='r')
    except (KeyError, ValueError):
        store = zarr.open(str(local_zarr), mode='r')

    attrs = dict(store.attrs)
    bounds = {
        'west': float(attrs['bounds_west']),
        'south': float(attrs['bounds_south']),
        'east': float(attrs['bounds_east']),
        'north': float(attrs['bounds_north']),
        'crs': attrs['crs'],
    }
    times = store['time'][:]
    finite = times[~np.isnat(times)] if times.dtype.kind == 'M' else times
    if len(finite) == 0:
        raise RuntimeError("Zarr has no valid time values after build")
    min_dt = datetime.fromtimestamp(
        finite.min().astype('datetime64[s]').astype(int), tz=timezone.utc)
    max_dt = datetime.fromtimestamp(
        finite.max().astype('datetime64[s]').astype(int), tz=timezone.utc)
    return bounds, min_dt, max_dt


def write_stac_catalog(zarr_s3_url: str, collection_id: str,
                       bounds: dict, min_dt: datetime, max_dt: datetime,
                       mode: str, output_dir: Path) -> Path:
    """Wrap a single Item (id `<collection-id>-timeseries`, asset key
    `data` pointing at the Zarr store) in Catalog→Collection→Item.
    Reruns produce the same item id, so the orchestrator's upsert step
    replaces the previous item."""
    bbox = [bounds['west'], bounds['south'], bounds['east'], bounds['north']]
    geom = {
        "type": "Polygon",
        "coordinates": [[
            [bounds['west'], bounds['south']],
            [bounds['east'], bounds['south']],
            [bounds['east'], bounds['north']],
            [bounds['west'], bounds['north']],
            [bounds['west'], bounds['south']],
        ]],
    }

    item_id = f"{collection_id}-timeseries"
    item = pystac.Item(
        id=item_id,
        geometry=geom,
        bbox=bbox,
        datetime=None,
        properties={
            "start_datetime": min_dt.isoformat(),
            "end_datetime": max_dt.isoformat(),
            "frozon:zarr_build_mode": mode,
            "proj:wkt2": bounds.get('crs'),
        },
    )
    item.add_asset("data", pystac.Asset(
        href=zarr_s3_url,
        media_type="application/vnd+zarr",
        roles=["data"],
        title=f"{collection_id} sparse Zarr time series",
    ))

    collection = pystac.Collection(
        id=collection_id,
        description=f"Frozon ISS Zarr time-series products for {collection_id}",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([bbox]),
            temporal=pystac.TemporalExtent([[min_dt, max_dt]]),
        ),
        license="proprietary",
    )
    collection.add_item(item)

    catalog = pystac.Catalog(
        id=f"frozon-zarr-{collection_id}",
        description=f"Frozon ISS Zarr ingest catalog for {collection_id}",
    )
    catalog.add_child(collection)
    catalog.normalize_hrefs(str(output_dir))
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

    catalog_path = output_dir / "catalog.json"
    logger.info(f"Wrote STAC catalog to {catalog_path}")
    return catalog_path


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Build or upsert a sparse Zarr time series from TIFF inputs."
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--input-s3-prefix", help="S3 prefix containing TIFF inputs")
    src.add_argument("--input-tiff-dir", help="Local TIFF directory (test mode)")
    src.add_argument("--input-https-urls",
                     help="JSON list of Earthdata-protected HTTPS URLs to download.")
    src.add_argument("--input-s3-urls",
                     help="JSON list of explicit s3:// URIs (use with "
                          "--desired-dates for sync mode).")

    parser.add_argument("--desired-dates", default=None,
                        help="Comma-separated YYYYMMDD list. When set, worker "
                             "runs in sync mode: opens the existing Zarr, drops "
                             "any slice whose acquisition date isn't in this "
                             "list, then merges in slices from --input-s3-urls "
                             "(or whichever input source) and writes back. "
                             "Slices missing on both sides become unrecoverable, "
                             "so the runner is responsible for passing both the "
                             "desired set and the new-to-add S3 URLs.")

    parser.add_argument("--earthdata-token-secret-name", default=None,
                        help="MAAP secret name holding EDL bearer token (or "
                             "username\\npassword). Required with --input-https-urls.")
    parser.add_argument("--retain-days", type=int, default=0,
                        help="If > 0, keep only the N most recent calendar "
                             "days of data after build/append; drop slices "
                             "from older dates. Counts distinct dates, not "
                             "a time window.")
    parser.add_argument("--collection-id", required=True)
    parser.add_argument("--s3-bucket", required=True)
    parser.add_argument("--s3-prefix", default="")
    parser.add_argument("--role-arn")
    parser.add_argument("--time-regex", default=None)
    parser.add_argument("--chunk-size", type=int, default=1024)
    parser.add_argument("--filter", dest="filter_pattern", default=None)
    parser.add_argument("--exclude", dest="exclude_pattern", default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--allow-bounds-expansion",
                        action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--output", default="output")

    args = parser.parse_args()

    work_dir = Path(args.output)
    work_dir.mkdir(parents=True, exist_ok=True)
    new_tiff_dir = work_dir / "new_tiffs"
    final_zarr = work_dir / "zarr" / f"{args.collection_id}.zarr"
    final_zarr.parent.mkdir(parents=True, exist_ok=True)
    stac_dir = work_dir / "stac"
    stac_dir.mkdir(parents=True, exist_ok=True)

    s3_prefix = args.s3_prefix.strip('/')
    parts = ([s3_prefix] if s3_prefix else []) + [
        args.collection_id, f"{args.collection_id}.zarr"
    ]
    zarr_key = '/'.join(parts)
    zarr_s3_url = f"s3://{args.s3_bucket}/{zarr_key}/"

    try:
        if args.input_tiff_dir:
            new_tiffs = sorted(Path(args.input_tiff_dir).glob('*.tif'))
            new_tiffs += sorted(Path(args.input_tiff_dir).glob('*.tiff'))
            if args.filter_pattern:
                new_tiffs = [f for f in new_tiffs if fnmatch(f.name, args.filter_pattern)]
            if args.exclude_pattern:
                new_tiffs = [f for f in new_tiffs if not fnmatch(f.name, args.exclude_pattern)]
            if args.limit:
                new_tiffs = new_tiffs[:args.limit]
        elif args.input_https_urls:
            urls = json.loads(args.input_https_urls)
            if args.filter_pattern:
                urls = [u for u in urls if fnmatch(os.path.basename(u.split('?',1)[0]), args.filter_pattern)]
            if args.exclude_pattern:
                urls = [u for u in urls if not fnmatch(os.path.basename(u.split('?',1)[0]), args.exclude_pattern)]
            if args.limit:
                urls = urls[:args.limit]
            new_tiffs = stage_inputs_from_https(
                urls, new_tiff_dir, args.earthdata_token_secret_name,
            )
        elif args.input_s3_urls:
            urls = json.loads(args.input_s3_urls)
            new_tiffs = stage_inputs_from_s3_urls(
                urls, new_tiff_dir, args.role_arn,
            )
        else:
            new_tiffs = stage_inputs_from_s3(
                args.input_s3_prefix, new_tiff_dir, args.role_arn,
                args.filter_pattern, args.exclude_pattern, args.limit,
            )

        # In sync mode the new-tiffs list can legitimately be empty (e.g.
        # COG retention dropped a date and nothing new landed) — we still
        # need to rewrite the Zarr to reflect the deletion. Reject empty
        # only for the original dispatch flow.
        if not new_tiffs and not args.desired_dates:
            raise RuntimeError("No TIFF inputs to process")

        if args.desired_dates:
            mode, min_dt, max_dt, bounds = sync_zarr(
                args, new_tiffs, work_dir, final_zarr, zarr_s3_url,
            )
        else:
            mode, min_dt, max_dt, bounds = build_or_upsert(
                args, new_tiffs, work_dir, final_zarr, zarr_s3_url
            )
        logger.info(f"Zarr built ({mode}); time range {min_dt.isoformat()} → {max_dt.isoformat()}")

        if args.retain_days > 0:
            prune_to_window(final_zarr, args.retain_days)
            bounds, min_dt, max_dt = read_zarr_summary(final_zarr)

        zarr_io.upload_zarr_store(
            final_zarr, zarr_s3_url, args.role_arn, delete_remote_first=True,
        )

        write_stac_catalog(
            zarr_s3_url, args.collection_id, bounds,
            min_dt, max_dt, mode, stac_dir,
        )

        logger.info(f"Zarr ingest complete: {zarr_s3_url}")
        return 0

    except FileNotFoundError as e:
        logger.error(f"TERMINATED: {e}")
        return 6
    except ValueError as e:
        logger.error(f"TERMINATED: invalid argument: {e}")
        return 6
    except RuntimeError as e:
        logger.error(f"TERMINATED: runtime error: {e}")
        return 7
    except Exception as e:
        logger.error(f"TERMINATED: unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
