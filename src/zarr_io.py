"""
Zarr↔S3 I/O and rebuild helpers for the Frozon ISS Zarr pipeline.

A Zarr "store" is a directory tree (many small files: `.zarray`, `.zattrs`,
chunk objects). Three responsibilities live here:

  1. `download_zarr_store` / `upload_zarr_store` — recursively sync a store
     between S3 and the local filesystem.
  2. `read_existing_zarr_attrs` / `read_existing_zarr_bounds` — pull
     bounds/CRS/resolution from a previously-written store's attributes
     so the worker can decide build vs. append vs. rebuild.
  3. `dump_zarr_slices_to_tiffs` — extract every time slice as a
     georeferenced TIFF, stamping the slice's datetime onto the file mtime
     so `build_zarr_sparse_streaming.extract_datetime_from_filename` can
     fall back to mtime cleanly.
"""

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import rasterio
from rasterio.transform import from_origin
import zarr

from common_utils import AWSUtils

logger = logging.getLogger(__name__)


def _strip_zarr_suffix(s3_url: str) -> Tuple[str, str]:
    """Parse `s3://bucket/path/foo.zarr[/]` → (bucket, "path/foo.zarr")
    with no trailing slash on the key."""
    bucket, key = AWSUtils.parse_s3_path(s3_url.rstrip('/'))
    return bucket, key


def zarr_store_exists_in_s3(s3_url: str, role_arn: Optional[str] = None) -> bool:
    """True if at least one object exists under the Zarr's prefix in S3.

    Uses a lightweight `list_objects_v2` with `MaxKeys=1` rather than a
    HEAD on a sentinel file — the entry point file differs between Zarr
    v2 (`.zgroup`/`.zarray`) and v3 (`zarr.json`)."""
    bucket, key = _strip_zarr_suffix(s3_url)
    s3 = AWSUtils.get_s3_client(role_arn=role_arn, bucket_name=bucket)
    prefix = key.rstrip('/') + '/'
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    return response.get('KeyCount', 0) > 0


def download_zarr_store(s3_url: str, local_path: Path,
                       role_arn: Optional[str] = None) -> Path:
    """Recursively download every object under `s3_url` into `local_path`,
    mirroring the relative layout. Returns `local_path` for chaining."""
    bucket, key = _strip_zarr_suffix(s3_url)
    s3 = AWSUtils.get_s3_client(role_arn=role_arn, bucket_name=bucket)
    prefix = key.rstrip('/') + '/'

    local_path.mkdir(parents=True, exist_ok=True)
    paginator = s3.get_paginator('list_objects_v2')
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            object_key = obj['Key']
            if object_key.endswith('/'):
                continue
            rel = object_key[len(prefix):]
            target = local_path / rel
            target.parent.mkdir(parents=True, exist_ok=True)
            s3.download_file(bucket, object_key, str(target))
            count += 1
    logger.info(f"Downloaded {count} object(s) from {s3_url} → {local_path}")
    return local_path


def upload_zarr_store(local_path: Path, s3_url: str,
                     role_arn: Optional[str] = None,
                     delete_remote_first: bool = True) -> str:
    """Recursively upload every file in `local_path` to `s3_url`.

    `delete_remote_first` clears the destination prefix before uploading.
    That's the safe default when the store has been rebuilt (chunk layouts
    may change) — leftover stale chunks would corrupt reads. Skip it only
    when you know you're appending in place to an unchanged grid."""
    bucket, key = _strip_zarr_suffix(s3_url)
    s3 = AWSUtils.get_s3_client(role_arn=role_arn, bucket_name=bucket)
    prefix = key.rstrip('/') + '/'

    if delete_remote_first:
        _delete_s3_prefix(s3, bucket, prefix)

    count = 0
    for root, _dirs, files in os.walk(local_path):
        for fname in files:
            fpath = Path(root) / fname
            rel = fpath.relative_to(local_path).as_posix()
            object_key = prefix + rel
            s3.upload_file(str(fpath), bucket, object_key)
            count += 1
    logger.info(f"Uploaded {count} object(s) from {local_path} → {s3_url}")
    return s3_url


def _delete_s3_prefix(s3_client, bucket: str, prefix: str) -> None:
    """Delete every object under prefix. Only runs when the prefix
    resolves to a Zarr store (the caller is upload_zarr_store)."""
    paginator = s3_client.get_paginator('list_objects_v2')
    keys: List[str] = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            keys.append(obj['Key'])
    for batch_start in range(0, len(keys), 1000):
        batch = keys[batch_start:batch_start + 1000]
        s3_client.delete_objects(
            Bucket=bucket,
            Delete={'Objects': [{'Key': k} for k in batch]},
        )
    if keys:
        logger.info(f"Cleared {len(keys)} stale object(s) under s3://{bucket}/{prefix}")


def read_existing_zarr_attrs(local_path: Path) -> Dict:
    """Read the top-level group attrs from a local Zarr store. Tolerates
    consolidated and unconsolidated stores."""
    try:
        store = zarr.open_consolidated(str(local_path), mode='r')
    except (KeyError, ValueError):
        store = zarr.open(str(local_path), mode='r')
    return dict(store.attrs)


def read_existing_zarr_bounds(local_path: Path) -> Optional[Dict]:
    """Return `{west, south, east, north, resolution, crs}` from the store
    attrs that build_zarr_sparse_streaming wrote, falling back to
    coord-derived values when attrs are absent."""
    attrs = read_existing_zarr_attrs(local_path)
    required = ('bounds_west', 'bounds_south', 'bounds_east', 'bounds_north',
                'resolution', 'crs')
    if all(k in attrs for k in required):
        return {
            'west': float(attrs['bounds_west']),
            'south': float(attrs['bounds_south']),
            'east': float(attrs['bounds_east']),
            'north': float(attrs['bounds_north']),
            'resolution': float(attrs['resolution']),
            'crs': attrs['crs'],
        }
    # Coord-derived fallback. bzss convention:
    #   x_coords[0] = bounds_west,  x_coords[-1]+res = bounds_east
    #   y_coords[0] = bounds_north, y_coords[-1]-res = bounds_south
    try:
        store = zarr.open_consolidated(str(local_path), mode='r')
    except (KeyError, ValueError):
        store = zarr.open(str(local_path), mode='r')
    try:
        x_coords = np.asarray(store['x'])
        y_coords = np.asarray(store['y'])
    except KeyError:
        return None
    if len(x_coords) < 2 or len(y_coords) < 2:
        return None
    resolution = abs(float(x_coords[1] - x_coords[0]))
    return {
        'west': float(x_coords[0]),
        'east': float(x_coords[-1]) + resolution,
        'north': float(y_coords[0]),
        'south': float(y_coords[-1]) - resolution,
        'resolution': resolution,
        'crs': attrs.get('crs', 'EPSG:3413'),
    }


def bounds_contain(outer: Dict, inner: Dict, tol: float = 1e-6) -> bool:
    """Does `outer` fully contain `inner`? Used to decide append-in-place
    vs. expand-and-rebuild."""
    return (
        outer['west'] - tol <= inner['west']
        and outer['south'] - tol <= inner['south']
        and outer['east'] + tol >= inner['east']
        and outer['north'] + tol >= inner['north']
    )


def dump_zarr_slices_to_tiffs(
    local_zarr: Path,
    output_dir: Path,
    keep_dates: Optional[set] = None,
) -> List[Path]:
    """Extract every time slice from a Zarr store (built by
    build_zarr_sparse_streaming) as a georeferenced GeoTIFF in
    `output_dir`. Each file's mtime is set to the slice's datetime so
    the streaming script's mtime fallback works without a custom regex.

    Returns the list of TIFFs in time order. NaN-only slices are skipped
    (no data to preserve)."""
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        store = zarr.open_consolidated(str(local_zarr), mode='r')
    except (KeyError, ValueError):
        store = zarr.open(str(local_zarr), mode='r')

    attrs = dict(store.attrs)
    data = store['data']
    times = store['time'][:]
    n = data.shape[0]

    # bzss writes resolution/bounds_*/crs as root attrs. When those are
    # missing (Zarr built via a code path that didn't persist root attrs
    # in a way zarr.open can see), fall back to deriving from the coord
    # arrays — bzss's convention is x[0] = bounds_west, y[0] = bounds_north.
    x_coords = np.asarray(store['x'])
    y_coords = np.asarray(store['y'])
    if all(k in attrs for k in ('bounds_west', 'bounds_north', 'resolution')):
        west = float(attrs['bounds_west'])
        north = float(attrs['bounds_north'])
        resolution = float(attrs['resolution'])
    else:
        west = float(x_coords[0])
        north = float(y_coords[0])
        resolution = abs(float(x_coords[1] - x_coords[0])) if len(x_coords) > 1 \
            else abs(float(y_coords[1] - y_coords[0]))
    crs = attrs.get('crs', 'EPSG:3413')

    transform = from_origin(west, north, resolution, resolution)
    written: List[Path] = []

    for t_idx in range(n):
        slice_arr = np.asarray(data[t_idx])
        if not np.any(np.isfinite(slice_arr) & (slice_arr != 0)):
            continue

        ts = times[t_idx]
        if isinstance(ts, np.datetime64):
            dt = datetime.fromtimestamp(ts.astype('datetime64[s]').astype(int), tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(int(ts) / 1e9, tz=timezone.utc)

        # Sync mode: caller passes the set of acquisition dates that should
        # survive into the rebuilt Zarr. Slices for any other date get
        # silently skipped here so they don't make it into the combined
        # input list. NB: keep_dates is YYYYMMDD strings; we derive the
        # same form from the slice's datetime.
        if keep_dates is not None and dt.strftime("%Y%m%d") not in keep_dates:
            continue

        # Include trailing 'Z_' so the filename matches a typical satellite
        # time_regex like `_(?P<start_date>\d{8}T\d{6})Z_`. The streaming
        # script also has an mtime fallback via os.utime below, but matching
        # by name keeps the pre-flight validation happy.
        fname = f"_zarr_slice_{dt.strftime('%Y%m%dT%H%M%S')}Z_{t_idx:06d}.tif"
        path = output_dir / fname

        with rasterio.open(
            str(path), 'w',
            driver='GTiff',
            height=slice_arr.shape[0],
            width=slice_arr.shape[1],
            count=1,
            dtype=slice_arr.dtype,
            crs=crs,
            transform=transform,
            compress='deflate',
        ) as dst:
            dst.write(slice_arr, 1)

        # Stamp mtime to the slice datetime so the streaming script's
        # mtime fallback recovers the correct timestamp without a regex.
        ts_epoch = dt.timestamp()
        os.utime(path, (ts_epoch, ts_epoch))
        written.append(path)

    logger.info(f"Dumped {len(written)} non-empty slice(s) to {output_dir}")
    return written
