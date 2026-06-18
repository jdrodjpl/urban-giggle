"""Shared COG-building helpers used by every Frozon ingest worker.

Pulled out of the retired OPERA RTC worker (`src/ingest_cog.py`).
Pure-library — no entry point, no argparse. Workers compose these
functions in their own per-source pipelines:

  - check_if_cog / get_file_info — diagnostic
  - convert_to_cog_lowmem — gdal_translate -of COG with a cache cap
  - mosaic_tiffs — gdalwarp-based per-day mosaic to a target CRS
  - build_stac_item / build_dated_s3_key / write_stac_catalog — STAC emit
  - upload_cog_to_key — S3 upload

See `src/ingest_s1grd.py` for a worker that uses these.
"""

import gc
import logging
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional, Tuple

import pystac
import rasterio
from rasterio.merge import merge as rio_merge
import rio_stac

from common_utils import AWSUtils

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s'
)
logger = logging.getLogger(__name__)


def check_if_cog(file_path: Path) -> bool:
    """Check whether a file is already a COG (tiled + has overviews)."""
    try:
        result = subprocess.run(
            ['gdalinfo', str(file_path)],
            capture_output=True, text=True, check=True
        )
        output = result.stdout
        has_tiles = False
        for line in output.split('\n'):
            if 'Block=' in line and 'x' in line:
                parts = line.split('Block=')[1].split()[0]
                dims = parts.split('x')
                if len(dims) == 2:
                    width, height = dims
                    has_tiles = (width == height and int(width) <= 1024)
        has_overviews = 'Overviews:' in output or 'Overviews' in output
        return has_tiles and has_overviews
    except subprocess.CalledProcessError:
        return False


def get_file_info(file_path: Path) -> dict:
    """Read width/height/bands from gdalinfo -json. Best-effort."""
    try:
        result = subprocess.run(
            ['gdalinfo', '-json', str(file_path)],
            capture_output=True, text=True, check=True
        )
        info = json.loads(result.stdout)
        size = info.get('size', [0, 0])
        return {
            'width': size[0],
            'height': size[1],
            'count': len(info.get('bands', [])),
            'crs': info.get('coordinateSystem', {}).get('wkt', 'unknown'),
        }
    except Exception as e:
        logger.debug(f"Could not read detailed file info: {e}")
        return {'width': 0, 'height': 0, 'count': 1}


def convert_to_cog_lowmem(
    input_file: Path,
    output_file: Path,
    overwrite: bool = False,
    compress: str = 'DEFLATE',
    blocksize: int = 512,
    max_memory_mb: int = 512,
    resampling: str = 'nearest',
    overview_resampling: str = 'average',
    overview_levels: Optional[List[int]] = None,
) -> Tuple[bool, str]:
    """Convert a single TIFF to COG using gdal_translate with a memory cap."""
    if overview_levels is None:
        overview_levels = [2, 4, 8, 16, 32]

    if output_file.exists() and not overwrite:
        return True, f"Output already exists (skipped): {output_file.name}"

    if check_if_cog(input_file):
        logger.info(f"{input_file.name} is already a COG, copying through")
        try:
            shutil.copy2(str(input_file), str(output_file))
            return True, f"Already COG, copied: {input_file.name}"
        except Exception as e:
            return False, f"Failed to copy COG: {e}"

    file_info = get_file_info(input_file)
    if file_info['width'] > 0 and file_info['height'] > 0:
        file_size_mb = file_info['width'] * file_info['height'] * 4 / (1024 * 1024)
        logger.info(f"Processing {input_file.name} (~{file_size_mb:.1f} MB uncompressed)")
    else:
        file_size_mb = 0
        logger.info(f"Processing {input_file.name}")

    cmd = [
        'gdal_translate',
        str(input_file),
        str(output_file),
        '-of', 'COG',
        '-co', f'COMPRESS={compress}',
        '-co', f'BLOCKSIZE={blocksize}',
        '-co', f'OVERVIEW_RESAMPLING={overview_resampling}',
        '-co', 'BIGTIFF=IF_SAFER',
        '-co', 'NUM_THREADS=ALL_CPUS',
        '-co', 'TILED=YES',
        '--config', 'GDAL_CACHEMAX', str(max_memory_mb),
        '--config', 'GDAL_NUM_THREADS', 'ALL_CPUS',
        '-r', resampling,
        '-q',
    ]
    if file_size_mb > 10:
        cmd.extend(['-co', f'OVERVIEW_COMPRESS={compress}'])

    logger.info(f"Converting to COG with {max_memory_mb}MB memory limit...")
    try:
        # 2-hour cap — full-Arctic daily mosaics (~30 GB compressed, ~180 GB
        # uncompressed) need substantially more than the original 10 min.
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
    except subprocess.TimeoutExpired:
        return False, f"Conversion timeout (>2 hr): {input_file.name}"
    finally:
        gc.collect()

    if result.returncode != 0:
        msg = f"GDAL conversion failed for {input_file.name}"
        if result.stderr:
            msg += f"\nError: {result.stderr}"
        return False, msg

    if not output_file.exists():
        return False, f"Output file was not created: {output_file.name}"

    if not check_if_cog(output_file):
        logger.warning(f"Output may not be a valid COG: {output_file.name}")

    output_size_mb = output_file.stat().st_size / (1024 * 1024)
    ratio = file_size_mb / output_size_mb if output_size_mb > 0 else 0
    return True, (f"Converted: {input_file.name} -> {output_file.name} "
                  f"({output_size_mb:.1f} MB, {ratio:.1f}x compression)")


def mosaic_tiffs(tiffs: List[Path], output_path: Path,
                 target_crs: str = "EPSG:3413",
                 target_res: float = 30.0,
                 nodata: Optional[float] = None) -> Path:
    """Mosaic + reproject to `target_crs` via gdalwarp.

    Defaults to EPSG:3413 (Arctic Polar Stereographic) since OPERA RTC
    tiles span many UTM zones — rasterio.merge requires a single CRS,
    so we let gdalwarp do reprojection + merge in one pass. For
    non-polar use cases, pass a different target_crs.

    Single-input case copies through without reprojection.
    """
    if len(tiffs) == 1:
        shutil.copy2(str(tiffs[0]), str(output_path))
        return output_path

    logger.info(
        f"Mosaicking {len(tiffs)} TIFF(s) via gdalwarp "
        f"→ {target_crs} @ {target_res}m → {output_path}"
    )

    if output_path.exists():
        output_path.unlink()

    # Pass input paths directly as args. Linux execve allows ~2MB of argv,
    # so 2700 inputs × ~300 chars ≈ 800KB fits.
    cmd = [
        "gdalwarp",
        "-t_srs", target_crs,
        "-tr", str(target_res), str(target_res),
        "-r", "nearest",
        "-multi",
        "-wo", "NUM_THREADS=ALL_CPUS",
        "-co", "COMPRESS=DEFLATE",
        "-co", "BIGTIFF=IF_SAFER",
        "-co", "TILED=YES",
    ]
    if nodata is not None:
        cmd += ["-dstnodata", str(nodata)]
    cmd += [str(t) for t in tiffs]
    cmd.append(str(output_path))

    logger.info(f"gdalwarp cmd: {' '.join(cmd[:13])} ... [{len(tiffs)} inputs] {output_path}")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
    if result.returncode != 0:
        logger.error(f"gdalwarp stderr (last 1500): {result.stderr[-1500:]}")
        raise RuntimeError(f"gdalwarp failed (exit {result.returncode})")

    logger.info(
        f"Mosaic written: {output_path} "
        f"({output_path.stat().st_size / 1e6:.1f} MB)"
    )
    return output_path


def build_stac_item(cog_local_path: Path, collection_id: str) -> pystac.Item:
    """Build a STAC item from the local COG. Asset href is the local path
    for now and will be rewritten to the final S3 URL after upload.

    rio_stac extracts datetime from TIFF tags (TIFFTAG_DATETIME or similar)
    when present; otherwise it falls back to the current UTC time."""
    item = rio_stac.create_stac_item(
        source=str(cog_local_path),
        id=cog_local_path.stem,
        collection=collection_id,
        asset_name="asset",
        asset_media_type=pystac.MediaType.COG,
        with_proj=True,
        with_raster=True,
    )
    if item.datetime is None:
        raise RuntimeError(
            f"Could not derive a datetime for {cog_local_path.name} — "
            "required for the dated S3 layout"
        )
    return item


def build_dated_s3_key(s3_prefix: str, collection_id: str,
                       item_dt, filename: str) -> str:
    """Compose an S3 key as <prefix>/<collection-id>/YYYY/MM/DD/<filename>.

    `s3_prefix` may be empty; it's prepended verbatim. The collection_id is
    sanitized only for path-illegal characters (slashes), not normalized."""
    safe_collection = collection_id.replace('/', '_')
    parts = []
    if s3_prefix:
        parts.append(s3_prefix.strip('/'))
    parts.extend([
        safe_collection,
        f"{item_dt.year:04d}",
        f"{item_dt.month:02d}",
        f"{item_dt.day:02d}",
        filename,
    ])
    return '/'.join(parts)


def upload_cog_to_key(cog_path: Path, bucket: str, key: str,
                      role_arn: Optional[str]) -> str:
    """Upload the COG to a fully-qualified S3 key and return its s3:// URL."""
    s3_client = AWSUtils.get_s3_client(role_arn=role_arn, bucket_name=bucket)
    return AWSUtils.upload_to_s3(str(cog_path), bucket, key, s3_client=s3_client)


def write_stac_catalog(item: pystac.Item, collection_id: str,
                       output_dir: Path) -> Path:
    """Wrap an item (with its final asset href already set) in a
    Catalog → Collection → Item structure and serialize to disk. The
    orchestrator picks up the resulting catalog.json from DPS output."""
    collection = pystac.Collection(
        id=collection_id,
        description=f"Frozon ISS COG products for {collection_id}",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[-180.0, -90.0, 180.0, 90.0]]),
            temporal=pystac.TemporalExtent([[item.datetime, item.datetime]]),
        ),
        license="proprietary",
    )
    collection.add_item(item)

    catalog = pystac.Catalog(
        id=f"frozon-cog-{collection_id}",
        description=f"Frozon ISS COG ingest catalog for {collection_id}",
    )
    catalog.add_child(collection)

    catalog.normalize_hrefs(str(output_dir))
    catalog.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

    catalog_path = output_dir / "catalog.json"
    logger.info(f"Wrote STAC catalog to {catalog_path}")
    return catalog_path

