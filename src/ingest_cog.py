#!/usr/bin/env python3
"""
Worker stage: convert a single TIFF input to a Cloud Optimized GeoTIFF (COG).

Runs inside a MAAP DPS job container. Steps:
  1. Resolve input — S3 download or local file.
  2. Run gdal_translate with COG driver under a memory cap (low-memory streaming).
  3. Validate output is a COG.
  4. Upload COG to s3://<s3-bucket>/<s3-prefix>/.
  5. Emit a minimal STAC catalog (catalog.json) so the orchestrator can
     hand it to the MMGIS cataloging step.

Logic adapted from convert_to_cog_lowmem.py.
"""

import argparse
import gc
import json
import logging
import os
import shutil
import subprocess
import sys
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
        '--config', 'GDAL_NUM_THREADS', '1',
        '-r', resampling,
        '-q',
    ]
    if file_size_mb > 10:
        cmd.extend(['-co', f'OVERVIEW_COMPRESS={compress}'])

    logger.info(f"Converting to COG with {max_memory_mb}MB memory limit...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    except subprocess.TimeoutExpired:
        return False, f"Conversion timeout (>10 min): {input_file.name}"
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


def stage_input(input_s3: Optional[str], input_tiff: Optional[str],
                input_https: Optional[str], work_dir: Path,
                role_arn: Optional[str],
                earthdata_token_secret_name: Optional[str] = None) -> Path:
    """Resolve a single TIFF input from S3, an HTTPS+EDL URL, or a local path."""
    if input_tiff:
        local = Path(input_tiff)
        if not local.exists():
            raise FileNotFoundError(f"Local input not found: {input_tiff}")
        return local

    if input_s3:
        bucket, key = AWSUtils.parse_s3_path(input_s3)
        local_path = work_dir / Path(key).name
        s3_client = AWSUtils.get_s3_client(role_arn=role_arn, bucket_name=bucket)
        AWSUtils.download_s3_file(bucket, key, str(local_path), s3_client=s3_client)
        return local_path

    if input_https:
        from input_sources.cmr_tiff import download_https_edl
        return download_https_edl(input_https, work_dir, earthdata_token_secret_name)

    raise ValueError("One of --input-s3, --input-https, or --input-tiff is required")


def stage_inputs_batch(input_s3_urls: Optional[List[str]],
                       input_https_urls: Optional[List[str]],
                       work_dir: Path,
                       role_arn: Optional[str],
                       earthdata_token_secret_name: Optional[str] = None) -> List[Path]:
    """Download a list of S3 or HTTPS+EDL inputs to local disk."""
    paths: List[Path] = []
    if input_s3_urls:
        for url in input_s3_urls:
            paths.append(stage_input(url, None, None, work_dir, role_arn))
    elif input_https_urls:
        from input_sources.cmr_tiff import download_https_edl
        for url in input_https_urls:
            paths.append(download_https_edl(url, work_dir, earthdata_token_secret_name))
    return paths


def mosaic_tiffs(tiffs: List[Path], output_path: Path,
                 nodata: Optional[float] = None) -> Path:
    """Mosaic a list of GeoTIFFs into one. Overlapping pixels are handled
    by rasterio.merge's default ('first' — first-wins). For backscatter
    data first-wins is fine; if we need a mean/max blend later we can
    add a `--mosaic-method` flag."""
    if len(tiffs) == 1:
        # Trivial case — just copy through. mosaic_tiffs is a no-op.
        shutil.copy2(str(tiffs[0]), str(output_path))
        return output_path

    logger.info(f"Mosaicking {len(tiffs)} TIFF(s) → {output_path}")
    srcs = [rasterio.open(str(t)) for t in tiffs]
    try:
        merged, transform = rio_merge(srcs, nodata=nodata)
        profile = srcs[0].profile.copy()
        profile.update({
            'height': merged.shape[1],
            'width': merged.shape[2],
            'transform': transform,
            'count': merged.shape[0],
            'driver': 'GTiff',
            'compress': 'deflate',
            'bigtiff': 'IF_SAFER',
        })
        if nodata is not None:
            profile['nodata'] = nodata
        with rasterio.open(str(output_path), 'w', **profile) as dst:
            dst.write(merged)
    finally:
        for s in srcs:
            s.close()
    logger.info(f"Mosaic written: {output_path} ({output_path.stat().st_size / 1e6:.1f} MB)")
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


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Convert a single TIFF (S3 or local) to a COG and emit STAC."
    )
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--input-s3", help="S3 URL of the input TIFF")
    src.add_argument("--input-https",
                     help="HTTPS URL of the input TIFF (Earthdata-protected; "
                          "requires --earthdata-token-secret-name)")
    src.add_argument("--input-tiff", help="Local TIFF file path")
    src.add_argument("--input-s3-urls",
                     help="JSON list of S3 URLs to mosaic into a single daily COG.")
    src.add_argument("--input-https-urls",
                     help="JSON list of Earthdata-protected HTTPS URLs to "
                          "mosaic into a single daily COG.")
    parser.add_argument("--earthdata-token-secret-name", default=None,
                        help="MAAP secret name holding the EDL bearer token "
                             "(or username\\npassword). Only required for "
                             "--input-https or --input-https-urls.")
    parser.add_argument("--mosaic-date", default=None,
                        help="YYYYMMDD used to name the daily mosaic output. "
                             "Required when --input-s3-urls / --input-https-urls "
                             "is given.")

    parser.add_argument("--collection-id", required=True)
    parser.add_argument("--s3-bucket", required=True, help="Output bucket for the COG")
    parser.add_argument("--s3-prefix", default="", help="Output prefix within the bucket")
    parser.add_argument("--role-arn", help="AWS IAM Role ARN")

    parser.add_argument("--compress", default="DEFLATE",
                        choices=["DEFLATE", "LZW", "JPEG", "WEBP", "NONE"])
    parser.add_argument("--blocksize", type=int, default=512, choices=[256, 512, 1024])
    parser.add_argument("--max-memory", type=int, default=512)
    parser.add_argument("--resampling", default="nearest",
                        choices=["nearest", "bilinear", "cubic", "average"])
    parser.add_argument("--overview-resampling", default="average",
                        choices=["nearest", "average", "cubic", "mode"])
    parser.add_argument("--overwrite", action="store_true")
    parser.add_argument("--output", default="output",
                        help="Local working directory for staged input/output")

    parser.add_argument("--scp-host", default=None,
                        help="Optional: SCP delivery hostname/IP. When set, "
                             "the COG is also pushed via SCP after S3 upload.")
    parser.add_argument("--scp-port", type=int, default=22)
    parser.add_argument("--scp-user", default=None)
    parser.add_argument("--scp-remote-dir", default=None,
                        help="Remote directory; the COG is written as "
                             "<remote-dir>/<filename> (path flattened).")
    parser.add_argument("--scp-key-secret-name", default=None,
                        help="MAAP secret name holding the SSH private key (PEM).")

    args = parser.parse_args()

    work_dir = Path(args.output)
    work_dir.mkdir(parents=True, exist_ok=True)
    cog_dir = work_dir / "cog"
    cog_dir.mkdir(parents=True, exist_ok=True)
    stac_dir = work_dir / "stac"
    stac_dir.mkdir(parents=True, exist_ok=True)

    try:
        if args.input_s3_urls or args.input_https_urls:
            # Daily-mosaic mode: download all inputs, merge, then COG-convert.
            if not args.mosaic_date:
                raise ValueError("--mosaic-date is required when using "
                                 "--input-s3-urls or --input-https-urls")
            s3_urls = json.loads(args.input_s3_urls) if args.input_s3_urls else None
            https_urls = json.loads(args.input_https_urls) if args.input_https_urls else None
            staged_dir = work_dir / "staged"
            staged_dir.mkdir(parents=True, exist_ok=True)
            staged = stage_inputs_batch(
                s3_urls, https_urls, staged_dir, args.role_arn,
                earthdata_token_secret_name=args.earthdata_token_secret_name,
            )
            if not staged:
                raise RuntimeError("Batch staging produced 0 input files")

            mosaic_name = f"{args.collection_id}_{args.mosaic_date}_daily.tif"
            mosaic_path = work_dir / "mosaic" / mosaic_name
            mosaic_path.parent.mkdir(parents=True, exist_ok=True)
            mosaic_tiffs(staged, mosaic_path)
            input_path = mosaic_path
        else:
            input_path = stage_input(
                args.input_s3, args.input_tiff, args.input_https,
                work_dir, args.role_arn,
                earthdata_token_secret_name=args.earthdata_token_secret_name,
            )
        cog_path = cog_dir / f"{input_path.stem}_COG.tif"

        ok, msg = convert_to_cog_lowmem(
            input_path,
            cog_path,
            overwrite=args.overwrite,
            compress=args.compress,
            blocksize=args.blocksize,
            max_memory_mb=args.max_memory,
            resampling=args.resampling,
            overview_resampling=args.overview_resampling,
        )
        logger.info(msg)
        if not ok:
            return 2

        # Build STAC item. For daily mosaics, override the datetime to
        # the mosaic date at 00:00 UTC so the dated S3 key reflects the
        # acquisition date rather than rio_stac's "now" fallback.
        item = build_stac_item(cog_path, args.collection_id)
        if args.mosaic_date:
            mosaic_dt = datetime.strptime(args.mosaic_date, "%Y%m%d").replace(tzinfo=timezone.utc)
            item.datetime = mosaic_dt
        s3_key = build_dated_s3_key(
            args.s3_prefix, args.collection_id, item.datetime, cog_path.name
        )
        cog_s3_url = upload_cog_to_key(cog_path, args.s3_bucket, s3_key, args.role_arn)
        item.assets["asset"].href = cog_s3_url
        write_stac_catalog(item, args.collection_id, stac_dir)

        # Optional SCP delivery — failures are logged but non-fatal so the
        # STAC catalog stays consistent with what's in S3.
        if args.scp_host:
            from delivery import upload_via_scp
            ok = upload_via_scp(
                local_path=cog_path,
                host=args.scp_host,
                port=args.scp_port,
                user=args.scp_user,
                remote_dir=args.scp_remote_dir,
                key_secret_name=args.scp_key_secret_name,
            )
            if not ok:
                logger.warning("SCP delivery did not complete; continuing")

        logger.info(f"COG ingest complete: {cog_s3_url}")
        return 0

    except FileNotFoundError as e:
        logger.error(f"TERMINATED: {e}")
        return 6
    except ValueError as e:
        logger.error(f"TERMINATED: invalid argument: {e}")
        return 6
    except Exception as e:
        logger.error(f"TERMINATED: unexpected error: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
