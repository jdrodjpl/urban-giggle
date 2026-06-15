"""Worker: ingest a day's worth of Sentinel-1 EW GRD HH+HV granules into
a single daily mosaic COG in EPSG:3413.

Pipeline per granule:

  1. Download the SAFE ZIP from ASF via asf_search (EDL bearer-token auth).
  2. Apply σ⁰ calibration to the HH band (see src/s1_calibration.py).
  3. Reproject from GCP geometry to EPSG:3413 via gdalwarp.

Then for all granules in the day:

  4. Mosaic the per-granule EPSG:3413 TIFFs (reused from ingest_cog).
  5. Convert to COG (reused).
  6. Build STAC item, upload to S3 (reused).

Input is supplied as either:
  --input-https-urls JSON_LIST   # list of asf datapool ZIP URLs
  --cmr-short-name + --cmr-temporal-start/-end + --cmr-bbox + --filter
                                 # worker re-queries CMR itself (matches
                                 # the CMR-self-query mode our COG worker
                                 # uses for daily mosaics)

Auth: --earthdata-token-secret-name names a MAAP secret holding the EDL
bearer token. The token has to belong to a user who's authorized the
"Alaska Satellite Facility Data Access" application on
https://urs.earthdata.nasa.gov/profile.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import subprocess
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Optional

import pystac

import ingest_cog  # reuse mosaic_tiffs / convert_to_cog_lowmem / build_stac_item / ...
from common_utils import AWSUtils, MaapUtils, LoggingUtils
from s1_calibration import calibrate_granule

logger = logging.getLogger("ingest_s1grd")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# --------------------------------------------------------------------------
# Granule discovery (when worker is asked to re-query CMR itself)
# --------------------------------------------------------------------------

def cmr_granule_urls(
    short_names: List[str],
    temporal_start: str, temporal_end: str,
    bbox: str,
    filter_pattern: Optional[str],
    earthdata_token_secret_name: str,
    maap_instance=None,
) -> List[str]:
    """Re-run the runner-side CMR query against ASF — one or more
    collection short_names, optional polarization filter. Returns the
    list of ZIP URLs to download.
    """
    import earthaccess
    # Reuse the EDL login helper from the OPERA worker — sets
    # EARTHDATA_TOKEN env var from the MAAP secret.
    from input_sources.cmr_tiff import login_from_maap_secret
    login_from_maap_secret(earthdata_token_secret_name, maap_instance=maap_instance)
    earthaccess.login(strategy="environment")

    bbox_tuple = tuple(float(c) for c in bbox.split(","))
    urls: List[str] = []
    for sn in short_names:
        results = earthaccess.search_data(
            short_name=sn,
            temporal=(temporal_start, temporal_end),
            bounding_box=bbox_tuple,
        )
        for g in results:
            try:
                links = g.data_links(access="external") or []
            except Exception:
                links = []
            for url in links:
                name = os.path.basename(url.split("?", 1)[0])
                if not name.lower().endswith(".zip"):
                    continue
                if filter_pattern:
                    from fnmatch import fnmatch
                    if not fnmatch(name, filter_pattern):
                        continue
                urls.append(url)

    logger.info(f"CMR returned {len(urls)} matching .zip URL(s)")
    return urls


# --------------------------------------------------------------------------
# Per-granule download + calibrate + reproject
# --------------------------------------------------------------------------

def download_granule_via_asf(url: str, dest_dir: Path,
                              edl_token: str) -> Path:
    """Download one SAFE ZIP via asf_search. Returns the local path."""
    import asf_search

    session = asf_search.ASFSession().auth_with_token(edl_token)
    dest_dir.mkdir(parents=True, exist_ok=True)
    local = dest_dir / os.path.basename(url.split("?", 1)[0])
    asf_search.download_url(url, path=str(dest_dir), session=session)
    if not local.exists():
        raise RuntimeError(f"asf_search.download_url claimed success but "
                            f"{local} doesn't exist")
    return local


def reproject_to_3413(in_tiff: Path, out_tiff: Path,
                      resolution_m: float = 40.0) -> Path:
    """gdalwarp the GCP-bearing intermediate σ⁰ TIFF to EPSG:3413."""
    out_tiff.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "gdalwarp",
        "-t_srs", "EPSG:3413",
        "-tr", str(resolution_m), str(resolution_m),
        "-r", "nearest",
        "-multi", "-wo", "NUM_THREADS=ALL_CPUS",
        "-of", "GTiff", "-overwrite",
        "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES", "-co", "BIGTIFF=IF_SAFER",
        str(in_tiff), str(out_tiff),
    ]
    logger.info(f"gdalwarp → {out_tiff.name}")
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    if r.returncode != 0:
        raise RuntimeError(f"gdalwarp failed for {in_tiff.name}: {r.stderr[-500:]}")
    return out_tiff


def process_granule(zip_path: Path, work_dir: Path,
                    polarization: str = "HH") -> Path:
    """Calibrate + reproject one downloaded SAFE ZIP. Returns the path
    of the reprojected EPSG:3413 σ⁰ TIFF, ready to mosaic."""
    cal_dir = work_dir / "calibrated"
    geo_dir = work_dir / "geocoded"

    cal_tiff = cal_dir / f"{zip_path.stem}_{polarization.upper()}_sigma0.tif"
    calibrate_granule(zip_path, polarization, cal_tiff)

    geo_tiff = geo_dir / f"{zip_path.stem}_{polarization.upper()}_3413.tif"
    reproject_to_3413(cal_tiff, geo_tiff)

    # Free the intermediate calibrated TIFF — it's ~1-2 GiB.
    try:
        cal_tiff.unlink()
    except OSError:
        pass
    return geo_tiff


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def _resolve_edl_token(secret_name: str, maap_instance=None) -> str:
    """Pull the EDL bearer token from a MAAP secret. Token-only format:
    either single-line `token=...` or just the raw token string."""
    maap = maap_instance or MaapUtils.get_maap_instance()
    secret = maap.secrets.get_secret(secret_name)
    body = secret if isinstance(secret, str) else secret.get("value", "")
    lines = [ln for ln in body.splitlines() if ln.strip()]
    if body.strip().startswith("token="):
        return body.split("=", 1)[-1].strip()
    if len(lines) == 1 and "=" not in lines[0]:
        return lines[0].strip()
    raise RuntimeError(
        f"S1 GRD worker needs a token-format EDL secret. {secret_name!r} "
        f"doesn't look like one (first line: {lines[0][:30] if lines else '<empty>'})."
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)

    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--input-https-urls",
                     help="JSON list of ASF datapool ZIP URLs")
    src.add_argument("--cmr-short-name", action="append",
                     help="CMR collection short_name (repeatable; "
                          "worker queries CMR itself)")

    p.add_argument("--cmr-temporal-start")
    p.add_argument("--cmr-temporal-end")
    p.add_argument("--cmr-bbox")
    p.add_argument("--filter", dest="filter_pattern",
                   help="Glob filter on granule names (e.g. *_1SDH_* for dual-pol HH)")

    p.add_argument("--polarization", default="HH",
                   choices=["HH", "HV", "VV", "VH"],
                   help="Polarization band to ingest (default HH)")
    p.add_argument("--mosaic-date", required=True,
                   help="YYYYMMDD label for the output COG and STAC datetime")

    p.add_argument("--earthdata-token-secret-name", required=True,
                   help="MAAP secret holding the EDL bearer token")

    p.add_argument("--collection-id", required=True)
    p.add_argument("--s3-bucket", required=True)
    p.add_argument("--s3-prefix", default="")
    p.add_argument("--role-arn")

    p.add_argument("--compress", default="DEFLATE")
    p.add_argument("--blocksize", type=int, default=512)
    p.add_argument("--max-memory", type=int, default=4096)
    p.add_argument("--resampling", default="nearest")
    p.add_argument("--overview-resampling", default="average")
    p.add_argument("--overwrite", action="store_true")

    p.add_argument("--output", default="output")
    args = p.parse_args()

    work_dir = Path(args.output)
    work_dir.mkdir(parents=True, exist_ok=True)
    zip_dir = work_dir / "zips"

    maap = MaapUtils.get_maap_instance()
    edl_token = _resolve_edl_token(args.earthdata_token_secret_name, maap)

    # --- 1. Resolve granule URLs ---
    if args.input_https_urls:
        urls = json.loads(args.input_https_urls)
    else:
        if not (args.cmr_short_name and args.cmr_temporal_start
                and args.cmr_temporal_end and args.cmr_bbox):
            print("--cmr-short-name with --cmr-temporal-* and --cmr-bbox required "
                  "when not passing --input-https-urls", file=sys.stderr)
            return 6
        urls = cmr_granule_urls(
            short_names=args.cmr_short_name,
            temporal_start=args.cmr_temporal_start,
            temporal_end=args.cmr_temporal_end,
            bbox=args.cmr_bbox,
            filter_pattern=args.filter_pattern,
            earthdata_token_secret_name=args.earthdata_token_secret_name,
            maap_instance=maap,
        )
    if not urls:
        raise RuntimeError("No S1 GRD granule URLs to process")
    logger.info(f"{len(urls)} granule(s) to process")

    # --- 2. Per-granule download → calibrate → reproject ---
    # Process one at a time and unlink the ZIP after to keep disk usage bounded.
    geocoded: List[Path] = []
    for i, url in enumerate(urls, 1):
        logger.info(f"[{i}/{len(urls)}] {os.path.basename(url.split('?',1)[0])}")
        zip_path = download_granule_via_asf(url, zip_dir, edl_token)
        try:
            geo_tiff = process_granule(zip_path, work_dir, args.polarization)
            geocoded.append(geo_tiff)
        finally:
            try:
                zip_path.unlink()
            except OSError:
                pass

    if not geocoded:
        raise RuntimeError("No granules survived calibration / geocoding")
    logger.info(f"{len(geocoded)} granule(s) geocoded to EPSG:3413; mosaicking")

    # --- 3. Mosaic all per-granule TIFFs into one daily raster ---
    mosaic_path = work_dir / f"{args.collection_id}_{args.mosaic_date}_daily.tif"
    ingest_cog.mosaic_tiffs(geocoded, mosaic_path,
                             target_crs="EPSG:3413",
                             target_res=40.0,
                             nodata=float("nan"))

    # --- 4. COG conversion (reuse) ---
    cog_path = work_dir / f"{args.collection_id}_{args.mosaic_date}_daily_COG.tif"
    ok, msg = ingest_cog.convert_to_cog_lowmem(
        input_file=mosaic_path,
        output_file=cog_path,
        overwrite=True,
        compress=args.compress,
        blocksize=args.blocksize,
        max_memory_mb=args.max_memory,
        resampling=args.resampling,
        overview_resampling=args.overview_resampling,
    )
    if not ok:
        raise RuntimeError(f"COG conversion failed: {msg}")
    logger.info(f"COG: {cog_path}  ({cog_path.stat().st_size / 1e9:.1f} GiB)")

    # --- 5. STAC item ---
    item = ingest_cog.build_stac_item(cog_path, args.collection_id)
    dt = datetime.strptime(args.mosaic_date, "%Y%m%d").replace(tzinfo=timezone.utc)
    item.datetime = dt

    # --- 6. S3 upload ---
    s3_key = ingest_cog.build_dated_s3_key(
        args.s3_prefix, args.collection_id, item.datetime, cog_path.name,
    )
    s3_url = ingest_cog.upload_cog_to_key(
        cog_path, args.s3_bucket, s3_key, args.role_arn,
    )
    item.assets["asset"].href = s3_url
    logger.info(f"uploaded → {s3_url}")

    # --- 7. STAC catalog ---
    stac_dir = work_dir / "stac"
    ingest_cog.write_stac_catalog(item, args.collection_id, stac_dir)
    logger.info(f"STAC catalog → {stac_dir}/catalog.json")

    return 0


if __name__ == "__main__":
    sys.exit(main())
