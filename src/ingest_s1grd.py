"""Worker: ingest a day's worth of Sentinel-1 EW GRD HH+HV granules into
a single daily mosaic COG in EPSG:3413.

Pipeline per granule:

  1. Download the SAFE ZIP from ASF via asf_search (EDL bearer-token auth).
  2. Apply σ⁰ calibration to the HH band (see src/s1_calibration.py).
  3. Reproject from GCP geometry to EPSG:3413 via gdalwarp.

Then for all granules in the day:

  4. Mosaic the per-granule EPSG:3413 TIFFs (reused from cog_helpers).
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

import cog_helpers  # reuse mosaic_tiffs / convert_to_cog_lowmem / build_stac_item / ...
from common_utils import AWSUtils, MaapUtils, LoggingUtils
from s1_calibration import (
    read_uncalib_dn,
    find_calibration_xml,
    parse_calibration_lut,
    apply_calibration,
    write_calibrated_tiff,
    LUT_ELEMENT_BY_CALIBRATION,
)

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
                              edl_creds: dict) -> Path:
    """Download one SAFE ZIP via asf_search. `edl_creds` is either
    {"token": "..."} or {"username": "...", "password": "..."} —
    whichever shape `_resolve_edl_creds` produced from the MAAP secret.
    Returns the local path."""
    import asf_search

    session = asf_search.ASFSession()
    if "token" in edl_creds:
        session.auth_with_token(edl_creds["token"])
    else:
        session.auth_with_creds(edl_creds["username"], edl_creds["password"])
    dest_dir.mkdir(parents=True, exist_ok=True)
    local = dest_dir / os.path.basename(url.split("?", 1)[0])
    asf_search.download_url(url, path=str(dest_dir), session=session)
    if not local.exists():
        raise RuntimeError(f"asf_search.download_url claimed success but "
                            f"{local} doesn't exist")
    return local


def reproject_to_3413(in_tiff: Path, out_tiff: Path,
                      resolution_m: float = 40.0) -> Path:
    """gdalwarp the GCP-bearing intermediate σ⁰ TIFF to EPSG:3413.

    `-order 2` forces a 2nd-order polynomial fit to the GCPs instead of
    letting GDAL auto-pick (which often selects TPS for 400+ GCPs — TPS
    is much slower and was timing out at 30 min on the worker even
    though the Jupyter test ran in 10 sec).
    """
    out_tiff.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        "gdalwarp",
        "-t_srs", "EPSG:3413",
        "-tr", str(resolution_m), str(resolution_m),
        "-r", "nearest",
        "-order", "2",
        "-multi",
        "-wo", "NUM_THREADS=ALL_CPUS",
        "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
        "--config", "GDAL_CACHEMAX", "4096",
        "-of", "GTiff", "-overwrite",
        "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES",
        "-co", "NUM_THREADS=ALL_CPUS",
        str(in_tiff), str(out_tiff),
    ]
    logger.info(f"gdalwarp → {out_tiff.name}")
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=3600)
    if r.returncode != 0:
        raise RuntimeError(f"gdalwarp failed for {in_tiff.name}: {r.stderr[-500:]}")
    return out_tiff


def process_granule(zip_path: Path, work_dir: Path,
                    polarization: str = "HH",
                    calibrations: tuple = ("sigma0",)) -> dict:
    """Calibrate + reproject one downloaded SAFE ZIP for each requested
    calibration. Returns `{calibration: geocoded_tiff_path}`.

    DN is read once from the SAFE bundle; the LUT and pixel-wise
    application run once per calibration. Each calibration becomes a
    separate intermediate TIFF and gets gdalwarp'd independently. The
    intermediate calibrated TIFFs are unlinked after warping to keep
    disk usage bounded — only the geocoded outputs remain.
    """
    cal_dir = work_dir / "calibrated"
    geo_dir = work_dir / "geocoded"

    # Read DN + GCPs once (this is the expensive disk I/O).
    dn, gcps, _, _ = read_uncalib_dn(zip_path, polarization)
    cal_xml = find_calibration_xml(zip_path, polarization.lower())

    geocoded: dict = {}
    for cal in calibrations:
        cal_tiff = cal_dir / f"{zip_path.stem}_{polarization.upper()}_{cal}.tif"
        geo_tiff = geo_dir / f"{zip_path.stem}_{polarization.upper()}_{cal}_3413.tif"

        lines, pixels, lut = parse_calibration_lut(zip_path, cal_xml, cal)
        calibrated = apply_calibration(dn, lines, pixels, lut)
        write_calibrated_tiff(calibrated, gcps, cal_tiff)

        reproject_to_3413(cal_tiff, geo_tiff)
        # Intermediate calibrated TIFF is ~1-2 GiB; drop it now that
        # gdalwarp has produced the geocoded EPSG:3413 version.
        try:
            cal_tiff.unlink()
        except OSError:
            pass
        geocoded[cal] = geo_tiff

    return geocoded


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def _resolve_edl_creds(secret_name: str, maap_instance=None) -> dict:
    """Pull EDL credentials from a MAAP secret. Returns either
    `{"token": "..."}` for a token-format secret or
    `{"username": "...", "password": "..."}` for the two-line
    username/password format. Both work with asf_search.
    """
    maap = maap_instance or MaapUtils.get_maap_instance()
    secret = maap.secrets.get_secret(secret_name)
    body = secret if isinstance(secret, str) else secret.get("value", "")
    lines = [ln for ln in body.splitlines() if ln.strip()]

    if body.strip().startswith("token="):
        return {"token": body.split("=", 1)[-1].strip()}
    if len(lines) == 1 and "=" not in lines[0]:
        return {"token": lines[0].strip()}
    if len(lines) >= 2:
        return {"username": lines[0].strip(), "password": lines[1].strip()}
    raise RuntimeError(
        f"EDL secret {secret_name!r} format not recognized "
        f"(first line: {lines[0][:30] if lines else '<empty>'})."
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

    p.add_argument("--calibrations", default="sigma0",
                   help="Comma-separated list of calibration conventions to emit "
                        "(any of sigma0, beta0, gamma0). Each produces its own "
                        "per-day mosaic COG in its own collection. Default sigma0 only.")
    p.add_argument("--collection-id-template",
                   help="Collection-id template where {calibration} is substituted, "
                        "e.g. 'frozon-s1-ew-hh-{calibration}-daily'. Required when "
                        "--calibrations names more than one convention.")
    p.add_argument("--collection-id",
                   help="Single collection id. Used only when --calibrations is a "
                        "single convention; ignored otherwise.")
    p.add_argument("--s3-bucket", required=True)
    p.add_argument("--s3-prefix", default="")
    p.add_argument("--role-arn")

    p.add_argument("--compress", default="DEFLATE")
    p.add_argument("--blocksize", type=int, default=512)
    p.add_argument("--max-memory", type=int, default=4096)
    p.add_argument("--resampling", default="nearest")
    p.add_argument("--overview-resampling", default="average")
    p.add_argument("--overwrite", action="store_true")

    p.add_argument("--output", default="output",
                   help="DPS-persisted output dir; only the STAC catalog is written here.")
    p.add_argument("--scratch-dir", default="scratch",
                   help="Working dir for zips / calibrated / geocoded / mosaic / "
                        "COG. NOT persisted by DPS; deleted on success unless --keep-scratch.")
    p.add_argument("--keep-scratch", action="store_true",
                   help="Keep the scratch dir for debugging.")
    args = p.parse_args()

    # `output/` (out_dir) is the only dir MAAP DPS persists to S3 — keep it to
    # just the STAC catalog (consumed by the MMGIS cataloging step). Heavy
    # intermediates go to scratch/, which DPS does not persist, so dps_output/
    # doesn't accrue tens of GB of geocoded tiffs + mosaics per daily job.
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    scratch_dir = Path(args.scratch_dir)
    scratch_dir.mkdir(parents=True, exist_ok=True)
    zip_dir = scratch_dir / "zips"

    maap = MaapUtils.get_maap_instance()
    edl_creds = _resolve_edl_creds(args.earthdata_token_secret_name, maap)
    logger.info(f"EDL auth via {'token' if 'token' in edl_creds else 'username/password'}")

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

    # Resolve calibrations and their per-cal collection-ids.
    calibrations = tuple(c.strip() for c in args.calibrations.split(",") if c.strip())
    unknown = [c for c in calibrations if c not in LUT_ELEMENT_BY_CALIBRATION]
    if unknown:
        raise ValueError(
            f"Unknown calibration(s) {unknown}; "
            f"valid: {list(LUT_ELEMENT_BY_CALIBRATION)}"
        )
    if len(calibrations) > 1:
        if not args.collection_id_template:
            raise ValueError(
                "Multi-calibration requires --collection-id-template "
                "(e.g. 'frozon-s1-ew-hh-{calibration}-daily')"
            )
        collection_ids = {
            c: args.collection_id_template.format(calibration=c)
            for c in calibrations
        }
    else:
        # Single calibration — either template OR --collection-id literal.
        if args.collection_id_template:
            collection_ids = {
                calibrations[0]: args.collection_id_template.format(calibration=calibrations[0])
            }
        elif args.collection_id:
            collection_ids = {calibrations[0]: args.collection_id}
        else:
            raise ValueError(
                "Need either --collection-id-template or --collection-id "
                "for single-calibration runs."
            )
    logger.info(f"Calibrations to emit: {calibrations}")
    for cal, cid in collection_ids.items():
        logger.info(f"  {cal} → {cid}")

    # --- 2. Per-granule download → calibrate (each cal) → reproject (each cal) ---
    # Process one ZIP at a time, unlink after, and collect per-calibration
    # geocoded tiffs in parallel lists.
    geocoded_by_cal: dict = {c: [] for c in calibrations}
    for i, url in enumerate(urls, 1):
        logger.info(f"[{i}/{len(urls)}] {os.path.basename(url.split('?',1)[0])}")
        zip_path = download_granule_via_asf(url, zip_dir, edl_creds)
        try:
            results = process_granule(
                zip_path, scratch_dir, args.polarization,
                calibrations=calibrations,
            )
            for cal, path in results.items():
                geocoded_by_cal[cal].append(path)
        finally:
            try:
                zip_path.unlink()
            except OSError:
                pass

    for cal, geocoded in geocoded_by_cal.items():
        if not geocoded:
            raise RuntimeError(f"No granules survived for calibration {cal}")
    logger.info(
        f"{len(geocoded_by_cal[calibrations[0]])} granule(s) per calibration "
        f"geocoded to EPSG:3413; mosaicking each."
    )

    # --- 3-7. Per-calibration: mosaic → COG → upload → STAC. ---
    stac_dir = out_dir / "stac"
    dt = datetime.strptime(args.mosaic_date, "%Y%m%d").replace(tzinfo=timezone.utc)
    for cal in calibrations:
        cid = collection_ids[cal]
        geocoded = geocoded_by_cal[cal]
        logger.info(f"=== calibration {cal} ({cid}) ===")

        mosaic_path = scratch_dir / f"{cid}_{args.mosaic_date}_daily.tif"
        cog_helpers.mosaic_tiffs(geocoded, mosaic_path,
                                 target_crs="EPSG:3413",
                                 target_res=40.0,
                                 nodata=float("nan"))

        cog_path = scratch_dir / f"{cid}_{args.mosaic_date}_daily_COG.tif"
        ok, msg = cog_helpers.convert_to_cog_lowmem(
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
            raise RuntimeError(f"COG conversion failed for {cal}: {msg}")
        logger.info(f"COG ({cal}): {cog_path}  ({cog_path.stat().st_size / 1e9:.1f} GiB)")

        item = cog_helpers.build_stac_item(cog_path, cid)
        item.datetime = dt
        s3_key = cog_helpers.build_dated_s3_key(
            args.s3_prefix, cid, item.datetime, cog_path.name,
        )
        s3_url = cog_helpers.upload_cog_to_key(
            cog_path, args.s3_bucket, s3_key, args.role_arn,
        )
        item.assets["asset"].href = s3_url
        logger.info(f"uploaded ({cal}) → {s3_url}")

        cog_helpers.write_stac_catalog(item, cid, stac_dir / cal)

        # Drop the mosaic + COG now that they're in S3. Frees ~10-20 GiB
        # before the next calibration starts its mosaic.
        for p in (mosaic_path, cog_path):
            try:
                p.unlink()
            except OSError:
                pass
    logger.info(f"STAC catalog → {stac_dir}/catalog.json")

    # Heavy intermediates (zips/calibrated/geocoded/mosaic/COG) live in scratch/,
    # outside the DPS-persisted output/, so they never reach S3 — drop them now.
    if not args.keep_scratch:
        shutil.rmtree(scratch_dir, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
