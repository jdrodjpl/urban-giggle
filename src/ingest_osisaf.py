"""Worker: ingest one OSI SAF daily sea-ice product (concentration / type /
edge) for the Northern Hemisphere into a single COG in canonical EPSG:3413.

OSI SAF is the EUMETSAT Ocean & Sea Ice SAF. Unlike the OPERA / S1 sources
this repo started with, OSI SAF is **not** on NASA CMR and needs no Earthdata
Login:

  * Distribution: anonymous HTTP from the Norwegian Met THREDDS server
    (thredds.met.no). Filenames are fully deterministic by date, so there is
    no granule search — the worker constructs the URL for its (product, date)
    and downloads the single NetCDF.
  * One NetCDF == one full-hemisphere grid already, so there is no mosaic
    step — just extract one variable, reproject, COG-ify.

Per (product, date) the worker:

  1. Build the THREDDS URL and download the NetCDF (anonymous HTTP).
  2. Extract the product variable to a GeoTIFF, *forcing* the source grid
     (EPSG:3411, NSIDC Hughes-ellipsoid polar stereographic) onto the clean
     10 km NH "polstere-100" geotransform. We force it because GDAL's netCDF
     driver reads a half-pixel-shifted, slightly-off geotransform for the
     type/edge files (it derives it from cell-centre xc/yc without the
     half-pixel offset); conc reads clean. Forcing makes all three identical.
  3. Reproject (gdalwarp) EPSG:3411 -> canonical EPSG:3413 10 km grid.
     Continuous concentration uses bilinear; categorical type/edge use
     nearest so class codes are never blended.
  4. COG-ify (reuse cog_helpers.convert_to_cog_lowmem).
  5. Upload the COG to its dated S3 key. No STAC catalog is written — OSI SAF
     has no MMGIS cataloging step to consume one, and the class/flag semantics
     already live in the COG band metadata.

Value semantics (see DATA_OSISAF.md):
  * ice_conc : Int16 stored, scale 0.01 -> we emit Float32 percent (0..100),
               NoData NaN.
  * ice_type : classes {1 open_water, 2 first_year_ice, 3 multi_year_ice,
               4 ambiguous}, NoData -1.
  * ice_edge : classes {1 open_water, 2 open_ice, 3 close_ice}, NoData -1.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import rasterio
from rasterio.transform import Affine

import cog_helpers  # reuse convert_to_cog_lowmem / build_stac_item / upload / catalog
from common_utils import AWSUtils  # noqa: F401  (kept for parity; uploads go via cog_helpers)

logger = logging.getLogger("ingest_osisaf")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# --------------------------------------------------------------------------
# Grid + product constants
# --------------------------------------------------------------------------

# Source: NSIDC Sea Ice Polar Stereographic North (Hughes 1980 ellipsoid,
# lat_ts=70, lon_0=-45). The OSI SAF NH "polstere-100" grid is exactly this.
SRC_EPSG = "EPSG:3411"
# Target: WGS84 polar stereographic (Frozon canonical Arctic grid).
TGT_EPSG = "EPSG:3413"

# The clean, edge-aligned 10 km NH polstere-100 grid. 760 cols x 1120 rows.
# Affine: (xres, 0, ulx, 0, yres, uly) — north-up, so yres is negative.
SRC_TRANSFORM = Affine(10000.0, 0.0, -3850000.0, 0.0, -10000.0, 5850000.0)
SRC_WIDTH = 760
SRC_HEIGHT = 1120

# Canonical EPSG:3413 target grid (same numeric extent; 3411->3413 is a small
# datum shift so the output footprint is effectively identical). te is
# minx,miny,maxx,maxy; tr is 10 km. Produces a clean 760x1120 raster.
TGT_TE = (-3850000.0, -5350000.0, 3750000.0, 5850000.0)
TGT_RES = 10000.0

THREDDS_BASE = "https://thredds.met.no/thredds/fileServer/osisaf/met.no/ice"

# Per-product configuration. `dir`/`stem`/`variable` build the URL + select
# the NetCDF subdataset; the rest drive value handling + resampling.
PRODUCTS = {
    "conc": {
        "dir": "conc",
        "stem": "ice_conc",
        "variable": "ice_conc",
        "categorical": False,
        "warp_resampling": "bilinear",   # gdalwarp -r
        "cog_resampling": "bilinear",     # gdal_translate -r (different spelling vocab)
        "ovr_resampling": "average",
        "out_dtype": "float32",
        "src_fill": -999,        # raw Int16 fill before scaling
        "scale_factor": 0.01,    # stored DN * 0.01 = percent
        "nodata": float("nan"),
        "units": "%",
        "flag_values": None,
        "flag_meanings": None,
        "default_collection": "frozon-osisaf-sic-daily",
    },
    "type": {
        "dir": "type",
        "stem": "ice_type",
        "variable": "ice_type",
        "categorical": True,
        "warp_resampling": "near",        # gdalwarp -r
        "cog_resampling": "nearest",       # gdal_translate -r
        "ovr_resampling": "mode",
        "out_dtype": "int16",    # Int16 keeps the -1 fill representable everywhere
        "src_fill": -1,
        "scale_factor": None,
        "nodata": -1,
        "units": "1",
        "flag_values": "1,2,3,4",
        "flag_meanings": "open_water first_year_ice multi_year_ice ambiguous",
        "default_collection": "frozon-osisaf-icetype-daily",
    },
    "edge": {
        "dir": "edge",
        "stem": "ice_edge",
        "variable": "ice_edge",
        "categorical": True,
        "warp_resampling": "near",        # gdalwarp -r
        "cog_resampling": "nearest",       # gdal_translate -r
        "ovr_resampling": "mode",
        "out_dtype": "int16",
        "src_fill": -1,
        "scale_factor": None,
        "nodata": -1,
        "units": "1",
        "flag_values": "1,2,3",
        "flag_meanings": "open_water open_ice close_ice",
        "default_collection": "frozon-osisaf-iceedge-daily",
    },
}


# --------------------------------------------------------------------------
# Download
# --------------------------------------------------------------------------

def build_thredds_url(product: str, hemisphere: str, date_yyyymmdd: str,
                      thredds_base: str = THREDDS_BASE) -> str:
    """Construct the deterministic THREDDS fileServer URL for one daily file.

    e.g. .../ice/conc/2026/06/ice_conc_nh_polstere-100_multi_202606161200.nc
    The nominal product timestamp is always 1200 (12:00 UTC).
    """
    cfg = PRODUCTS[product]
    dt = datetime.strptime(date_yyyymmdd, "%Y%m%d")
    fname = f"{cfg['stem']}_{hemisphere}_polstere-100_multi_{date_yyyymmdd}1200.nc"
    return f"{thredds_base}/{cfg['dir']}/{dt.year:04d}/{dt.month:02d}/{fname}"


def download_netcdf(url: str, dest: Path, retries: int = 3,
                    timeout: int = 300) -> Path:
    """Download a NetCDF over anonymous HTTP with a few retries. Raises
    FileNotFoundError on a 404 (file not published yet) so the caller can
    treat 'not landed' distinctly from a transient failure."""
    import requests

    dest.parent.mkdir(parents=True, exist_ok=True)
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as r:
                if r.status_code == 404:
                    raise FileNotFoundError(f"OSI SAF file not found (404): {url}")
                r.raise_for_status()
                with open(dest, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=1 << 20):
                        if chunk:
                            fh.write(chunk)
            size = dest.stat().st_size
            if size < 1024:
                raise RuntimeError(f"Downloaded file suspiciously small ({size} B)")
            logger.info(f"Downloaded {dest.name} ({size / 1e6:.1f} MB)")
            return dest
        except FileNotFoundError:
            raise
        except Exception as e:  # noqa: BLE001 — retry transient HTTP/IO errors
            last_err = e
            logger.warning(f"Download attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(5 * attempt)
    raise RuntimeError(f"Failed to download {url} after {retries} attempts: {last_err}")


# --------------------------------------------------------------------------
# Extract variable -> EPSG:3411 GeoTIFF (forced grid) -> warp to EPSG:3413
# --------------------------------------------------------------------------

def extract_variable(nc_path: Path, product: str, out_tiff: Path) -> Path:
    """Read the product variable from the NetCDF and write a GeoTIFF in the
    forced source grid (EPSG:3411, clean 10 km geotransform).

    * Concentration: unscale Int16 DN * 0.01 -> Float32 percent; the raw
      fill (-999) becomes NaN.
    * Type / edge: keep integer class codes, NoData -1 (widened to Int16).
    """
    cfg = PRODUCTS[product]
    sds = f'NETCDF:"{nc_path}":{cfg["variable"]}'
    out_tiff.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(sds) as src:
        raw = src.read(1)  # (time=1, y, x) -> band 1 == (y, x)

    if raw.shape != (SRC_HEIGHT, SRC_WIDTH):
        raise RuntimeError(
            f"Unexpected {product} grid shape {raw.shape}; "
            f"expected ({SRC_HEIGHT}, {SRC_WIDTH}). OSI SAF grid may have changed."
        )

    if cfg["categorical"]:
        data = raw.astype("int16")
        nodata = cfg["nodata"]
    else:
        data = raw.astype("float32")
        fill_mask = raw == cfg["src_fill"]
        data *= cfg["scale_factor"]
        data[fill_mask] = np.nan
        nodata = cfg["nodata"]

    profile = {
        "driver": "GTiff",
        "height": SRC_HEIGHT,
        "width": SRC_WIDTH,
        "count": 1,
        "dtype": cfg["out_dtype"],
        "crs": SRC_EPSG,
        "transform": SRC_TRANSFORM,
        "nodata": nodata,
        "compress": "DEFLATE",
        "tiled": True,
    }
    with rasterio.open(out_tiff, "w", **profile) as dst:
        dst.write(data, 1)
        # Embed class semantics so they survive into the COG band metadata.
        if cfg["flag_values"]:
            dst.update_tags(1,
                            flag_values=cfg["flag_values"],
                            flag_meanings=cfg["flag_meanings"])
        if cfg["units"]:
            dst.update_tags(1, units=cfg["units"])

    logger.info(f"Extracted {product} -> {out_tiff.name} "
                f"(dtype={cfg['out_dtype']}, EPSG:3411 forced grid)")
    return out_tiff


def warp_to_3413(in_tiff: Path, out_tiff: Path, product: str) -> Path:
    """gdalwarp the forced-grid EPSG:3411 GeoTIFF onto the canonical
    EPSG:3413 10 km grid. Resampling is per-product (bilinear for the
    continuous concentration, nearest for categorical type/edge)."""
    cfg = PRODUCTS[product]
    out_tiff.parent.mkdir(parents=True, exist_ok=True)
    if out_tiff.exists():
        out_tiff.unlink()

    nodata = cfg["nodata"]
    dstnodata = "nan" if (isinstance(nodata, float) and np.isnan(nodata)) else str(nodata)

    cmd = [
        "gdalwarp",
        "-s_srs", SRC_EPSG,
        "-t_srs", TGT_EPSG,
        "-te", str(TGT_TE[0]), str(TGT_TE[1]), str(TGT_TE[2]), str(TGT_TE[3]),
        "-tr", str(TGT_RES), str(TGT_RES),
        "-r", cfg["warp_resampling"],
        "-srcnodata", dstnodata,
        "-dstnodata", dstnodata,
        "-multi",
        "-wo", "NUM_THREADS=ALL_CPUS",
        "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
        "-of", "GTiff", "-overwrite",
        "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES",
        str(in_tiff), str(out_tiff),
    ]
    logger.info(f"gdalwarp {product} EPSG:3411 -> EPSG:3413 "
                f"(-r {cfg['warp_resampling']}) -> {out_tiff.name}")
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=1800)
    if r.returncode != 0:
        raise RuntimeError(f"gdalwarp failed for {in_tiff.name}: {r.stderr[-800:]}")
    return out_tiff


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--product", required=True, choices=sorted(PRODUCTS),
                   help="OSI SAF product to ingest")
    p.add_argument("--date", required=True,
                   help="Acquisition date YYYYMMDD (the daily file timestamp)")
    p.add_argument("--hemisphere", default="nh", choices=["nh"],
                   help="Hemisphere (only nh / Arctic supported)")
    p.add_argument("--thredds-base", default=THREDDS_BASE,
                   help="Override THREDDS fileServer base URL (testing)")
    p.add_argument("--input-nc", default=None,
                   help="Local NetCDF path; skips download (testing)")

    p.add_argument("--collection-id", default=None,
                   help="STAC collection ID (defaults to the per-product collection)")
    p.add_argument("--s3-bucket", required=True)
    p.add_argument("--s3-prefix", default="")
    p.add_argument("--role-arn", default=None)

    p.add_argument("--compress", default="DEFLATE")
    p.add_argument("--blocksize", type=int, default=512)
    p.add_argument("--max-memory", type=int, default=512)
    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--output", default="output",
                   help="DPS-persisted output dir; only the STAC catalog is "
                        "written here (the COG goes to S3 directly).")
    p.add_argument("--scratch-dir", default="scratch",
                   help="Working dir for the NetCDF download + intermediate "
                        "GeoTIFFs + COG. NOT persisted by DPS; deleted on exit "
                        "unless --keep-scratch.")
    p.add_argument("--keep-scratch", action="store_true",
                   help="Keep the scratch dir for debugging.")
    args = p.parse_args()

    cfg = PRODUCTS[args.product]
    collection_id = args.collection_id or cfg["default_collection"]

    try:
        datetime.strptime(args.date, "%Y%m%d")
    except ValueError:
        logger.error(f"TERMINATED: --date must be YYYYMMDD, got {args.date!r}")
        return 6

    # The canonical COG goes straight to s3://.../cogs/; nothing else needs to
    # be stored. All work (NetCDF download, EPSG:3411 + 3413 GeoTIFFs, the local
    # COG) happens in `scratch/`, deleted on exit. `output/` (the only dir DPS
    # persists) is left empty — no STAC catalog, since nothing consumes it.
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    scratch_dir = Path(args.scratch_dir)
    scratch_dir.mkdir(parents=True, exist_ok=True)

    try:
        # --- 1. Resolve + download the NetCDF ---
        if args.input_nc:
            nc_path = Path(args.input_nc)
            if not nc_path.exists():
                raise FileNotFoundError(f"--input-nc not found: {nc_path}")
        else:
            url = build_thredds_url(args.product, args.hemisphere, args.date,
                                    args.thredds_base)
            logger.info(f"OSI SAF {args.product} {args.date}: {url}")
            nc_path = download_netcdf(url, scratch_dir / "nc" / Path(url).name)

        # --- 2. Extract variable onto the forced EPSG:3411 grid ---
        src_tiff = extract_variable(nc_path, args.product,
                                    scratch_dir / "src" / f"{args.product}_{args.date}_3411.tif")

        # --- 3. Reproject to canonical EPSG:3413 ---
        warped = warp_to_3413(src_tiff,
                              scratch_dir / "warp" / f"{args.product}_{args.date}_3413.tif",
                              args.product)

        # --- 4. COG conversion (reuse) ---
        cog_name = f"{collection_id}_{args.date}_COG.tif"
        cog_path = scratch_dir / "cog" / cog_name
        cog_path.parent.mkdir(parents=True, exist_ok=True)
        ok, msg = cog_helpers.convert_to_cog_lowmem(
            input_file=warped,
            output_file=cog_path,
            overwrite=True,
            compress=args.compress,
            blocksize=args.blocksize,
            max_memory_mb=args.max_memory,
            resampling=cfg["cog_resampling"],
            overview_resampling=cfg["ovr_resampling"],
        )
        logger.info(msg)
        if not ok:
            return 2

        # --- 5. Upload the COG to its dated S3 key ---
        # Date partition comes straight from --date (nominal 12:00 UTC); the
        # class/flag semantics are embedded in the COG band metadata. No STAC
        # item/catalog is built — nothing in the OSI SAF pipeline consumes one.
        item_dt = datetime.strptime(args.date, "%Y%m%d").replace(
            hour=12, tzinfo=timezone.utc)
        s3_key = cog_helpers.build_dated_s3_key(
            args.s3_prefix, collection_id, item_dt, cog_path.name)
        s3_url = cog_helpers.upload_cog_to_key(
            cog_path, args.s3_bucket, s3_key, args.role_arn)
        logger.info(f"OSI SAF {args.product} ingest complete: {s3_url}")
        return 0

    except FileNotFoundError as e:
        # 404 / missing input — distinct exit code so the runner can tell
        # "not published yet" apart from a real failure.
        logger.error(f"TERMINATED: {e}")
        return 6
    except Exception as e:  # noqa: BLE001
        logger.error(f"TERMINATED: unexpected error: {e}", exc_info=True)
        return 1
    finally:
        # Drop the scratch dir so DPS only persists output/ (the STAC catalog);
        # the COG already lives in S3 under the cogs/ prefix.
        if not args.keep_scratch:
            shutil.rmtree(scratch_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
