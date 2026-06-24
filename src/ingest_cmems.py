"""Worker: ingest one CMEMS daily-mean surface ocean-current component
(eastward `uo` / northward `vo`) for one date into a single COG on the
canonical EPSG:3413 Arctic grid.

Copernicus Marine Service (CMEMS) is the EU Copernicus marine data store. Unlike
ECMWF Open Data (anonymous HTTP), CMEMS is **credentialed** — it needs a free
Copernicus Marine account. We pull the username/password from a MAAP secret and
pass them straight to the `copernicusmarine` toolbox (no global login/config
file). Access is a server-side **subset**, so a single (variable, day, Arctic
bbox, surface-only) request returns a small NetCDF rather than a global volume.

Source product: GLOBAL_ANALYSISFORECAST_PHY_001_024
Dataset:        cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m  (currents, daily mean,
                1/12° global regular lat/lon, analysis+forecast)

Per (product, date) the worker:

  1. `copernicusmarine.subset(...)` the one variable (uo|vo) for the day, surface
     layer only (depth 0–1 m), cropped to an Arctic lat/lon bbox -> NetCDF.
  2. Extract the variable to a Float32 EPSG:4326 GeoTIFF (squeeze time+depth;
     land/no-ocean cells are NaN).
  3. Reproject (gdalwarp) EPSG:4326 -> canonical EPSG:3413 10 km Arctic grid —
     the SAME grid OSI SAF / S1 / ECMWF land on, so every Frozon collection
     co-registers. Continuous field -> bilinear.
  4. COG-ify (reuse cog_helpers.convert_to_cog_lowmem).
  5. Upload the COG to its dated S3 key.

Value semantics (see DATA_CMEMS.md):
  * ocean_u (`uo`) : eastward sea-water velocity, Float32 m s-1, NoData NaN.
  * ocean_v (`vo`) : northward sea-water velocity, Float32 m s-1, NoData NaN.

NOTE on vectors: `uo`/`vo` are GEOGRAPHIC eastward/northward components (same
convention as the ECMWF 10m wind `10u`/`10v`). They are reprojected as scalar
fields — the values stay eastward/northward m s-1; only the pixel locations move
onto the polar grid. They are NOT rotated to grid-relative axes. Combine the two
collections downstream for speed/direction: speed = hypot(u, v),
dir = atan2(v, u).
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import rioxarray  # noqa: F401 — registers the .rio accessor on xarray objects
import xarray as xr

import cog_helpers  # reuse convert_to_cog_lowmem / build_dated_s3_key / upload
from common_utils import AWSUtils, MaapUtils  # noqa: F401  (AWSUtils via cog_helpers)

logger = logging.getLogger("ingest_cmems")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# --------------------------------------------------------------------------
# Grid + product constants
# --------------------------------------------------------------------------

SRC_EPSG = "EPSG:4326"
TGT_EPSG = "EPSG:3413"

# Canonical EPSG:3413 target grid — identical to the other Frozon workers so the
# CMEMS COGs co-register pixel-for-pixel and stack cleanly in the Zarr.
TGT_TE = (-3850000.0, -5350000.0, 3750000.0, 5850000.0)
TGT_RES = 10000.0

# Currents, daily mean, 1/12° global, analysis+forecast.
DATASET_ID = "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m"

# Arctic lat/lon crop for the subset request (minlon,minlat,maxlon,maxlat). Goes
# down to 20°N so the EPSG:3413 grid's mid-latitude corners are fully covered
# after the warp, with margin.
DEFAULT_BBOX = (-180.0, 20.0, 180.0, 90.0)

# Per-product config. `variable` selects the CMEMS array; the rest drive value
# handling + STAC/COG naming. Both are continuous fields -> bilinear / Float32.
PRODUCTS = {
    "ocean_u": {
        "variable": "uo",              # eastward sea-water velocity
        "long_name": "ocean_current_u_velocity",
        "units": "m s-1",
        "default_collection": "frozon-cmems-ocean-u-daily",
    },
    "ocean_v": {
        "variable": "vo",              # northward sea-water velocity
        "long_name": "ocean_current_v_velocity",
        "units": "m s-1",
        "default_collection": "frozon-cmems-ocean-v-daily",
    },
}

WARP_RESAMPLING = "bilinear"
COG_RESAMPLING = "bilinear"
OVR_RESAMPLING = "average"

DEFAULT_SECRET_NAME = "copernicus-marine-frozon"


class NotPublishedError(Exception):
    """The requested (dataset, date) isn't available in the CMEMS store yet."""


# --------------------------------------------------------------------------
# Credentials
# --------------------------------------------------------------------------

def resolve_cmems_creds(secret_name: str, maap_instance=None) -> tuple[str, str]:
    """Pull (username, password) from a MAAP secret. The secret body is two
    lines: `username` then `password` (same convention the EDL secrets use)."""
    maap = maap_instance or MaapUtils.get_maap_instance()
    secret = maap.secrets.get_secret(secret_name)
    body = secret if isinstance(secret, str) else secret.get("value", "")
    lines = [ln for ln in body.splitlines() if ln.strip()]
    if len(lines) >= 2:
        return lines[0].strip(), lines[1].strip()
    raise RuntimeError(
        f"CMEMS secret {secret_name!r} must be two lines (username\\npassword); "
        f"got {len(lines)} non-empty line(s)."
    )


# --------------------------------------------------------------------------
# Download (server-side subset) -> NetCDF
# --------------------------------------------------------------------------

def download_subset(product: str, date_yyyymmdd: str, dest_dir: Path,
                    username: str, password: str, dataset_id: str,
                    bbox: tuple[float, float, float, float],
                    min_depth: float, max_depth: float) -> Path:
    """Subset one variable for one day (surface only, Arctic bbox) to NetCDF via
    the copernicusmarine toolbox. Raises NotPublishedError if the date has no
    data (so the caller can tell 'not published' from a real failure)."""
    import copernicusmarine

    cfg = PRODUCTS[product]
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_name = f"{product}_{date_yyyymmdd}.nc"
    nc_path = dest_dir / out_name
    if nc_path.exists():
        nc_path.unlink()  # avoid the toolbox's "data already exists" branch

    y, m, d = date_yyyymmdd[:4], date_yyyymmdd[4:6], date_yyyymmdd[6:8]
    minlon, minlat, maxlon, maxlat = bbox
    logger.info(f"copernicusmarine subset {product} ({cfg['variable']}) {date_yyyymmdd}: "
                f"{dataset_id} depth[{min_depth},{max_depth}] "
                f"bbox[{minlon},{minlat},{maxlon},{maxlat}]")
    try:
        copernicusmarine.subset(
            dataset_id=dataset_id,
            variables=[cfg["variable"]],
            minimum_longitude=minlon, maximum_longitude=maxlon,
            minimum_latitude=minlat, maximum_latitude=maxlat,
            minimum_depth=min_depth, maximum_depth=max_depth,
            start_datetime=f"{y}-{m}-{d}T00:00:00",
            end_datetime=f"{y}-{m}-{d}T23:59:59",
            output_filename=out_name,
            output_directory=str(dest_dir),
            username=username,
            password=password,
        )
    except Exception as e:  # noqa: BLE001
        msg = str(e).lower()
        if any(s in msg for s in ("no data", "out of range", "no files",
                                  "not found", "empty", "outside")):
            raise NotPublishedError(
                f"CMEMS subset returned nothing for {product} {date_yyyymmdd} "
                f"({dataset_id}): {e}"
            ) from e
        raise

    if not nc_path.exists() or nc_path.stat().st_size < 1024:
        raise NotPublishedError(
            f"CMEMS subset produced no usable file for {product} {date_yyyymmdd} "
            f"(date likely outside the dataset's available range)."
        )
    logger.info(f"Subset wrote {nc_path.name} ({nc_path.stat().st_size / 1e6:.1f} MB)")
    return nc_path


# --------------------------------------------------------------------------
# Extract variable -> EPSG:4326 GeoTIFF -> warp to EPSG:3413
# --------------------------------------------------------------------------

def extract_variable(nc_path: Path, product: str, out_tiff: Path) -> Path:
    """Read the variable from the NetCDF, squeeze time+depth to a 2-D (lat, lon)
    field, and write a Float32 EPSG:4326 GeoTIFF. Land / no-ocean cells are NaN.

    CMEMS is on a regular -180..180 lat/lon grid (ascending latitude); rioxarray
    derives the correct north-up geotransform from the coordinates, so no manual
    grid forcing or longitude roll is needed."""
    cfg = PRODUCTS[product]
    out_tiff.parent.mkdir(parents=True, exist_ok=True)

    ds = xr.open_dataset(nc_path)
    try:
        da = ds[cfg["variable"]]
        for dim in ("time", "depth"):
            if dim in da.dims:
                da = da.isel({dim: 0})
        da = da.squeeze(drop=True)
        if da.ndim != 2:
            raise RuntimeError(
                f"Expected a 2-D field after squeeze, got dims {da.dims} for "
                f"{cfg['variable']} in {nc_path.name}"
            )
        da = da.astype("float32")
        da = da.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")
        da = da.rio.write_crs(SRC_EPSG)
        da = da.rio.write_nodata(float("nan"))  # GTiff nodata tag = NaN
        da.rio.to_raster(out_tiff, driver="GTiff", compress="DEFLATE")
    finally:
        ds.close()

    logger.info(f"Extracted {product} -> {out_tiff.name} (Float32 {SRC_EPSG})")
    return out_tiff


def warp_to_3413(in_tiff: Path, out_tiff: Path) -> Path:
    """gdalwarp the EPSG:4326 GeoTIFF onto the canonical EPSG:3413 10 km Arctic
    grid (bilinear; ocean currents are continuous fields)."""
    out_tiff.parent.mkdir(parents=True, exist_ok=True)
    if out_tiff.exists():
        out_tiff.unlink()

    cmd = [
        "gdalwarp",
        "-s_srs", SRC_EPSG,
        "-t_srs", TGT_EPSG,
        "-te", str(TGT_TE[0]), str(TGT_TE[1]), str(TGT_TE[2]), str(TGT_TE[3]),
        "-tr", str(TGT_RES), str(TGT_RES),
        "-r", WARP_RESAMPLING,
        "-srcnodata", "nan",
        "-dstnodata", "nan",
        "-multi",
        "-wo", "NUM_THREADS=ALL_CPUS",
        "--config", "GDAL_NUM_THREADS", "ALL_CPUS",
        "-of", "GTiff", "-overwrite",
        "-co", "COMPRESS=DEFLATE", "-co", "TILED=YES",
        str(in_tiff), str(out_tiff),
    ]
    logger.info(f"gdalwarp EPSG:4326 -> EPSG:3413 (-r {WARP_RESAMPLING}) -> {out_tiff.name}")
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
                   help="CMEMS ocean-current component to ingest")
    p.add_argument("--date", required=True,
                   help="Daily-mean date YYYYMMDD")
    p.add_argument("--dataset-id", default=DATASET_ID,
                   help="CMEMS dataset id (default: daily-mean currents).")
    p.add_argument("--cmems-secret-name", default=DEFAULT_SECRET_NAME,
                   help="MAAP secret holding two-line username\\npassword.")
    p.add_argument("--bbox", default=None,
                   help="Subset crop minlon,minlat,maxlon,maxlat "
                        f"(default {DEFAULT_BBOX}).")
    p.add_argument("--min-depth", type=float, default=0.0)
    p.add_argument("--max-depth", type=float, default=1.0,
                   help="Surface layer only (~0.49 m sits in [0,1]).")
    p.add_argument("--input-nc", default=None,
                   help="Local NetCDF path; skips the subset download (testing).")

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
                   help="DPS-persisted output dir; left empty (the COG goes to S3).")
    p.add_argument("--scratch-dir", default="scratch",
                   help="Working dir for the NetCDF + intermediate GeoTIFFs + COG. "
                        "NOT persisted by DPS; deleted on exit unless --keep-scratch.")
    p.add_argument("--keep-scratch", action="store_true")
    args = p.parse_args()

    cfg = PRODUCTS[args.product]
    collection_id = args.collection_id or cfg["default_collection"]

    try:
        datetime.strptime(args.date, "%Y%m%d")
    except ValueError:
        logger.error(f"TERMINATED: --date must be YYYYMMDD, got {args.date!r}")
        return 6

    if args.bbox:
        try:
            bbox = tuple(float(x) for x in args.bbox.split(","))
            assert len(bbox) == 4
        except Exception:
            logger.error(f"TERMINATED: --bbox must be minlon,minlat,maxlon,maxlat, "
                         f"got {args.bbox!r}")
            return 6
    else:
        bbox = DEFAULT_BBOX

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    scratch_dir = Path(args.scratch_dir)
    scratch_dir.mkdir(parents=True, exist_ok=True)

    try:
        # --- 1. Resolve creds + subset-download the NetCDF ---
        if args.input_nc:
            nc_path = Path(args.input_nc)
            if not nc_path.exists():
                raise FileNotFoundError(f"--input-nc not found: {nc_path}")
        else:
            username, password = resolve_cmems_creds(args.cmems_secret_name)
            nc_path = download_subset(
                args.product, args.date, scratch_dir / "nc",
                username, password, args.dataset_id, bbox,
                args.min_depth, args.max_depth)

        # --- 2. Extract the variable onto an EPSG:4326 GeoTIFF ---
        src_tiff = extract_variable(
            nc_path, args.product,
            scratch_dir / "src" / f"{args.product}_{args.date}_4326.tif")

        # --- 3. Reproject to the canonical EPSG:3413 Arctic grid ---
        warped = warp_to_3413(
            src_tiff,
            scratch_dir / "warp" / f"{args.product}_{args.date}_3413.tif")

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
            resampling=COG_RESAMPLING,
            overview_resampling=OVR_RESAMPLING,
        )
        logger.info(msg)
        if not ok:
            return 2

        # --- 5. Upload the COG to its dated S3 key ---
        # Date partition from --date (daily mean -> nominal 12:00 UTC); units live
        # in the COG band metadata. No STAC item/catalog (mirrors OSI SAF/ECMWF).
        item_dt = datetime.strptime(args.date, "%Y%m%d").replace(
            hour=12, tzinfo=timezone.utc)
        s3_key = cog_helpers.build_dated_s3_key(
            args.s3_prefix, collection_id, item_dt, cog_path.name)
        s3_url = cog_helpers.upload_cog_to_key(
            cog_path, args.s3_bucket, s3_key, args.role_arn)
        logger.info(f"CMEMS {args.product} ingest complete: {s3_url}")
        return 0

    except (NotPublishedError, FileNotFoundError) as e:
        logger.error(f"TERMINATED: {e}")
        return 6
    except Exception as e:  # noqa: BLE001
        logger.error(f"TERMINATED: unexpected error: {e}", exc_info=True)
        return 1
    finally:
        if not args.keep_scratch:
            shutil.rmtree(scratch_dir, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(main())
