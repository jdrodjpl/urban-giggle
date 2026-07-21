"""Worker: ingest one ECMWF Open Data near-surface field (2 m air temperature,
10 m northward wind, 10 m eastward wind) for one date into a single COG in the
canonical EPSG:3413 Arctic grid.

ECMWF Open Data is the free real-time IFS forecast feed (https://data.ecmwf.int).
Like OSI SAF — and unlike the OPERA / S1 sources this repo started with — it is
**not** on NASA CMR and needs no Earthdata Login:

  * Distribution: anonymous HTTP. We use the `ecmwf-opendata` client, which
    reads the per-run `.index` and pulls only the GRIB messages we ask for via
    byte-range requests — so a single-variable retrieve is a few MB even though
    the full step-0 file carries every parameter.
  * One GRIB message == one full 0.25° global grid already, so there is no
    mosaic step — just extract the field, roll it onto a clean −180..180 grid,
    reproject, COG-ify.

We ingest the **step-0 (T+0 analysis) field of the 00 UTC oper HRES run** as the
daily snapshot: one global grid per variable per day, valid 00:00 UTC.

Per (product, date) the worker:

  1. Retrieve the single-parameter GRIB2 (anonymous byte-range via ecmwf-opendata).
  2. Extract the field to a GeoTIFF. ECMWF's global grid is published with
     longitudes 0..360; we roll it onto a clean −180..180 EPSG:4326 grid so the
     downstream warp has no antimeridian seam. (Mirrors OSI SAF's "force the
     clean source grid" step.)
  3. Reproject (gdalwarp) EPSG:4326 -> canonical EPSG:3413 10 km Arctic grid —
     the *same* grid OSI SAF / S1 land on, so every Frozon collection co-registers.
     All three products are continuous fields -> bilinear.
  4. COG-ify (reuse cog_helpers.convert_to_cog_lowmem).
  5. Upload the COG to its dated S3 key. No STAC catalog is written — like OSI
     SAF, nothing in this pipeline consumes one and the units live in the COG
     band metadata.

Value semantics (see DATA_ECMWF.md):
  * airtemp : 2 m temperature, Float32 Kelvin (native; no conversion), NoData NaN.
  * wind_ns : 10 m northward (V) wind component, Float32 m s-1, NoData NaN.
  * wind_ew : 10 m eastward (U) wind component, Float32 m s-1, NoData NaN.

NOTE: ocean-current U/V are intentionally absent — ECMWF Open Data's real-time
feed does not publish surface ocean currents (they live in MARS / Copernicus
Marine). See DATA_ECMWF.md.
"""

from __future__ import annotations

import argparse
import logging
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import rasterio
from rasterio.transform import Affine

import cog_helpers  # reuse convert_to_cog_lowmem / build_dated_s3_key / upload
from common_utils import AWSUtils  # noqa: F401  (parity; uploads go via cog_helpers)

logger = logging.getLogger("ingest_ecmwf")
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


# --------------------------------------------------------------------------
# Grid + product constants
# --------------------------------------------------------------------------

# Source: ECMWF Open Data is plain geographic WGS84 (regular lat/lon).
SRC_EPSG = "EPSG:4326"
# Target: WGS84 polar stereographic (Frozon canonical Arctic grid).
TGT_EPSG = "EPSG:3413"

# Canonical EPSG:3413 target grid — identical to src/ingest_osisaf.py so ECMWF
# COGs co-register pixel-for-pixel with the sea-ice collections and stack cleanly
# in the Zarr. te is minx,miny,maxx,maxy; tr is 10 km -> a clean 760x1120 raster.
TGT_TE = (-3850000.0, -5350000.0, 3750000.0, 5850000.0)
TGT_RES = 10000.0

# Per-product configuration. `param` is the ECMWF Open Data short name selected
# from the step-0 oper file; the rest drive value handling + STAC/COG naming.
# All three are continuous near-surface analysis fields -> bilinear / Float32.
PRODUCTS = {
    "airtemp": {
        "param": "2t",                # 2 m temperature
        "long_name": "atmospheric_temperature",
        # The Open Data feed delivers 2t in degrees Celsius (validated against
        # the live feed 2026-06: equatorial ~29, Sahara ~34), not the Kelvin
        # the ECMWF parameter DB documents. We pass values through unchanged.
        "units": "degC",
        "default_collection": "frozon-ecmwf-airtemp-daily",
    },
    "wind_ns": {
        "param": "10v",               # 10 m northward (meridional, V) wind
        "long_name": "wind_direction_ns",
        "units": "m s-1",
        "default_collection": "frozon-ecmwf-wind-ns-daily",
    },
    "wind_ew": {
        "param": "10u",               # 10 m eastward (zonal, U) wind
        "long_name": "wind_direction_ew",
        "units": "m s-1",
        "default_collection": "frozon-ecmwf-wind-ew-daily",
    },
    # Derived product: decimated point GeoJSON combining 10u + 10v for the
    # MMGIS arrow layer (vector layer + bearing marker attachment). Not a COG.
    # `param` is informational only — the worker retrieves via the wind_ew /
    # wind_ns configs and pairs them.
    "wind_arrows": {
        "param": "10u+10v",
        "long_name": "wind_speed_and_bearing",
        "units": "m s-1 / deg",
        "default_collection": "frozon-ecmwf-wind-arrows-daily",
    },
}

WARP_RESAMPLING = "bilinear"   # gdalwarp -r (continuous fields)
COG_RESAMPLING = "bilinear"    # gdal_translate -r
OVR_RESAMPLING = "average"     # overview build

# wind_arrows: sample every Nth pixel of the 10 km EPSG:3413 grid. Because the
# grid IS the display projection, a fixed stride gives arrows evenly spaced on
# the polar map. 15 -> 150 km spacing -> ~3.4k points from the 760x1120 grid
# (full resolution would be ~850k divIcon markers in MMGIS — a browser-killer).
ARROW_STRIDE = 15

# Pre-rendered glyph rasters (the GIBS/Worldview-style presentation): thin,
# anti-aliased, speed-colored quiver arrows with length proportional to speed,
# burned into transparent RGBA COGs served through the standard tile path.
# Glyphs are screen-scale entities, so one raster per zoom band:
#   (arrow spacing m, raster m/px)  — band A serves ~z0-3, band B ~z3-5.
# Above that, the interactive full-res geodataset layer takes over.
GLYPH_BANDS = [(160000.0, 4096.0), (40000.0, 1024.0)]
GLYPH_MAX_SPEED = 25.0     # m/s at which shaft length = 80% of arrow spacing
GLYPH_CLIM = (0.0, 20.0)   # colormap range, matches the layer legend


# --------------------------------------------------------------------------
# Retrieve
# --------------------------------------------------------------------------

class NotPublishedError(Exception):
    """The requested run/step is not (yet) on the Open Data feed."""


def retrieve_grib(product: str, date_yyyymmdd: str, run_time: int, step: int,
                  dest: Path, resol: str = "0p25") -> Path:
    """Pull the single-parameter GRIB2 for (product, date, time, step) from the
    ECMWF Open Data feed via the `ecmwf-opendata` client (anonymous byte-range).

    Raises NotPublishedError if the run isn't on the feed (so the caller can
    distinguish "not landed yet" from a real failure)."""
    from ecmwf.opendata import Client

    cfg = PRODUCTS[product]
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest.unlink()

    client = Client(source="ecmwf", model="ifs", resol=resol)
    request = dict(
        date=date_yyyymmdd,
        time=run_time,
        step=step,
        stream="oper",
        type="fc",
        param=cfg["param"],
        target=str(dest),
    )
    logger.info(f"ecmwf-opendata retrieve {product} ({cfg['param']}): "
                f"date={date_yyyymmdd} time={run_time:02d} step={step} resol={resol}")
    try:
        client.retrieve(**request)
    except Exception as e:  # noqa: BLE001
        # The client raises a plain Exception on a missing run (404 on the
        # .index / data file). Treat that as "not published"; re-raise anything
        # that already produced a file as a hard error below.
        msg = str(e).lower()
        if "not found" in msg or "404" in msg or "no index" in msg or "no data" in msg:
            raise NotPublishedError(
                f"ECMWF Open Data run not published: {product} {date_yyyymmdd} "
                f"{run_time:02d}z step {step} ({e})"
            ) from e
        raise

    if not dest.exists() or dest.stat().st_size < 1024:
        raise NotPublishedError(
            f"ECMWF Open Data retrieve produced no data for {product} "
            f"{date_yyyymmdd} {run_time:02d}z step {step} "
            f"(param {cfg['param']} likely absent from this run)"
        )
    logger.info(f"Retrieved {dest.name} ({dest.stat().st_size / 1e6:.1f} MB)")
    return dest


# --------------------------------------------------------------------------
# Extract field -> clean −180..180 EPSG:4326 GeoTIFF -> warp to EPSG:3413
# --------------------------------------------------------------------------

def extract_variable(grib_path: Path, product: str, out_tiff: Path) -> Path:
    """Read the single GRIB message and write a Float32 EPSG:4326 GeoTIFF.

    The ecmwf-opendata client returns the global 0.25° grid already on the
    standard −180..180 longitude convention (origin ≈ −180.125) — confirmed
    against the live feed (2026-06). In that (normal) case the field is written
    through unchanged.

    As a defensive fallback, if a source ever arrives on the 0..360 convention
    (origin ≈ 0), gdalwarp into a polar projection would drop the western
    hemisphere (negative target longitudes fall outside a 0..360 source). So we
    detect that case (global extent AND origin near 0) and roll the array onto
    −180..180. With the −180..180 feed this branch does NOT trigger."""
    out_tiff.parent.mkdir(parents=True, exist_ok=True)

    with rasterio.open(grib_path) as src:
        # Single-parameter retrieve -> one message -> band 1.
        data = src.read(1).astype("float32")
        transform = src.transform
        width, height = src.width, src.height
        src_nodata = src.nodata

    # Mask any GRIB bitmap-missing values to NaN.
    if src_nodata is not None and not np.isnan(src_nodata):
        data[data == src_nodata] = np.nan

    # Defensive only: roll iff the source is global AND its origin is near 0
    # (the 0..360 convention). The ecmwf-opendata feed already gives −180..180
    # (origin ≈ −180), so this normally does NOT trigger.
    xres = transform.a
    ulx = transform.c
    is_global = abs(width * xres - 360.0) < 1e-3
    if is_global and ulx > -1.0 and width % 2 == 0:
        half = width // 2
        data = np.roll(data, half, axis=1)
        transform = Affine(transform.a, transform.b, ulx - 180.0,
                           transform.d, transform.e, transform.f)
        logger.info(f"Rolled global 0..360 grid -> −180..180 (origin {ulx} -> {ulx - 180.0})")
    else:
        logger.info(f"Source grid not rolled (width={width}, xres={xres}, ulx={ulx})")

    profile = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": 1,
        "dtype": "float32",
        "crs": SRC_EPSG,
        "transform": transform,
        "nodata": float("nan"),
        "compress": "DEFLATE",
        "tiled": True,
    }
    with rasterio.open(out_tiff, "w", **profile) as dst:
        dst.write(data, 1)
        dst.update_tags(1, units=PRODUCTS[product]["units"],
                        long_name=PRODUCTS[product]["long_name"],
                        ecmwf_param=PRODUCTS[product]["param"])

    logger.info(f"Extracted {product} -> {out_tiff.name} "
                f"(Float32 {SRC_EPSG}, {width}x{height})")
    return out_tiff


def warp_to_3413(in_tiff: Path, out_tiff: Path) -> Path:
    """gdalwarp the clean EPSG:4326 GeoTIFF onto the canonical EPSG:3413 10 km
    Arctic grid (bilinear; all ECMWF products here are continuous fields)."""
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
# wind_arrows: decimated speed/bearing point GeoJSON from the u + v grids
# --------------------------------------------------------------------------

def render_wind_glyphs(u_tiff: Path, v_tiff: Path, out_tiff: Path,
                       spacing_m: float, px_m: float) -> Path:
    """Render quiver glyphs (shaft length ∝ speed, colored by speed) from the
    co-registered EPSG:3413 u/v grids into a transparent RGBA GeoTIFF.

    Drawn in map coordinates with the analytic local basis: in EPSG:3413 the
    meridians are straight lines through the projection origin, so true north
    at any point is exactly the unit vector toward (0, 0) — correct at the
    pole with no small-angle approximations. gdalwarp does not rotate vector
    components, so u/v remain true-east/true-north m/s."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    with rasterio.open(u_tiff) as usrc, rasterio.open(v_tiff) as vsrc:
        if usrc.transform != vsrc.transform or usrc.shape != vsrc.shape:
            raise RuntimeError("u/v grids not co-registered")
        u = usrc.read(1)
        v = vsrc.read(1)
        grid_transform = usrc.transform

    stride = max(1, round(spacing_m / TGT_RES))
    rows = np.arange(stride // 2, u.shape[0], stride)
    cols = np.arange(stride // 2, u.shape[1], stride)
    cgrid, rgrid = np.meshgrid(cols, rows)
    rs, cs = rgrid.ravel(), cgrid.ravel()
    us, vs = u[rs, cs], v[rs, cs]
    valid = np.isfinite(us) & np.isfinite(vs)
    rs, cs, us, vs = rs[valid], cs[valid], us[valid], vs[valid]

    xs, ys = rasterio.transform.xy(grid_transform, rs, cs)
    xs, ys = np.asarray(xs), np.asarray(ys)

    # Local true-north/east unit vectors in map space (analytic; pole-safe).
    r = np.hypot(xs, ys)
    r[r == 0] = 1.0
    nx, ny = -xs / r, -ys / r
    ex, ey = ny, -nx

    scale = 0.8 * spacing_m / GLYPH_MAX_SPEED   # map meters per (m/s)
    dx = (us * ex + vs * nx) * scale
    dy = (us * ey + vs * ny) * scale
    speed = np.hypot(us, vs)

    width_px = int(round((TGT_TE[2] - TGT_TE[0]) / px_m))
    height_px = int(round((TGT_TE[3] - TGT_TE[1]) / px_m))
    fig = plt.figure(figsize=(width_px / 100.0, height_px / 100.0), dpi=100)
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(TGT_TE[0], TGT_TE[2])
    ax.set_ylim(TGT_TE[1], TGT_TE[3])
    ax.axis("off")
    fig.patch.set_alpha(0)
    ax.patch.set_alpha(0)
    ax.quiver(xs, ys, dx, dy, speed,
              cmap="Spectral_r", clim=GLYPH_CLIM,
              angles="xy", scale_units="xy", scale=1.0,
              units="dots", width=1.3,
              headwidth=4, headlength=5, headaxislength=4.5)
    fig.canvas.draw()
    rgba = np.asarray(fig.canvas.buffer_rgba()).copy()
    plt.close(fig)

    h, w = rgba.shape[:2]
    out_tiff.parent.mkdir(parents=True, exist_ok=True)
    transform = Affine((TGT_TE[2] - TGT_TE[0]) / w, 0, TGT_TE[0],
                       0, -(TGT_TE[3] - TGT_TE[1]) / h, TGT_TE[3])
    with rasterio.open(out_tiff, "w", driver="GTiff", height=h, width=w,
                       count=4, dtype="uint8", crs=TGT_EPSG,
                       transform=transform, compress="DEFLATE",
                       tiled=True) as dst:
        for b in range(4):
            dst.write(rgba[:, :, b], b + 1)
        dst.colorinterp = [rasterio.enums.ColorInterp.red,
                           rasterio.enums.ColorInterp.green,
                           rasterio.enums.ColorInterp.blue,
                           rasterio.enums.ColorInterp.alpha]
    logger.info(f"wind glyphs: {rs.size} arrows @ {spacing_m/1000:.0f} km "
                f"-> {out_tiff.name} ({w}x{h}px)")
    return out_tiff


def build_wind_arrows(u_tiff: Path, v_tiff: Path, out_geojson: Path,
                      stride: int = ARROW_STRIDE,
                      time_iso: Optional[str] = None) -> Path:
    """Sample the co-registered EPSG:3413 u (eastward) and v (northward) wind
    grids every `stride` pixels and write a Point GeoJSON with per-point
    `speed` (m/s) and `dir_to` (compass bearing the wind blows TOWARD, degrees
    clockwise from true north).

    gdalwarp reprojects the raster grid but does not rotate vector components,
    so the pixel values remain true-east/true-north m/s — atan2(u, v) is
    therefore a true-north bearing regardless of the 3413 warp. MMGIS's bearing
    marker attachment expects exactly that and applies the per-marker
    projection-north correction itself (LayerConstructors.js), which is also
    why this product exists: the leaflet-velocity streamline layer breaks at
    the pole on polar-stereographic maps (onaci/leaflet-velocity#41).

    Both are emitted as JSON numbers, never strings — MMGIS's continuous legend
    styling gates on `typeof value === 'number'` and silently falls back to
    discrete matching on strings.

    With stride=1 the full grid is emitted (~850k points, ~120 MB) — the
    geodataset-loading path. `time_iso`, when given, is stamped on every
    feature as properties.time so the Geodatasets append can map it to the
    indexed start_time/end_time columns (start_prop=time&end_prop=time)."""
    import json

    from rasterio.warp import transform as rio_transform

    with rasterio.open(u_tiff) as usrc, rasterio.open(v_tiff) as vsrc:
        if usrc.transform != vsrc.transform or usrc.shape != vsrc.shape:
            raise RuntimeError(
                f"u/v grids not co-registered: {u_tiff.name} vs {v_tiff.name}")
        u = usrc.read(1)
        v = vsrc.read(1)
        grid_transform = usrc.transform
        grid_crs = usrc.crs

    rows = np.arange(stride // 2, u.shape[0], stride)
    cols = np.arange(stride // 2, u.shape[1], stride)
    cgrid, rgrid = np.meshgrid(cols, rows)
    rs, cs = rgrid.ravel(), cgrid.ravel()

    us, vs = u[rs, cs], v[rs, cs]
    valid = np.isfinite(us) & np.isfinite(vs)
    rs, cs, us, vs = rs[valid], cs[valid], us[valid], vs[valid]
    if rs.size == 0:
        raise RuntimeError("wind_arrows: no valid u/v samples on the grid")

    # Pixel centers in map coords, then to lon/lat in one vectorized call.
    xs, ys = rasterio.transform.xy(grid_transform, rs, cs)
    lons, lats = rio_transform(grid_crs, "EPSG:4326", xs, ys)

    speed = np.hypot(us, vs)
    # Compass bearing toward: atan2(east, north), clockwise from true north.
    dir_to = np.degrees(np.arctan2(us, vs)) % 360.0

    time_props = {"time": time_iso} if time_iso else {}
    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point",
                         "coordinates": [round(lon, 4), round(lat, 4)]},
            "properties": {
                "speed": round(float(sp), 2),
                "dir_to": round(float(dr), 1),
                "u": round(float(uu), 2),
                "v": round(float(vv), 2),
                **time_props,
            },
        }
        for lon, lat, sp, dr, uu, vv in zip(lons, lats, speed, dir_to, us, vs)
    ]

    out_geojson.parent.mkdir(parents=True, exist_ok=True)
    with open(out_geojson, "w") as f:
        json.dump({"type": "FeatureCollection", "features": features}, f,
                  separators=(",", ":"))
    logger.info(f"wind_arrows: {len(features)} points (stride {stride} = "
                f"{stride * TGT_RES / 1000:.0f} km) -> {out_geojson.name} "
                f"({out_geojson.stat().st_size / 1e6:.2f} MB)")
    return out_geojson


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--product", required=True, choices=sorted(PRODUCTS),
                   help="ECMWF Open Data product to ingest")
    p.add_argument("--date", required=True,
                   help="Forecast reference date YYYYMMDD")
    p.add_argument("--time", type=int, default=0, choices=[0, 6, 12, 18],
                   help="Forecast reference run hour (UTC). Default 0 (00z).")
    p.add_argument("--step", type=int, default=0,
                   help="Forecast lead-time step in hours. Default 0 (T+0 analysis).")
    p.add_argument("--resol", default="0p25",
                   help="Open Data grid resolution (default 0p25).")
    p.add_argument("--input-grib", default=None,
                   help="Local GRIB2 path; skips retrieve (testing).")

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
                   help="Working dir for the GRIB + intermediate GeoTIFFs + COG. "
                        "NOT persisted by DPS; deleted on exit unless --keep-scratch.")
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

    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    scratch_dir = Path(args.scratch_dir)
    scratch_dir.mkdir(parents=True, exist_ok=True)

    try:
        # --- wind_arrows: derived 10u+10v -> decimated point GeoJSON, no COG ---
        if args.product == "wind_arrows":
            input_gribs = {}
            if args.input_grib:
                # Testing path: two comma-separated local GRIBs, u then v.
                parts = [Path(s) for s in args.input_grib.split(",")]
                if len(parts) != 2:
                    raise FileNotFoundError(
                        "--input-grib for wind_arrows needs two comma-separated "
                        "paths: <10u.grib2>,<10v.grib2>")
                for comp, pth in zip(("wind_ew", "wind_ns"), parts):
                    if not pth.exists():
                        raise FileNotFoundError(f"--input-grib not found: {pth}")
                    input_gribs[comp] = pth

            warped = {}
            for comp in ("wind_ew", "wind_ns"):
                grib_path = input_gribs.get(comp) or retrieve_grib(
                    comp, args.date, args.time, args.step,
                    scratch_dir / "grib" / f"{comp}_{args.date}.grib2",
                    resol=args.resol)
                src_tiff = extract_variable(
                    grib_path, comp,
                    scratch_dir / "src" / f"{comp}_{args.date}_4326.tif")
                warped[comp] = warp_to_3413(
                    src_tiff,
                    scratch_dir / "warp" / f"{comp}_{args.date}_3413.tif")

            item_dt = datetime.strptime(args.date, "%Y%m%d").replace(
                hour=args.time, tzinfo=timezone.utc)
            time_iso = item_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

            # Decimated file for the static map layer...
            geojson_path = build_wind_arrows(
                warped["wind_ew"], warped["wind_ns"],
                scratch_dir / "arrows" / f"{collection_id}_{args.date}.geojson")
            # ...and the full-resolution grid for the Geodatasets loader
            # (~850k points; time stamped per feature for temporal queries).
            full_path = build_wind_arrows(
                warped["wind_ew"], warped["wind_ns"],
                scratch_dir / "arrows" / f"{collection_id}_{args.date}_full.geojson",
                stride=1, time_iso=time_iso)

            for pth in (geojson_path, full_path):
                s3_key = cog_helpers.build_dated_s3_key(
                    args.s3_prefix, collection_id, item_dt, pth.name)
                s3_url = cog_helpers.upload_cog_to_key(
                    pth, args.s3_bucket, s3_key, args.role_arn)
                logger.info(f"ECMWF wind_arrows uploaded: {s3_url}")

            # Pre-rendered glyph COGs (one per zoom band) for the tile layers.
            glyph_collection = "frozon-ecmwf-wind-glyphs-daily"
            for spacing_m, px_m in GLYPH_BANDS:
                km = int(spacing_m / 1000)
                raw = render_wind_glyphs(
                    warped["wind_ew"], warped["wind_ns"],
                    scratch_dir / "glyphs" / f"glyphs_{km}km_{args.date}.tif",
                    spacing_m, px_m)
                cog_path = (scratch_dir / "glyphs" /
                            f"{glyph_collection}_{args.date}_{km}km_COG.tif")
                ok, msg = cog_helpers.convert_to_cog_lowmem(
                    input_file=raw, output_file=cog_path, overwrite=True,
                    compress="DEFLATE", blocksize=args.blocksize,
                    max_memory_mb=args.max_memory,
                    resampling="bilinear", overview_resampling="average")
                logger.info(msg)
                if not ok:
                    return 2
                s3_key = cog_helpers.build_dated_s3_key(
                    args.s3_prefix, glyph_collection, item_dt, cog_path.name)
                s3_url = cog_helpers.upload_cog_to_key(
                    cog_path, args.s3_bucket, s3_key, args.role_arn)
                logger.info(f"ECMWF wind glyphs uploaded: {s3_url}")
            return 0

        # --- 1. Resolve + retrieve the GRIB ---
        if args.input_grib:
            grib_path = Path(args.input_grib)
            if not grib_path.exists():
                raise FileNotFoundError(f"--input-grib not found: {grib_path}")
        else:
            grib_path = retrieve_grib(
                args.product, args.date, args.time, args.step,
                scratch_dir / "grib" / f"{args.product}_{args.date}.grib2",
                resol=args.resol)

        # --- 2. Extract field onto the clean −180..180 EPSG:4326 grid ---
        src_tiff = extract_variable(
            grib_path, args.product,
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
        # Date partition comes from the run reference date at its run hour; the
        # units live in the COG band metadata. No STAC item/catalog is built —
        # nothing in the ECMWF pipeline consumes one (mirrors OSI SAF).
        item_dt = datetime.strptime(args.date, "%Y%m%d").replace(
            hour=args.time, tzinfo=timezone.utc)
        s3_key = cog_helpers.build_dated_s3_key(
            args.s3_prefix, collection_id, item_dt, cog_path.name)
        s3_url = cog_helpers.upload_cog_to_key(
            cog_path, args.s3_bucket, s3_key, args.role_arn)
        logger.info(f"ECMWF {args.product} ingest complete: {s3_url}")
        return 0

    except (NotPublishedError, FileNotFoundError) as e:
        # Not published / missing input — distinct exit code so the runner can
        # tell "not landed yet" apart from a real failure.
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
