#!/usr/bin/env python3
"""Build the S1 GRD granule overlap index for the currently-ingested dates.

Emits, for every pair of Sentinel-1 EW GRD granules whose footprints
intersect, the shared area and how many days apart they were acquired —
the candidate AOIs for sea-ice drift estimation.

Why this exists
---------------
`ingest_s1grd.py` mosaics a whole day's granules into one daily COG and
stamps it with a single midnight-UTC datetime. `cog_helpers.mosaic_tiffs`
feeds every granule to one gdalwarp call, so in overlap zones the last
input silently wins and no record survives of which granule produced which
pixel. This index is the only place granule identity, footprint, and
acquisition time are retained.

Scope
-----
"Currently ingested" is defined by the S3 date folders that actually hold a
COG (`--from-s3`), matching the retention rule in
`submit_s1grd_pipeline.prune_old_cogs`: the RETAIN_DAYS most recent dates
that *have data*, not the last N calendar days. Pass `--dates` or
`--lookback-days` instead for backfill or local runs.

Geometry
--------
All geometry is computed in EPSG:3413 (Arctic Polar Stereographic) — the
same CRS the daily COGs use. This matters: granule footprints come from CMR
as lon/lat corner quads, and at 60-90N straight lines in lon/lat are neither
metrically meaningful nor correct across the antimeridian. Rings are
unwrapped, densified, then projected, so edges follow the real swath shape
and areas are true.

Outputs (written to --output, uploaded with --s3-bucket/--s3-prefix):
  footprints.parquet  — GeoParquet, one row per granule
  overlaps.parquet    — GeoParquet, one row per intersecting pair; carries the
                        attributes AND the intersection polygon (the drift AOI)

Both store geometry in EPSG:3413 with the CRS in GeoParquet metadata, so
readers reproject correctly and DuckDB's ST_* functions operate in metres.
`--geojson` additionally emits lon/lat GeoJSON for viewers that need it.

Query example (DuckDB, straight off S3, no download):
    SELECT date_a, date_b, dt_hours, intersect_km2, iou
    FROM 's3://.../overlap/overlaps.parquet'
    WHERE day_diff = 4 AND iou >= 0.70
    ORDER BY intersect_km2 DESC;

No Earthdata auth required: CMR granule search is public. S3 listing/upload
uses MAAP workspace credentials, same as the ingest orchestrator.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone
from fnmatch import fnmatch

from pathlib import Path
from typing import Iterator, List, Optional, Sequence

from pyproj import Transformer
from shapely.affinity import translate
from shapely.geometry import MultiPolygon, Polygon, box, mapping
from shapely.strtree import STRtree

logger = logging.getLogger("s1_overlap")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

CMR_GRANULES_URL = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

DEFAULT_SHORT_NAMES = ["SENTINEL-1A_DP_GRD_MEDIUM", "SENTINEL-1C_DP_GRD_MEDIUM"]
DEFAULT_BBOX = "-180,60,180,90"
DEFAULT_FILTER = "*_1SDH_*"

# Match the daily COGs' CRS so areas are metric and the antimeridian /
# pole singularities of lon/lat never come up. See module docstring.
WORKING_EPSG = 3413

# Points inserted along each footprint edge before projecting. CMR gives 4
# corners; a straight line between two corners 25 degrees of longitude apart at
# 80N is not the swath edge. 20 is well past the point of diminishing returns
# (areas change <0.1% beyond ~8) and costs nothing at this granule count.
DENSIFY_PER_EDGE = 20

_TO_3413 = Transformer.from_crs("EPSG:4326", f"EPSG:{WORKING_EPSG}", always_xy=True)
_TO_4326 = Transformer.from_crs(f"EPSG:{WORKING_EPSG}", "EPSG:4326", always_xy=True)

# S1A_EW_GRDM_1SDH_20260628T015949_...  -> satellite + acquisition start
_GRANULE_RE = re.compile(r"^(S1[A-D])_(\w+?)_(\w+?)_(\w+?)_(\d{8}T\d{6})_")


# ---------------------------------------------------------------------------
# CMR
# ---------------------------------------------------------------------------

def _cmr_page(params: dict, search_after: Optional[str]) -> tuple[list, Optional[str]]:
    url = f"{CMR_GRANULES_URL}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url)
    if search_after:
        req.add_header("CMR-Search-After", search_after)
    with urllib.request.urlopen(req, timeout=120) as r:
        body = json.load(r)
        return body.get("items", []), r.headers.get("CMR-Search-After")


def cmr_granules(short_name: str, day: date, bbox: str) -> Iterator[dict]:
    """Yield raw CMR umm_json items for one collection on one UTC day."""
    params = {
        "short_name": short_name,
        "temporal": f"{day.isoformat()}T00:00:00Z,"
                    f"{(day + timedelta(days=1)).isoformat()}T00:00:00Z",
        "bounding_box": bbox,
        "page_size": 2000,
    }
    search_after = None
    while True:
        items, search_after = _cmr_page(params, search_after)
        if not items:
            return
        yield from items
        if not search_after:
            return


# ---------------------------------------------------------------------------
# Geometry
# ---------------------------------------------------------------------------

def _unwrap_lons(lons: Sequence[float]) -> List[float]:
    """Remove 2pi jumps so a ring crossing the antimeridian interpolates the
    short way round. Without this, a corner pair (179, -179) interpolates
    westward across the entire globe and the footprint becomes garbage."""
    out = [lons[0]]
    for lon in lons[1:]:
        prev = out[-1]
        delta = lon - prev
        if delta > 180:
            lon -= 360
        elif delta < -180:
            lon += 360
        out.append(lon)
    return out


def _densify_ring(lons: Sequence[float], lats: Sequence[float],
                  per_edge: int = DENSIFY_PER_EDGE) -> tuple[List[float], List[float]]:
    dl, da = [], []
    for i in range(len(lons) - 1):
        for k in range(per_edge):
            t = k / per_edge
            dl.append(lons[i] + (lons[i + 1] - lons[i]) * t)
            da.append(lats[i] + (lats[i + 1] - lats[i]) * t)
    dl.append(lons[-1])
    da.append(lats[-1])
    return dl, da


def gpolygon_to_3413(gpolys: List[dict]) -> Optional[Polygon | MultiPolygon]:
    """CMR GPolygons (lon/lat corner rings) -> shapely geometry in EPSG:3413."""
    parts = []
    for gp in gpolys:
        points = gp.get("Boundary", {}).get("Points", [])
        if len(points) < 4:
            continue
        lons = [p["Longitude"] for p in points]
        lats = [p["Latitude"] for p in points]
        if (lons[0], lats[0]) != (lons[-1], lats[-1]):
            lons.append(lons[0])
            lats.append(lats[0])
        lons = _unwrap_lons(lons)
        lons, lats = _densify_ring(lons, lats)
        xs, ys = _TO_3413.transform(lons, lats)
        ring = [(x, y) for x, y in zip(xs, ys)
                if x == x and y == y and abs(x) != float("inf")]
        if len(ring) < 4:
            continue
        poly = Polygon(ring)
        if not poly.is_valid:
            poly = poly.buffer(0)
        if poly.is_empty or poly.area <= 0:
            continue
        parts.append(poly)
    if not parts:
        return None
    if len(parts) == 1:
        return parts[0]
    merged = MultiPolygon(parts).buffer(0)
    return merged if not merged.is_empty else None


def _ring_to_unwrapped_4326(coords) -> List[tuple]:
    xs = [c[0] for c in coords]
    ys = [c[1] for c in coords]
    lons, lats = _TO_4326.transform(xs, ys)
    return list(zip(_unwrap_lons(lons), lats))


def _split_antimeridian(poly: Polygon) -> List[Polygon]:
    """Cut a polygon living in unwrapped longitude space back into pieces
    inside [-180, 180], per RFC 7946.

    18 of the ~380 Arctic granules per week straddle the dateline. Left
    unwrapped their GeoJSON spans 359.9 degrees of longitude and renders as a
    smear across the whole map. (Areas are computed in EPSG:3413 and were
    never affected by this — it is purely a lon/lat representation problem.)
    """
    minx, _, maxx, _ = poly.bounds
    import math
    kmin = math.floor((minx + 180.0) / 360.0)
    kmax = math.floor((maxx + 180.0) / 360.0)
    parts: List[Polygon] = []
    for k in range(kmin, kmax + 1):
        window = box(-180.0 + 360.0 * k, -90.0, 180.0 + 360.0 * k, 90.0)
        piece = poly.intersection(window)
        if piece.is_empty:
            continue
        piece = translate(piece, xoff=-360.0 * k)
        parts.extend(piece.geoms if piece.geom_type == "MultiPolygon" else [piece])
    return [p for p in parts if p.geom_type == "Polygon" and not p.is_empty]


def to_4326(geom):
    """EPSG:3413 geometry -> antimeridian-safe WGS84 for GeoJSON output.

    Rings are unwrapped before splitting, so a footprint crossing the dateline
    becomes a two-part MultiPolygon instead of a globe-spanning artifact. Safe
    here because no S1 granule reaches the pole (max observed latitude 87.6N,
    inclination 98.18deg) — a pole-enclosing ring would need different handling.
    """
    src = geom.geoms if geom.geom_type == "MultiPolygon" else [geom]
    parts: List[Polygon] = []
    for p in src:
        unwrapped = Polygon(
            _ring_to_unwrapped_4326(p.exterior.coords),
            [_ring_to_unwrapped_4326(r.coords) for r in p.interiors],
        )
        if not unwrapped.is_valid:
            unwrapped = unwrapped.buffer(0)
        if unwrapped.is_empty:
            continue
        for q in (unwrapped.geoms if unwrapped.geom_type == "MultiPolygon"
                  else [unwrapped]):
            parts.extend(_split_antimeridian(q))
    if not parts:
        return geom
    return parts[0] if len(parts) == 1 else MultiPolygon(parts)


# ---------------------------------------------------------------------------
# Granule model
# ---------------------------------------------------------------------------

class Granule:
    __slots__ = ("ur", "satellite", "mode", "start", "end", "geom_3413", "area_km2")

    def __init__(self, ur: str, satellite: str, mode: str,
                 start: datetime, end: datetime, geom_3413):
        self.ur = ur
        self.satellite = satellite
        self.mode = mode
        self.start = start
        self.end = end
        self.geom_3413 = geom_3413
        self.area_km2 = geom_3413.area / 1e6

    @property
    def acq_date(self) -> date:
        return self.start.date()


def _parse_dt(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc)


def parse_granule(item: dict) -> Optional[Granule]:
    umm = item.get("umm", {})
    ur = umm.get("GranuleUR")
    if not ur:
        return None
    m = _GRANULE_RE.match(ur)
    if not m:
        logger.debug(f"Unparseable granule name, skipping: {ur}")
        return None
    satellite, mode = m.group(1), m.group(2)

    rng = umm.get("TemporalExtent", {}).get("RangeDateTime", {})
    if not rng.get("BeginningDateTime"):
        return None
    start = _parse_dt(rng["BeginningDateTime"])
    end = _parse_dt(rng.get("EndingDateTime") or rng["BeginningDateTime"])

    gpolys = (umm.get("SpatialExtent", {})
                 .get("HorizontalSpatialDomain", {})
                 .get("Geometry", {})
                 .get("GPolygons", []))
    if not gpolys:
        return None
    geom = gpolygon_to_3413(gpolys)
    if geom is None:
        logger.warning(f"No usable footprint for {ur}")
        return None
    return Granule(ur, satellite, mode, start, end, geom)


def collect_granules(days: Sequence[date], short_names: Sequence[str],
                     bbox: str, filter_pattern: str) -> List[Granule]:
    granules: dict[str, Granule] = {}
    for day in days:
        n_day = 0
        for sn in short_names:
            for item in cmr_granules(sn, day, bbox):
                ur = item.get("umm", {}).get("GranuleUR", "")
                if filter_pattern and not fnmatch(ur, filter_pattern):
                    continue
                g = parse_granule(item)
                if g is None:
                    continue
                # A granule can surface on two adjacent day queries if its
                # acquisition straddles midnight; key by UR to keep one.
                granules[g.ur] = g
                n_day += 1
        logger.info(f"  {day.isoformat()}: {n_day} granule(s)")
    return sorted(granules.values(), key=lambda g: (g.start, g.ur))


# ---------------------------------------------------------------------------
# Overlap
# ---------------------------------------------------------------------------

def compute_overlaps(granules: List[Granule], min_area_km2: float) -> List[dict]:
    """Pairwise intersections via an STRtree. At ~100 granules/day over a
    handful of retained dates this is a few hundred polygons — the tree is
    built here on the fly rather than persisted, because building it costs
    milliseconds and a persisted index could only go stale."""
    if not granules:
        return []
    geoms = [g.geom_3413 for g in granules]
    tree = STRtree(geoms)
    rows: List[dict] = []
    seen: set[tuple[int, int]] = set()

    for i, g_a in enumerate(granules):
        for j in tree.query(g_a.geom_3413):
            j = int(j)
            if j == i:
                continue
            key = (i, j) if i < j else (j, i)
            if key in seen:
                continue
            seen.add(key)
            a, b = granules[key[0]], granules[key[1]]
            inter = a.geom_3413.intersection(b.geom_3413)
            if inter.is_empty:
                continue
            area_km2 = inter.area / 1e6
            if area_km2 < min_area_km2:
                continue
            union_km2 = a.area_km2 + b.area_km2 - area_km2
            rows.append({
                "granule_a": a.ur,
                "granule_b": b.ur,
                "sat_a": a.satellite,
                "sat_b": b.satellite,
                "date_a": a.acq_date.isoformat(),
                "date_b": b.acq_date.isoformat(),
                "day_diff": abs((b.acq_date - a.acq_date).days),
                "start_a": a.start.isoformat(),
                "start_b": b.start.isoformat(),
                "dt_hours": round(abs((b.start - a.start).total_seconds()) / 3600.0, 3),
                "area_a_km2": round(a.area_km2, 3),
                "area_b_km2": round(b.area_km2, 3),
                "intersect_km2": round(area_km2, 3),
                "frac_of_a": round(area_km2 / a.area_km2, 6) if a.area_km2 else 0.0,
                "frac_of_b": round(area_km2 / b.area_km2, 6) if b.area_km2 else 0.0,
                # union = a + b - intersection, so IoU needs no union geometry.
                "iou": round(area_km2 / union_km2, 6) if union_km2 else 0.0,
                "_geom": inter,
            })
    rows.sort(key=lambda r: (-r["intersect_km2"], r["granule_a"], r["granule_b"]))
    return rows


# ---------------------------------------------------------------------------
# Output
# ---------------------------------------------------------------------------

OVERLAP_COLUMNS = [
    "granule_a", "granule_b", "sat_a", "sat_b",
    "date_a", "date_b", "day_diff",
    "start_a", "start_b", "dt_hours",
    "area_a_km2", "area_b_km2", "intersect_km2",
    "frac_of_a", "frac_of_b", "iou",
]

# Parquet is the primary artifact. Geometry is stored in EPSG:3413, the CRS
# everything was computed in: GeoParquet carries the CRS in its metadata, so
# consumers reproject correctly and DuckDB's ST_* functions work in metres.
# Storing native also means the antimeridian never arises — the dateline split
# is only needed because GeoJSON (RFC 7946) mandates lon/lat. GeoJSON output
# stays available behind --geojson for viewers that want lon/lat.


def _typed_overlaps_frame(rows: List[dict]):
    """Attribute frame with real dtypes. day_diff/date columns are what get
    filtered on, so they are typed rather than left as strings — that is what
    lets a reader push `day_diff = 4` down to the row group."""
    import pandas as pd
    df = pd.DataFrame([{k: r[k] for k in OVERLAP_COLUMNS} for r in rows],
                      columns=OVERLAP_COLUMNS)
    if df.empty:
        return df
    for col in ("granule_a", "granule_b", "sat_a", "sat_b"):
        df[col] = df[col].astype("string")
    for col in ("date_a", "date_b"):
        df[col] = pd.to_datetime(df[col]).dt.date
    for col in ("start_a", "start_b"):
        df[col] = pd.to_datetime(df[col], utc=True)
    df["day_diff"] = df["day_diff"].astype("int16")
    for col in ("dt_hours", "area_a_km2", "area_b_km2", "intersect_km2",
                "frac_of_a", "frac_of_b", "iou"):
        df[col] = df[col].astype("float64")
    return df


def write_footprints_parquet(granules: List[Granule], path: Path) -> None:
    import geopandas as gpd
    import pandas as pd
    df = pd.DataFrame({
        "granule_ur": pd.array([g.ur for g in granules], dtype="string"),
        "satellite": pd.array([g.satellite for g in granules], dtype="string"),
        "mode": pd.array([g.mode for g in granules], dtype="string"),
        "acq_date": [g.acq_date for g in granules],
        "start": pd.to_datetime([g.start for g in granules], utc=True),
        "end": pd.to_datetime([g.end for g in granules], utc=True),
        "area_km2": [round(g.area_km2, 3) for g in granules],
    })
    gdf = gpd.GeoDataFrame(df, geometry=[g.geom_3413 for g in granules],
                           crs=f"EPSG:{WORKING_EPSG}")
    gdf.to_parquet(path, index=False, compression="zstd")
    logger.info(f"footprints → {path} ({len(gdf)} granule(s), "
                f"{path.stat().st_size / 1024:.0f} KiB)")


def write_overlaps_parquet(rows: List[dict], path: Path) -> None:
    import geopandas as gpd
    df = _typed_overlaps_frame(rows)
    gdf = gpd.GeoDataFrame(df, geometry=[r["_geom"] for r in rows],
                           crs=f"EPSG:{WORKING_EPSG}")
    # Sorting by the columns people filter on clusters row groups, so a
    # `day_diff = 4` predicate can skip most of the file outright.
    if not gdf.empty:
        gdf = gdf.sort_values(["day_diff", "intersect_km2"],
                              ascending=[True, False])
    gdf.to_parquet(path, index=False, compression="zstd")
    logger.info(f"overlaps → {path} ({len(gdf)} pair(s), "
                f"{path.stat().st_size / 1024:.0f} KiB)")


def write_footprints_geojson(granules: List[Granule], path: Path) -> None:
    feats = [{
        "type": "Feature",
        "id": g.ur,
        "geometry": mapping(to_4326(g.geom_3413)),
        "properties": {
            "granule_ur": g.ur,
            "satellite": g.satellite,
            "mode": g.mode,
            "acq_date": g.acq_date.isoformat(),
            "start": g.start.isoformat(),
            "end": g.end.isoformat(),
            "area_km2": round(g.area_km2, 3),
        },
    } for g in granules]
    path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))
    logger.info(f"footprints (lon/lat) → {path}")


def write_overlaps_geojson(rows: List[dict], path: Path) -> None:
    feats = [{
        "type": "Feature",
        "id": f"{r['granule_a']}__{r['granule_b']}",
        "geometry": mapping(to_4326(r["_geom"])),
        "properties": {k: r[k] for k in OVERLAP_COLUMNS},
    } for r in rows]
    path.write_text(json.dumps({"type": "FeatureCollection", "features": feats}))
    logger.info(f"overlap AOIs (lon/lat) → {path}")


# ---------------------------------------------------------------------------
# Date resolution
# ---------------------------------------------------------------------------

def _maap_s3_client(maap):
    import boto3
    response = maap.aws.workspace_bucket_credentials()
    creds = response.get("credentials") if isinstance(response, dict) else None
    if not creds or "aws_access_key_id" not in creds:
        raise RuntimeError(f"Unexpected creds shape: {response!r}")
    return boto3.client(
        "s3",
        aws_access_key_id=creds["aws_access_key_id"],
        aws_secret_access_key=creds["aws_secret_access_key"],
        aws_session_token=creds["aws_session_token"],
    )


def ingested_dates_from_s3(bucket: str, prefix: str, collection_id: str,
                           limit: Optional[int]) -> List[date]:
    """The dates that actually hold a COG, newest-first-truncated to `limit`.

    Mirrors prune_old_cogs: retention keeps the N most recent dates that have
    data, so this is the authoritative definition of "currently ingested" —
    it is NOT the last N calendar days.
    """
    from maap.maap import MAAP
    import os
    token = os.environ.get("MAAP_TOKEN")
    if token:
        os.environ["MAAP_PGT"] = token
    maap = MAAP(maap_host=os.environ.get("MAAP_HOST", "api.maap-project.org"))
    s3 = _maap_s3_client(maap)

    key_prefix = "/".join(p for p in (prefix.strip("/"), collection_id) if p) + "/"
    pattern = re.compile(r"/(\d{4})/(\d{2})/(\d{2})/")
    found: set[date] = set()
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=key_prefix):
        for obj in page.get("Contents", []):
            m = pattern.search(obj["Key"])
            if m:
                found.add(date(int(m.group(1)), int(m.group(2)), int(m.group(3))))
    dates = sorted(found, reverse=True)
    if limit:
        dates = dates[:limit]
    logger.info(f"S3 reports {len(dates)} ingested date(s) under "
                f"s3://{bucket}/{key_prefix}")
    return sorted(dates)


def upload(path: Path, bucket: str, key: str) -> str:
    from maap.maap import MAAP
    import os
    token = os.environ.get("MAAP_TOKEN")
    if token:
        os.environ["MAAP_PGT"] = token
    maap = MAAP(maap_host=os.environ.get("MAAP_HOST", "api.maap-project.org"))
    s3 = _maap_s3_client(maap)
    s3.upload_file(str(path), bucket, key)
    url = f"s3://{bucket}/{key}"
    logger.info(f"uploaded → {url}")
    return url


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    scope = p.add_mutually_exclusive_group(required=True)
    scope.add_argument("--from-s3", action="store_true",
                       help="Scope to the dates that actually hold a COG in S3 "
                            "(the real 'currently ingested' set). Needs MAAP_TOKEN.")
    scope.add_argument("--dates",
                       help="Comma-separated YYYYMMDD list. For backfill / local runs.")
    scope.add_argument("--lookback-days", type=int,
                       help="Every calendar day in the last N days. Note this "
                            "differs from --from-s3 when the feed has gaps.")

    p.add_argument("--retain-days", type=int, default=7,
                   help="With --from-s3, cap to the N most recent ingested dates "
                        "(default 7, matching the pipeline's RETAIN_DAYS).")
    p.add_argument("--collection-id", default="frozon-s1-ew-hh-daily")
    p.add_argument("--s3-bucket", default="maap-ops-workspace")
    p.add_argument("--s3-prefix", default="jdrodrig/frozon/cogs/")
    p.add_argument("--cmr-short-names", default=",".join(DEFAULT_SHORT_NAMES))
    p.add_argument("--cmr-bbox", default=DEFAULT_BBOX)
    p.add_argument("--filter", dest="filter_pattern", default=DEFAULT_FILTER)
    p.add_argument("--min-overlap-km2", type=float, default=100.0,
                   help="Drop intersections smaller than this. Sliver overlaps "
                        "carry no trackable features (default 100).")
    p.add_argument("--output", default="output")
    p.add_argument("--geojson", action="store_true",
                   help="Also emit lon/lat GeoJSON alongside the GeoParquet, for "
                        "viewers that can't read GeoParquet. Antimeridian-split "
                        "per RFC 7946; much larger than the Parquet.")
    p.add_argument("--upload", action="store_true",
                   help="Upload artifacts to S3 under <prefix>/<collection>/overlap/.")
    args = p.parse_args()

    short_names = [s.strip() for s in args.cmr_short_names.split(",") if s.strip()]

    # --- resolve the date scope ---
    if args.from_s3:
        days = ingested_dates_from_s3(args.s3_bucket, args.s3_prefix,
                                      args.collection_id, args.retain_days)
    elif args.dates:
        days = sorted(datetime.strptime(d.strip(), "%Y%m%d").date()
                      for d in args.dates.split(",") if d.strip())
    else:
        today = datetime.now(timezone.utc).date()
        days = sorted(today - timedelta(days=i)
                      for i in range(1, args.lookback_days + 1))
    if not days:
        logger.error("No dates in scope; nothing to do.")
        return 1
    logger.info(f"Scope: {len(days)} date(s) {days[0]} … {days[-1]}")

    # --- gather granules ---
    logger.info(f"Querying CMR across {short_names} bbox={args.cmr_bbox} "
                f"filter={args.filter_pattern!r}")
    granules = collect_granules(days, short_names, args.cmr_bbox, args.filter_pattern)
    if not granules:
        logger.error("No granules found in scope.")
        return 1
    logger.info(f"{len(granules)} granule(s) with usable footprints")

    # --- overlaps ---
    rows = compute_overlaps(granules, args.min_overlap_km2)
    logger.info(f"{len(rows)} overlapping pair(s) ≥ {args.min_overlap_km2} km²")

    by_diff: dict[int, int] = {}
    for r in rows:
        by_diff[r["day_diff"]] = by_diff.get(r["day_diff"], 0) + 1
    if by_diff:
        logger.info("pairs by day_diff: " + ", ".join(
            f"{d}d={n}" for d, n in sorted(by_diff.items())))

    # --- write ---
    out_dir = Path(args.output)
    out_dir.mkdir(parents=True, exist_ok=True)
    artifacts = [out_dir / "footprints.parquet", out_dir / "overlaps.parquet"]
    write_footprints_parquet(granules, artifacts[0])
    write_overlaps_parquet(rows, artifacts[1])

    if args.geojson:
        fg = out_dir / "footprints.geojson"
        og = out_dir / "overlaps.geojson"
        write_footprints_geojson(granules, fg)
        write_overlaps_geojson(rows, og)
        artifacts += [fg, og]

    if args.upload:
        base = "/".join(p for p in (args.s3_prefix.strip("/"),
                                    args.collection_id, "overlap") if p)
        for path in artifacts:
            upload(path, args.s3_bucket, f"{base}/{path.name}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
