# Data Notes — OSI SAF Sea Ice (concentration / type / edge)

Reference notes for the OSI SAF ingestion pipeline (`src/ingest_osisaf.py`).
Companion to `DATA.md` (OPERA RTC-S1). Read alongside `PIPELINE_TEMPLATE.md`.

## Source

**OSI SAF** = EUMETSAT Ocean & Sea Ice Satellite Application Facility.
Operational daily sea-ice products, multi-sensor (SSMIS + AMSR2 + ASCAT).
Product page: https://osi-saf.eumetsat.int/products/sea-ice-products

This pipeline ingests **three** Northern-Hemisphere products, each as its own
COG collection:

| Product | OSI ID | Variable | Collection | Type |
|---|---|---|---|---|
| Sea Ice Concentration | OSI-401-d | `ice_conc` | `frozon-osisaf-sic-daily` | continuous % |
| Sea Ice Type | OSI-403-d | `ice_type` | `frozon-osisaf-icetype-daily` | categorical |
| Sea Ice Edge | OSI-402-d | `ice_edge` | `frozon-osisaf-iceedge-daily` | categorical |

### Why OSI SAF is *not* like OPERA/S1

- **Not on NASA CMR / no Earthdata Login.** Distributed by the Norwegian Met
  THREDDS server over **anonymous HTTP**. No tokens, no MAAP EDL secret.
- **Deterministic filenames** — no granule search. The worker constructs the
  URL from (product, date) and downloads one file.
- **One file == one full hemisphere grid** — no mosaicking.
- **Two of three products are categorical** — nearest-neighbor resampling
  only; class codes must never be blended.

## Access — met.no THREDDS

Anonymous HTTP fileServer. URL pattern (NH):

```
https://thredds.met.no/thredds/fileServer/osisaf/met.no/ice/<dir>/<YYYY>/<MM>/<file>
```

where `<dir>` ∈ {`conc`, `type`, `edge`} and the filename is:

```
ice_conc_nh_polstere-100_multi_<YYYYMMDD>1200.nc
ice_type_nh_polstere-100_multi_<YYYYMMDD>1200.nc
ice_edge_nh_polstere-100_multi_<YYYYMMDD>1200.nc
```

- Nominal product time is always **1200** (12:00 UTC daily analysis).
- `nh` = Northern Hemisphere; `sh` exists but is out of scope (Frozon is Arctic).
- `polstere-100` = polar-stereographic, 10 km (the "100" is 100×100 → 10 km).
- `multi` = multi-sensor blend.
- met.no asks: **don't spawn parallel downloads** — the worker fetches one
  file per job; the runner only does light sequential HEAD checks.

FTP mirror also exists (`ftp://osisaf.met.no/prod` last 31 days, `/archive`
since 2005) but HTTP is more firewall-friendly from GH Actions / MAAP.

## Grid / projection — the important gotcha

**Source CRS is EPSG:3411, not 3413.** All three NH files are on the NSIDC
Sea Ice Polar Stereographic North grid:

- **Hughes 1980 ellipsoid** (`a = 6378273 m`, `1/f = 298.279…`)
- `lat_ts = 70°N`, `lon_0 = -45°`, pole at (0,0) — i.e. **EPSG:3411**.
- 760 cols × 1120 rows, 10 km, edge-aligned origin **(-3850000, 5850000)** m.

`EPSG:3413` (Frozon's canonical Arctic grid) is the *WGS84* version of the
same projection, so 3411→3413 is a real but small datum shift — handled by
`gdalwarp`.

### GDAL geotransform quirk (why the worker *forces* the grid)

GDAL's netCDF driver reads the geotransform inconsistently across the three:

- `ice_conc` → clean (origin -3850000, pixel 10000). ✅
- `ice_type` / `ice_edge` → **half-pixel-shifted & slightly-off** (origin
  -3845000, pixel 9986.84 × 9991.07). GDAL derives it from cell-centre
  `xc`/`yc` without applying the half-pixel offset. ❌ (~5 km misregistration)

All three are really the **same 760×1120 grid**. The worker therefore
*forces* the source geotransform (`SRC_TRANSFORM` / EPSG:3411) on every
product rather than trusting per-file auto-detect, then warps to canonical
EPSG:3413 (`-te -3850000 -5350000 3750000 5850000 -tr 10000 10000`).

> Each NetCDF also carries 2-D `lat`/`lon` geolocation arrays; do **not**
> warp via `-geoloc`. The projected `xc`/`yc` grid is exact and far faster.

## Value semantics

### `ice_conc` (concentration)

- Stored Int16, `scale_factor = 0.01`, `units = %`, `_FillValue = -999`.
- Worker emits **Float32 percent, 0–100**, NoData **NaN** (unscale ×0.01,
  fill → NaN). Warp resampling: **bilinear**; overviews: average.
- Sanity: a mid-June NH grid reads min 0, max 100, mean ≈ 19% (mostly ocean +
  land-masked, ice toward the pole).

### `ice_type` (type) — categorical

`flag_values = {1,2,3,4}`, `_FillValue = -1`:

| Code | Meaning |
|---|---|
| 1 | open_water (no ice / very open ice) |
| 2 | first_year_ice |
| 3 | multi_year_ice |
| 4 | ambiguous |

- Worker emits **Int16** (so `-1` fill is representable everywhere), NoData
  `-1`. Warp resampling: **nearest**; overviews: **mode**.
- Summer melt note: in June the type retrieval often collapses to mostly
  `1` (open water) + `4` (ambiguous), with FYI/MYI sparse — this is expected,
  not a bug. (Validated 2026-06-16: classes present were {1, 4}.)

### `ice_edge` (edge) — categorical

`flag_values = {1,2,3}`, `_FillValue = -1`:

| Code | Meaning | Concentration band |
|---|---|---|
| 1 | open_water | < 30% |
| 2 | open_ice | 30–70% |
| 3 | close_ice | > 70% |

- Worker emits **Int16**, NoData `-1`, nearest / mode (same as type).
- Validated 2026-06-16: classes present were {1, 2, 3}.

The product's `flag_values` / `flag_meanings` are written onto the COG band
metadata and into STAC item properties (`osisaf:flag_values`,
`osisaf:flag_meanings`).

## Output layout

```
s3://maap-ops-workspace/jdrodrig/frozon/cogs/<collection-id>/YYYY/MM/DD/<collection-id>_YYYYMMDD_COG.tif
```

STAC item datetime is stamped to the date at **12:00 UTC** (the nominal
analysis time), driving the `YYYY/MM/DD` partition.

## Zarr time series

Each COG collection has an optional daily-synced Zarr time series, built by
the **data-agnostic** Zarr worker (`frozon-iss-ingest-zarr`) — no OSI SAF
worker code is involved. Wiring is config-only
(`.github/workflows/daily-osisaf-zarr-ingest.yml`, cron 12:30 UTC):

| COG collection | Zarr store |
|---|---|
| `frozon-osisaf-sic-daily` | `frozon-osisaf-sic-zarr` |
| `frozon-osisaf-icetype-daily` | `frozon-osisaf-icetype-zarr` |
| `frozon-osisaf-iceedge-daily` | `frozon-osisaf-iceedge-zarr` |

Stored at `s3://<bucket>/jdrodrig/frozon/zarrs/<zarr-collection>/<zarr-collection>.zarr/`.

Two things make the categorical (type/edge) Zarrs safe despite the builder
being written for continuous data:

- **No resampling on stack.** The streaming builder places each slice by
  offset on a union grid at the median resolution — it does *not* reproject.
  Because every OSI SAF COG is already on the identical canonical 760×1120
  EPSG:3413 grid, the union grid *is* that grid and slices drop in 1:1, so
  class codes are never blended. (This is the payoff of resampling COGs to a
  fixed canonical grid rather than keeping native extents.)
- **Time regex differs from S1/OPERA.** OSI SAF COGs are named
  `<collection>_YYYYMMDD_COG.tif` (no `_daily_` segment), so the workflow sets
  `TIME_REGEX = _(?P<start_date>\d{8})_COG`.

Nuance: the builder casts every slice to `float32` with `NaN` fill. For
concentration that's lossless. For type/edge the integer class codes become
floats (`1.0`–`4.0`) and the `-1` nodata is preserved as `-1.0` (it is *not*
remapped to `NaN`). Downstream readers should treat both `-1.0` and `NaN` as
no-data for the categorical Zarrs.

## Coverage expectations

Unlike Sentinel-1 SAR, these passive-microwave/scatterometer blends give
**full pan-Arctic coverage every day** (no swath gaps). A single daily COG is
already spatially complete — the Zarr time series mainly adds the temporal
dimension, not spatial fill-in.

## Files validated against

`ice_{conc,type,edge}_nh_polstere-100_multi_202606161200.nc` (2026-06-16),
full extract→warp→COG chain run locally before the worker was deployed.
