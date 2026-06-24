# Data Notes — CMEMS Ocean Currents (surface eastward / northward velocity)

Reference notes for the CMEMS ingestion pipeline (`src/ingest_cmems.py`).
Companion to `DATA.md`, `DATA_OSISAF.md`, and `DATA_ECMWF.md`. Read alongside
`PIPELINE_TEMPLATE.md`.

## Source

**CMEMS** = Copernicus Marine Service, the EU Copernicus marine data store
(https://marine.copernicus.eu). Free and open data, but — unlike ECMWF Open
Data — **credentialed**: it requires a free Copernicus Marine account.

This pipeline ingests **two** surface ocean-current components, each as its own
COG collection:

| Product key | Variable | CMEMS var | Collection | Units |
|---|---|---|---|---|
| `ocean_u` | eastward (zonal) sea-water velocity   | `uo` | `frozon-cmems-ocean-u-daily` | m s⁻¹ |
| `ocean_v` | northward (meridional) sea-water velocity | `vo` | `frozon-cmems-ocean-v-daily` | m s⁻¹ |

- **Product:** GLOBAL_ANALYSISFORECAST_PHY_001_024
- **Dataset:** `cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m`
  (currents, **daily mean**, 1/12° ≈ 0.083° global regular lat/lon,
  analysis + forecast)

> **`ocean_u` / `ocean_v` are vector *components*, not a speed or bearing.** They
> are GEOGRAPHIC eastward (`uo`) / northward (`vo`) velocity — the same
> convention as the ECMWF 10 m wind components. Combine the two collections for
> speed/direction: `speed = hypot(u, v)`, `dir = atan2(v, u)`.

### Why CMEMS, and why it's *not* like ECMWF Open Data

These two variables were in the original five-dataset request but are **absent
from the ECMWF Open Data feed** (which publishes only a curated atmospheric/wave
subset — see `DATA_ECMWF.md`). Surface ocean currents live in Copernicus Marine,
so they come from here instead.

- **Credentialed.** Needs a free Copernicus Marine account. The worker pulls the
  `username\npassword` from a MAAP secret (default `copernicus-marine-frozon`)
  and passes them straight to the `copernicusmarine` toolbox — no global login
  or config file. The **GH Actions runner never touches Copernicus**; only the
  DPS worker authenticates.
- **Server-side subset, not a whole-file download.** `copernicusmarine.subset()`
  fetches just (one variable × one day × surface layer × Arctic bbox), so each
  job pulls a small NetCDF rather than a global volume — analogous to the ECMWF
  byte-range fetch.
- **No deterministic per-file URL.** Unlike OSI SAF / ECMWF there's nothing cheap
  to HEAD-probe, so the runner's "discovery" is just the most recent N calendar
  days; the worker returns exit code 6 if a date isn't in the store yet.

## Surface layer

The dataset is 3-D (has a `depth` axis). We subset `minimum_depth=0,
maximum_depth=1` to grab only the **uppermost layer** (~0.49 m sits in that
range), then squeeze the `depth` (and `time`) dimension to a 2-D field.

## Grid handling

- **Source grid:** global 1/12° regular lat/lon (EPSG:4326, −180..180,
  ascending latitude). The subset is cropped to an Arctic bbox
  (`-180,20,180,90` by default) — down to 20°N so the EPSG:3413 grid's
  mid-latitude corners are fully covered after the warp, with margin.
- **NetCDF → GeoTIFF:** read with xarray, squeeze time+depth, write via
  rioxarray (it derives the correct north-up geotransform from the lat/lon
  coordinates — no manual grid forcing or longitude roll needed). Land /
  no-ocean cells are NaN.
- **Reproject:** `gdalwarp` EPSG:4326 → the **canonical EPSG:3413 10 km Arctic
  grid** (same `TGT_TE`/`TGT_RES` as every other Frozon worker), bilinear
  (continuous field). The current components are reprojected as **scalar fields**
  — the values stay geographic eastward/northward m s⁻¹; only the pixel
  locations move onto the polar grid. They are NOT rotated to grid-relative
  axes (same treatment as the ECMWF winds).

## Value semantics

- **ocean_u (`uo`)** — eastward sea-water velocity, m s⁻¹ (can be negative).
- **ocean_v (`vo`)** — northward sea-water velocity, m s⁻¹ (can be negative).

NoData is NaN (land and any masked cells). Surface currents are typically a few
tenths of a m s⁻¹, with stronger boundary currents up to ~1–2 m s⁻¹.

## S3 layout

`s3://maap-ops-workspace/jdrodrig/frozon/cogs/<collection-id>/YYYY/MM/DD/<collection-id>_YYYYMMDD_COG.tif`

The date partition comes from the daily-mean date (nominal 12:00 UTC). No STAC
catalog is written — like OSI SAF / ECMWF, nothing in this pipeline consumes one
and the units live in the COG band metadata.

## Deploy note — the one extra step vs ECMWF

Because CMEMS is credentialed, deploying this worker has one extra step the
ECMWF worker didn't:

1. Register a free account at https://marine.copernicus.eu.
2. Store it as a MAAP secret named `copernicus-marine-frozon`, body = two lines:
   ```
   <username>
   <password>
   ```
   (e.g. from a notebook: `maap.secrets.add_secret("copernicus-marine-frozon", "<user>\n<pass>")`).
3. Register + smoke-test as usual. The worker reads that secret at runtime via
   `maap.secrets.get_secret(...)`.
