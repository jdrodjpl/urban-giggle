# Data Notes — ECMWF Open Data (near-surface air temperature + 10 m wind)

Reference notes for the ECMWF ingestion pipeline (`src/ingest_ecmwf.py`).
Companion to `DATA.md` (OPERA RTC-S1) and `DATA_OSISAF.md`. Read alongside
`PIPELINE_TEMPLATE.md`.

## Source

**ECMWF Open Data** = the European Centre for Medium-Range Weather Forecasts'
free real-time forecast feed (IFS HRES). Distributed over **anonymous HTTP**
from https://data.ecmwf.int/forecasts — no account, no token, no EDL.
Portal / docs: https://www.ecmwf.int/en/forecasts/datasets/open-data

This pipeline ingests **three** near-surface fields, each as its own COG
collection:

| Product key | ECMWF param | Field | Collection | Units |
|---|---|---|---|---|
| `airtemp` | `2t`  | 2 m air temperature        | `frozon-ecmwf-airtemp-daily` | °C (see unit note below) |
| `wind_ns` | `10v` | 10 m northward (V) wind    | `frozon-ecmwf-wind-ns-daily` | m s⁻¹ |
| `wind_ew` | `10u` | 10 m eastward (U) wind     | `frozon-ecmwf-wind-ew-daily` | m s⁻¹ |

All three are **continuous** fields → bilinear resampling, Float32, NoData NaN.

> **`wind_direction_ns` / `wind_direction_ew` are wind vector *components*, not
> a direction in degrees.** NS = northward component (`10v`), EW = eastward
> component (`10u`). To get speed/direction, combine the two collections:
> `speed = hypot(ew, ns)`, `dir = atan2(ns, ew)`.

### Ocean currents are intentionally absent

The original request also asked for ocean-current U/V surface velocity. **ECMWF
Open Data's real-time feed does not publish surface ocean currents.** They exist
in the ECMWF GRIB parameter database (`ocu`=151131, `ocv`=151132) and the MARS
archive — and the 2026 Cycle 50r1 NEMO4-SI³ ocean model added 40+ ocean/sea-ice
variables to MARS — but the `ecmwf-opendata` client's single-level fields are
only: `10u, 10v, 2t, msl, ro, skt, sp, st, stl1, tcwv, tp`. To add ocean
currents later, pull them from **Copernicus Marine (CMEMS)** (`uo`/`vo`) as a
separate source worker — that's a deliberately deferred task, not an oversight.

### Why ECMWF is *not* like OPERA/S1 (and how it compares to OSI SAF)

- **Not on NASA CMR / no Earthdata Login.** Like OSI SAF, anonymous HTTP.
- **No granule search.** Per-run files live at deterministic URLs.
- **One GRIB message == one full global grid** — no mosaicking.
- **Forecast feed, not an analysis archive.** Each date has runs at 00/06/12/18
  UTC, each with many lead-time steps. We pin **00z, step 0** (the T+0 analysis)
  as the daily snapshot: one global grid per variable per day, valid 00:00 UTC.
- **Short retention.** The feed keeps only ~4 days; discovery walks a tight
  window and retention defaults to 14 date-folders in S3.

## Access — `ecmwf-opendata` client (byte-range)

The worker uses the `ecmwf-opendata` Python client, which reads the per-run
`.index` and pulls only the requested GRIB messages via HTTP byte-range — so a
single-variable retrieve is a few MB even though the full step-0 file carries
every parameter:

```python
from ecmwf.opendata import Client
Client(source="ecmwf", model="ifs", resol="0p25").retrieve(
    date="20260620", time=0, step=0, stream="oper", type="fc",
    param="2t", target="airtemp.grib2")
```

The **runner** (`scripts/submit_ecmwf_pipeline.py`) doesn't use the client — it
just HEAD-checks the public step-0 GRIB URL to learn which dates exist. Because
the step-0 file holds every parameter, one HEAD per date covers all three
products:

```
https://data.ecmwf.int/forecasts/<YYYYMMDD>/00z/ifs/0p25/oper/<YYYYMMDD>000000-0h-oper-fc.grib2
```

## Grid handling

- **Source grid:** the `ecmwf-opendata` client returns the global 0.25° grid on
  the standard **−180..180** longitude convention (origin ≈ −180.125), lat
  90.125..−90.125 (1440 × 721, EPSG:4326). Confirmed against the live feed
  (validated 2026-06).
- **No roll in the normal path.** Because the feed is already −180..180,
  `extract_variable` writes the field through unchanged. The roll-to-−180..180
  code is a **defensive fallback** only — it triggers solely if some source ever
  arrives on the 0..360 convention (origin near 0), which would otherwise make
  `gdalwarp` drop the western hemisphere when reprojecting to a polar grid.
- **Edge latitudes.** The grid's cell-edge rows sit at ±90.125° (just outside
  the pole), so `gdalwarp` logs non-fatal `PROJ: stere: Invalid latitude`
  warnings and drops those edge pixels; the true pole (90.0°) is interior and
  reprojects fine.
- **Target grid:** the **canonical EPSG:3413 10 km Arctic grid** — byte-for-byte
  the same `TGT_TE` / `TGT_RES` as `src/ingest_osisaf.py`, so every Frozon
  collection co-registers and stacks cleanly in the Zarr. Resampling to 10 km
  oversamples the ~28 km native field (no new information), but keeps the grid
  consistent across collections.

## Value semantics

- **airtemp (`2t`)** — 2 m temperature in **degrees Celsius**. NOTE: ECMWF's
  parameter database documents `2t` as Kelvin, but the live Open Data feed
  delivers it in °C — validated 2026-06 (equatorial Pacific ≈ 29, Sahara ≈ 34;
  global min/max ≈ −72…+42, mean ≈ 8). The worker tags the band `units=degC`
  and passes values through unchanged. If ECMWF ever switches to Kelvin, the
  values will jump by ~273; update `PRODUCTS["airtemp"]["units"]` accordingly.
- **wind_ns (`10v`)** — 10 m northward wind component, m s⁻¹ (can be negative).
- **wind_ew (`10u`)** — 10 m eastward wind component, m s⁻¹ (can be negative).

NoData is NaN. ECMWF surface fields are globally complete (defined over land and
ocean), so the Arctic crop has no internal gaps.

## S3 layout

`s3://maap-ops-workspace/jdrodrig/frozon/cogs/<collection-id>/YYYY/MM/DD/<collection-id>_YYYYMMDD_COG.tif`

The date partition comes from the run reference date at its run hour (00:00
UTC). No STAC catalog is written — like OSI SAF, nothing in this pipeline
consumes one and the units live in the COG band metadata. (Easy to enable later
by reusing `cog_helpers.build_stac_item` / `write_stac_catalog`.)
