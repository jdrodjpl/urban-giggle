# CLAUDE.md

Guidance for Claude Code working in this repository.

## Project Overview

Frozon ISS Ingest Job — a NASA MAAP-deployed pipeline that ingests Frozon
project rasters into S3-backed products plus STAC metadata. Modeled on the
sibling repo `czdt-iss-ingest-job` and reusing its DPS-job orchestration
patterns.

Two parallel pipelines, both registered as MAAP algorithms:

1. **COG pipeline** — TIFF → Cloud Optimized GeoTIFF, one DPS worker job per input.
2. **Zarr pipeline** — many TIFFs → a single sparse Zarr time series, single worker job.

Each pipeline pair (`*-pipeline` orchestrator + `ingest-*` worker) is registered
independently. The COG pipeline has been extended with a pluggable
**input-source framework** so inputs can come from S3 prefixes or NASA CMR
searches (Earthdata Login).

## Common Commands

### Environment
```bash
conda env update -f environment.yml
conda activate ingest
```

### Local development (per-source worker)
Each worker takes the same general shape — a CMR / source-specific
input flag plus collection / S3 / token options. Example (S1 GRD):

```bash
python src/ingest_s1grd.py --input-https-urls '["https://..."]' \
  --mosaic-date 20260615 --polarization HH \
  --earthdata-token-secret-name earthdata-token-frozon \
  --collection-id frozon-s1-ew-hh-daily \
  --s3-bucket maap-ops-workspace --s3-prefix jdrodrig/frozon/cogs/
```

Orchestration lives in `scripts/submit_<source>_pipeline.py` (runs from
GH Actions, not MAAP). The worker scripts above just process one
acquisition date at a time.

### MAAP execution
```bash
.maap/ingest-s1grd/run-ingest-s1grd.sh    # S1 GRD HH+HV worker (per acquisition date)
.maap/ingest-osisaf/run-ingest-osisaf.sh  # OSI-SAF worker
.maap/ingest-ecmwf/run-ingest-ecmwf.sh    # ECMWF Open Data worker (airtemp + 10m wind)
.maap/ingest-zarr/run-ingest-zarr.sh      # Zarr sync worker (used by Zarr cron)
```

Each pipeline follows the same shape: a GH Actions cron does discovery
+ filtering + submitJob, then one MAAP DPS worker job runs per
acquisition date. The orchestrator-as-DPS-job pattern was retired
(MAAP's runtime kept serving stale orchestrator images regardless of
how many fresh builds we pushed — see PIPELINE_TEMPLATE.md gotchas).

## Architecture

### General pipeline flow (applies to S1 GRD, OSI-SAF, and any future source)
1. **GH Actions cron** (`scripts/submit_<source>_pipeline.py`) — runs
   daily. Authenticates via the `earthdata-token-frozon` MAAP secret;
   walks back day-by-day querying CMR for granule counts; drops the
   newest date (still landing) plus any date below `MIN_GRANULE_FRACTION`
   of the max count in the window; pre-checks S3 to skip dates whose
   COG already exists; submits one `frozon-iss-ingest-<source>:vN` worker
   per remaining date. Also runs a retention sweep first, keeping the
   `RETAIN_DAYS` most recent date folders and pruning the rest.
2. **Worker** (`ingest_<source>.py`) — downloads source granules, applies
   any source-specific processing (calibration, reprojection, ZIP
   unwrapping…), uses `cog_helpers.mosaic_tiffs` + `convert_to_cog_lowmem`
   to build a per-day mosaic COG, then `upload_cog_to_key` + `write_stac_catalog`
   to land it in S3 at `<prefix>/<collection>/YYYY/MM/DD/<file>`.

### Zarr sync flow
The Zarr is a time-series mirror of one of the COG collections.
1. **GH Actions cron** (`scripts/submit_zarr_pipeline.py`) — lists COG
   dates under the source collection in S3, opens the existing Zarr's
   `time` coord, computes the diff, submits a single worker job with
   `--desired-dates` + `--input-s3-urls` (only the dates the Zarr
   doesn't already have).
2. **Worker** (`ingest_zarr.py`, `sync_zarr` mode) — opens the existing
   Zarr if any, dumps the kept slices to TIFFs, then streams the new
   COGs in one at a time: download → append → unlink. Final write goes
   to a local Zarr, then uploaded to S3 atomically.
3. Output lands at `s3://<bucket>/<prefix>/<collection>/<collection>.zarr/`.
   STAC item id is `<collection-id>-timeseries`; asset key is `data`
   with media-type `application/vnd+zarr`.

### Shared library (`src/cog_helpers.py`)
Pulled out of the retired OPERA RTC worker. Every source-specific
worker composes these:
- `check_if_cog` / `get_file_info` — diagnostics.
- `convert_to_cog_lowmem` — `gdal_translate -of COG` under a cache cap.
- `mosaic_tiffs` — `gdalwarp` per-day mosaic to a target CRS.
- `build_stac_item` / `build_dated_s3_key` / `write_stac_catalog` — STAC emit.
- `upload_cog_to_key` — S3 upload.

### Input-source framework (`src/input_sources/`)
- `base.InputRef` — one resolved input (url, name, `auth_kind`).
- `base.InputSource` — protocol with `list_inputs() -> List[InputRef]`.
- `S3PrefixSource` — wraps the legacy S3 listing.
- `CMRTiffSource` — `earthaccess`-backed CMR search (short_name + temporal
  + bbox + granule_ids); returns HTTPS or S3 URLs depending on
  `prefer_https`.
- `make_source(args)` — factory dispatching on `args.input_source_type`.
- `ensure_edl_login(secret_name)` — pulls an EDL bearer token (or
  username/password) from a MAAP secret and runs `earthaccess.login()`.
- `login_from_maap_secret` lives in `cmr_tiff.py` for the underlying
  one-shot auth.

### Key components
- **AWS S3 integration** — `common_utils.AWSUtils` (download, upload, parse
  s3:// paths, role assumption).
- **MAAP DPS** — `common_utils.MaapUtils` (client init with backoff,
  job error parsing, output discovery).
- **Logging** — `common_utils.LoggingUtils` (CMSS log + product
  notifications + post-STAC webhook stub).
- **Zarr I/O** — `zarr_io` (download/upload Zarr stores, bounds checks,
  slice-to-TIFF dump for rebuild path).
- **Vendored** — `build_zarr_sparse_streaming.py` is the streaming
  TIFFs→Zarr builder (pre-allocates the full grid, writes spatial chunks).

### Data flow
```
COG:  S3/CMR input → worker (download → gdal_translate) → S3 COG
      → STAC catalog.json → MMGIS upsert

Zarr: S3 inputs → worker (download existing Zarr → fresh/append/rebuild
      → upload Zarr) → STAC catalog.json → MMGIS upsert
```

## Project Structure

### Core files
- `scripts/submit_<source>_pipeline.py` — runner cron, one per source
- `src/ingest_<source>.py` — worker, one per source
- `src/cog_helpers.py` — shared COG building (mosaic, convert, STAC, upload)
- `src/s1_calibration.py` — S1 GRD σ⁰ calibration helpers
- `src/ingest_zarr.py` — Zarr sync worker (one for all sources)
- `src/zarr_io.py` — Zarr store sync + bounds/slice helpers
- `src/build_zarr_sparse_streaming.py` — streaming Zarr builder (vendored)
- `src/create_stac_items.py` — MMGIS upsert helpers
- `src/common_utils.py` — shared AWS / MAAP / logging / argparse utilities
- `src/input_sources/`  — pluggable input-source package
  - `base.py`        — `InputRef` + `InputSource` protocol
  - `s3_prefix.py`   — S3 prefix listing
  - `cmr_tiff.py`    — earthaccess-backed CMR getter + EDL login helper
  - `__init__.py`    — `make_source(args)` factory + `ensure_edl_login`

### MAAP configs
- `.maap/ingest-s1grd/`  — build + run for the Sentinel-1 GRD HH+HV worker
- `.maap/ingest-osisaf/` — build + run for the OSI-SAF worker
- `.maap/ingest-ecmwf/`  — build + run for the ECMWF Open Data worker
- `.maap/ingest-zarr/`   — build + run for the Zarr sync worker
- `.maap/sample-algo-configs/*.yml` — register-able algorithm definitions.
  See `PIPELINE_TEMPLATE.md` for the recipe to add another source.

## Important Implementation Details

### COG conversion
- `gdal_translate -of COG` for streaming I/O instead of loading whole rasters.
- `GDAL_CACHEMAX` controlled by `--max-memory` (default 4096MB; YAML default
  matches). Drop to ~512MB for the 8GB queue, leave at 4096MB for the
  32vcpu-64gb queue used for full-Arctic daily mosaics.
- `GDAL_NUM_THREADS=ALL_CPUS` + COG-driver `NUM_THREADS=ALL_CPUS` so resampling,
  overview build, and tile write all use every available CPU.
- 2-hour subprocess timeout per file to bound runaway conversions; full-Arctic
  daily mosaics (~30GB compressed, ~180GB uncompressed) typically take 30-90 min.

### Zarr build/append/rebuild dispatch
- Append uses `xarray.Dataset.to_zarr(mode='a', append_dim='time')` — only
  legal when existing bounds contain the new bounds.
- Rebuild path uses `zarr_io.dump_zarr_slices_to_tiffs` to materialize
  each existing time slice back to a GeoTIFF with the slice datetime
  stamped onto mtime via `os.utime()`, then feeds the combined set
  through the streaming builder. This relies on the streaming script's
  mtime fallback so we don't need a regex change.
- NaN-only slices are skipped on dump.
- A trailing `_delete_s3_prefix` cleanup removes stale chunk files
  before upload to avoid orphaned chunks from a smaller previous shape.

### Memory budgets and queues
- COG worker: `maap-dps-worker-8gb` / 200GB disk for single-file mode;
  `maap-dps-worker-32vcpu-64gb` for full-Arctic daily mosaics (5000+ granules,
  ~30GB intermediate). Default `--max-memory` is 4096MB.
- COG orchestrator: `maap-dps-worker-8gb` / 20GB disk. Pure async submitter.
- Zarr worker: `maap-dps-worker-16gb` / 200GB disk. Heaviest job —
  downloads existing Zarr + all new TIFFs, may rebuild whole grid.
- Zarr orchestrator: `maap-dps-worker-8gb` / 20GB disk.

### MAAP integration
- Orchestrators submit child jobs by `algo_id` + `version="main"` (MAAP
  routes "main" to the latest registered version on the algo's `main`
  branch).
- Worker shell scripts read params from `_job.json` via `jq` — every
  positional input declared in the algo YAML needs to be parsed here and
  forwarded to the Python entry point.
- Output discovery uses `MaapUtils.get_dps_output` to filter job
  results for `catalog.json`.

### CMR / EDL integration
- `--input-source-type=cmr` activates `CMRTiffSource`. Required:
  `--cmr-short-name`. Common: `--cmr-temporal-start/-end`, `--cmr-bbox`.
- HTTPS granule URLs (default) require an EDL bearer token. The
  orchestrator does an `ensure_edl_login()` once and forwards the
  secret name to each worker; the worker repeats the login locally and
  streams the download through `earthaccess`'s authenticated session.
- Only `.tif`/`.tiff` granule URLs are kept — ZIP-bundled SAR products
  need an unwrap step (not yet implemented).
- MAAP secret format: single-line `token=...` or two-line
  `username\npassword`.

### S3 output layout
- COG: `s3://<bucket>/<prefix>/<collection-id>/YYYY/MM/DD/<input>_COG.tif`.
  The date partition comes from the STAC item's `datetime`. The worker
  raises if no datetime can be derived — we want loud failure here,
  not silent today-folder dumps.
- Zarr: `s3://<bucket>/<prefix>/<collection-id>/<collection-id>.zarr/`.
  Single store updated in place across runs.

### Post-STAC webhook (stub)
`LoggingUtils.post_stac_webhook` POSTs
`{event, collection_id, item_id, asset_uri}` after each item upsert
when `--post-stac-webhook-url` is set. JSON contract is invented — swap
for the actual receiving-service shape (HMAC, retry, async fan-out)
before relying on it.

### Followups
See `TODO.md` for three parked items: post-COG SFTP push, cleanup of
old inputs based on filename timestamp, and externally-scheduled runs.

## Dependencies

- `gdal`, `rasterio`, `rioxarray`, `xarray` — geospatial I/O
- `numpy`, `zarr` — array storage
- `pystac`, `rio-stac`, `pystac-client` — STAC metadata generation
- `boto3` — AWS S3
- `earthaccess` — CMR search + EDL auth
- `maap-py` — MAAP DPS client
- `backoff`, `aiohttp`, `fsspec`, `requests`
