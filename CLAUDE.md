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

### Local development (COG)
```bash
# Worker, single file
python src/ingest_cog.py --input-tiff path/to/file.tif \
  --collection-id C123-FROZON --s3-bucket bucket --role-arn arn:...

# Worker, HTTPS+EDL input (Earthdata-protected)
python src/ingest_cog.py --input-https https://... \
  --earthdata-token-secret-name earthdata-token-frozon \
  --collection-id C123-FROZON --s3-bucket bucket --role-arn arn:...

# Orchestrator, S3 prefix (default --input-source-type=s3)
python src/pipeline_cog.py --input-s3-prefix s3://bucket/prefix/ \
  --collection-id C123-FROZON --s3-bucket bucket --role-arn arn:... \
  --job-queue maap-dps-worker-8gb

# Orchestrator, CMR search
python src/pipeline_cog.py --input-source-type cmr \
  --cmr-short-name SENTINEL-1A_DUAL_POL_METADATA_GRD_HD \
  --cmr-temporal-start 2024-01-01 --cmr-temporal-end 2024-01-02 \
  --cmr-bbox -180,60,180,90 \
  --earthdata-token-secret-name earthdata-token-frozon \
  --collection-id frozon-test-s1 --s3-bucket bucket --role-arn arn:... \
  --job-queue maap-dps-worker-8gb
```

### Local development (Zarr)
```bash
# Worker, batch of TIFFs from S3
python src/ingest_zarr.py --input-s3-prefix s3://bucket/prefix/ \
  --collection-id frozon-timeseries --s3-bucket bucket --role-arn arn:...

# Orchestrator
python src/pipeline_zarr.py --input-s3-prefix s3://bucket/prefix/ \
  --collection-id frozon-timeseries --s3-bucket bucket --role-arn arn:... \
  --job-queue maap-dps-worker-16gb
```

### MAAP execution
```bash
.maap/cog-pipeline/run-cog-pipeline.sh    # COG orchestrator
.maap/ingest-cog/run-ingest-cog.sh        # COG worker (per-TIFF)
.maap/zarr-pipeline/run-zarr-pipeline.sh  # Zarr orchestrator
.maap/ingest-zarr/run-ingest-zarr.sh      # Zarr worker (batch)
```

## Architecture

### COG pipeline flow
1. **Orchestrator** (`pipeline_cog.py`) — resolves inputs via an `InputSource`
   (S3 prefix or CMR search), submits one DPS worker job per resolved
   `InputRef` against `algo_id="frozon-iss-ingest-cog"`, awaits all jobs,
   walks **every** worker `catalog.json`, upserts collections/items into
   MMGIS (default `upsert_items=True`), and optionally fires a per-item
   post-STAC webhook.
2. **Worker** (`ingest_cog.py`) — stages input (S3 download or
   HTTPS+EDL fetch) → `gdal_translate -of COG` under a `GDAL_CACHEMAX`
   cap → validates → builds a STAC item via `rio_stac` → uploads to a
   dated key `<prefix>/<collection>/YYYY/MM/DD/<file>` → finalizes the
   asset href → emits `output/stac/catalog.json`.

### Zarr pipeline flow
1. **Orchestrator** (`pipeline_zarr.py`) — submits a **single** worker job
   (concurrent Zarr writes would race) against
   `algo_id="frozon-iss-ingest-zarr"`, awaits it, then catalogs the
   `data` asset into MMGIS using the shared catalog-orchestration module.
2. **Worker** (`ingest_zarr.py`) — dispatches one of three modes based on
   existing-Zarr presence and bounds compatibility:
   - **fresh**: no remote Zarr — build from scratch via the streaming
     script.
   - **append**: remote Zarr exists and its bounds contain all new inputs —
     `xarray.to_zarr(mode='a', append_dim='time')` in place.
   - **rebuild**: remote exists but new inputs extend beyond it — dump
     existing slices back to TIFFs (timestamps stamped onto mtime via
     `os.utime`), merge with new TIFFs, rebuild on the unioned grid.
3. Output lands at `s3://<bucket>/<prefix>/<collection>/<collection>.zarr/`.
   STAC item id is `<collection-id>-timeseries` (stable across reruns); the
   asset key is `data` with media-type `application/vnd+zarr`.

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

### Catalog orchestration (`catalog_orchestration.py`)
- Shared between COG and Zarr pipelines.
- `catalog_products(args, maap, worker_jobs, primary_asset_keys)` —
  iterates **every** worker's `stac_cat_files` (not just the first) so
  multi-worker COG pipelines accrete via idempotent
  `upsert_collection`. `primary_asset_keys` is `("asset",)` for COG
  and `("data",)` for Zarr (the worker decides which key it uses).

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
- `src/pipeline_cog.py`   — COG orchestrator
- `src/ingest_cog.py`     — COG worker
- `src/pipeline_zarr.py`  — Zarr orchestrator (single-job submitter)
- `src/ingest_zarr.py`    — Zarr worker (fresh/append/rebuild dispatch)
- `src/zarr_io.py`        — Zarr store sync + bounds/slice helpers
- `src/build_zarr_sparse_streaming.py` — streaming Zarr builder (vendored)
- `src/catalog_orchestration.py` — shared STAC/MMGIS upsert + webhook
- `src/create_stac_items.py` — MMGIS upsert helpers
- `src/common_utils.py` — shared AWS / MAAP / logging / argparse utilities
- `src/input_sources/`  — pluggable input-source package
  - `base.py`        — `InputRef` + `InputSource` protocol
  - `s3_prefix.py`   — S3 prefix listing
  - `cmr_tiff.py`    — earthaccess-backed CMR getter + EDL login helper
  - `__init__.py`    — `make_source(args)` factory + `ensure_edl_login`

### MAAP configs
- `.maap/cog-pipeline/`  — build + run for the COG orchestrator
- `.maap/ingest-cog/`    — build + run for the COG worker
- `.maap/zarr-pipeline/` — build + run for the Zarr orchestrator
- `.maap/ingest-zarr/`   — build + run for the Zarr worker
- `.maap/sample-algo-configs/*.yml` — register-able algorithm definitions
  for all four algos.

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
