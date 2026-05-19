# CLAUDE.md

Guidance for Claude Code working in this repository.

## Project Overview

Frozon ISS Ingest Job — a NASA MAAP-deployed pipeline that ingests Frozon
project rasters and converts them to Cloud Optimized GeoTIFFs (COGs) with
STAC metadata. Modeled on the sibling repo `czdt-iss-ingest-job` and reusing
its DPS-job orchestration patterns. The first implemented stage is
TIFF→COG, using low-memory streaming logic ported from
`/Users/jdrodrig/frozon/convert_to_cog_lowmem.py`.

## Common Commands

### Environment
```bash
conda env update -f environment.yml
conda activate ingest
```

### Local development
```bash
# Worker, single file
python src/ingest_cog.py --input-tiff path/to/file.tif \
  --collection-id C123-FROZON --s3-bucket bucket --role-arn arn:...

# Orchestrator, S3 prefix
python src/pipeline_cog.py --input-s3-prefix s3://bucket/prefix/ \
  --collection-id C123-FROZON --s3-bucket bucket --role-arn arn:... \
  --job-queue maap-dps-czdt-worker-8gb
```

### MAAP execution
```bash
.maap/cog-pipeline/run-cog-pipeline.sh   # orchestrator entry point
.maap/ingest-cog/run-ingest-cog.sh       # per-TIFF worker entry point
```

## Architecture

### Pipeline flow
1. **Orchestrator** (`pipeline_cog.py`) — resolves inputs (single S3 URL or
   prefix listing), submits one DPS job per TIFF against `algo_id =
   frozon-iss-ingest-cog`, awaits all jobs, iterates **every** worker
   `catalog.json`, upserts collections/items into MMGIS (default
   `upsert_items=True` so reruns merge), and optionally fires a per-item
   post-STAC webhook.
2. **Worker** (`ingest_cog.py`) — stages input → `gdal_translate -of COG`
   under a `GDAL_CACHEMAX` cap → validates → builds a STAC item locally
   (datetime from TIFF tags via `rio_stac`) → uploads to a dated key
   `<prefix>/<collection>/YYYY/MM/DD/<file>` → finalizes the asset href →
   emits `output/stac/catalog.json`.
3. **Cataloging** (`create_stac_items.py`) — copied from the sibling repo;
   handles MMGIS collection/item upsert. `upsert_collection` is idempotent
   on the collection (PUT on exist, POST on new) and merges temporal
   extents across runs; item conflict behavior is gated by `upsert_items`.

### Key components
- **AWS S3 integration** — `common_utils.AWSUtils` (download, upload, parse
  s3:// paths, role assumption).
- **MAAP DPS** — `common_utils.MaapUtils` (client init with backoff,
  job error parsing, output discovery).
- **Logging** — `common_utils.LoggingUtils` (CMSS log + product
  notifications).

### Data flow
```
S3 TIFF input(s) → DPS worker (low-memory gdal_translate) → S3 COG upload
→ STAC catalog.json → MMGIS upsert
```

## Project Structure

### Core files
- `src/pipeline_cog.py` — orchestrator (async, submits/waits on DPS jobs)
- `src/ingest_cog.py` — per-TIFF worker (low-memory GDAL conversion)
- `src/create_stac_items.py` — MMGIS upsert helpers
- `src/common_utils.py` — shared AWS / MAAP / logging / argparse utilities

### MAAP configs
- `.maap/cog-pipeline/` — build + run for the orchestrator algorithm
- `.maap/ingest-cog/` — build + run for the per-TIFF worker algorithm
- `.maap/sample-algo-configs/*.yml` — register-able algorithm definitions

## Important Implementation Details

### Low-memory COG conversion
- Single-threaded GDAL (`GDAL_NUM_THREADS=1`) with explicit `GDAL_CACHEMAX`
- `gdal_translate -of COG` for streaming I/O instead of loading whole rasters
- Forced `gc.collect()` between files in batch mode
- 10-minute subprocess timeout per file to bound runaway conversions

### Memory budgets
- MAAP worker queue is 8 GB / 50 GB disk by default (matches the sibling repo)
- Default `--max-memory` 512 MB leaves headroom inside the 8 GB worker
- `--blocksize 256` further trims working-set size for very large rasters

### MAAP integration
- Orchestrator submits jobs against `algo_id="frozon-iss-ingest-cog"` —
  the registered algorithm name must match.
- Worker reads its parameters from `_job.json` via `jq` in the run script.
- Output discovery uses the same `MaapUtils.get_dps_output` pattern as
  the sibling repo (filters job results for `catalog.json`).

### S3 output layout
COGs land at `s3://<s3-bucket>/<s3-prefix>/<collection-id>/YYYY/MM/DD/<input>_COG.tif`.
The date partition comes from the STAC item's `datetime`. If a TIFF lacks
datetime tags, `rio_stac` falls back to UTC now and the worker raises if
even that fails (we want loud failure here, not silent today-folder dumps).

### Post-STAC webhook (stub)
`LoggingUtils.post_stac_webhook` is a stub: when `--post-stac-webhook-url`
is set, the orchestrator POSTs `{event, collection_id, item_id, asset_uri}`
after each item upsert. Receiver is expected to fetch the COG from S3.
The stub is production-shaped (real `requests.post`) but the JSON contract
is invented — swap to whatever the actual receiving service expects
(HMAC signing, retry policy, async fan-out) before relying on it.

## Dependencies

- `gdal`, `rasterio`, `rioxarray` — geospatial I/O
- `pystac`, `rio-stac`, `pystac-client` — STAC metadata generation
- `boto3` — AWS S3
- `maap-py` — MAAP DPS client
- `backoff`, `aiohttp`, `fsspec`, `requests`
