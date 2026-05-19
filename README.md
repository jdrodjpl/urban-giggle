# Frozon ISS Ingest Job

Sibling pipeline to `czdt-iss-ingest-job`, using Frozon's processing logic.
First stage implemented: TIFF → Cloud Optimized GeoTIFF (COG) using the
low-memory streaming approach from `convert_to_cog_lowmem.py`.

## Pipeline Architecture

### Data Flow
```
S3 TIFF input(s) → COG (gdal_translate, low-memory) → S3 upload → STAC cataloging
```

### Components
- **frozon-iss-ingest-cog** — per-TIFF DPS worker. Downloads, runs
  `gdal_translate -of COG` with a `GDAL_CACHEMAX` cap, validates, uploads,
  emits a STAC catalog.
- **frozon-iss-cog-pipeline** — orchestrator. Resolves the input set
  (single S3 URL or S3 prefix listing), submits one worker job per TIFF,
  awaits completion, then upserts STAC into MMGIS.

## Usage

### Single S3 TIFF

```bash
python src/pipeline_cog.py \
  --input-s3 "s3://source-bucket/path/file.tif" \
  --collection-id "C123456789-FROZON" \
  --s3-bucket "target-bucket" \
  --s3-prefix "data/cogs" \
  --role-arn "arn:aws:iam::123456789:role/S3AccessRole" \
  --cmss-logger-host "https://logger.example.com" \
  --mmgis-host "https://mmgis.example.com" \
  --titiler-token-secret-name "mmgis-token" \
  --job-queue "maap-dps-czdt-worker-8gb"
```

### S3 Prefix (batch)

```bash
python src/pipeline_cog.py \
  --input-s3-prefix "s3://source-bucket/path/" \
  --filter "*ssha*.tif" \
  --collection-id "C123456789-FROZON" \
  --s3-bucket "target-bucket" \
  --s3-prefix "data/cogs" \
  --role-arn "arn:aws:iam::123456789:role/S3AccessRole" \
  --job-queue "maap-dps-czdt-worker-8gb"
```

### Worker (direct, local or DPS)

```bash
python src/ingest_cog.py \
  --input-s3 "s3://source-bucket/path/file.tif" \
  --collection-id "C123456789-FROZON" \
  --s3-bucket "target-bucket" \
  --s3-prefix "data/cogs" \
  --role-arn "arn:aws:iam::123456789:role/S3AccessRole" \
  --max-memory 512 --blocksize 512 --compress DEFLATE
```

### Tuning

| Flag | Default | Notes |
|---|---|---|
| `--max-memory` | 512 | `GDAL_CACHEMAX` MB for the conversion subprocess |
| `--blocksize` | 512 | 256 / 512 / 1024 — drop to 256 for tighter memory |
| `--compress` | DEFLATE | DEFLATE / LZW / JPEG / WEBP / NONE |
| `--resampling` | nearest | base data resampling |
| `--overview-resampling` | average | overview pyramid resampling |
| `--filter` | — | glob applied to file basenames during prefix listing |
| `--limit` | — | cap discovered inputs (testing) |
| `--overwrite` | off | replace existing S3 outputs |

### Environment

```bash
conda env update -f environment.yml
conda activate ingest
```

## MAAP Deployment

Two algorithms register as separate MAAP DPS configs:

| Algorithm | Build / run | YAML |
|---|---|---|
| `frozon-iss-ingest-cog` | `.maap/ingest-cog/` | `.maap/sample-algo-configs/frozon-iss-ingest-cog.yml` |
| `frozon-iss-cog-pipeline` | `.maap/cog-pipeline/` | `.maap/sample-algo-configs/frozon-iss-cog-pipeline.yml` |

The orchestrator submits jobs against `algo_id="frozon-iss-ingest-cog"`,
matching the worker's algorithm name. Both target the
`maap-dps-czdt-worker-8gb` queue by default.

## Output Products

- **Cloud Optimized GeoTIFFs** in
  `s3://<s3-bucket>/<s3-prefix>/<collection-id>/YYYY/MM/DD/<input>_COG.tif`
  — date is derived from the STAC item's `datetime` (read from TIFF tags
  by `rio_stac`; falls back to current UTC time when absent).
- **STAC Items** upserted into MMGIS under `<collection-id>`. Reruns merge
  into the existing collection by default (`--upsert`); pass `--no-upsert`
  to fail on item conflicts.
- **Product Notifications** via the CMSS logger.
- **Optional post-STAC webhook** — when `--post-stac-webhook-url` is set,
  the orchestrator POSTs `{event, collection_id, item_id, asset_uri}` to
  that URL after each successful item upsert, prompting the receiver to
  fetch the COG from S3. Failures are logged and swallowed. Provide
  `--post-stac-webhook-token-secret-name` to attach a bearer token.

## Dependencies

Python 3.11+, GDAL, rasterio, rioxarray, pystac, rio-stac, boto3, maap-py.

## License

Apache 2.0
