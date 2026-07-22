# Building a New Frozon Pipeline

A recipe for adding another satellite-data ingestion pipeline to this
repo, based on the patterns we settled on after building the OPERA RTC
and Sentinel-1 GRD ones. Use this as the starting prompt for a fresh
Claude session — it captures both *what to do* and *why* (i.e. the
gotchas we already paid for so you don't have to).

## Mental model

The pipeline shape we settled on:

```
GH Actions cron  (daily, scripts/submit_<source>_pipeline.py)
       │
       │  discover dates via CMR / source-specific API
       │  drop newest (partial); threshold-filter outliers
       │  S3 pre-check; submit one worker per missing date
       ▼
MAAP DPS worker  (.maap/ingest-<source>/run-ingest-<source>.sh
                   → src/ingest_<source>.py)
       │
       │  per granule: download → (calibrate) → reproject → unlink
       │  per day: mosaic per-granule tiffs → COG → S3 upload + STAC
       ▼
S3:  jdrodrig/frozon/cogs/<collection-id>/YYYY/MM/DD/<file>_COG.tif

(separate cron, hours later)
GH Actions cron  (scripts/submit_zarr_pipeline.py)
       │
       │  list COGs in S3, read existing Zarr time coord
       │  diff → submit Zarr sync worker if changed
       ▼
MAAP DPS Zarr worker (ingest-zarr)
       │
       │  download existing Zarr, drop dropped slices,
       │  stream new COGs in one at a time, write back
       ▼
S3:  jdrodrig/frozon/zarrs/<collection-id>/<collection-id>.zarr/
```

The COG worker and Zarr worker are **separate algorithms**. Adding a new
data source means a new COG worker; the Zarr worker is data-source
agnostic — just point its runner at the new COG prefix.

## What lives where

| Code | Path | Reusable? |
|---|---|---|
| COG worker entry | `src/ingest_<source>.py` | New per source |
| Source-specific helpers | `src/<source>_calibration.py`, etc | New per source |
| Mosaic / COG / STAC / upload | `src/ingest_cog.py` (mosaic_tiffs, convert_to_cog_lowmem, build_stac_item, build_dated_s3_key, upload_cog_to_key, write_stac_catalog) | **Reuse** |
| Common utils (S3, MAAP, logging) | `src/common_utils.py` | Reuse |
| CMR / EDL helpers | `src/input_sources/cmr_tiff.py` (login_from_maap_secret) | Reuse |
| MAAP algo scaffolding | `.maap/ingest-<source>/{build.sh, run-ingest-<source>.sh, environment.yml}` | Pattern-copy from `.maap/ingest-cog/` or `.maap/ingest-s1grd/` |
| Algo registration YAML | `.maap/sample-algo-configs/frozon-iss-ingest-<source>.yml` | Pattern-copy |
| Runner | `scripts/submit_<source>_pipeline.py` | Pattern-copy from `scripts/submit_cog_pipeline.py` or `scripts/submit_s1grd_pipeline.py` |
| GH Actions workflow | `.github/workflows/daily-<source>-ingest.yml` | Pattern-copy |
| Zarr sync (data-agnostic) | `src/ingest_zarr.py`, `scripts/submit_zarr_pipeline.py` | Reuse — just update collection IDs / S3 prefixes |

## Step-by-step recipe

### 0. Source discovery (~1 hour)
- What collection in CMR? Find via `earthaccess.search_datasets(keyword=...)` or NASA Worldview.
- What file format? TIFF (direct), HDF5, NetCDF, ZIP-wrapped SAFE bundle?
- Need calibration / DN-to-physical-units conversion?
- Native CRS? Already mapped, or in raw sensor geometry (needs GCP/RPC reprojection)?
- Polarization / band / variable to ingest?
- Filename pattern (date regex, polarization indicator, etc.)?
- Coverage pattern (single-day swath count, polar gaps, etc.)?

### 1. Validate single-granule end-to-end in Jupyter (~1 hour)
**Do this before writing the worker.** Build the full chain manually for one
granule:
1. Download (via earthaccess for NASA DAACs, asf-search for ASF, others
   as appropriate).
2. Apply any required conversion (calibration, mask application,
   subset of variables, etc.).
3. Reproject to EPSG:3413 with `gdalwarp`. Check it's geographically
   sensible and pixel values are in the expected range.
4. Open the output, plot it, eyeball it.

If this fails you don't have a pipeline — back up to step 0.

### 2. Write the worker (~3-6 hours)
Copy `src/ingest_cog.py` (OPERA-style, TIFFs direct from CMR) or
`src/ingest_s1grd.py` (S1-style, ZIP/SAFE with calibration step) — whichever
matches your data source.

Worker should:
- Accept either `--input-https-urls` (JSON list from runner) OR a
  source-specific discovery mode (e.g. CMR-self-query for huge URL
  lists that would blow the submitJob payload limit).
- Process granules **one at a time** — download, transform, append
  to mosaic list, unlink. Disk on the worker is capped at whatever the
  queue gives you (~200 GB practical max); pre-staging 7 × 25 GB is the
  fastest way to crash on disk-full.
- Reuse `ingest_cog.mosaic_tiffs`, `convert_to_cog_lowmem`,
  `build_stac_item`, `build_dated_s3_key`, `upload_cog_to_key`,
  `write_stac_catalog` for everything after the per-granule processing.

### 3. MAAP algorithm scaffolding (~1 hour)
Copy `.maap/ingest-s1grd/`:
- **`environment.yml`** — conda env. Add source-specific deps. Always
  pin `python>=3.11`, include `gdal`, `rasterio`, `boto3`, `maap-py`,
  `earthaccess` at minimum.
- **`build.sh`** — runs `conda env create --prefix /opt/conda/envs/ingest`
  (the `--prefix` is critical — without it, conda picks a build-host path
  that the runtime image can't see, builds silently succeed, runtime
  silently fails). Writes a `.build-stamp` file so runtime diagnostics
  can prove which image is being used.
- **`run-ingest-<source>.sh`** — bash runner. Pre-declares all `_job.json`
  vars, runs `eval "$(/opt/conda/envs/ingest/bin/python .maap/_lib/load_job_params.py _job.json)"`,
  sets PROJ/GDAL env vars (the build doesn't `conda activate`, you have
  to do it manually), invokes the Python entry point.

### 4. Algorithm YAML (~30 min)
Copy `.maap/sample-algo-configs/frozon-iss-ingest-s1grd.yml`. Important fields:
- `algorithm_version: v1` — **start at v1, never reuse `main`**. The
  MAAP image cache is keyed on (algo_name, algo_version) and won't
  reliably refresh `main`-tagged images. When you eventually need to
  invalidate the cache, bump to v2, v3, etc. (You'll also need to push
  a matching `v1` / `v2` git branch — MAAP's CI does `git checkout v1`.)
- `disk_space` — **advisory only**, queue caps the actual disk.
  Design the worker to fit in ~200 GB peak even if you request more.
- `queue` — use `maap-dps-worker-32vcpu-64gb` for anything mosaicking
  the whole Arctic; 8gb/16gb queues are too small for that scale.
- `positional` inputs — must match what the run script reads from
  `_job.json`. Every field with `required: true` blocks job submission
  if the runner omits it.

### 5. Runner script (~2 hours)
Copy `scripts/submit_cog_pipeline.py` or `scripts/submit_s1grd_pipeline.py`.
Structure:
- `DEFAULTS` dict at top with every knob (use env vars to override).
- `_maap_s3_client(maap)` — uses
  `maap.aws.workspace_bucket_credentials()` for short-lived AWS creds
  scoped to `s3://maap-ops-workspace/jdrodrig/*`. **Do not** put long-term
  AWS keys in GH Actions secrets — workspace creds are sufficient and
  auto-rotated.
- `discover_acquisition_dates(maap)` — walk back day-by-day, query CMR
  for granule counts only (cheap, no full granule metadata fetch).
  Stop when you have `MOSAIC_LAST_N_COMPLETE_DAYS + 1 + DISCOVERY_BUFFER`
  dates with data.
- `prune_old_cogs(maap)` — list S3 collection prefix, group by date,
  keep top N, delete rest. Runs at the top of every cron.
- Threshold filter — drop newest + any date below
  `MIN_GRANULE_FRACTION * max_count`. Catches partial-day acquisitions.
- S3 pre-check — `cog_exists_in_s3(s3, date_key)` before each
  submitJob. **Critical**: the worker doesn't pre-check S3 itself
  (only LOCAL `output_file.exists()`), so without this you'll re-mosaic
  and clobber existing COGs every run.

### 6. GH Actions workflow (~30 min)
Copy `.github/workflows/daily-cog-ingest.yml` or
`daily-s1grd-ingest.yml`:
- Stagger cron times: existing crons run at 06:15 (OPERA), 06:30
  (S1GRD), 12:00 (Zarr). Pick a slot 15 min off the others.
- Install deps: at minimum `pip install maap-py boto3 earthaccess`.
  Zarr-syncing runners also need `xarray`, `zarr`, `s3fs`.
- One secret: `MAAP_TOKEN` (already in repo settings).

### 7. Deploy & test (~2 hours)
```bash
git -C ~/frozon/urban-giggle push origin main           # main
git -C ~/frozon/urban-giggle push origin main:v1        # match algo_version
```

From Jupyter:
```python
from maap.maap import MAAP
import json
maap = MAAP(maap_host="api.maap-project.org")
result = maap.register_algorithm_from_yaml_file(
    "/home/jovyan/urban-giggle/.maap/sample-algo-configs/frozon-iss-ingest-<source>.yml"
)
parsed = json.loads(result.text)
print("Pipeline URL:", parsed.get("message", {}).get("last_pipeline", {}).get("web_url"))
```

Open the pipeline URL, wait for **green build**. Verify:
```python
info = maap.describeAlgorithm(algoid="frozon-iss-ingest-<source>:v1")
print("REGISTERED" if "ProcessOfferings" in info.text else "MISSING")
```

Smoke test with **one worker**:
```
Actions → "Daily <source> ingest" → Run workflow
  mosaic_last_n_complete_days: 1
```

### 8. Hook into Zarr (~30 min)
If you want a Zarr time series:
- Update `scripts/submit_zarr_pipeline.py` DEFAULTS so `COG_COLLECTION_ID`
  points at the new COG output.
- The Zarr worker is data-agnostic; nothing else changes.

Test by triggering the Zarr cron after at least one COG has landed.

## Gotchas (paid in blood)

These are the ones we hit. New pipelines will hit some new ones, but
these are baseline.

1. **MAAP bakes source into the image at build time.** Pushing to
   GitHub doesn't update a registered algorithm. You **must re-register**
   to pick up any Python source change. Conda env cache-hits so it's
   fast, but it's a required step.

2. **`algorithm_version: main` is a trap.** MAAP's image cache becomes
   unreliable for `main`-tagged images — successful builds happen but
   workers keep running stale images. Always use a numeric version
   (`v1`, `v2`, ...). When you need to invalidate, bump to the next
   integer AND push the matching git branch.

3. **`disk_space` in the YAML is advisory.** The queue caps actual disk.
   Bumping `disk_space: 500GB` had no effect on our 200GB-capped queue.
   Design the worker to stream — process one granule at a time, delete
   each after use. Don't rely on being able to stage N × 25 GB.

4. **Workspace credentials only authorize `jdrodrig/*`.** Any S3 prefix
   you write to must start with `jdrodrig/`. (`maap-ops-workspace/frozon/...`
   appears writable from Jupyter because Jupyter uses long-term keys,
   but the runner using `maap.aws.workspace_bucket_credentials()` will
   403 on anything outside the namespace.)

5. **The worker's `--overwrite=false` only guards a local filesystem
   path.** It does NOT check S3 before uploading; it just clobbers
   whatever's there. Always do S3 pre-check on the runner side, never
   trust the worker to dedupe.

6. **CMR temporal_end is exclusive at day-resolution.** A query for
   `temporal=("2026-06-09", "2026-06-09")` returns 0 granules. Use
   `temporal=("2026-06-09", "2026-06-10")` to include the full day.

7. **Sentinel-1 GCP-based gdalwarp needs `-order 2`.** Without it,
   GDAL auto-picks TPS (thin-plate spline) for 400+ GCP products and
   spends 30 min per warp. `-order 2` forces a fast polynomial fit;
   accuracy is sub-pixel for sea-ice work.

8. **ASF has its own EDL app authorization.** A working EDL token for
   CMR / OPERA downloads doesn't automatically work for ASF datapool.
   Authorize "Alaska Satellite Facility Data Access" on
   https://urs.earthdata.nasa.gov/profile/authorized_apps **once per
   user account**.

9. **Don't use the orchestrator-as-DPS-job pattern.** We tried — MAAP's
   runtime persistently serves stale orchestrator images even after
   successful rebuilds. Workers work fine; orchestrators get nuked
   by the cache. Put the orchestration logic in the GH Actions runner
   instead, and only run per-granule/per-day jobs on MAAP.

10. **Stream sync for Zarr; never pre-stage.** The Zarr worker processes
    COGs one-at-a-time via download → append → unlink. Pre-staging
    blew through 200 GB on first-build. The same lesson — design for
    bounded disk regardless of YAML requests.

11. **Don't materialize a full slice in numpy when the input is
    grid-sized.** The first implementation of `append_in_place` (in
    `src/ingest_zarr.py`) allocated a `np.full((grid_height,
    grid_width), np.nan, dtype=np.float32)` buffer per slice so it
    could place a small per-granule TIFF into the right sub-region.
    Fine for per-granule inputs (~50km×50km), catastrophic for
    full-Arctic per-day COGs — one slice is 136535×145704×4 bytes
    = 74 GiB, more than the worker has. Pattern that works regardless
    of input size: open the Zarr's data array via `zarr.open(...,
    mode='a')`, resize by +1 along the time axis, then stream the
    source TIFF via `rasterio.Window` reads tile-by-tile (sized to
    match the Zarr's spatial chunk) and assign each tile directly
    into the matching `data_array[t_idx, gy0:gy1, gx0:gx1]` region.
    Peak in-flight memory is one tile (~16 MB) not one slice.

12. **conda-forge GDAL 3.9+ splits format drivers into plugins.** `gdal`
    no longer bundles the netCDF/HDF5/etc readers, so a worker dies with
    `RasterioIOError: ... plugin gdal_netCDF.so is not available` even though
    `gdal` is installed. Add the specific plugin to `environment.yml`
    (`libgdal-netcdf`, `libgdal-hdf5`, `libgdal-jp2openjpeg`, …) for whatever
    format your source reads, and **assert it in `build.sh`**
    (`gdal.GetDriverByName('netCDF')`) so a missing plugin fails the *build*,
    not the job. Bit the OSI SAF worker (NetCDF).

13. **A dep in `environment.yml` can still be missing at runtime.** MAAP
    rebuilds the image on every register, but the expensive conda layer
    cache-hits when `build.sh` content is unchanged — so a *source-only*
    re-register runs new code on a **stale conda env**, and any dep added
    without bumping the build marker never installs. Bit us three times in one
    week (`zarr`, `libgdal-netcdf`, `scipy`), each a confusing runtime
    `ModuleNotFoundError` despite the dep being right there in the env file.
    Rule: **bump `BUILD_BUST` whenever you touch `environment.yml`.** Automated
    by `.githooks/pre-commit` — it auto-bumps the sibling `build.sh` marker when
    an `environment.yml` is staged. Enable per clone (local *and* MAAP Jupyter):
    `git config core.hooksPath .githooks`.

14. **Algorithm version ↔ git branch — fixes must land on the right branch.**
    MAAP does `git checkout <algorithm_version>` at build time, so a fix only
    takes effect if it's on *that version's* branch. With algos pinned to
    different versions (`osisaf:v1`, `s1grd:v3`, `zarr:v3`, …) it's easy to push
    a fix to `v1` while the algo actually builds from `v3`, and rebuild the same
    broken image. Keep **every version branch fast-forwarded to `main`** (each
    algo builds only its own files, so one shared tree is safe):
    `for b in v1 v2 v3; do git push origin main:$b; done`. Check what version an
    algo is *now* (`grep algorithm_version .maap/sample-algo-configs/<algo>.yml`)
    before assuming — versions get bumped during refactors.

15. **Bumping `algorithm_version` in the YAML does NOT register that version.**
    Editing the YAML only changes what version the *runner* asks MAAP to submit
    to; MAAP's registry doesn't learn about the new version until you actually
    call `register_algorithm_from_yaml_file`. Otherwise the runner submits to
    a version MAAP has never heard of and gets exactly this error:

        Failed to submit job of type job-<algo>:<vN>.
        Exception Message: 'NoneType' object has no attribute 'get'

    Every YAML version bump is a **two-step** operation: (1) edit + commit +
    push to `main`, (2) push to the matching `vN` branch, (3) `register_algorithm_from_yaml_file`
    from Jupyter. Skip (3) and every runner submit for `:vN` fails with the
    NoneType error. Bit us at v2→v3 and v3→v4 for the S1 GRD worker.

16. **One image per repo:version, shared across ALL algos — and MAAP skips
    the build if that version's image already exists.** The Docker image is
    keyed by repo + version branch, not by algorithm. Registering
    `osisaf:v2` after `s1grd:v2` had ever been registered produced a
    pipeline that concluded "skipped", which `register_and_wait.py` used to
    report as success — and every `osisaf:v2` job then ran S1 GRD's
    June-era conda env (no `libgdal-netcdf`, no `rioxarray`). This is also
    why re-registering `v1` with a bumped `BUILD_BUST` never fixed anything:
    BUILD_BUST only matters when a build *runs*, and for an existing
    version image no build ever runs. Corollaries:
    - A version bump must go to a number **never used by ANY algo in this
      repo** (check: `git log --all -p -- .maap/sample-algo-configs/ |
      grep '+algorithm_version'`), and each algo needs its **own** fresh
      number — two algos bumped to the same new version would share one
      image and one of them gets the wrong env.
    - `register_and_wait.py` now treats a "skipped" pipeline as a hard
      failure for this reason.
    - The per-algo `environment.yml` files differ, so a shared image is
      never safe in this repo. (Long-term alternative: one union env for
      all algos, then sharing becomes harmless — not done.)
    Bit the OSI-SAF (netCDF plugin) and CMEMS (`rioxarray`) workers on
    2026-07-22, after two BUILD_BUST bumps and a v1→v2 bump all failed to
    dislodge the stale image.

## Patterns worth knowing

These aren't gotchas but they save time:

- **Token vs username/password EDL secrets.** Existing code accepts
  both. The convention: single-line `token=...` (or just the bare
  token) is token format; two-line `username\npassword` is the
  alternative. `_resolve_edl_creds` in `src/ingest_s1grd.py` handles
  both; copy that helper if your new source needs EDL.

- **Granule discovery via `.hits()`.** When CMR has 100k+ matching
  granules in your discovery window, `earthaccess.search_data()`'s
  `get_all()` call can flake with `RemoteDisconnected`. Use per-day
  walks with `earthaccess.DataGranules().short_name(...).temporal(...).hits()`
  — O(KB) per request, bails early once you have enough dates.

- **Per-collection retention is independent.** S3 prefixes for
  different collection_ids are separate "retention universes." The
  runner only prunes within its own collection_id. Adding a new
  pipeline doesn't interfere with the others.

- **DATA.md per data source.** When you add a new pipeline, add a
  parallel notes file (`DATA_<source>.md` or extend `DATA.md`)
  documenting pixel value semantics, calibration convention, coverage
  expectations, filename anatomy. Future-you will thank you.

- **Validate first, build second.** Step 1 above is worth its hour.
  The two times we skipped straight to building the worker, we wasted
  the day chasing issues that would've taken 15 min to find in Jupyter.

## Tasks to spawn (for the new Claude session)

When starting a fresh session to build a new pipeline, this is the
template task list to create:

1. Validate single-granule processing in Jupyter
2. Write source-specific helpers (calibration, etc.) in `src/`
3. Write worker `src/ingest_<source>.py`
4. Worker YAML / build / run scripts under `.maap/ingest-<source>/`
5. Runner script `scripts/submit_<source>_pipeline.py`
6. GH Actions workflow `.github/workflows/daily-<source>-ingest.yml`
7. Register algorithm, verify build
8. Smoke test with `mosaic_last_n_complete_days=1`
9. Validate output (gdalinfo + visual)
10. Hook Zarr cron to new COG collection (optional)
11. Document in DATA.md

## Reference files (concrete examples)

For a TIFF-direct CMR source (no calibration):
- `src/ingest_cog.py`
- `.maap/ingest-cog/`
- `scripts/submit_cog_pipeline.py`
- `.github/workflows/daily-cog-ingest.yml`

For a ZIP-bundled source with calibration + GCP geocoding:
- `src/ingest_s1grd.py` (orchestration)
- `src/s1_calibration.py` (calibration helpers)
- `.maap/ingest-s1grd/`
- `scripts/submit_s1grd_pipeline.py`
- `.github/workflows/daily-s1grd-ingest.yml`

Skim both before designing a new one — your data source will resemble
one shape more than the other, and a lot of decisions are already made.
